#!/usr/bin/env python

import datetime
import logging
import numbers
import sys

from collections import namedtuple

import DAQTime

from HsException import HsException


if sys.version_info >= (3, 0):
    # pylint: disable=invalid-name
    # unicode isn't present in Python3
    unicode = str


# default log directory
LOG_PATH = "/mnt/data/pdaqlocal/HsInterface/logs/"
# dictionary of year->datetime("Jan 1 %d" % year)
JAN1 = {}

# I3Live status types
STATUS_REQUEST_ERROR = "REQUEST ERROR"
STATUS_QUEUED = "QUEUED"
STATUS_IN_PROGRESS = "IN PROGRESS"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAIL = "FAIL"
STATUS_PARTIAL = "PARTIAL"


def assemble_email_dict(address_list, header, message,
                        description="HsInterface Data Request",
                        prio=2, short_subject=True, quiet=True):
    if address_list is None or len(address_list) == 0:
        return None

    notifies = []
    for email in address_list:
        ndict = {
            "receiver": email,
            "notifies_txt": message,
            "notifies_header": header,
        }
        notifies.append(ndict)

    now = datetime.datetime.now()
    return {
        "service": "HSiface",
        "varname": "alert",
        "prio": prio,
        "time": now.strftime("%Y-%m-%d %H:%M:%S"),
        "value": {
            "condition": header,
            "desc": description,
            "notifies": notifies,
            "short_subject": "true" if short_subject else "false",
            "quiet": "true" if quiet else "false",
        },
    }


def dict_to_object(xdict, expected_fields, objtype):
    """
    Convert a dictionary (which must have the expected keys) into
    a named tuple
    """
    if not isinstance(xdict, dict):
        raise HsException("Bad object \"%s\"<%s>" % (xdict, type(xdict)))

    missing = []
    for k in expected_fields:
        if k not in xdict:
            missing.append(k)

    if len(missing) > 0:
        raise HsException("Missing fields %s from %s" %
                          (tuple(missing), xdict))

    return namedtuple(objtype, list(xdict.keys()))(**xdict)


def get_daq_ticks(start_time, end_time, is_ns=False):
    """
    Get the difference between two datetimes.
    If `is_ns` is True, returned value is in nanoseconds.
    Otherwise the value is in DAQ ticks (0.1ns)
    """
    if is_ns:
        multiplier = 1E3
    else:
        multiplier = 1E4

    # XXX this should use leapseconds
    delta = end_time - start_time

    return int(((delta.days * 24 * 3600 + delta.seconds) * 1E6 +
                delta.microseconds) * multiplier)


def hub_name_to_id(hostname):
    "Convert hub name to component ID"
    name = hostname.split('.', 1)[0]

    if name == "scube":
        return 99

    if name == "dmice":
        return 300

    if name.startswith("ichub"):
        offset = 0
    elif name.startswith("ithub"):
        offset = 200
    else:
        raise ValueError("Unrecognized hub name \"%s\"" % (hostname, ))

    try:
        return int(name[5:]) + offset
    except ValueError:
        raise ValueError("Bad numeric value for hub \"%s\"" % (hostname, ))


def hubs_to_string(hublist):
    """
    Convert list of hub hostnames to a compact string of ranges
    like "1-7 9-45 48-86"
    """
    if len(hublist) == 0:
        return None

    # convert hub names to numeric values
    hub_ids = []
    for hub in hublist:
        try:
            hub_ids.append(hub_name_to_id(hub))
        except ValueError:
            logging.error("Bad hub name \"%s\"", hub)

    # if we didn't find any valid names, we're done
    if len(hub_ids) == 0:
        return None

    # sort hub numbers
    hub_ids.sort()

    # build comma-separated groups of ranges
    num_str = None
    prev_num = -1
    in_range = False
    for num in hub_ids:
        if num_str is None:
            num_str = str(num)
        else:
            if prev_num + 1 == num:
                if not in_range:
                    in_range = True
            else:
                if in_range:
                    num_str += "-" + str(prev_num)
                    in_range = False
                num_str += "," + str(num)
        prev_num = num
    if num_str is None:
        # this should never happen?
        num_str = ""
    elif in_range:
        # append end of final range
        num_str += "-" + str(prev_num)

    return num_str


def send_live_status(i3socket, req_id, username, prefix, start_ticks,
                     stop_ticks, copydir, status, success=None,
                     failed=None):
    if status is None:
        raise HsException("Status is not set")
    if req_id is None:
        raise HsException("Request ID is not set")
    if copydir is None:
        raise HsException("Destination directory is not set")

    if start_ticks is None:
        if status != STATUS_REQUEST_ERROR:
            raise HsException("Start time is not set")
        start_utc = ""
    elif isinstance(start_ticks, numbers.Number):
        start_utc = DAQTime.ticks_to_utc(start_ticks)
    else:
        raise HsException("Bad start time %s<%s>" %
                          (start_ticks, type(start_ticks).__name__))

    if stop_ticks is None:
        if status != STATUS_REQUEST_ERROR:
            raise HsException("Stop time is not set")
        stop_utc = ""
    elif isinstance(stop_ticks, numbers.Number):
        stop_utc = DAQTime.ticks_to_utc(stop_ticks)
    else:
        raise HsException("Bad stop time %s<%s>" %
                          (stop_ticks, type(stop_ticks).__name__))

    nowstr = str(datetime.datetime.utcnow())

    value = {
        "request_id": req_id,
        "username": username,
        "prefix": prefix,
        "start_time": str(start_utc),
        "stop_time": str(stop_utc),
        "destination_dir": copydir,
        "update_time": nowstr,
        "status": status,
    }
    if success is not None:
        value["success"] = success
    if failed is not None:
        value["failed"] = failed

    i3json = {
        "service": "hitspool",
        "varname": "hsrequest_info",
        "time": nowstr,
        "value": value,
        "prio": 1,
    }
    i3socket.send_json(i3json)


def split_rsync_host_and_path(rsync_path):
    "Remove leading 'user@host:' from rsync path"
    if not isinstance(rsync_path, (str, unicode)):
        raise HsException("Illegal rsync path \"%s\"<%s>" %
                          (rsync_path, type(rsync_path)))

    parts = rsync_path.split(":", 1)
    if len(parts) > 1 and parts[0].find("/") < 0:
        return parts

    # either no embedded colons or colons are part of the path
    return "", rsync_path
