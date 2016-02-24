#!/usr/bin/env python


import datetime
import numbers
import re

from collections import namedtuple

from HsException import HsException


# default log directory
LOG_PATH = "/mnt/data/pdaqlocal/HsInterface/logs/"
# dictionary of year->datetime("Jan 1 %d" % year)
JAN1 = {}

# I3Live status types
STATUS_QUEUED = "QUEUED"
STATUS_IN_PROGRESS = "IN PROGRESS"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAIL = "FAIL"
STATUS_PARTIAL = "PARTIAL"


def assemble_email_dict(address_list, header, description, message, prio=2,
                        short_subject=True, quiet=True):
    if address_list is None or len(address_list) == 0:
        raise HsException("No addresses specified")

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

    return namedtuple(objtype, xdict.keys())(**xdict)


def get_daq_ticks(start_time, end_time, is_sn_ns=False):
    """
    Get the difference between two datetimes.
    If `is_sn_ns` is True, returned value is in nanoseconds.
    Otherwise the value is in DAQ ticks (0.1ns)
    """
    if is_sn_ns:
        multiplier = 1E3
    else:
        multiplier = 1E4

    # XXX this should use leapseconds
    delta = end_time - start_time

    return int(((delta.days * 24 * 3600 + delta.seconds) * 1E6 +
                delta.microseconds) * multiplier)


def fix_date_or_timestamp(ticks, time, is_sn_ns=False):
    """
    convert to UTC or DAQ whatever direction is needed
    """
    if time is not None:
        jan1 = jan1_by_year(time.year)
    else:
        jan1 = jan1_by_year(None)

    if (ticks is None or ticks == 0) and time is not None:
        ticks = get_daq_ticks(jan1, time, is_sn_ns=is_sn_ns)

    if is_sn_ns:
        multiplier = 1E9
    else:
        multiplier = 1E10

    if time is None and (ticks is not None and ticks > 0):
        time = jan1 + datetime.timedelta(seconds=ticks / multiplier)

    return (ticks, time)


def jan1_by_year(year_index=None):
    """
    Return the datetime value for January 1 of the specified year.
    If year is None, return January 1 for the current year.
    """
    if year_index not in JAN1:
        if year_index is not None:
            year = year_index
        else:
            year = datetime.datetime.utcnow().year

        JAN1[year_index] = datetime.datetime(year, 1, 1)

    return JAN1[year_index]


def parse_sntime(arg, is_sn_ns=True):
    """
    Parse string as either a timestamp (in nanoseconds) or a date string.
    Return a tuple containing the nanosecond timestamp and a date string,
    deriving one value from the other.
    """
    if arg is None:
        raise HsException("No date/time specified")

    if isinstance(arg, numbers.Number):
        nsec = arg
        utc = None
    elif (isinstance(arg, str) or isinstance(arg, unicode)) and arg.isdigit():
        nsec = int(arg)
        utc = None
    else:
        nsec = 0
        dstr = str(arg)
        try:
            utc = datetime.datetime.strptime(dstr, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError, err:
            try:
                short = re.sub(r"\.[0-9]{9}", '', dstr)
                utc = datetime.datetime.strptime(short, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise HsException("Problem with the time-stamp format"
                                  " \"%s\": %s" % (arg, err))

    return fix_date_or_timestamp(nsec, utc, is_sn_ns=is_sn_ns)


def send_live_status(i3socket, req_id, username, prefix, start_utc, stop_utc,
                     copydir, status, success=None, failed=None):
    if status is None:
        raise HsException("Status is not set")
    if req_id is None:
        raise HsException("Request ID is not set")
    if start_utc is None:
        raise HsException("Start time is not set")
    if stop_utc is None:
        raise HsException("Stop time is not set")
    if copydir is None:
        raise HsException("Destination directory is not set")

    if not isinstance(start_utc, datetime.datetime):
        raise Exception("Bad start time %s<%s>" % (start_utc, type(start_utc)))
    if not isinstance(stop_utc, datetime.datetime):
        raise Exception("Bad stop time %s<%s>" % (stop_utc, type(stop_utc)))

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
    if not isinstance(rsync_path, str) and not isinstance(rsync_path, unicode):
        return None, None

    parts = rsync_path.split(":", 1)
    if len(parts) > 1 and parts[0].find("/") < 0:
        return parts

    # either no embedded colons or colons are part of the path
    return "", rsync_path
