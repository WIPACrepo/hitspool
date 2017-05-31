#!/usr/bin/env python


import getpass
import struct
import threading
import time

from HsBase import DAQTime
from HsException import HsException
from HsPrefix import HsPrefix
from HsUtil import dict_to_object


# version number for current message format
DEFAULT_VERSION = 2

# message types
MESSAGE_INITIAL = "REQUEST"
MESSAGE_STARTED = "STARTED"
MESSAGE_WORKING = "WORKING"
MESSAGE_DONE = "DONE"
MESSAGE_FAILED = "FAILED"

# mandatory message fields
__MANDATORY_FIELDS = ("username", "prefix", "start_time", "stop_time",
                      "destination_dir", "host", "version")


class ID(object):
    __seed = 0
    __seed_lock = threading.Lock()

    @classmethod
    def generate(cls):
        with cls.__seed_lock:
            val = cls.__seed
            cls.__seed = (cls.__seed + 1) % 0xFFFFFF
        x = struct.pack('>i', int(time.time()))
        x += struct.pack('>i', val)[1:4]
        return x.encode('hex')


def dict_to_message(mdict):
    return dict_to_object(fix_message_dict(mdict), __MANDATORY_FIELDS,
                          "Message")


def fix_message_dict(mdict):
    if mdict is None:
        return None

    if not isinstance(mdict, dict):
        raise HsException("Message is not a dictionary: \"%s\"<%s>" %
                          (mdict, type(mdict)))

    # check for mandatory fields
    if "msgtype" not in mdict:
        mdict["msgtype"] = MESSAGE_INITIAL
    if "request_id" not in mdict:
        if mdict["msgtype"] != MESSAGE_INITIAL:
            raise HsException("No request ID found in %s" % str(mdict))
        mdict["request_id"] = ID.generate()
    if "start_time" in mdict and not isinstance(mdict["start_time"], DAQTime):
        mdict["start_time"] = DAQTime(mdict["start_time"])
    if "stop_time" in mdict and not isinstance(mdict["stop_time"], DAQTime):
        mdict["stop_time"] = DAQTime(mdict["stop_time"])
    if "copy_dir" not in mdict:
        mdict["copy_dir"] = None
    if "extract" not in mdict:
        mdict["extract"] = False

    return mdict


def receive(sock):
    mdict = sock.recv_json()
    if mdict is None:
        return None
    return dict_to_message(mdict)


def send(sock, msgtype, req_id, user, start_time, stop_time, destdir,
         prefix=None, copydir=None, extract=None, host=None, version=None,
         use_ticks=True):
    # check for required fields
    if req_id is None:
        raise HsException("Request ID is not set")
    if msgtype is None:
        raise HsException("Message type is not set for Req#" + str(req_id))
    if user is None:
        raise HsException("Username is not set for Req#" + str(req_id))
    if start_time is None:
        raise HsException("Start time is not set for Req#" + str(req_id))
    elif not isinstance(start_time, DAQTime):
        raise TypeError("Start time %s<%s> should be DAQTime for Req#%s" %
                        (start_time, type(start_time), req_id))
    if stop_time is None:
        raise HsException("Stop time is not set for Req#" + str(req_id))
    elif not isinstance(stop_time, DAQTime):
        raise TypeError("Stop time %s<%s> should be DAQTime for Req#%s" %
                        (stop_time, type(stop_time), req_id))
    if destdir is None:
        raise HsException("Destination directory is not set for Req#" +
                          str(req_id))

    # fill in optional or deriveable fields
    if prefix is None:
        prefix = HsPrefix.guess_from_dir(destdir)
    extract = extract is not None and extract
    if version is None:
        version = DEFAULT_VERSION

    msg = {
        "msgtype": msgtype,
        "version": version,
        "request_id": req_id,
        "username": user,
        "start_time": start_time.ticks if use_ticks else str(start_time.utc),
        "stop_time": stop_time.ticks if use_ticks else str(stop_time.utc),
        "copy_dir": copydir,
        "destination_dir": destdir,
        "prefix": prefix,
        "extract": extract,
        "host": host,
    }

    return sock.send_json(msg) is None


def send_for_request(sock, req, host, copydir, destdir, msgtype,
                     use_ticks=False):
    return send(sock, msgtype, req.request_id, req.username, req.start_time,
                req.stop_time, destdir, prefix=req.prefix, copydir=copydir,
                extract=req.extract, host=host, version=None,
                use_ticks=use_ticks)


def send_initial(sock, req_id, start_time, stop_time, destdir, prefix=None,
                 extract_hits=False, host=None, username=None):
    if req_id is None:
        req_id = ID.generate()
    if username is None:
        username = getpass.getuser()

    return send(sock, MESSAGE_INITIAL, req_id, username, start_time,
                stop_time, destdir, prefix=prefix, copydir=None,
                extract=extract_hits, host=host, version=DEFAULT_VERSION)
