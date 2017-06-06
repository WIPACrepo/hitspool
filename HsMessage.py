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
INITIAL = "REQUEST"
STARTED = "STARTED" # hub has received a request
WORKING = "WORKING" # hub is sending data (can be sent many times)
DONE = "DONE"       # hub has finished sending data
FAILED = "FAILED"   # hub failed to fill request
IGNORED = "IGNORED" # hub is not part of the request

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
        mdict["msgtype"] = INITIAL
    if "request_id" not in mdict:
        if mdict["msgtype"] != INITIAL:
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
    if "hubs" not in mdict:
        mdict["hubs"] = ""

    return mdict


def receive(sock):
    mdict = sock.recv_json()
    if mdict is None:
        return None
    return dict_to_message(mdict)


def send(sock, msgtype, req_id, user, start_time, stop_time, destdir,
         prefix=None, copydir=None, extract=None, host=None, hubs=None,
         version=None):
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
        "start_time": start_time.ticks,
        "stop_time": stop_time.ticks,
        "copy_dir": copydir,
        "destination_dir": destdir,
        "prefix": prefix,
        "extract": extract,
        "hubs": hubs,
        "host": host,
    }

    return sock.send_json(msg) is None


def send_for_request(sock, req, host, copydir, destdir, msgtype):
    return send(sock, msgtype, req.request_id, req.username, req.start_time,
                req.stop_time, destdir, prefix=req.prefix, copydir=copydir,
                extract=req.extract, host=host, version=None)


def send_initial(sock, req_id, start_time, stop_time, destdir, prefix=None,
                 extract_hits=False, hubs=None, host=None, username=None):
    "Send initial request"
    if req_id is None:
        req_id = ID.generate()
    if username is None:
        username = getpass.getuser()

    return send(sock, INITIAL, req_id, username, start_time,
                stop_time, destdir, prefix=prefix, copydir=None,
                extract=extract_hits, hubs=hubs, host=host,
                version=DEFAULT_VERSION)


def send_worker_status(sock, req, host, copydir, destdir, msgtype):
    "Send worker status to HsSender based on the request"
    return send(sock, msgtype, req.request_id, req.username, req.start_time,
                req.stop_time, destdir, prefix=req.prefix, copydir=copydir,
                extract=req.extract, host=host, version=None)
