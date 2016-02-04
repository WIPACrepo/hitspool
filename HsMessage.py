#!/usr/bin/env python

from datetime import datetime

from HsException import HsException
from HsUtil import dict_to_object, parse_sntime


# internal message fields
MSG_FLDS = ("msgtype", "request_id", "username", "start_time", "stop_time",
            "copy_dir", "destination_dir", "prefix", "extract", "host")

# internal message types
MESSAGE_INITIAL = "REQUEST"
MESSAGE_STARTED = "STARTED"
MESSAGE_DONE = "DONE"
MESSAGE_FAILED = "FAILED"

# database request states
DBSTATE_INITIAL = 0
DBSTATE_START = 10
DBSTATE_ERROR = 50
DBSTATE_DONE = 90


def receive(sock):
    mdict = sock.recv_json()
    if mdict is None:
        return None

    if not isinstance(mdict, dict):
        raise HsException("Message is not a dictionary: \"%s\"<%s>" %
                          (mdict, type(mdict)))

    if "start_time" in mdict:
        _, mdict["start_time"] = parse_sntime(mdict["start_time"],
                                              is_sn_ns=False)
    if "stop_time" in mdict:
        _, mdict["stop_time"] = parse_sntime(mdict["stop_time"],
                                             is_sn_ns=False)
    return dict_to_object(mdict, MSG_FLDS, "Message")


def send(sock, msgtype, req_id, user, start_utc, stop_utc, copydir, destdir,
         prefix, extract, host):
    if msgtype is None:
        raise HsException("Message type is not set")
    if req_id is None:
        raise HsException("Message request ID is not set")
    if start_utc is None:
        raise HsException("Message start time is not set")
    if stop_utc is None:
        raise HsException("Message stop time is not set")
    if destdir is None:
        raise HsException("Message destination directory is not set")

    # json doesn't know about datetime, convert to string
    if isinstance(start_utc, datetime):
        start_utc = str(start_utc)
    if isinstance(stop_utc, datetime):
        stop_utc = str(stop_utc)

    msg = {
        "msgtype": msgtype,
        "request_id": req_id,
        "username": user,
        "start_time": start_utc,
        "stop_time": stop_utc,
        "copy_dir": copydir,
        "destination_dir": destdir,
        "prefix": prefix,
        "extract": extract,
        "host": host,
    }

    return sock.send_json(msg)


def state_name(state):
    if state == DBSTATE_INITIAL:
        return "INITIAL"
    if state == DBSTATE_START:
        return "START"
    if state == DBSTATE_DONE:
        return "DONE"
    if state == DBSTATE_ERROR:
        return "ERROR"
    return "??#%d??" % state
