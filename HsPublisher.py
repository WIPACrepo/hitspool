#!/usr/bin/env python


import ast
import json
import logging
import os
import signal
import struct
import sys
import threading
import time
import zmq

from datetime import datetime

import HsConstants
import HsMessage
import HsUtil

from HsBase import HsBase
from HsException import HsException
from HsPrefix import HsPrefix


def add_arguments(parser):
    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hspublisher.log")

    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-T", "--is-test", dest="is_test",
                        action="store_true", default=False,
                        help="Ignore SPS/SPTS status for tests")


class MessageID(object):
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


class Receiver(HsBase):
    """
    "sndaq"           "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        ------------
    | REQUEST | <----->| REPLY   |         | IcHub n |        | 2ndbuild |
    -----------        | PUB     | ------> | SUB    n|        | PULL     |
                       ----------          |PUSH     | ---->  |          |
                                            ---------          -----------
    Handle incoming request message from sndaq or any other process.
    Monitors ALERT Socket for the request coming from sndaq.
    Sends log messages to I3Live.
    """

    DEFAULT_USERNAME = 'unknown'
    BAD_DESTINATION = "/unknown/path"

    def __init__(self, host=None, is_test=False):
        super(Receiver, self).__init__(host=host, is_test=is_test)

        if self.is_cluster_sps or self.is_cluster_spts:
            expcont = "expcont"
            sec_bldr = "2ndbuild"
        else:
            expcont = "localhost"
            sec_bldr = "localhost"

        self.__context = zmq.Context()
        self.__socket = self.create_alert_socket()
        self.__workers = self.create_workers_socket()
        self.__i3socket = self.create_i3socket(expcont)
        self.__sender = self.create_sender_socket(sec_bldr)

        self.__jan1 = None

    def __build_json_email(self, alert, sn_start_utc, sn_stop_utc, extract):
        alertmsg = "%s\nstart in UTC : %s\nstop  in UTC : %s\n" \
                   "(no possible leapseconds applied)" % \
                   (alert, sn_start_utc, sn_stop_utc)
        if extract:
            alertmsg += "\nExtracting matching hits"

        notify_hdr = "DATA REQUEST HsInterface Alert: %s" % self.cluster
        notify_desc = "HsInterface Data Request"

        address_list = HsConstants.ALERT_EMAIL_DEV[:]
        if "prefix" in alert and alert["prefix"] == HsPrefix.SNALERT:
            address_list += HsConstants.ALERT_EMAIL_SN

        return HsUtil.assemble_email_dict(address_list, notify_hdr,
                                          notify_desc, alertmsg, prio=1)

    def __handle_request(self, alertdict):
        bad_request = False

        start_ns, sn_start_utc = self.__parse_time(alertdict, "start")
        if start_ns == 0 or sn_start_utc is None:
            if not bad_request:
                logging.error("Request contains bad start time: %s",
                              str(alertdict))
                bad_request = True
            if sn_start_utc is None:
                sn_start_utc = datetime.now()

        stop_ns, sn_stop_utc = self.__parse_time(alertdict, "stop")
        if stop_ns == 0 or sn_stop_utc is None:
            if not bad_request:
                logging.error("Request contains bad stop time: %s",
                              str(alertdict))
                bad_request = True
            if sn_stop_utc is None:
                sn_stop_utc = datetime.now()

        try:
            destdir, bad_flag = self.__parse_destination_dir(alertdict)
            bad_request |= bad_flag
        except:
            logging.exception("Failed to parse destination directory")
            destdir = None
            bad_request = True
        if destdir == None:
            destdir = self.BAD_DESTINATION
            bad_request = True

        if 'request_id' in alertdict:
            req_id = alertdict['request_id']
        else:
            req_id = MessageID.generate()

        if 'username' in alertdict:
            user = alertdict['username']
        else:
            user = self.DEFAULT_USERNAME

        if 'prefix' in alertdict:
            prefix = alertdict["prefix"]
        else:
            prefix = HsPrefix.guess_from_dir(destdir)

        if 'extract' not in alertdict:
            extract = False
        elif isinstance(alertdict['extract'], bool):
            extract = alertdict['extract']
        else:
            if not bad_request:
                logging.error("Assuming 'extract' value \"%s\" is True",
                              alertdict["extract"])
            extract = True

        exc_info = None
        if not bad_request:
            try:
                # fill in new fields
                copydir = None
                host = self.shorthost

                # tell HsSender about the request
                HsMessage.send(self.__sender, HsMessage.MESSAGE_INITIAL,
                               req_id, user, sn_start_utc, sn_stop_utc,
                               copydir, destdir, prefix, extract, host)

                # send request to workers
                HsMessage.send(self.__workers, HsMessage.MESSAGE_INITIAL,
                               req_id, user, start_ns, stop_ns,
                               copydir, destdir, prefix, extract, host)

                # log alert
                logging.info("Publisher published: %s", str(alertdict))

                # send Live alert JSON for email notification:
                alertjson = self.__build_json_email(alertdict, sn_start_utc,
                                                    sn_stop_utc, extract)
                self.__i3socket.send_json(alertjson)
            except:
                exc_info = sys.exc_info()
                bad_request = True

        if not bad_request:
            # all is well!
            return True

        # let Live know there was a problem with this request
        HsUtil.send_live_status(self.__i3socket, req_id, user, prefix,
                                sn_start_utc, sn_stop_utc, destdir,
                                HsUtil.STATUS_REQUEST_ERROR)
        if exc_info is not None:
            # if there was an exception, re-raise it
            raise exc_info[0], exc_info[1], exc_info[2]

        # let caller know there was a problem
        return False

    def __parse_destination_dir(self, alertdict):
        # extract destination directory from initial request
        if 'destination_dir' in alertdict:
            destdir = alertdict['destination_dir']
        elif 'copy' in alertdict:
            destdir = alertdict['copy']
        else:
            logging.error("Request did not contain a copy directory: %s",
                          alertdict)
            return None, True

        # if no destination directory was provided, we're done
        if destdir is None:
            logging.error("Destination directory is not specified")
            return None, True

        # split directory into 'user@host' and path
        try:
            hs_ssh_access, hs_ssh_dir \
                = HsUtil.split_rsync_host_and_path(destdir)
        except:
            logging.error("Unusable destination directory \"%s\"<%s>" %
                          (destdir, type(destdir)))
            return destdir, True

        # if no user/hst was specified, return the path
        if hs_ssh_access != "":
            # only the standard user and host are allowed
            if hs_ssh_access.find("@") < 0:
                hs_user = self.rsync_user
                hs_host = hs_ssh_access
            else:
                hs_user, hs_host = hs_ssh_access.split("@", 1)

            if hs_user != self.rsync_user:
                logging.error("rsync user must be %s, not %s (from \"%s\")",
                              self.rsync_user, hs_user, destdir)
                return destdir, True
            if hs_host != self.rsync_host:
                logging.error("rsync host must be %s, not %s (from \"%s\")",
                              self.rsync_host, hs_host, destdir)
                return destdir, True

        return hs_ssh_dir, False

    def __parse_time(self, alertdict, name):
        if name + '_time' in alertdict:
            fldname = name + '_time'
        elif name in alertdict:
            fldname = name
        else:
            logging.error("Request did not contain a %s time:\n%s", name,
                          alertdict)
            fldname = None

        nsec = 0
        utc = None
        if fldname is not None:
            tstr = alertdict[fldname]
            try:
                nsec, utc = HsUtil.parse_sntime(tstr)
            except HsException, hsex:
                logging.exception("Bad %s time \"%s\"", fldname, tstr)

        return nsec, utc

    @property
    def alert_socket(self):
        return self.__socket

    def close_all(self):
        self.__socket.close()
        self.__workers.close()
        self.__i3socket.close()
        self.__sender.close()
        self.__context.term()

    def create_alert_socket(self):
        # Socket to receive alert message
        sock = self.__context.socket(zmq.REP)
        sock.bind("tcp://*:%d" % HsConstants.ALERT_PORT)
        logging.info("bind REP socket for receiving alert messages to port %d",
                     HsConstants.ALERT_PORT)
        return sock

    def create_i3socket(self, host):
        # Socket for I3Live on expcont
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, HsConstants.I3LIVE_PORT))
        logging.info("connect PUSH socket to i3live on %s port %d", host,
                     HsConstants.I3LIVE_PORT)
        return sock

    def create_sender_socket(self, host):
        if host is None:
            return None

        # Socket to send message to
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, HsConstants.SENDER_PORT))
        logging.info("connect PUSH socket to sender on %s port %d", host,
                     HsConstants.SENDER_PORT)
        return sock

    def create_workers_socket(self):
        # Socket to talk to Workers
        sock = self.__context.socket(zmq.PUB)
        sock.bind("tcp://*:%d" % HsConstants.PUBLISHER_PORT)
        logging.info("bind PUB socket to port %d", HsConstants.PUBLISHER_PORT)
        return sock

    def handler(self, signum, _):
        """Clean exit when program is terminated from outside (via pkill)"""
        logging.warning("Signal Handler called with signal %s", signum)
        logging.warning("Shutting down...\n")

        i3live_dict = {}
        i3live_dict["service"] = "HSiface"
        i3live_dict["varname"] = "HsPublisher"
        i3live_dict["value"] = "INFO: SHUT DOWN called by external signal."
        self.__i3socket.send_json(i3live_dict)

        i3live_dict = {}
        i3live_dict["service"] = "HSiface"
        i3live_dict["varname"] = "HsPublisher"
        i3live_dict["value"] = "STOPPED"
        self.__i3socket.send_json(i3live_dict)

        self.close_all()

        raise SystemExit(0)

    @property
    def i3socket(self):
        return self.__i3socket

    @property
    def jan1(self):
        if self.__jan1 is None:
            self.__jan1 = datetime(datetime.utcnow().year, 1, 1)
        return self.__jan1

    def reply_request(self):
        # Wait for next request from client
        alert = str(self.__socket.recv())
        logging.info("received request:\n%s", alert)

        # SnDAQ alerts are NOT real JSON so try to eval first
        try:
            alertdict = ast.literal_eval(alert)
        except (SyntaxError, ValueError):
            try:
                alertdict = json.loads(alert)
            except:
                logging.exception("Cannot decode %s", alert)
                alertdict = None

        if alertdict is None:
            logging.error("Ignoring bad request: %s", alert)
            success = False
        else:
            try:
                success = self.__handle_request(alertdict)
            except:
                success = False
                logging.exception("Request error: %s", alertdict)

        if success:
            rtnmsg = "DONE"
        else:
            rtnmsg = "ERROR"

        # reply to requester:
        #  added \0 to fit C/C++ zmq message termination
        answer = self.__socket.send(rtnmsg + "\0")
        if answer is None:
            logging.info("Sent response back to requester: %s", rtnmsg)
        else:
            logging.error("Failed sending %s to requester", rtnmsg)

    @property
    def sender(self):
        return self.__sender

    @property
    def workers(self):
        return self.__workers


if __name__ == '__main__':
    import argparse

    def main():
        p = argparse.ArgumentParser()

        add_arguments(p)

        args = p.parse_args()

        receiver = Receiver(is_test=args.is_test)

        # handler is called when SIGTERM is called (via pkill)
        signal.signal(signal.SIGTERM, receiver.handler)

        receiver.init_logging(args.logfile, basename="hspublisher",
                              basehost="expcont")

        logging.info("HsPublisher started on %s", receiver.shorthost)

        # We want to have a stable connection FOREVER to the client
        while True:
            try:
                receiver.reply_request()
            except SystemExit:
                raise
            except KeyboardInterrupt:
                # catch terminatation signals: can be Ctrl+C (if started
                # locally) or another termination message from fabfile
                logging.warning("Interruption received, shutting down...")
                break
            except zmq.ZMQError:
                logging.exception("ZMQ error received, shutting down...")
                raise SystemExit(1)
            except:
                logging.exception("Caught exception, continuing")

    main()
