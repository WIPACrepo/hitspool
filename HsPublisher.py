#!/usr/bin/env python

import argparse
import ast
import json
import logging
import os
import signal
import sys
import zmq

import DAQTime
import HsConstants
import HsMessage
import HsUtil

from HsBase import HsBase
from HsException import HsException
from HsPrefix import HsPrefix
from i3helper import reraise_excinfo


def add_arguments(parser):
    "Add all command line arguments to the argument parser"

    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hspublisher.log")

    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-T", "--is-test", dest="is_test",
                        action="store_true", default=False,
                        help="Ignore SPS/SPTS status for tests")


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
        self.__alert_socket = self.create_alert_socket()
        self.__i3socket = self.create_i3socket(expcont)
        self.__sender = self.create_sender_socket(sec_bldr)

    def __handle_request(self, alertdict):
        _, start_ticks, stop_ticks, is_valid \
                = self.__parse_version_and_times(alertdict)

        bad_request = not is_valid
        try:
            destdir, bad_flag = self.__parse_destination_dir(alertdict)
            bad_request |= bad_flag
        except:
            logging.exception("Could not parse destination directory")
            destdir = None
            bad_request = True

        if destdir is None:
            destdir = self.BAD_DESTINATION
            bad_request = True

        if 'request_id' in alertdict:
            req_id = alertdict['request_id']
        else:
            req_id = HsMessage.ID.generate()

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

        if 'hubs' not in alertdict or alertdict["hubs"] is None:
            hubs = None
        else:
            hubs = alertdict["hubs"]

        exc_info = None
        if not bad_request:
            try:
                # forward initial request to HsSender
                HsMessage.send_initial(self.__sender, req_id, start_ticks,
                                       stop_ticks, destdir, prefix, extract,
                                       hubs=hubs, host=self.shorthost,
                                       username=user)

                # log alert
                logging.info("Publisher published: %s", str(alertdict))
            except:
                exc_info = sys.exc_info()
                bad_request = True

        if not bad_request:
            # all is well!
            return True

        # let Live know there was a problem with this request
        try:
            HsUtil.send_live_status(self.__i3socket, req_id, user, prefix,
                                    start_ticks, stop_ticks, destdir,
                                    HsUtil.STATUS_REQUEST_ERROR)
        except:
            logging.exception("Failed to send ERROR status to Live")

        if exc_info is not None:
            # if there was an exception, re-raise it
            reraise_excinfo(exc_info)

        # let caller know there was a problem
        return False

    def __parse_destination_dir(self, alertdict):
        # extract destination directory from initial request
        if 'destination_dir' in alertdict:
            destdir = alertdict['destination_dir']
        elif 'copy' in alertdict:
            destdir = alertdict['copy']
        else:
            destdir = None

        # if no destination directory was provided, we're done
        if destdir is None:
            logging.error("Request did not specify a destination directory")
            return None, True

        # split directory into 'user@host' and path
        try:
            hs_ssh_access, hs_ssh_dir \
                = HsUtil.split_rsync_host_and_path(destdir)
        except:
            logging.error("Unusable destination directory \"%s\"<%s>", destdir,
                          type(destdir))
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

    @classmethod
    def __parse_version_and_times(cls, alertdict):
        if "version" in alertdict and "start_ticks" in alertdict and \
           "stop_ticks" in alertdict:
            version = int(alertdict["version"])
            start_ticks = alertdict["start_ticks"]
            stop_ticks = alertdict["stop_ticks"]
            is_valid = True
        else:
            # XXX remove this block of code after the Jem release
            version = None
            start_ticks = None
            stop_ticks = None
            is_valid = True

            for timetype in ("start", "stop"):
                if timetype + "_ticks" in alertdict:
                    # save tick value and set assumed version number
                    fldname = timetype + "_ticks"
                    try:
                        val = int(alertdict[fldname])
                    except ValueError:
                        logging.error("Bad %s ticks \"%s\"", timetype,
                                      alertdict[fldname])
                        is_valid = False
                        break

                    if timetype == "start":
                        start_ticks = val
                    elif timetype == "stop":
                        stop_ticks = val
                    version = 2
                    continue

                if timetype + '_time' in alertdict:
                    fldname = timetype + '_time'
                    newvers = 1
                elif timetype in alertdict:
                    fldname = timetype
                    newvers = 0
                else:
                    logging.error("Request did not contain a %s time:\n%s",
                                  timetype, alertdict)
                    is_valid = False
                    break

                # update the request version
                if version is None:
                    version = newvers
                elif version != newvers:
                    logging.error("Request contained old and new times:\n%s",
                                  alertdict)
                    is_valid = False
                    break

                # old requests sent times in nanoseconds, not 0.1ns ticks
                try:
                    val = DAQTime.string_to_ticks(alertdict[fldname],
                                                  is_ns=True)
                except HsException:
                    logging.error("Bad %s time \"%s\"", timetype,
                                  alertdict[fldname])
                    is_valid = False
                    break

                if timetype == "start":
                    start_ticks = val
                elif timetype == "stop":
                    stop_ticks = val
                else:
                    logging.error("Ignoring unknown time type \"%s\"",
                                  timetype)

        if start_ticks is None or stop_ticks is None:
            logging.error("Could not find start/stop time in request:\n%s",
                          alertdict)
            is_valid = False

        return (version, start_ticks, stop_ticks, is_valid)

    @property
    def alert_socket(self):
        return self.__alert_socket

    def close_all(self):
        self.__alert_socket.close()
        self.__i3socket.close()
        self.__sender.close()
        self.__context.term()

    def create_alert_socket(self):
        # Socket to receive alert message
        sock = self.__context.socket(zmq.REP)
        sock.identity = "Alert".encode("ascii")
        sock.bind("tcp://*:%d" % HsConstants.OLDALERT_PORT)
        logging.info("bind REP socket for receiving alert messages to port %d",
                     HsConstants.OLDALERT_PORT)
        return sock

    def create_i3socket(self, host):
        # Socket for I3Live on expcont
        sock = self.__context.socket(zmq.PUSH)
        sock.identity = "I3Socket".encode("ascii")
        sock.connect("tcp://%s:%d" % (host, HsConstants.I3LIVE_PORT))
        logging.info("connect PUSH socket to i3live on %s port %d", host,
                     HsConstants.I3LIVE_PORT)
        return sock

    def create_sender_socket(self, host):
        if host is None:
            return None

        # Socket to send message to
        sock = self.__context.socket(zmq.PUSH)
        sock.identity = "Sender".encode("ascii")
        sock.connect("tcp://%s:%d" % (host, HsConstants.SENDER_PORT))
        logging.info("connect PUSH socket to sender on %s port %d", host,
                     HsConstants.SENDER_PORT)
        return sock

    def handler(self, signum, _):
        """Clean exit when program is terminated from outside (via pkill)"""
        logging.warning("Signal Handler called with signal %s", signum)
        logging.warning("Shutting down...\n")

        self.close_all()

        raise SystemExit(0)

    @property
    def i3socket(self):
        return self.__i3socket

    def reply_request(self):
        # Wait for next request from client
        alert = self.__alert_socket.recv()
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
        answer = self.__alert_socket.send_string(rtnmsg + "\0")
        if answer is None:
            logging.info("Sent response back to requester: %s", rtnmsg)
        else:
            logging.error("Failed sending %s to requester: %s", rtnmsg, answer)

    @property
    def sender(self):
        return self.__sender


def main():
    "Main program"

    parser = argparse.ArgumentParser()

    add_arguments(parser)

    args = parser.parse_args()

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

if __name__ == '__main__':
    main()
