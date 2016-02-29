#!/usr/bin/env python
#
#
# Hit Spool Grabber to be run on access
# author: dheereman
#

import getpass
import json
import logging
import os
import re
import sys
import zmq

import HsUtil

from HsBase import HsBase
from HsConstants import ALERT_PORT, I3LIVE_PORT
from HsException import HsException
from HsPrefix import HsPrefix


STD_SECONDS = 95
MAX_SECONDS = 610


def add_arguments(parser):
    copy_dflt = "%s@%s:%s" % (HsBase.DEFAULT_RSYNC_USER,
                              HsBase.DEFAULT_RSYNC_HOST,
                              HsBase.DEFAULT_COPY_PATH)

    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hsgrabber.log")

    parser.add_argument("-b", "--begin", dest="begin_time", required=True,
                        help="Beginning UTC time (YYYY-mm-dd HH:MM:SS[.us])"
                        " or SnDAQ timestamp (ns from start of year)")
    parser.add_argument("-c", "--copydir", dest="copydir", default=copy_dflt,
                        help="rsync destination directory for hitspool files")
    parser.add_argument("-e", "--end", dest="end_time", required=True,
                        help="Ending UTC time (YYYY-mm-dd HH:MM:SS[.us])"
                        " or SnDAQ timestamp (ns from start of year)")
    parser.add_argument("-i", "--request-id", dest="request_id",
                        help="Unique ID used to track this request")
    parser.add_argument("-J", "--send-json", dest="send_json",
                        action="store_true", default=False,
                        help="Send request as a string")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-p", "--prefix", dest="prefix",
                        help="Subsystem prefix (SNALERT, HESE, etc.)")
    parser.add_argument("-u", "--username", dest="username",
                        help="Name of user making the requests")
    parser.add_argument("-x", "--extract", dest="extract",
                        action="store_true", default=False,
                        help="Don't copy files directory, extract hits into"
                        " a new file")


class LogToConsole(object):  # pragma: no cover
    def info(self, msg, *args):
        print msg % args

    def warn(self, msg, *args):
        print >>sys.stderr, msg % args

    def error(self, msg, *args):
        print >>sys.stderr, msg % args


class HsGrabber(HsBase):
    '''
    Grab hs data from hubs independently (without sndaq providing alert).
    HsGrabber sends json to HsPublisher to grab hs data from hubs.
    Uses the Hs Interface infrastructure.
    '''

    # pattern used to validate rsync copy URL
    COPY_PATH_PAT = re.compile("((.*)@)?(([^:]+):)?(/.*)$")

    def __init__(self):
        super(HsGrabber, self).__init__()

        if self.is_cluster_sps or self.is_cluster_spts:
            expcont = "expcont"
        else:
            expcont = "localhost"

        self.__context = zmq.Context()
        self.__grabber = self.create_grabber(expcont)
        self.__poller = self.create_poller(self.__grabber)
        self.__i3socket = self.create_i3socket(expcont)

    def close_all(self):
        if self.__i3socket is not None:
            self.__i3socket.close()
        if self.__poller is not None:
            self.__poller.close()
        if self.__grabber is not None:
            self.__grabber.close()
        self.__context.term()

    def create_grabber(self, host):  # pragma: no cover
        # Socket to send alert message to HsPublisher
        sock = self.__context.socket(zmq.REQ)
        sock.connect("tcp://%s:%d" % (host, ALERT_PORT))
        return sock

    def create_poller(self, grabber):  # pragma: no cover
        # needed for handling timeout if Publisher doesnt answer
        sock = zmq.Poller()
        sock.register(grabber, zmq.POLLIN)
        return sock

    def create_i3socket(self, host):  # pragma: no cover
        # Socket for I3Live on expcont
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, I3LIVE_PORT))
        return sock

    @property
    def grabber(self):
        return self.__grabber

    @property
    def i3socket(self):
        return self.__i3socket

    @property
    def poller(self):
        return self.__poller

    def send_alert(self, alert_start_sn, alert_begin_utc, alert_stop_sn,
                   alert_end_utc, copydir, request_id=None, username=None,
                   prefix=None, extract_hits=False, send_json=True,
                   send_old_dates=False, print_to_console=False):
        '''
        Send request to Publisher and wait for response
        '''

        if print_to_console:
            print_log = LogToConsole
        else:
            print_log = logging

        # get number of seconds of data requested
        secrange = (alert_stop_sn - alert_start_sn) / 1E9

        # catch negative ranges
        if secrange <= 0:
            print_log.error("Requesting negative time range (%.2f).\n"
                            "Try another time window.", secrange)
            return False

        # catch large ranges
        if secrange > MAX_SECONDS:
            print_log.error("Request for %.2f seconds is too huge.\nHsWorker "
                            "processes request only up to %d seconds.\n"
                            "Try a smaller time window.", secrange,
                            MAX_SECONDS)
            return False

        if secrange > STD_SECONDS:
            print_log.error("Warning: You are requesting %.2f seconds of"
                            " data\nNormal requests are %d seconds or less",
                            secrange, STD_SECONDS)

        if copydir is None:
            print_log.error("Destination directory has not been specified")
            return False

        if print_to_console:
            answer = raw_input("Do you want to proceed? [y/n] : ")
            if not answer.lower().startswith("y"):
                return False

        logging.info("Requesting %.2f seconds of HS data [%d-%d]",
                     secrange, alert_start_sn, alert_stop_sn)

        if prefix is None and username is None:
            if send_old_dates:
                start = str(alert_begin_utc)
                stop = str(alert_end_utc)
            else:
                start = alert_start_sn
                stop = alert_stop_sn

            alert = {
                "start": start,
                "stop": stop,
                "copy": copydir,
            }
        else:
            if send_old_dates:
                logging.error("Requested old-dates but sending a new-style"
                              " request")

            if prefix is None:
                prefix = HsPrefix.guess_from_dir(copydir)

            if username is None:
                username = getpass.getuser()

            alert = {
                "username": username,
                "prefix": prefix,
                "start_time": str(alert_begin_utc),
                "stop_time": str(alert_end_utc),
                "destination_dir": copydir,
            }

        if request_id is not None:
            alert["request_id"] = str(request_id)

        if extract_hits:
            alert["extract"] = True

        if send_json:
            self.__grabber.send_json(alert)
        else:
            self.__grabber.send(json.dumps(alert))
        logging.info("HsGrabber sent Request %s", str(alert))

        return True

    def split_rsync_path(self, rsync_path):
        """
        Return a tuple containing (user, host, path) pieces of the rsync path,
        filling in default values for any missing pieces
        """
        if rsync_path is None:
            user = None
            host = None
            path = self.DEFAULT_COPY_PATH
        else:
            m = self.COPY_PATH_PAT.match(rsync_path)
            if m is None:
                raise HsException("Bad copy path \"%s\"" % rsync_path)

            user = m.group(2)
            host = m.group(4)
            path = m.group(5)

        if user is None:
            if self.is_cluster_sps or self.is_cluster_spts:
                user = self.DEFAULT_RSYNC_USER
            else:
                user = getpass.getuser()

        if host is None:
            if self.is_cluster_sps or self.is_cluster_spts:
                host = self.DEFAULT_RSYNC_HOST
            else:
                host = "localhost"

        return (user, host, path)

    def wait_for_response(self, timeout=10, print_to_console=False):
        "Wait for an answer from Publisher"
        count = 0
        while True:
            result = self.__poller.poll(timeout * 100)
            count += 1
            if count > timeout:
                break

            socks = dict(result)  # poller takes msec argument
            if self.__grabber in socks and socks[self.__grabber] == zmq.POLLIN:
                message = self.__grabber.recv()
                if message.startswith("DONE"):
                    logging.info("Request sent.")
                    return True
                elif message.startswith("ERROR"):
                    logging.error("Request ERROR")
                    return False

                logging.info("Received response: %s", message)

            if print_to_console:
                print ".",
                sys.stdout.flush()

        logging.error("no connection to expcont's HsPublisher"
                      " within %s seconds.\nAbort request.", timeout)
        logging.info("""
        Debugging hints:
        1. check HsPublisher's logfile on EXPCONT:
        /mnt/data/pdaqlocal/HsInterface/current/hspublisher_stdout_stderr.log
        and
        /mnt/data/pdaqlocal/HsInterface/logs/hspublisher_expcont.log

        2. restart HsPublisher.py via fabric on ACCESS:
        fab -f /home/pdaq/HsInterface/current/fabfile.py hs_stop_pub
        fab -f /home/pdaq/HsInterface/current/fabfile.py hs_start_pub_bkg
        """)

        return False


if __name__ == "__main__":
    import argparse
    import traceback

    def main():
        ''''Process arguments'''
        alert_start_sn = 0
        alert_begin_utc = None
        alert_stop_sn = 0
        alert_end_utc = None

        p = argparse.ArgumentParser(epilog="HsGrabber reads UTC timestamps or"
                                    " SNDAQ timestamps and sends SNDAQ"
                                    " timestamps to HsPublisher.")

        add_arguments(p)

        args = p.parse_args()

        usage = False
        if not usage:
            try:
                # get both SnDAQ timestamp (in ns) and UTC datetime
                (alert_start_sn, alert_begin_utc) \
                    = HsUtil.parse_sntime(args.begin_time)
            except HsException:
                traceback.print_exc()
                usage = True

        if not usage:
            try:
                # get both SnDAQ timestamp (in ns) and UTC datetime
                (alert_stop_sn, alert_end_utc) \
                    = HsUtil.parse_sntime(args.end_time)
            except HsException:
                traceback.print_exc()
                usage = True

        if usage:
            p.print_help()
            sys.exit(1)

        hsg = HsGrabber()

        hsg.init_logging(args.logfile, level=logging.INFO)

        logging.info("This HsGrabber runs on: %s", hsg.fullhost)

        print "Request start: %s (%d ns)" % (alert_begin_utc, alert_start_sn)
        print "Request end: %s (%d ns)" % (alert_end_utc, alert_stop_sn)

        # make sure rsync destination is fully specified
        (user, host, path) = hsg.split_rsync_path(args.copydir)
        copydir = "%s@%s:%s" % (user, host, path)

        if not hsg.send_alert(alert_start_sn, alert_begin_utc, alert_stop_sn,
                              alert_end_utc, copydir,
                              request_id=args.request_id,
                              username=args.username, prefix=args.prefix,
                              extract_hits=args.extract,
                              send_json=args.send_json, print_to_console=True):
            raise SystemExit(1)

        hsg.wait_for_response()

    main()
