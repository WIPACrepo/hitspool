#!/usr/bin/env python
#
#
# Hit Spool Grabber to be run on access
# author: dheereman
#

import datetime
import getpass
import json
import logging
import os
import re
import sys
import traceback  # used by LogToConsole
import zmq

import DAQTime
import HsMessage

from HsBase import HsBase
from HsConstants import ALERT_PORT, OLDALERT_PORT
from HsException import HsException
from HsPrefix import HsPrefix


# requests longer than this will provoke a warning message
#  (requests longer than HsBase.MAX_REQUEST_SECONDS will fail)
WARN_SECONDS = 95


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
    parser.add_argument("-d", "--duration", dest="duration", default=None,
                        help="Duration of request (1s, 12m, etc.)")
    parser.add_argument("-e", "--end", dest="end_time",
                        help="Ending UTC time (YYYY-mm-dd HH:MM:SS[.us])"
                        " or SnDAQ timestamp (ns from start of year)")
    parser.add_argument("-h", "--hub", dest="hub", action="append",
                        help="Name of one or more hubs which should respond"
                        " to this request")
    parser.add_argument("-i", "--request-id", dest="request_id",
                        help="Unique ID used to track this request")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-p", "--prefix", dest="prefix",
                        help="Subsystem prefix (SNALERT, HESE, etc.)")
    parser.add_argument("-T", "--is-test", dest="is_test",
                        action="store_true", default=False,
                        help="Ignore SPS/SPTS status for tests")
    parser.add_argument("-u", "--username", dest="username",
                        help="Name of user making the requests")
    parser.add_argument("-x", "--extract", dest="extract",
                        action="store_true", default=False,
                        help="Don't copy files directory, extract hits into"
                        " a new file")


def getDurationFromString(s):
    """
    Return duration in seconds based on string <s>
    """
    m = re.search(r'^(\d+)$', s)
    if m:
        return int(m.group(1))
    m = re.search(r'^(\d+)s(?:ec(?:s)?)?$', s)
    if m:
        return int(m.group(1))
    m = re.search(r'^(\d+)m(?:in(?:s)?)?$', s)
    if m:
        return int(m.group(1)) * 60
    m = re.search(r'^(\d+)h(?:r(?:s)?)?$', s)
    if m:
        return int(m.group(1)) * 3600
    m = re.search(r'^(\d+)d(?:ay(?:s)?)?$', s)
    if m:
        return int(m.group(1)) * 86400
    raise ValueError('String "%s" is not a known duration format.  Try'
                     '30sec, 10min, 2days etc.' % s)


class LogToConsole(object):  # pragma: no cover
    @staticmethod
    def info(msg, *args):
        print msg % args

    @staticmethod
    def warn(msg, *args):
        print >>sys.stderr, msg % args

    @staticmethod
    def error(msg, *args):
        print >>sys.stderr, msg % args

    @staticmethod
    def exception(msg, *args):
        print >>sys.stderr, msg % args
        traceback.print_exc()


class HsGrabber(HsBase):
    '''
    Grab hs data from hubs independently (without sndaq providing alert).
    HsGrabber sends json to HsPublisher to grab hs data from hubs.
    Uses the Hs Interface infrastructure.
    '''

    # pattern used to validate rsync copy URL
    COPY_PATH_PAT = re.compile("((.*)@)?(([^:]+):)?(/.*)$")

    def __init__(self, is_test=False):
        super(HsGrabber, self).__init__(is_test=is_test)

        if self.is_cluster_sps or self.is_cluster_spts:
            expcont = "expcont"
            sec_bldr = "2ndbuild"
        else:
            expcont = "localhost"
            sec_bldr = "localhost"

        self.__context = zmq.Context()
        self.__sender = self.create_sender(sec_bldr)
        self.__publisher = self.create_publisher(expcont)
        self.__poller = self.create_poller((self.__publisher, self.__sender))

    def close_all(self):
        self.__poller.unregister(self.__publisher)
        self.__poller.unregister(self.__sender)
        if self.__publisher is not None:
            self.__publisher.close()
        if self.__sender is not None:
            self.__sender.close()
        self.__context.term()

    def create_sender(self, host):  # pragma: no cover
        # Socket to send alert message to HsSender
        sock = self.__context.socket(zmq.REQ)
        sock.identity = "Sender".encode("ascii")
        sock.connect("tcp://%s:%d" % (host, ALERT_PORT))
        return sock

    def create_publisher(self, host):  # pragma: no cover
        # Socket to send alert message to HsSender
        sock = self.__context.socket(zmq.REQ)
        sock.identity = "Publisher".encode("ascii")
        sock.connect("tcp://%s:%d" % (host, OLDALERT_PORT))
        return sock

    def create_poller(self, sockets):  # pragma: no cover
        # needed for handling timeout if Publisher doesnt answer
        poller = zmq.Poller()
        for sock in sockets:
            poller.register(sock, zmq.POLLIN)
        return poller

    @property
    def poller(self):
        return self.__poller

    @property
    def publisher(self):
        return self.__publisher

    @property
    def sender(self):
        return self.__sender

    def send_alert(self, start_ticks, stop_ticks, destdir, request_id=None,
                   username=None, prefix=None, extract_hits=False, hubs=None,
                   print_to_console=False):
        '''
        Send request to Sender and wait for response
        '''

        if print_to_console:
            print_log = LogToConsole
        else:
            print_log = logging

        # get number of seconds of data requested
        secrange = (stop_ticks - start_ticks) / 1E10

        # catch negative ranges
        if secrange <= 0:
            print_log.error("Requesting negative time range (%.2f).\n"
                            "Try another time window.", secrange)
            return False

        # catch large ranges
        if secrange > self.MAX_REQUEST_SECONDS:
            print_log.error("Request for %.2f seconds is too huge.\nHsWorker "
                            "processes request only up to %d seconds.\n"
                            "Try a smaller time window.", secrange,
                            self.MAX_REQUEST_SECONDS)
            return False

        if secrange > WARN_SECONDS:
            print_log.error("Warning: You are requesting %.2f seconds of"
                            " data\nNormal requests are %d seconds or less",
                            secrange, WARN_SECONDS)

        if print_to_console:
            answer = raw_input("Do you want to proceed? [y/n] : ")
            if not answer.lower().startswith("y"):
                return False

        logging.info("Requesting %.2f seconds of HS data [%d-%d]",
                     secrange, start_ticks, stop_ticks)

        try:
            if not HsMessage.send_initial(self.__sender, request_id,
                                          start_ticks, stop_ticks,
                                          destdir, prefix=prefix,
                                          extract_hits=extract_hits,
                                          hubs=hubs, host=self.shorthost,
                                          username=None):
                print_log.error("Initial message was not sent!")
            else:
                print_log.info("HsGrabber sent request")

        except:
            print_log.exception("Failed to send request")

        return True

    def send_old_alert(self, start_ticks, stop_ticks, destdir,
                       request_id=None, username=None, prefix=None,
                       extract_hits=False, hubs=None):
        '''
        Send request to Publisher and wait for response
        '''

        if hubs is not None:
            raise HsException("'hubs' parameter is not used for old alers")

        print_log = logging

        # get number of seconds of data requested
        secrange = (stop_ticks - start_ticks) / 1E10

        # catch negative ranges
        if secrange <= 0:
            print_log.error("Requesting negative time range (%.2f).\n"
                            "Try another time window.", secrange)
            return False

        # catch large ranges
        if secrange > self.MAX_REQUEST_SECONDS:
            print_log.error("Request for %.2f seconds is too huge.\nHsWorker "
                            "processes request only up to %d seconds.\n"
                            "Try a smaller time window.", secrange,
                            self.MAX_REQUEST_SECONDS)
            return False

        if secrange > WARN_SECONDS:
            print_log.error("Warning: You are requesting %.2f seconds of"
                            " data\nNormal requests are %d seconds or less",
                            secrange, WARN_SECONDS)

        if destdir is None:
            print_log.error("Destination directory has not been specified")
            return False

        logging.info("Requesting %.2f seconds of HS data [%d-%d]",
                     secrange, start_ticks, stop_ticks)

        if prefix is None and username is None:
            # if user didn't specify prefix or username,
            #  this is an ancient request format
            if request_id is not None:
                print_log.error("Version 0 request cannot include"
                                " request ID \"%s\"", request_id)
                return False

            if extract_hits:
                print_log.error("Version 0 request cannot include"
                                " \"extract\" value")
                return False

            alert = {
                "start": start_ticks / 10,
                "stop": stop_ticks / 10,
                "copy": destdir,
            }
        else:
            # if missing, fill in either prefix or username (but not both)
            if prefix is None:
                prefix = HsPrefix.guess_from_dir(destdir)

            if username is None:
                username = getpass.getuser()

            alert = {
                "username": username,
                "prefix": prefix,
                "start_ticks": start_ticks,
                "stop_ticks": stop_ticks,
                "destination_dir": destdir,
            }

            if request_id is not None:
                alert["request_id"] = str(request_id)

            if extract_hits:
                alert["extract"] = True

        self.__publisher.send(json.dumps(alert))
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
            user = self.rsync_user

        if host is None:
            host = self.rsync_host

        return (user, host, path)

    def wait_for_response(self, timeout=10, print_to_console=False):
        "Wait for an answer from Publisher"
        count = 0
        while True:
            count += 1
            if count > timeout:
                break

            for sock, event in self.__poller.poll(timeout * 100):
                if event != zmq.POLLIN:
                    logging.error("Unknown event \"%s\"<%s> for %s<%s>", event,
                                  type(event).__name__, sock,
                                  type(sock).__name__)
                    continue

                if sock != self.__sender and sock != self.__publisher:
                    if sock is not None:
                        logging.error("Ignoring unknown incoming socket"
                                      " %s<%s>", sock.identity,
                                      type(sock).__name__)
                    continue

                try:
                    msg = sock.recv()
                    if msg is None:
                        continue
                except:
                    logging.exception("Cannot receive message from %s",
                                      sock.identity)
                    continue

                if msg.startswith("DONE"):
                    logging.info("Request sent.")
                    return True
                elif msg.startswith("ERROR"):
                    logging.error("Request ERROR")
                    return False

                logging.info("Unknown response: %s", msg)

            if print_to_console:
                print ".",
                sys.stdout.flush()

        logging.error("No response from expcont's HsPublisher"
                      " within %s seconds.\nAbort request.", timeout)

        logging.info("""
        Debugging hints:
        1. check HsPublisher's logfile on EXPCONT:
        /mnt/data/pdaqlocal/HsInterface/logs/hspublisher_expcont.log

        2. restart HsPublisher.py via fabric on ACCESS:
        fab -f /home/pdaq/HsInterface/current/fabfile.py hs_stop_pub
        fab -f /home/pdaq/HsInterface/current/fabfile.py hs_start_pub_bkg
        """)

        return False


if __name__ == "__main__":
    import argparse

    def parse_time(name, rawval, now_ticks):
        try:
            # convert string to starting time
            daq_ticks = DAQTime.string_to_ticks(rawval, is_ns=True)
        except HsException:
            return None

        # if time is in the future, assume they specified ticks instead of NS
        if daq_ticks < now_ticks:
            return daq_ticks

        print >>sys.stderr, "WARNING: %s %s is %s" % \
            (name, rawval, DAQTime.ticks_to_utc(daq_ticks))
        print >>sys.stderr, \
            "         Assuming ticks instead of nanoseconds"
        print >>sys.stderr

        return DAQTime.string_to_ticks(rawval)


    def main():
        ''''Process arguments'''
        start_ticks = None
        stop_ticks = None
        now_ticks = DAQTime.utc_to_ticks(datetime.datetime.now())

        p = argparse.ArgumentParser(epilog="HsGrabber submits a request to"
                                    " the HitSpool system", add_help=False)
        p.add_argument("-?", "--help", action="help",
                       help="show this help message and exit")

        add_arguments(p)

        args = p.parse_args()

        usage = False
        if not usage:
            tmptime = parse_time("Starting time", args.begin_time, now_ticks)
            if tmptime is None:
                print >>sys.stderr, "Invalid starting time \"%s\"" % \
                        args.begin_time
                usage = True

            start_ticks = tmptime

        if not usage:
            if args.end_time is not None:
                if args.duration is not None:
                    print >>sys.stderr, \
                        "Cannot specify -d(uration) and -e(nd_time) together"
                    usage = True
                else:
                    tmptime = parse_time("Stopping time", args.end_time,
                                         now_ticks)
                    if tmptime is None:
                        print >>sys.stderr, "Invalid ending time \"%s\"" % \
                            args.end_time
                        usage = True
                    stop_ticks = tmptime
            elif args.duration is not None:
                try:
                    dur = getDurationFromString(args.duration)
                    stop_ticks = start_ticks + int(dur * 1E10)
                except ValueError:
                    print >>sys.stderr, "Invalid duration \"%s\"" % \
                        args.duration
                    usage = True
            else:
                print >>sys.stderr, \
                    "Please specify either -d(uration) or -e(nd_time)"
                usage = True

        if usage:
            p.print_help()
            sys.exit(1)

        hsg = HsGrabber(is_test=args.is_test)

        hsg.init_logging(args.logfile, level=logging.INFO)

        # build 'hubs' string from arguments
        if args.hub is None or len(args.hub) == 0:
            hubs = None
        else:
            hubs = ",".join(args.hub)

        logging.info("This HsGrabber runs on: %s", hsg.fullhost)

        print "Request start: %s (%d ns)" % \
            (DAQTime.ticks_to_utc(start_ticks), start_ticks / 10)
        print "Request end: %s (%d ns)" % \
            (DAQTime.ticks_to_utc(stop_ticks), stop_ticks / 10)
        if hubs is not None:
            print "Hubs: %s" % (hubs, )

        # make sure rsync destination is fully specified
        (user, host, path) = hsg.split_rsync_path(args.copydir)
        destdir = "%s@%s:%s" % (user, host, path)

        if not hsg.send_alert(start_ticks, stop_ticks, destdir,
                              request_id=args.request_id,
                              username=args.username, prefix=args.prefix,
                              extract_hits=args.extract, hubs=hubs,
                              print_to_console=True):
            raise SystemExit(1)

        hsg.wait_for_response()

    main()
