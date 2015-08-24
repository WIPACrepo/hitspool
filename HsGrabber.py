#!/usr/bin/env python
#
#
#Hit Spool Grabber to be run on access
#author: dheereman
#

import json
import logging
import sys
import zmq

import HsBase

from HsConstants import ALERT_PORT, I3LIVE_PORT
from HsException import HsException


STD_SECONDS = 95
MAX_SECONDS = 610


class HsGrabber(HsBase.HsBase):
    '''
    Grab hs data from hubs independently (without sndaq providing alert).
    HsGrabber sends json to HsPublisher to grab hs data from hubs.
    Uses the Hs Interface infrastructure.
    '''

    def __init__(self):
        super(HsGrabber, self).__init__()

        if self.is_cluster_local():
            expcont = "localhost"
        else:
            expcont = "expcont"

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

    def create_grabber(self, host):
        # Socket to send alert message to HsPublisher
        sock = self.__context.socket(zmq.REQ)
        sock.connect("tcp://%s:%d" % (host, ALERT_PORT))
        return sock

    def create_poller(self, grabber):
        # needed for handling timeout if Publisher doesnt answer
        sock = zmq.Poller()
        sock.register(grabber, zmq.POLLIN)
        return sock

    def create_i3socket(self, host):
        # Socket for I3Live on expcont
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, I3LIVE_PORT))
        return sock

    def grabber(self):
        return self.__grabber

    def i3socket(self):
        return self.__i3socket

    def poller(self):
        return self.__poller

    def send_alert(self, timeout, alert_start_sn, alert_stop_sn, copydir,
                   extract_hits=False, print_dots=True):
        '''
        Send request to Publisher and wait for response
        '''

        # -- checking data range ---#
        secrange = (alert_stop_sn - alert_start_sn) / 1E9
        logging.info("requesting %.2f seconds of HS data", secrange)

        logging.info("(maxrange: %d seconds)", MAX_SECONDS)

        should_continue = False
        if secrange < 0:
            logging.error("Requesting negative time range (%.2f)."
                          " Try another time window.", secrange)
        elif secrange <= STD_SECONDS:
            # standard case
            should_continue = True
        elif secrange > MAX_SECONDS:
            logging.error("Request for %.2f seconds is too huge.\n"
                          "HsWorker processes request only up to %d sec.\n"
                          "Try a smaller time window.", secrange, MAX_SECONDS)
        else:
            answer = raw_input("Warning: You are requesting more HS data"
                               " than usual (%d sec). Sure you want to"
                               "  proceed? [y/n] : " % STD_SECONDS)
            if answer in ["Yes", "yes", "y", "Y"]:
                should_continue = True
            else:
                logging.info("HS data request stopped."
                             " Try a smaller time window.")

        if should_continue:
            alert = {"start": alert_start_sn,
                     "stop": alert_stop_sn,
                     "copy": copydir}

            if extract_hits:
                alert["extract"] = True

            self.__grabber.send(json.dumps(alert))
            logging.info("HsGrabber sent Request")

        #--- waiting for answer from Publisher ---#
        count = 0
        while should_continue:
            result = self.__poller.poll(timeout * 100)
            socks = dict(result) # poller takes msec argument
            count += 1
            if count > timeout:
                logging.error("no connection to expcont's HsPublisher"
                              " within %s seconds.\nAbort request.", timeout)
                logging.info("Debugging hints:")
                logging.info("""
                1. check HsPublisher's logfile on EXPCONT:
                /mnt/data/pdaqlocal/HsInterface/trunk/hspublisher_stdout_stderr.log
                and
                /mnt/data/pdaqlocal/HsInterface/logs/hspublisher_expcont.log

                2. restart HsPublisher.py via fabric on ACCESS:
                fab -f /home/pdaq/HsInterface/trunk/fabfile.py hs_stop_pub
                fab -f /home/pdaq/HsInterface/trunk/fabfile.py hs_start_pub_bkg
                """)

                logging.info(">>>>>> Hit CTRL + C for exiting HsGrabber now ...")
                raise SystemExit(0)

            if self.__grabber in socks and socks[self.__grabber] == zmq.POLLIN:
                message = self.__grabber.recv()
                logging.info("received control command: %s", message)
                if message == "DONE\0":
                    self.__i3socket.send_json({"service": "HSiface",
                                               "varname": "HsGrabber",
                                               "value": "pushed request"})
                    logging.info("Received DONE. Not waiting for more messages."
                                 " Shutting down...")
                    should_continue = False
                    raise SystemExit(0)
            elif print_dots:
                print ".",
                sys.stdout.flush()


if __name__ == "__main__":
    import getopt

    from HsUtil import fix_dates_or_timestamps, parse_date


    def process_args():
        ''''Process arguments'''
        alert_start_sn = 0
        alert_begin_utc = None
        alert_stop_sn = 0
        alert_end_utc = None
        copydir = None
        logfile = None
        extract_hits = False

        ##take arguments from command line and check for correct input
        usage = False
        try:
            opts, _ = getopt.getopt(sys.argv[1:], 'b:c:e:hl:x',
                                    ['begin', 'copydir', 'end', 'help',
                                     'logfile', 'extract'])
        except getopt.GetoptError, err:
            print >>sys.stderr, str(err)
            opts = []
            usage = True

        for opt, arg in opts:
            if opt == '-b':
                try:
                    (alert_start_sn, alert_begin_utc) = parse_date(arg)
                except HsException, hsex:
                    print >>sys.stderr, str(hsex)
                    usage = True
            elif opt == '-e':
                try:
                    (alert_stop_sn, alert_end_utc) = parse_date(arg)
                except HsException, hsex:
                    print >>sys.stderr, str(hsex)
                    usage = True
            elif opt == '-c':
                copydir = str(arg)
            elif opt == '-l':
                logfile = str(arg)
            elif opt == '-x':
                extract_hits = True
            elif opt == '-h' or opt == '--help':
                usage = True

        if not usage:
            # convert times to SN timestamps and/or vice versa
            (alert_start_sn, alert_stop_sn, alert_begin_utc, alert_end_utc) \
                = fix_dates_or_timestamps(alert_start_sn, alert_stop_sn,
                                          alert_begin_utc, alert_end_utc,
                                          is_sn_ns=True)

            if alert_begin_utc is None:
                print >>sys.stderr, "Please specify start time using '-b'"
                usage = True
            elif alert_end_utc is None:
                print >>sys.stderr, "Please specify end time using '-e'"
                usage = True

        if usage:
            print >>sys.stderr, """
usage :: HsGrabber.py [options]
  -b  |  [b]egin of data: "YYYY-mm-dd HH:MM:SS.[us]"
      |    or SNDAQ timestamp [ns from beginning of the year]
  -e  |  [e]nd of data "YYYY-mm-dd HH:MM:SS.[us]"
      |    or SNDAQ timestamp [ns from beginning of the year]
  -c  |  [c]opydir on 2NDBUILD as user PDAQ
      |    default is "pdaq@2ndbuild:/mnt/data/pdaqlocal/HsDataCopy"
  -l  |  logfile
      |    e.g. /mnt/data/pdaqlocal/HsInterface/logs/hsgrabber.log

HsGrabber reads UTC timestamps or SNDAQ timestamps.
It sends SNDAQ timestamps to HsInterface (HsPublisher).
"""
            raise SystemExit(1)

        return (alert_start_sn, alert_begin_utc, alert_stop_sn, alert_end_utc,
                copydir, logfile, extract_hits)

    def main():
        (alert_start_sn, alert_begin_utc, alert_stop_sn, alert_end_utc, copydir,
         logfile, extract_hits) = process_args()

        hsg = HsGrabber()

        hsg.init_logging(logfile, level=logging.INFO)

        logging.info("This HsGrabber runs on: %s", hsg.fullhost())

        if copydir is None:
            if hsg.is_cluster_local():
                import getpass

                sec_bldr = "localhost"
                user = getpass.getuser()
            else:
                sec_bldr = "2ndbuild"
                user = "pdaq"

            copydir = "%s@%s:/mnt/data/pdaqlocal/HsDataCopy" % (user, sec_bldr)

        timeout = 10 # number of tries to wait for answer from Publisher

        logging.info("HS DATA BEGIN UTC time: %s", alert_begin_utc)
        logging.info("HS DATA END UTC time: %s", alert_end_utc)
        logging.info("HS DATA BEGIN SNDAQ time: %s", alert_start_sn)
        logging.info("HS DATA END SNDAQ time: %s", alert_stop_sn)

        hsg.send_alert(timeout, alert_start_sn, alert_stop_sn, copydir,
                       extract_hits=extract_hits)

    main()
