#!/usr/bin/env python


"""
#Hit Spool Worker to be run on hubs
#author: dheereman i3.hsinterface@gmail.com
#check out the icecube wiki page for instructions:
https://wiki.icecube.wisc.edu/index.php/HitSpool_Interface_Operation_Manual
"""
import logging
import os
import random
import re
import signal
import time
import traceback
import zmq

from datetime import datetime, timedelta
from zmq import ZMQError

import HsMessage
import HsUtil

from HsBase import HsBase
from HsException import HsException
from HsPrefix import HsPrefix
from HsRSyncFiles import HsRSyncFiles


def add_arguments(parser):
    dflt_copydir = "%s@%s:%s" % (HsBase.DEFAULT_RSYNC_USER,
                                 HsBase.DEFAULT_RSYNC_HOST,
                                 HsBase.DEFAULT_COPY_PATH)

    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hsworker.log")

    parser.add_argument("-C", "--copydir", dest="copydir",
                        default=os.path.join(dflt_copydir, "test"),
                        help="Final directory on 2ndbuild as user pdaq")
    parser.add_argument("-H", "--hostname", dest="hostname",
                        default=None,
                        help="Name of this host")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-R", "--hubroot", dest="hubroot",
                        default=os.path.join(dflt_copydir, "test"),
                        help="Final directory on 2ndbuild as user pdaq")


class Worker(HsRSyncFiles):
    """
    "sndaq/HsGrabber"  "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        --------------
    | REQ     | <----->| REP     |         | IcHub n |        | 2ndbuild    |
    -----------        | PUB     | ------> | SUB   n |        | PUSH(13live)|
                       ----------          | PUSH    | ---->  | PULL        |
                                            ---------         --------------
    HsWorker.py of the HitSpool Interface.
    This class
    1. analyzes the alert message
    2. looks for the requested files / directory
    3. copies them over to the requested directory specified in the message.
    4. writes a short report about was has been done.
    """

    # location of copy directory used for testing HsInterface
    TEST_COPY_DIR = "/home/david/data/HitSpool/copytest"

    # should worker logfiles be rsynced to 2ndbuild after every request?
    RSYNC_LOGFILE = False

    def __init__(self, progname, host=None, is_test=False):
        super(Worker, self).__init__(host=host, is_test=is_test)

        self.__copydir_dft = None
        self.__service = "HSiface"
        self.__varname = "%s@%s" % (progname, self.shorthost)

    def alert_parser(self, req, logfile, sleep_secs=4):
        """
        Parse the Alert message for starttime, stoptime, sn-alert-time-stamp
        and directory where-to the data has to be copied.
        """

        try:
            # timestamp in ns as a string
            sn_start, start_utc = HsUtil.parse_sntime(req.start_time)
        except:
            raise HsException("Bad start time \"%s\": %s" %
                              (req.start_time, traceback.format_exc()))

        (sn_start, start_utc) \
            = HsUtil.fix_date_or_timestamp(sn_start, start_utc, is_sn_ns=True)

        try:
            # timestamp in ns as a string
            sn_stop, stop_utc = HsUtil.parse_sntime(req.stop_time)
        except:
            raise HsException("Bad stop time in %s: %s" %
                              (req.stop_time, traceback.format_exc()))

        (sn_stop, stop_utc) \
            = HsUtil.fix_date_or_timestamp(sn_stop, stop_utc, is_sn_ns=True)

        # should we extract only the matching hits to a new file?
        extract_hits = req.extract

        # should be something like: pdaq@expcont:/mnt/data/pdaqlocal/HsDataCopy
        hs_user_machinedir = req.destination_dir
        logging.info("HS machinedir = %s", hs_user_machinedir)

        # initialize a couple of time values
        utc_now = datetime.utcnow()
        jan1 = datetime(utc_now.year, 1, 1)

        logging.info("SN START [ns] = %d", sn_start)
        # sndaq time units are nanoseconds
        alertstart = jan1 + timedelta(seconds=sn_start*1.0E-9)
        logging.info("ALERTSTART: %s", alertstart)

        logging.info("SN STOP [ns] = %s", sn_stop)
        # sndaq time units are nanosecond
        alertstop = jan1 + timedelta(seconds=sn_stop*1.0E-9)
        logging.info("ALERTSTOP = %s", alertstop)

        # -----after correcting parsing, check for data range ------------#
        datarange = alertstop - alertstart
        datamax = timedelta(0, self.MAX_REQUEST_SECONDS)
        if datarange > datamax:
            range_secs = datarange.days * (24 * 60 * 60) + \
                datarange.seconds + (datarange.microseconds / 1E6)
            max_secs = datamax.days * (24 * 60 * 60) + datamax.seconds + \
                (datamax.microseconds / 1E6)
            errmsg = "Request for %.2fs exceeds limit of allowed data time" \
                     " range of %.2fs. Abort request..." % \
                     (range_secs, max_secs)
            self.send_alert("ERROR: " + errmsg)
            logging.error(errmsg)
            return None

        result = self.request_parser(req.prefix, alertstart, alertstop,
                                     hs_user_machinedir,
                                     extract_hits=extract_hits,
                                     sender=self.sender,
                                     sleep_secs=sleep_secs,
                                     make_remote_dir=False)
        if result is None:
            logging.error("Request failed")
            return None

        if self.RSYNC_LOGFILE:
            # -- also transmit the log file to the HitSpool copy directory:
            if self.is_cluster_sps or self.is_cluster_spts:
                logfiledir = os.path.join(HsBase.DEFAULT_LOG_PATH,
                                          "workerlogs")
                user_host, _ \
                    = HsUtil.split_rsync_host_and_path(hs_user_machinedir)
                logtargetdir = "%s@%s:%s" % (self.rsync_user, self.rsync_host,
                                             logfiledir)
            else:
                logfiledir = os.path.join(self.TEST_COPY_DIR, "logs")
                logtargetdir = logfiledir

            try:
                outlines = self.rsync((logfile, ), logtargetdir,
                                      relative=False)
                logging.info("logfile transmitted to copydir: %s", outlines)
            except HsException:
                logging.exception("Logfile RSync failed")
                # rsync of logfile should not cause request to fail

        return result

    def add_rsync_delay(self, sleep_secs):
        """
        Add random Sleep time window
        Necessary in order to stretch time window of rsync requests.
        Simultaneously rsyncing from 97 hubs caused issues in the past
        """
        if sleep_secs > 0:
            wait_time = random.uniform(1, 3)
            logging.info("wait with the rsync request for some seconds: %d",
                         wait_time)
            time.sleep(wait_time)

    def extract_ssh_access(self, hs_user_machinedir):
        if self.is_cluster_sps or self.is_cluster_spts:
            # for the REAL interface
            #  data ALWAYS goes to the default user/host target
            return '%s@%s' % (self.rsync_user, self.rsync_host)

        return re.sub(r':/[\w+/]*', "", hs_user_machinedir)

    def get_copy_destination(self, hs_copydir, timetag_dir):
        return os.path.join(self.__copydir_dft, timetag_dir)

    def get_timetag_tuple(self, prefix, hs_copydir, starttime):
        if prefix == HsPrefix.SNALERT:
            # this is a SNDAQ request -> SNALERT tag
            # time window around trigger is [-30,+60], so add 30 seconds
            plus30 = starttime + timedelta(0, 30)
            timetag = plus30.strftime("%Y%m%d_%H%M%S")
        else:
            timetag = starttime.strftime("%Y%m%d_%H%M%S")

        return prefix, timetag

    # --- Clean exit when program is terminated from outside (via pkill) ---#
    def handler(self, signum, _):
        logging.warning("Signal Handler called with signal %s", signum)
        logging.warning("Shutting down...\n")

        if self.i3socket is not None:
            i3live_dict = {}
            i3live_dict["service"] = "HSiface"
            i3live_dict["varname"] = "HsWorker@%s" % self.shorthost
            i3live_dict["value"] = "INFO: SHUT DOWN called by external signal."
            self.i3socket.send_json(i3live_dict)

            i3live_dict = {}
            i3live_dict["service"] = "HSiface"
            i3live_dict["varname"] = "HsWorker@%s" % self.shorthost
            i3live_dict["value"] = "STOPPED"
            self.i3socket.send_json(i3live_dict)

        self.close_all()

        raise SystemExit(0)

    def mainloop(self, logfile):
        if self.subscriber is None:
            raise Exception("Subscriber has not been initialized")

        logging.info("ready for new alert...")

        try:
            req = self.receive_request(self.subscriber)
        except KeyboardInterrupt:
            raise
        except ZMQError:
            raise
        except:
            logging.exception("Cannot read request")
            return False

        logging.info("HsWorker received request:\n"
                     "%s\nfrom Publisher", req)

        HsMessage.send(self.sender, HsMessage.MESSAGE_STARTED, req.request_id,
                       req.username, req.start_time, req.stop_time,
                       req.copy_dir, req.destination_dir, req.prefix,
                       req.extract, self.shorthost)

        try:
            rsyncdir = self.alert_parser(req, logfile)
        except:
            logging.exception("Cannot process request \"%s\"", req)
            rsyncdir = None

        if rsyncdir is not None:
            msgtype = HsMessage.MESSAGE_DONE
        else:
            msgtype = HsMessage.MESSAGE_FAILED

        # send final destination as a simple path
        _, destdir = HsUtil.split_rsync_host_and_path(req.destination_dir)

        HsMessage.send(self.sender, msgtype, req.request_id, req.username,
                       req.start_time, req.stop_time, rsyncdir, destdir,
                       req.prefix, req.extract, self.shorthost)

    def receive_request(self, sock):
        req_dict = sock.recv_json()
        if not isinstance(req_dict, dict):
            raise HsException("JSON message should be a dict: \"%s\"<%s>" %
                              (req_dict, type(req_dict)))

        # ensure 'extract' field is present and is a boolean value
        req_dict["extract"] = "extract" in req_dict and \
            req_dict["extract"] is True

        alert_flds = ("request_id", "username", "start_time", "stop_time",
                      "destination_dir", "prefix", "extract")

        return HsUtil.dict_to_object(req_dict, alert_flds, 'WorkerRequest')

    def rsync_target(self, hs_user_machinedir, timetag_dir, hs_copydest):
        if self.is_cluster_sps or self.is_cluster_spts:
            return '%s@%s::hitspool/%s' % (self.rsync_user, self.rsync_host,
                                           timetag_dir)

        return os.path.join(self.__copydir_dft, timetag_dir)

    def set_default_copydir(self, hs_copydir):
        '''Set default copy destination'''
        if self.is_cluster_sps or self.is_cluster_spts:
            self.__copydir_dft = "/mnt/data/pdaqlocal/HsDataCopy"
        else:
            self.__copydir_dft = self.TEST_COPY_DIR

        if self.__copydir_dft != hs_copydir:
            logging.warning("Requested HS data copy destination differs"
                            " from default!")
            logging.warning("data will be sent to default destination: %s",
                            self.__copydir_dft)
            logging.info("HsSender will redirect it later on to:"
                         " %s on %s", hs_copydir, HsBase.DEFAULT_RSYNC_HOST)


if __name__ == '__main__':
    import argparse
    import sys

    def main():
        p = argparse.ArgumentParser()

        add_arguments(p)

        args = p.parse_args()

        usage = False
        if not usage:
            if args.copydir is not None and not os.path.exists(args.copydir):
                print >>sys.stderr, \
                    "Copy directory \"%s\" does not exist" % args.copydir
                usage = True
            elif args.hubroot is not None and not os.path.exists(args.hubroot):
                print >>sys.stderr, \
                    "Hub directory \"%s\" does not exist" % args.hubroot
                usage = True

        worker = Worker("HsWorker", host=args.hostname)

        # override some defaults (generally only used for debugging)
        if args.copydir is not None:
            worker.TEST_COPY_DIR = args.copydir
        if args.hubroot is not None:
            worker.TEST_HUB_DIR = args.hubroot

        # handler is called when SIGTERM is called (via pkill)
        signal.signal(signal.SIGTERM, worker.handler)

        logfile = worker.init_logging(args.logfile, basename="hsworker",
                                      basehost="testhub")

        logging.info("this Worker runs on: %s", worker.shorthost)

        while True:
            try:
                worker.mainloop(logfile)
            except SystemExit:
                raise
            except KeyboardInterrupt:
                logging.warning("Interruption received, shutting down...")
                raise SystemExit(0)
            except zmq.ZMQError:
                logging.exception("ZMQ error received, shutting down...")
                raise SystemExit(1)
            except:
                logging.exception("Caught exception, continuing")

    main()
