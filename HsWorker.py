#!/usr/bin/env python


"""
#Hit Spool Worker to be run on hubs
#author: dheereman i3.hsinterface@gmail.com
#check out the icecube wiki page for instructions:
https://wiki.icecube.wisc.edu/index.php/HitSpool_Interface_Operation_Manual
"""
import json
import logging
import os
import random
import re
import signal
import sys
import time
import traceback

from datetime import datetime, timedelta

import HsUtil

from HsException import HsException
from HsRSyncFiles import HsRSyncFiles


# maximum number of seconds of data which can be requested
MAX_REQUEST_SECONDS = 610


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

    def __init__(self, progname, is_test=False):
        super(Worker, self).__init__(host=None, is_test=is_test)

        self.__copydir_dft = None
        self.__service = "HSiface"
        self.__varname = "%s@%s" % (progname, self.shorthost)

    def alert_parser(self, alert, logfile, sleep_secs=4):
        """
        Parse the Alert message for starttime, stoptime, sn-alert-time-stamp
        and directory where-to the data has to be copied.
        """

        utc_now = datetime.utcnow()
        jan1 = datetime(utc_now.year, 1, 1)

        # --- Parsing alert message JSON ----- :
        # XXX do json.loads() translation before calling alert_parser()
        try:
            alert_info = json.loads(alert)
        except:
            raise HsException("Cannot load \"%s\": %s" %
                              (alert, traceback.format_exc()))

        if alert_info is None:
            raise HsException("JSON message \"%s\" cannot be parsed" % alert)

        if len(alert_info) > 1:
            logging.error("Ignoring all but first of %d alerts",
                          len(alert_info))

        try:
            # timestamp in ns as a string
            sn_start, start_utc = HsUtil.parse_date(alert_info[0]['start'])
        except:
            raise HsException("Bad start time \"%s\": %s" %
                              (alert, traceback.format_exc()))

        try:
            # timestamp in ns as a string
            sn_stop, stop_utc = HsUtil.parse_date(alert_info[0]['stop'])
        except:
            raise HsException("Bad stop time \"%s\": %s" %
                              (alert, traceback.format_exc()))

        (sn_start, sn_stop, start_utc, stop_utc) \
            = HsUtil.fix_dates_or_timestamps(sn_start, sn_stop, start_utc,
                                             stop_utc, is_sn_ns=True)

        # should we extract only the matching hits to a new file?
        extract_hits = alert_info[0].has_key('extract')

        if not alert_info[0].has_key('copy') or alert_info[0]['copy'] is None:
            raise HsException("Copy directory must be set")

        # should be something like: pdaq@expcont:/mnt/data/pdaqlocal/HsDataCopy
        hs_user_machinedir = alert_info[0]['copy']
        logging.info("HS machinedir = %s", hs_user_machinedir)

        self.send_alert("received request at %s " % utc_now)

        logging.info("SN START [ns] = %d", sn_start)
        #sndaq time units are nanoseconds
        alertstart = jan1 + timedelta(seconds=sn_start*1.0E-9)
        logging.info("ALERTSTART: %s", alertstart)

        logging.info("SN STOP [ns] = %s", sn_stop)
        #sndaq time units are nanosecond
        alertstop = jan1 + timedelta(seconds=sn_stop*1.0E-9)
        logging.info("ALERTSTOP = %s", alertstop)

        # -----after correcting parsing, check for data range ------------#
        datarange = alertstop - alertstart
        datamax = timedelta(0, MAX_REQUEST_SECONDS)
        if datarange > datamax:
            range_secs = datarange.days * (24 * 60 * 60) + datarange.seconds + \
                         (datarange.microseconds / 1E6)
            max_secs = datamax.days * (24 * 60 * 60) + datamax.seconds + \
                       (datamax.microseconds / 1E6)
            errmsg = "Request for %.2fs exceeds limit of allowed data time" \
                     " range of %.2fs. Abort request..." % \
                     (range_secs, max_secs)
            self.send_alert("ERROR: " + errmsg)
            logging.error(errmsg)
            return None

        # send conversion of the timestamps:
        self.send_alert({
            "START": sn_start,
            "STOP": sn_stop,
            "UTCSTART": str(alertstart),
            "UTCSTOP": str(alertstop)
        })

        rtnval = self.request_parser(alertstart, alertstop, hs_user_machinedir,
                                     extract_hits=extract_hits,
                                     sender=self.sender,
                                     sleep_secs=sleep_secs,
                                     make_remote_dir=False)
        if rtnval is None:
            return None

        timetag = rtnval

        # -- also transmit the log file to the HitSpool copy directory:
        if self.is_cluster_sps or self.is_cluster_spts:
            logfiledir = "/mnt/data/pdaqlocal/HsInterface/logs/workerlogs"
            logtargetdir = self.extract_ssh_access(hs_user_machinedir) + ":" + \
                           logfiledir
        else:
            logfiledir = os.path.join(self.TEST_COPY_DIR, "logs")
            logtargetdir = logfiledir

        (log_rsync_out, log_rsync_err) \
            = self.rsync(logfile, logtargetdir, relative=False)
        if len(log_rsync_err) > 0:
            logging.error("failed to rsync logfile:\n%s", log_rsync_err)
            return None

        logging.info("logfile transmitted to copydir: %s", log_rsync_out)
        if self.sender is not None:
            log_json = {"hubname": self.shorthost,
                        "alertid": timetag,
                        "logfiledir": logfiledir,
                        "logfile_hsworker": logfile,
                        "msgtype": "log_done"}
            # XXX lose the json.dumps()
            self.sender.send_json(json.dumps(log_json))
            logging.info("sent json to HsSender: %s", log_json)

        return True

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
            #for the REAL interface
            # data goes ALWAYS to 2ndbuild with user pdaq:
            return 'pdaq@2ndbuild'

        return re.sub(r':/[\w+/]*', "", hs_user_machinedir)

    def get_copy_destination(self, hs_copydir, timetag_dir):
        return os.path.join(self.__copydir_dft, timetag_dir)

    def get_timetag_tuple(self, hs_copydir, starttime):
        if hs_copydir == self.__copydir_dft:
            #this is a SNDAQ request -> SNALERT tag
            # time window around trigger is [-30,+60], so add 30 seconds
            prefix = "SNALERT"
            plus30 = starttime + timedelta(0, 30)
            timetag = plus30.strftime("%Y%m%d_%H%M%S")
        elif 'hese' in hs_copydir:
            #this is a HESE request -> HESE tag
            prefix = "HESE"
            timetag = starttime.strftime("%Y%m%d_%H%M%S")
        else:
            prefix = "ANON"
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
        message = self.subscriber.recv()
        logging.info("HsWorker received alert message:\n"
                     "%s\nfrom Publisher", message)
        logging.info("start processing alert...")
        try:
            self.alert_parser(message, logfile)
        except KeyboardInterrupt:
            # allow keyboard interrupts to pass through
            raise
        except:
            logging.error("Request failed:\n%s\n%s" %
                          (message, traceback.format_exc()))

    def rsync_target(self, hs_user_machinedir, hs_ssh_access, timetag_dir,
                     hs_copydest):
        if self.is_cluster_sps or self.is_cluster_spts:
            return hs_ssh_access + '::hitspool/' + timetag_dir

        return self.__copydir_dft + timetag_dir

    def send_alert(self, value, prio=None):
        if self.i3socket is not None:
            alert = {}
            alert["service"] = self.__service
            alert["varname"] = self.__varname
            alert["value"] = value
            if prio is not None:
                alert["prio"] = prio
            self.i3socket.send_json(alert)

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
                         " %s on 2ndbuild", hs_copydir)


if __name__ == '__main__':
    import getopt


    def process_args():
        copydir = None
        hubdir = None
        logfile = None

        usage = False
        try:
            opts, _ = getopt.getopt(sys.argv[1:], 'C:H:hl:',
                                    ['copydir', 'hubdir', 'help', 'logfile'])
        except getopt.GetoptError, err:
            print >>sys.stderr, str(err)
            opts = []
            usage = True

        for opt, arg in opts:
            if opt == '-C':
                copydir = str(arg)
            elif opt == '-H':
                hubdir = str(arg)
            elif opt == '-l':
                logfile = str(arg)
            elif opt == '-h' or opt == '--help':
                usage = True

        if not usage:
            if copydir is not None and not os.path.exists(copydir):
                print >>sys.stderr, \
                    "Copy directory \"%s\" does not exist" % copydir
                usage = True
            elif hubdir is not None and not os.path.exists(hubdir):
                print >>sys.stderr, \
                    "Hub directory \"%s\" does not exist" % hubdir
                usage = True

        if usage:
            print >>sys.stderr, "usage :: HsWorker.py [-l logfile]"
            raise SystemExit(1)

        return (copydir, hubdir, logfile)

    def main():
        (copydir, hubdir, logfile) = process_args()

        worker = Worker("HsWorker")

        #handler is called when SIGTERM is called (via pkill)
        signal.signal(signal.SIGTERM, worker.handler)

        # override some defaults (generally only used for debugging)
        if copydir is not None:
            worker.TEST_COPY_DIR = copydir
        if hubdir is not None:
            worker.TEST_HUB_DIR = hubdir

        if logfile is None:
            if worker.is_cluster_sps or worker.is_cluster_spts:
                logdir = "/mnt/data/pdaqlocal/HsInterface/logs"
            else:
                logdir = os.path.join(worker.TEST_HUB_DIR, "logs")
            logfile = os.path.join(logdir,
                                   "hsworker_%s.log" % worker.shorthost)

        worker.init_logging(logfile)

        logging.info("this Worker runs on: %s", worker.shorthost)

        while True:
            try:
                worker.mainloop(logfile)
            except KeyboardInterrupt:
                logging.warning("interruption received, shutting down...")
                raise SystemExit(0)

    main()
