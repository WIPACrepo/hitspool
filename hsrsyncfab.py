#!/usr/bin/env python

import re
from fabric.api import *

from HsRSyncFiles import HsRSyncFiles


class HsRSyncFab(HsRSyncFiles):
    def __init__(self, host=None, is_test=False):
        super(HsRSyncFab, self).__init__(host=host, is_test=is_test)

    def extract_ssh_access(self, hs_user_machinedir):
        if self.is_cluster_sps or self.is_cluster_spts:
            #for the REAL interface
            return re.sub(r':/\w+/\w+/\w+/\w+/', "", hs_user_machinedir)

        return re.sub(r':/[\w+/]*', "", hs_user_machinedir)

    def get_timetag_tuple(self, hs_copydir, starttime):
        return "HitSpoolData", starttime.strftime("%Y%m%d_%H%M%S")

    def mkdir(self, host, new_dir):
        fabcmd = 'mkdir -p %s' % new_dir
        with settings(host_string=host):
            if self.is_cluster_sps or self.is_cluster_spts:
                run(fabcmd)
            else:
                local(fabcmd)

    def set_default_copydir(self, hs_copydir):
        pass


if __name__ == "__main__":
    import getopt
    import logging
    import os
    import sys

    from HsException import HsException
    from HsUtil import fix_dates_or_timestamps, parse_sntime


    def process_args():
        request_start = 0
        request_stop = 0
        request_begin_utc = None
        request_end_utc = None
        copydir = None
        logfile = None
        myhost = None

        ##take arguments from command line and check for correct input
        usage = False
        try:
            opts, _ = getopt.getopt(sys.argv[1:], 'b:d:e:hl:s:',
                                    ['begin', 'destination', 'end', 'help',
                                     'logfile', 'source'])
        except getopt.GetoptError, err:
            print >>sys.stderr, str(err)
            opts = []
            usage = True

        for opt, arg in opts:
            if opt == '-b':
                try:
                    (request_start, request_begin_utc) = parse_sntime(arg)
                except HsException, hsex:
                    print >>sys.stderr, str(hsex)
                    usage = True
            elif opt == '-e':
                try:
                    (request_stop, request_end_utc) = parse_sntime(arg)
                except HsException, hsex:
                    print >>sys.stderr, str(hsex)
                    usage = True
            elif opt == '-d':
                copydir = str(arg)
            elif opt == '-l':
                logfile = str(arg)
            elif opt == '-s':
                myhost = str(arg)
            elif opt == '-h' or opt == '--help':
                usage = True

        if not usage:
            # convert times to SN timestamps and/or vice versa
            (request_start, request_stop, request_begin_utc, request_end_utc) \
                = fix_dates_or_timestamps(request_start, request_stop,
                                          request_begin_utc, request_end_utc)

            if request_begin_utc is None:
                print >>sys.stderr, "Please specify start time using '-b'"
                usage = True
            elif request_end_utc is None:
                print >>sys.stderr, "Please specify end time using '-e'"
                usage = True

        if usage:
            print >>sys.stderr, """
usage: hsrsyncfab.py [options]
  -b  |  begin of data: "YYYY-mm-dd HH:MM:SS.[us]"
      |    or DAQ timestamp [0.1 ns from beginning of the year]
  -e  |  end of data "YYYY-mm-dd HH:MM:SS.[us]"
      |    or DAQ timestamp [0.1 ns from beginning of the year]
  -s  |  source hub: ithub[1-11] or ichub[1-86]
  -d  |  destination e.g. "pdaq@2ndbuild:/mnt/data/pdaqlocal/HsDataCopy/test"

To be run on sps-access
HsRSyncFab reads UTC timestamps or DAQ timestamps, calculates the requested
hitspool file indexes and ships the data to copydir.
"""
            raise SystemExit(1)

        return (request_start, request_stop, request_begin_utc,
                request_end_utc, copydir, logfile, myhost)

    def main():
        (request_start, request_stop, request_begin_utc, request_end_utc,
         copydir, logfile, myhost) = process_args()

        hsr = HsRSyncFab(host=myhost)

        if logfile is None:
            if hsr.is_cluster_sps or hsr.is_cluster_spts:
                logdir = "/home/pdaq/HsInterface/logs"
            else:
                logfile = os.path.join(hsr.TEST_HUB_DIR, "logs")
            logfile = os.path.join(logdir, "hsrsyncfab.log")

            hsr.init_logging(logfile)

        logging.info('')
        logging.info("NEW HS REQUEST ")
        logging.info('')
        logging.info("REQUESTED STRING: %s", hsr.fullhost)
        logging.info("HS REQUEST DATA BEGIN UTC time: %s", request_begin_utc)
        logging.info("HS REQUEST DATA END UTC time: %s", request_end_utc)
        logging.info("HS REQUEST DATA BEGIN DAQ time: %s", request_start)
        logging.info("HS REQUEST DATA END DAQ time: %s", request_stop)
        logging.info("HS Data Destination: %s", copydir)

        hsr.request_parser(request_begin_utc, request_end_utc, copydir,
                           make_remote_dir=True)

    main()
