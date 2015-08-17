#!/usr/bin/env python


import os

from HsRSyncFiles import HsRSyncFiles


class HsRSync(HsRSyncFiles):
    '''
    For grabbing hs data from hubs (without explicit sndaq request).
    '''
    def __init__(self, host=None, is_test=False):
        super(HsRSync, self).__init__(host=host, is_test=is_test)

    def get_timetag_tuple(self, hs_copydir, starttime):
        # note starting slash!
        return "/HitSpoolData", starttime.strftime("%Y%m%d_%H%M%S")

    def set_default_copydir(self, hs_copydir):
        pass


if __name__ == "__main__":
    import getopt
    import logging
    import sys

    from HsException import HsException
    from HsUtil import fix_dates_or_timestamps, parse_date


    def process_args():
        request_start = 0
        request_begin_utc = None
        request_stop = 0
        request_end_utc = None
        copydir = None
        logfile = None

        ##take arguments from command line and check for correct input
        usage = False
        try:
            opts, _ = getopt.getopt(sys.argv[1:], 'b:c:e:hl:',
                                    ['begin', 'copydir', 'end', 'help',
                                     'logfile'])
        except getopt.GetoptError, err:
            print >>sys.stderr, str(err)
            opts = []
            usage = True

        for opt, arg in opts:
            if opt == '-b':
                try:
                    (request_start, request_begin_utc) = parse_date(arg)
                except HsException, hsex:
                    print >>sys.stderr, str(hsex)
                    usage = True
            elif opt == '-e':
                try:
                    (request_stop, request_end_utc) = parse_date(arg)
                except HsException, hsex:
                    print >>sys.stderr, str(hsex)
                    usage = True
            elif opt == '-c':
                copydir = str(arg)
            elif opt == '-l':
                logfile = str(arg)
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
            elif copydir is None:
                print >>sys.stderr, "Please specify copy directory using '-c'"
                usage = True

        if usage:
            print >>sys.stderr, """
usage :: hsrsync.py [options]
  -b  |  begin of data: "YYYY-mm-dd HH:MM:SS.[us]"
      |    or DAQ timestamp [0.1 ns from beginning of the year]
  -e  |  end of data "YYYY-mm-dd HH:MM:SS.[us]"
      |    or DAQ timestamp [0.1 ns from beginning of the year]
  -c  | copydir e.g. "pdaq@2ndbuild:/mnt/data/pdaqlocal/HsDataCopy"
  -l  | name of log file

Rsync-wrapper for transferring Hitspool data to destination based on
requested time range.  MUST BE RUN ON DATA SOURCE MACHINE.
"""
            raise SystemExit(1)

        return (request_start, request_begin_utc, request_stop,
                request_end_utc, copydir, logfile)

    def main():
        (request_start, request_begin_utc, request_stop, request_end_utc,
         copydir, logfile) = process_args()

        hsr = HsRSync()

        if logfile is None:
            if hsr.is_cluster_sps() or hsr.is_cluster_spts():
                logdir = "/mnt/data/pdaqlocal/HsInterface/logs"
            else:
                logdir = os.path.join(hsr.TEST_HUB_DIR, "logs")
            logfile = os.path.join(logdir, "hsrsync_%s.log" % hsr.shorthost())

        hsr.init_logging(logfile)

        logging.info("This HitSpool Data Grabbing Service runs on: %s",
                     hsr.shorthost())

        logging.info('')
        logging.info("NEW HS REQUEST ")
        logging.info('')
        logging.info("REQUESTED STRING: %s", hsr.shorthost())
        logging.info("HS REQUEST DATA BEGIN UTC time: %s", request_begin_utc)
        logging.info("HS REQUEST DATA END UTC time: %s", request_end_utc)
        logging.info("HS REQUEST DATA BEGIN DAQ time: %s", request_start)
        logging.info("HS REQUEST DATA END DAQ time: %s", request_stop)
        logging.info("HS Data Destination: %s", copydir)

        hsr.request_parser(request_begin_utc, request_end_utc, copydir)


    main()
