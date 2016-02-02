#!/usr/bin/env python


import os

import HsBase
import HsUtil

from HsRSyncFiles import HsRSyncFiles


def add_arguments(parser):
    dflt_copydir = "%s@%s:%s" % (HsBase.DEFAULT_RSYNC_USER,
                                 HsBase.DEFAULT_RSYNC_HOST,
                                 HsBase.DEFAULT_COPY_PATH)

    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hsrsync.log")

    parser.add_argument("-b", "--begin", dest="begin_time",
                        help="Beginning UTC time (YYYY-mm-dd HH:MM:SS[.us])"
                        " or SnDAQ timestamp (ns from start of year)")
    parser.add_argument("-c", "--copydir", dest="copydir",
                        default=dflt_copydir,
                        help="rsync destination directory for hitspool files")
    parser.add_argument("-e", "--end", dest="end_time",
                        help="Ending UTC time (YYYY-mm-dd HH:MM:SS[.us])"
                        " or SnDAQ timestamp (ns from start of year)")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)


class HsRSync(HsRSyncFiles):
    '''
    For grabbing hs data from hubs (without explicit sndaq request).
    '''
    def __init__(self, host=None, is_test=False):
        super(HsRSync, self).__init__(host=host, is_test=is_test)

    def get_timetag_tuple(self, prefix, hs_copydir, starttime):
        # note starting slash!
        return "/HitSpoolData", starttime.strftime("%Y%m%d_%H%M%S")

    def set_default_copydir(self, hs_copydir):
        pass


if __name__ == "__main__":
    import argparse
    import logging
    import sys
    import traceback

    from HsException import HsException

    def main():
        request_start = 0
        request_begin_utc = None
        request_stop = 0
        request_end_utc = None

        desc = "Rsync-wrapper for transferring Hitspool data to" \
               " destination based on requested time range." \
               "  MUST BE RUN ON DATA SOURCE MACHINE."
        p = argparse.ArgumentParser(description=desc)

        add_arguments(p)

        args = p.parse_args()

        usage = False
        if not usage:
            try:
                # get both pDAQ timestamp (in 0.1 ns) and UTC datetime
                (alert_start_sn, alert_begin_utc) \
                    = HsUtil.parse_sntime(args.begin_time, is_sn_ns=False)
            except HsException, hsex:
                traceback.print_exc()
                usage = True

        if not usage:
            try:
                # get both pDAQ timestamp (in 0.1 ns) and UTC datetime
                (alert_stop_sn, alert_end_utc) \
                    = HsUtil.parse_sntime(args.end_time, is_sn_ns=False)
            except HsException, hsex:
                traceback.print_exc()
                usage = True

        if usage:
            p.print_help()
            sys.exit(1)

        hsr = HsRSync()

        hsr.init_logging(args.logfile, basename="hsrsync", basehost="testhub")

        logging.info("This HitSpool Data Grabbing Service runs on: %s",
                     hsr.shorthost)

        logging.info('')
        logging.info("NEW HS REQUEST ")
        logging.info('')
        logging.info("REQUESTED STRING: %s", hsr.shorthost)
        logging.info("HS REQUEST DATA BEGIN UTC time: %s", request_begin_utc)
        logging.info("HS REQUEST DATA END UTC time: %s", request_end_utc)
        logging.info("HS REQUEST DATA BEGIN DAQ time: %s", request_start)
        logging.info("HS REQUEST DATA END DAQ time: %s", request_stop)
        logging.info("HS Data Destination: %s", args.copydir)

        hsr.request_parser(None, request_begin_utc, request_end_utc,
                           args.copydir)

    main()
