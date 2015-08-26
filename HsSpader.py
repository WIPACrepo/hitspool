#!/usr/bin/env python
#
#Hit Spool SPADE-ing to be run on access
#author: dheereman
#
import glob
import logging
import os

import HsBase


class HsSpader(HsBase.HsBase):
    '''
    Copy the HS data to the SPADE queue on sps-2ndbuild.
    Create tarballs and semafore files for each hub's data
    and move it to SPADE directory
    '''

    def __init__(self):
        super(HsSpader, self).__init__()

    def find_matching_files(self, basedir, alertname):
        "This wrapper exists only so tests can override it"
        return glob.glob(os.path.join(basedir, "*" + alertname + "*"))

    def makedirs(self, path):
        "This wrapper exists only so tests can override it"
        if not os.path.exists(path):
            os.makedirs(path)

    def spade_pickup_data(self, basedir, alertname, spadedir):
        '''
        Create subdir for folder related to the alert
        Move folder in subdir
        tar & bzip folder
        create semaphore file for folder
        move .sem & .dat.tar file in Spade directory
        '''
        logging.info("Preparation for SPADE Pickup of HS data started"
                     " manually via HsSpader...")

        # create spadedir if it doen't exist
        self.makedirs(spadedir)

        datalist = self.find_matching_files(basedir, alertname)
        if len(datalist) == 0:
            logging.info("no HS data found for this alert time pattern.")
            return

        logging.info("found HS data:\n%s", datalist)

        datalistlocal = [os.path.basename(s) for s in datalist]

        hubnamelist = ["ichub%02d" %i for i in xrange(1, 87)] + \
                      ["ithub%02d" %i for i in xrange(1, 12)]
        for hub in hubnamelist:
            tarname = "HS_SNALERT_" + alertname + "_" + hub + ".dat.tar.bz2"
            semname = "HS_SNALERT_" + alertname + "_" + hub + ".sem"
            data = [s for s in datalistlocal if hub in s]
            if len(data) != 1:
                logging.info("no or ambiguous HS data found"
                             " for %s in this directory.", hub)
                continue

            dataname = data[0]
            logging.info("data: %s will be tarred to: %s", dataname, tarname)

            if not self.queue_for_spade(basedir, dataname, spadedir, tarname,
                                        semname):
                logging.info("Please put the data manually in"
                             " the SPADE directory")
                continue

            logging.info("Preparation for SPADE Pickup of %s DONE", tarname)

if __name__ == "__main__":
    import getopt
    import sys


    def process_args():
        indir = None
        pattern = None
        outdir = None
        logfile = None

        #take arguments from command line and check for correct input
        usage = False
        try:
            opts, _ = getopt.getopt(sys.argv[1:], 'hi:l:o:p:',
                                    ['help', 'indir=', 'log=', 'outdir=',
                                     'pattern='])
        except getopt.GetoptError, err:
            print >>sys.stderr, str(err)
            opts = []
            usage = True

        for opt, arg in opts:
            if opt == "-i" or opt == "--indir":
                indir = str(arg)
            elif opt == "-p" or opt == "--pattern":
                pattern = str(arg)
            elif opt == "-o" or opt == "--outdir":
                outdir = str(arg)
            elif opt == "-l" or opt == "--log":
                logfile = str(arg)
            elif opt == '-h' or opt == '--help':
                usage = True

        if not usage:
            if indir is None:
                print >>sys.stderr, "Please specify input directory with \"-i\""
                usage = True
            elif pattern is None:
                print >>sys.stderr, "Please specify time pattern with \"-p\""
                usage = True
            elif outdir is None:
                print >>sys.stderr, "Please specify output directory with \"-o\""
                usage = True

        if usage:
            print >>sys.stderr, """
usage :: HsSpader.py [options]
  -i  |  Input dir where hs data is located
      |    e.g. /mnt/data/pdaqlocal/HsDataCopy/
  -p  |  time pattern of HS data alert:<yyyymmdd>_<hhmmss>
      |    e.g. 20131101_045126
  -o  |  Output directory
      |    default is /mnt/data/HitSpool/
  -l  |  logfile
      |    e.g. /mnt/data/pdaqlocal/HsInterface/logs/hssender.log
"""
            raise SystemExit(1)

        return indir, pattern, outdir, logfile

    def main():
        indir, pattern, outdir, logfile = process_args()

        hsp = HsSpader()

        hsp.init_logging(logfile)

        hsp.spade_pickup_data(indir, pattern, outdir)

    main()
