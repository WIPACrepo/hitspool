#!/usr/bin/env python
#
# Hit Spool SPADE-ing to be run on access
# author: dheereman
#
import glob
import logging
import os

from HsBase import HsBase


def add_arguments(parser):
    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hsspader.log")

    parser.add_argument("-i", "--indir", dest="indir", required=True,
                        help="Input dir where hs data is located,"
                        " e.g. /mnt/data/pdaqlocal/HsDataCopy/")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-o", "--outdir", dest="spadedir", required=True,
                        help="Directory where files are queued to SPADE")
    parser.add_argument("-p", "--pattern", dest="pattern", required=True,
                        help="time pattern of HS data alert: "
                        " <yyyymmdd>_<hhmmss>, e.g. 20131101_045126")


class HsSpader(HsBase):
    '''
    Copy the HS data to the SPADE queue on sps-2ndbuild.
    Create tarballs and semaphore files for each hub's data
    and move it to SPADE directory
    '''

    def __init__(self):
        super(HsSpader, self).__init__()

    # pylint: disable=no-self-use
    def find_matching_files(self, basedir, alertname):
        "This wrapper exists only so tests can override it"
        return glob.glob(os.path.join(basedir, "*" + alertname + "*"))

    # pylint: disable=no-self-use
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
        move semaphore & tar file to Spade directory
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

        hubnamelist = ["ichub%02d" % i for i in range(1, 87)] + \
                      ["ithub%02d" % i for i in range(1, 12)]
        for hub in hubnamelist:
            data = [s for s in datalistlocal if hub in s]
            if len(data) != 1:
                logging.info("no or ambiguous HS data found"
                             " for %s in this directory.", hub)
                continue

            dataname = data[0]
            basename = "HS_SNALERT_" + alertname + "_" + hub

            logging.info("data: %s will be tarred to: %s", dataname,
                         basename + self.TAR_SUFFIX)

            result = self.queue_for_spade(basedir, dataname, spadedir,
                                          basename)
            if result is None:
                logging.info("Please put the data manually in"
                             " the SPADE directory")
                continue

            logging.info("Preparation for SPADE Pickup of %s DONE", basename)


if __name__ == "__main__":
    import argparse

    def main():
        parser = argparse.ArgumentParser()

        add_arguments(parser)

        args = parser.parse_args()

        hsp = HsSpader()

        hsp.init_logging(args.logfile, basename="hsspader",
                         basehost="access")

        hsp.spade_pickup_data(args.indir, args.pattern, args.spadedir)

    main()
