#!/usr/bin/env python

#import json
import logging
import os
import shutil
#import tarfile
import tempfile
import unittest

import HsSpader

from HsException import HsException

from LoggingTestCase import LoggingTestCase


class MySpader(HsSpader.HsSpader):
    def __init__(self):
        super(MySpader, self).__init__()

        self.__filelist = []
        self.__fail_move_file = False
        self.__fail_touch_file = False
        self.__fail_tar_file = 0

    def fail_create_sem_file(self):
        self.__fail_touch_file = True

    def fail_create_tar_file(self, num_fails=1):
        self.__fail_tar_file = num_fails

    def fail_move_file(self):
        self.__fail_move_file = True

    def find_matching_files(self, basedir, alertname):
        return self.__filelist

    def makedirs(self, path):
        pass

    def move_file(self, src, dst):
        if self.__fail_move_file:
            raise HsException("Fake Move Error")

    def set_file_list(self, files):
        self.__filelist = files

    def touch_file(self, name, times=None):
        if self.__fail_touch_file:
            raise HsException("Fake Touch Error")

    def write_tarfile(self, sourcedir, sourcefiles, tarname):
        if self.__fail_tar_file > 0:
            self.__fail_tar_file -= 1
            raise HsException("Fake Tar Error")


class HsSpaderTest(LoggingTestCase):
    COPY_DIR = None

    def __create_copydir(self, real_stuff=False):
        """create temporary copy directory"""
        if self.COPY_DIR is None:
            if real_stuff:
                self.COPY_DIR = tempfile.mkdtemp()
            else:
                self.COPY_DIR = "/cloud/cuckoo/land"
        return self.COPY_DIR

    def __create_hitspool_copy(self, prefix, timetag, host, startnum, numfiles,
                               real_stuff=False):
        """create copy directory and fill with fake hitspool files"""
        copydir = self.__create_copydir(real_stuff=real_stuff)

        # create copy directory
        path = os.path.join(copydir, "%s_%s_%s" % (prefix, timetag, host))

        # if caller wants actual directory and files, create them
        if real_stuff:
            if not os.path.exists(path):
                os.makedirs(path)

            # create all fake hitspool files
            for num in xrange(startnum, startnum + numfiles):
                fpath = os.path.join(path, "HitSpool-%d" % num)
                with open(fpath, "w") as fout:
                    print >>fout, "Fake#%d" % num

        return path

    def setUp(self):
        super(HsSpaderTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)

    def tearDown(self):
        try:
            super(HsSpaderTest, self).tearDown()
        finally:
            if self.COPY_DIR is not None and os.path.exists(self.COPY_DIR):
                # clear lingering files
                shutil.rmtree(self.COPY_DIR)

    def test_real_spade_pickup_data_empty_dir(self):
        hsp = HsSpader.HsSpader()

        # initialize directory parts
        category = "XXX"
        timetag = "12345678_987654"
        host = "ichub01"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create directory and files
        self.__create_hitspool_copy(category, timetag, host, firstnum, numfiles,
                                    real_stuff=True)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.INFO)

        outdir = tempfile.mkdtemp()

        # add all expected log messages
        self.expectLogMessage("Preparation for SPADE Pickup of HS data"
                              " started manually via HsSpader...")
        self.expectLogMessage("no HS data found for this alert time pattern.")

        hsp.spade_pickup_data(outdir, timetag, outdir)

    def test_real_spade_pickup_data_bad_out_dir(self):
        hsp = HsSpader.HsSpader()

        # initialize directory parts
        category = "XXX"
        timetag = "12345678_987654"
        host = "ichub01"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create directory and files
        self.__create_hitspool_copy(category, timetag, host, firstnum, numfiles,
                                    real_stuff=True)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.INFO)

        # pylint: disable=anomalous-backslash-in-string
        # this is meant to be an illegal directory path on all OSes
        outdir = "/xxx/yyy/zzz\/\/foo::bar"

        # add all expected log messages
        self.expectLogMessage("Preparation for SPADE Pickup of HS data"
                              " started manually via HsSpader...")

        try:
            hsp.spade_pickup_data(outdir, timetag, outdir)
            self.fail("Should not be able to create %s" % outdir)
        except OSError, err:
            import errno
            if err.errno != errno.EACCES:
                raise

    def test_real_spade_pickup_data(self):
        hsp = HsSpader.HsSpader()

        # initialize directory parts
        category = "XXX"
        timetag = "12345678_987654"
        host = "ichub01"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create directory and files
        hsdir = self.__create_hitspool_copy(category, timetag, host, firstnum,
                                            numfiles, real_stuff=True)

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        tarname = "HS_SNALERT_%s_%s.dat.tar.bz2" % (timetag, host)
        outdir = tempfile.mkdtemp()

        # add all expected log messages
        self.expectLogMessage("Preparation for SPADE Pickup of HS data"
                              " started manually via HsSpader...")
        self.expectLogMessage("found HS data:\n%s" % [hsdir, ])
        self.expectLogMessage("data: %s_%s_%s will be tarred to: %s" %
                              (category, timetag, host, tarname))
        self.expectLogMessage("Preparation for SPADE Pickup of %s DONE" %
                              tarname)
        for hub in xrange(2, 87):
            self.expectLogMessage("no or ambiguous HS data found for ichub%02d"
                                  " in this directory." % hub)
        for hub in xrange(1, 12):
            self.expectLogMessage("no or ambiguous HS data found for ithub%02d"
                                  " in this directory." % hub)

        hsp.spade_pickup_data(self.COPY_DIR, timetag, outdir)

    def test_fake_spade_pickup_data_no_data(self):
        hsp = MySpader()

        # initialize directory parts
        indir = "/path/from/input"
        timetag = "12345678_987654"
        outdir = "/path/to/output"

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expectLogMessage("Preparation for SPADE Pickup of HS data"
                              " started manually via HsSpader...")
        self.expectLogMessage("no HS data found for this alert time pattern.")

        hsp.spade_pickup_data(indir, timetag, outdir)

    def test_fake_spade_pickup_data_ambiguous(self):
        hsp = MySpader()

        # initialize directory parts
        indir = "/path/from/input"
        timetag = "12345678_987654"
        outdir = "/path/to/output"

        filelist = [os.path.join(indir, "XXX_%s_ichub01" % timetag),
                    os.path.join(indir, "YYY_%s_ichub01" % timetag), ]
        hsp.set_file_list(filelist)

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expectLogMessage("Preparation for SPADE Pickup of HS data"
                              " started manually via HsSpader...")
        self.expectLogMessage("found HS data:\n%s" % filelist)
        for hub in xrange(1, 87):
            self.expectLogMessage("no or ambiguous HS data found for ichub%02d"
                                  " in this directory." % hub)
        for hub in xrange(1, 12):
            self.expectLogMessage("no or ambiguous HS data found for ithub%02d"
                                  " in this directory." % hub)

        hsp.spade_pickup_data(indir, timetag, outdir)

    def test_fake_spade_pickup_data_fail_tar(self):
        hsp = MySpader()

        # initialize directory parts
        indir = "/path/from/input"
        category = "XXX"
        timetag = "12345678_987654"
        host = "ichub86"
        outdir = "/path/to/output"

        basename = "%s_%s_%s" % (category, timetag, host)
        tarname = "HS_SNALERT_%s_%s.dat.tar.bz2" % (timetag, host)

        filelist = [os.path.join(indir, basename), ]
        hsp.set_file_list(filelist)

        hsp.fail_create_tar_file()

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expectLogMessage("Preparation for SPADE Pickup of HS data"
                              " started manually via HsSpader...")
        self.expectLogMessage("found HS data:\n%s" % filelist)
        for hub in xrange(1, 86):
            self.expectLogMessage("no or ambiguous HS data found for ichub%02d"
                                  " in this directory." % hub)
        self.expectLogMessage("data: %s_%s_%s will be tarred to: %s" %
                              (category, timetag, host, tarname))
        self.expectLogMessage("Fake Tar Error")
        self.expectLogMessage("Please put the data manually in"
                              " the SPADE directory")
        for hub in xrange(1, 12):
            self.expectLogMessage("no or ambiguous HS data found for ithub%02d"
                                  " in this directory." % hub)

        hsp.spade_pickup_data(indir, timetag, outdir)

    def test_fake_spade_pickup_data_fail_move(self):
        hsp = MySpader()

        # initialize directory parts
        indir = "/path/from/input"
        category = "XXX"
        timetag = "12345678_987654"
        host = "ichub86"
        outdir = "/path/to/output"

        basename = "%s_%s_%s" % (category, timetag, host)
        tarname = "HS_SNALERT_%s_%s.dat.tar.bz2" % (timetag, host)

        filelist = [os.path.join(indir, basename), ]
        hsp.set_file_list(filelist)

        hsp.fail_move_file()

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expectLogMessage("Preparation for SPADE Pickup of HS data"
                              " started manually via HsSpader...")
        self.expectLogMessage("found HS data:\n%s" % filelist)
        for hub in xrange(1, 86):
            self.expectLogMessage("no or ambiguous HS data found for ichub%02d"
                                  " in this directory." % hub)
        self.expectLogMessage("data: %s_%s_%s will be tarred to: %s" %
                              (category, timetag, host, tarname))
        self.expectLogMessage("Fake Move Error")
        self.expectLogMessage("Please put the data manually in"
                              " the SPADE directory")
        for hub in xrange(1, 12):
            self.expectLogMessage("no or ambiguous HS data found for ithub%02d"
                                  " in this directory." % hub)

        hsp.spade_pickup_data(indir, timetag, outdir)

    def test_fake_spade_pickup_data_fail_sem(self):
        hsp = MySpader()

        # initialize directory parts
        indir = "/path/from/input"
        category = "XXX"
        timetag = "12345678_987654"
        host = "ichub86"
        outdir = "/path/to/output"

        basename = "HS_SNALERT_%s_%s" % (timetag, host)
        tarname = basename + ".dat.tar.bz2"

        filelist = [os.path.join(indir, "%s_%s_%s" %
                                 (category, timetag, host)), ]
        hsp.set_file_list(filelist)

        hsp.fail_create_sem_file()

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expectLogMessage("Preparation for SPADE Pickup of HS data"
                              " started manually via HsSpader...")
        self.expectLogMessage("found HS data:\n%s" % filelist)
        for hub in xrange(1, 86):
            self.expectLogMessage("no or ambiguous HS data found for ichub%02d"
                                  " in this directory." % hub)
        self.expectLogMessage("data: %s_%s_%s will be tarred to: %s" %
                              (category, timetag, host, tarname))
        self.expectLogMessage("Fake Touch Error")
        self.expectLogMessage("Please put the data manually in"
                              " the SPADE directory")

        for hub in xrange(1, 12):
            self.expectLogMessage("no or ambiguous HS data found for ithub%02d"
                                  " in this directory." % hub)

        hsp.spade_pickup_data(indir, timetag, outdir)

    def test_fake_spade_pickup_data(self):
        hsp = MySpader()

        # initialize directory parts
        indir = "/path/from/input"
        category = "XXX"
        timetag = "12345678_987654"
        host = "ichub86"
        outdir = "/path/to/output"

        basename = "HS_SNALERT_%s_%s" % (timetag, host)
        tarname = basename + ".dat.tar.bz2"

        filelist = [os.path.join(indir, "%s_%s_%s" %
                                 (category, timetag, host)), ]
        hsp.set_file_list(filelist)

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expectLogMessage("Preparation for SPADE Pickup of HS data"
                              " started manually via HsSpader...")
        self.expectLogMessage("found HS data:\n%s" % filelist)
        for hub in xrange(1, 86):
            self.expectLogMessage("no or ambiguous HS data found for ichub%02d"
                                  " in this directory." % hub)
        self.expectLogMessage("data: %s_%s_%s will be tarred to: %s" %
                              (category, timetag, host, tarname))
        self.expectLogMessage("Preparation for SPADE Pickup of %s DONE" %
                              tarname)

        for hub in xrange(1, 12):
            self.expectLogMessage("no or ambiguous HS data found for ithub%02d"
                                  " in this directory." % hub)

        hsp.spade_pickup_data(indir, timetag, outdir)


if __name__ == '__main__':
    unittest.main()
