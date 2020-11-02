#!/usr/bin/env python
"""
Test the HsSpader class
"""


import errno
import logging
import os
import tempfile
import unittest

import HsSpader

from HsException import HsException
from HsTestUtil import MockHitspool
from LoggingTestCase import LoggingTestCase


class MySpader(HsSpader.HsSpader):
    "Extend HsSpader and/or stub out some methods for unit tests"

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

    def remove_tree(self, path):
        pass

    def set_file_list(self, files):
        self.__filelist = files

    def write_meta_xml(self, spadedir, basename, start_ticks=None,
                       stop_ticks=None):
        if self.__fail_touch_file:
            raise HsException("Fake Touch Error")
        return "fake.meta.xml"

    def write_sem(self, spadedir, basename):
        if self.__fail_touch_file:
            raise HsException("Fake Touch Error")
        return "fake.sem"

    def write_tarfile(self, sourcedir, sourcefiles, tarname):
        if self.__fail_tar_file > 0:
            self.__fail_tar_file -= 1
            raise HsException("Fake Tar Error")


class HsSpaderTest(LoggingTestCase):
    "Test the HsSpader class"

    def setUp(self):
        super(HsSpaderTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)

    def tearDown(self):
        try:
            super(HsSpaderTest, self).tearDown()
        finally:
            # clear lingering files
            try:
                MockHitspool.destroy()
            except:
                pass

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
        MockHitspool.create_copy_files(category, timetag, host, firstnum,
                                       numfiles, real_stuff=True)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.INFO)

        outdir = tempfile.mkdtemp()

        # add all expected log messages
        self.expect_log_message("Preparation for SPADE Pickup of HS data"
                                " started manually via HsSpader...")
        self.expect_log_message("no HS data found for this alert time "
                                "pattern.")

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
        MockHitspool.create_copy_files(category, timetag, host, firstnum,
                                       numfiles, real_stuff=True)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.INFO)

        # pylint: disable=anomalous-backslash-in-string
        # this is meant to be an illegal directory path on all OSes
        outdir = "/xxx/yyy/zzz\/\/foo::bar"

        # add all expected log messages
        self.expect_log_message("Preparation for SPADE Pickup of HS data"
                                " started manually via HsSpader...")

        try:
            hsp.spade_pickup_data(outdir, timetag, outdir)
            self.fail("Should not be able to create %s" % outdir)
        except OSError as err:
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
        hsdir = MockHitspool.create_copy_files(category, timetag, host,
                                               firstnum, numfiles,
                                               real_stuff=True)

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        basename = "HS_SNALERT_%s_%s" % (timetag, host)
        tarname = basename + HsSpader.HsSpader.TAR_SUFFIX
        outdir = tempfile.mkdtemp()

        # add all expected log messages
        self.expect_log_message("Preparation for SPADE Pickup of HS data"
                                " started manually via HsSpader...")
        self.expect_log_message("found HS data:\n%s" % [hsdir, ])
        self.expect_log_message("data: %s_%s_%s will be tarred to: %s" %
                                (category, timetag, host, tarname))
        self.expect_log_message("Preparation for SPADE Pickup of %s DONE" %
                                basename)
        for hub in range(2, 87):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ichub%02d in this directory." % hub)
        for hub in range(1, 12):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ithub%02d in this directory." % hub)

        hsp.spade_pickup_data(MockHitspool.COPY_DIR, timetag, outdir)

    def test_fake_spade_pickup_data_no_data(self):
        hsp = MySpader()

        # initialize directory parts
        indir = "/path/from/input"
        timetag = "12345678_987654"
        outdir = "/path/to/output"

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expect_log_message("Preparation for SPADE Pickup of HS data"
                                " started manually via HsSpader...")
        self.expect_log_message("no HS data found for this alert time "
                                "pattern.")

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
        self.expect_log_message("Preparation for SPADE Pickup of HS data"
                                " started manually via HsSpader...")
        self.expect_log_message("found HS data:\n%s" % filelist)
        for hub in range(1, 87):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ichub%02d in this directory." % hub)
        for hub in range(1, 12):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ithub%02d in this directory." % hub)

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
        tarname = "HS_SNALERT_%s_%s%s" % \
                  (timetag, host, HsSpader.HsSpader.TAR_SUFFIX)

        filelist = [os.path.join(indir, basename), ]
        hsp.set_file_list(filelist)

        hsp.fail_create_tar_file()

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expect_log_message("Preparation for SPADE Pickup of HS data"
                                " started manually via HsSpader...")
        self.expect_log_message("found HS data:\n%s" % filelist)
        for hub in range(1, 86):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ichub%02d in this directory." % hub)
        self.expect_log_message("data: %s_%s_%s will be tarred to: %s" %
                                (category, timetag, host, tarname))
        self.expect_log_message("Fake Tar Error")
        self.expect_log_message("Please put the data manually in"
                                " the SPADE directory")
        for hub in range(1, 12):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ithub%02d in this directory." % hub)

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
        tarname = "HS_SNALERT_%s_%s%s" % \
                  (timetag, host, HsSpader.HsSpader.TAR_SUFFIX)

        filelist = [os.path.join(indir, basename), ]
        hsp.set_file_list(filelist)

        hsp.fail_move_file()

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expect_log_message("Preparation for SPADE Pickup of HS data"
                                " started manually via HsSpader...")
        self.expect_log_message("found HS data:\n%s" % filelist)
        for hub in range(1, 86):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ichub%02d in this directory." % hub)
        self.expect_log_message("data: %s_%s_%s will be tarred to: %s" %
                                (category, timetag, host, tarname))
        self.expect_log_message("Fake Move Error")
        self.expect_log_message("Please put the data manually in"
                                " the SPADE directory")
        for hub in range(1, 12):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ithub%02d in this directory." % hub)

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
        tarname = basename + HsSpader.HsSpader.TAR_SUFFIX

        filelist = [os.path.join(indir, "%s_%s_%s" %
                                 (category, timetag, host)), ]
        hsp.set_file_list(filelist)

        hsp.fail_create_sem_file()

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expect_log_message("Preparation for SPADE Pickup of HS data"
                                " started manually via HsSpader...")
        self.expect_log_message("found HS data:\n%s" % filelist)
        for hub in range(1, 86):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ichub%02d in this directory." % hub)
        self.expect_log_message("data: %s_%s_%s will be tarred to: %s" %
                                (category, timetag, host, tarname))
        self.expect_log_message("Fake Touch Error")
        self.expect_log_message("Please put the data manually in"
                                " the SPADE directory")

        for hub in range(1, 12):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ithub%02d in this directory." % hub)

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
        tarname = basename + HsSpader.HsSpader.TAR_SUFFIX

        filelist = [os.path.join(indir, "%s_%s_%s" %
                                 (category, timetag, host)), ]
        hsp.set_file_list(filelist)

        # don't check DEBUG log messages
        self.setLogLevel(logging.INFO)

        # add all expected log messages
        self.expect_log_message("Preparation for SPADE Pickup of HS data"
                                " started manually via HsSpader...")
        self.expect_log_message("found HS data:\n%s" % filelist)
        for hub in range(1, 86):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ichub%02d in this directory." % hub)
        self.expect_log_message("data: %s_%s_%s will be tarred to: %s" %
                                (category, timetag, host, tarname))
        self.expect_log_message("Preparation for SPADE Pickup of %s DONE" %
                                basename)

        for hub in range(1, 12):
            self.expect_log_message("no or ambiguous HS data found for"
                                    " ithub%02d in this directory." % hub)

        hsp.spade_pickup_data(indir, timetag, outdir)


if __name__ == '__main__':
    unittest.main()
