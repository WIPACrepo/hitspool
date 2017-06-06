#!/usr/bin/env python


import logging
import os
import re
import shutil
import socket

import HsTestUtil

from HsRSyncFiles import HsRSyncFiles
from LoggingTestCase import LoggingTestCase


class HsRSyncTestCase(LoggingTestCase):
    # pylint: disable=too-many-public-methods
    # Really?!?!  In a test class?!?!  Shut up, pylint!

    TICKS_PER_SECOND = 10000000000
    INTERVAL = 15 * TICKS_PER_SECOND
    ONE_MINUTE = 60 * TICKS_PER_SECOND

    # this is initialized in HsTestUtil.populate()
    HUB_DIR = None

    @property
    def real_object(self):
        raise NotImplementedError()

    @property
    def wrapped_object(self):
        raise NotImplementedError()

    def setUp(self):
        super(HsRSyncTestCase, self).setUp()

        # by default, don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

    def tearDown(self):
        try:
            super(HsRSyncTestCase, self).tearDown()
        finally:
            if self.HUB_DIR is not None:
                # remove hitspool cache directory
                shutil.rmtree(self.HUB_DIR)
                self.HUB_DIR = None

    def test_nothing(self):
        # create the worker object
        hsr = self.real_object

        # check all log messages
        self.setLogLevel(0)

        self.expectLogMessage("Missing start/stop time(s). Abort request.")

        # test parser
        try:
            hsr.request_parser(None, None, None, None, delay_rsync=False)
        except TypeError, terr:
            self.assertEquals(str(terr), "expected string or buffer",
                              "Unexpected %s exception: %s" %
                              (type(terr), terr))

    def test_not_first_current_file(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - self.ONE_MINUTE
        last_stop = stop_ticks + (self.ONE_MINUTE * 5)

        # create currentRun directory
        cur_stop = start_ticks + (self.ONE_MINUTE * 60)
        cur_start = start_ticks + (self.ONE_MINUTE * 70)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)
        tstrun.set_current_interval(self.INTERVAL / 100)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_bad_alert_range(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks - self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 5)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # add all expected log messages
        self.expectLogMessage("sn_start & sn_stop time-stamps inverted."
                              " Abort request.")

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_partial_current_front(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 5)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_between_runs(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)

        # create currentRun directory
        cur_start = start_ticks + (self.ONE_MINUTE * 5)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 10)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage(re.compile(r"No data found between \d+ and \d+"))

        if HsRSyncFiles.DEBUG_EMPTY:
            hsr.i3socket.addDebugEMail(hsr.shorthost)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_partial_last_front(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks + (self.TICKS_PER_SECOND * 5)
        last_stop = stop_ticks + (self.ONE_MINUTE * 5)

        # create currentRun directory
        cur_start = start_ticks + (self.ONE_MINUTE * 10)
        cur_stop = start_ticks + (self.ONE_MINUTE * 15)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_partial_last_end(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 5)
        last_stop = start_ticks + (self.TICKS_PER_SECOND * 5)

        # create currentRun directory
        cur_start = start_ticks + self.ONE_MINUTE + self.TICKS_PER_SECOND
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_span_time_gap(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 5)
        last_stop = start_ticks + (self.TICKS_PER_SECOND * 5)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 50)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_span_link_fail(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_hardlink()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 5)
        last_stop = start_ticks + (self.TICKS_PER_SECOND * 5)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 50)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # add all expected log messages
        self.expectLogMessage("failed to link HitSpool-575.dat to tmp dir:"
                              " Fake Hardlink Error")
        self.expectLogMessage("failed to link HitSpool-576.dat to tmp dir:"
                              " Fake Hardlink Error")
        self.expectLogMessage("failed to link HitSpool-579.dat to tmp dir:"
                              " Fake Hardlink Error")
        self.expectLogMessage("No relevant files found")

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast,
                                  fail_links=True)
        tstrun.add_expected_files(start_ticks, curfile, numcur,
                                  fail_links=True)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_before_last_start(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks + (self.ONE_MINUTE * 2)
        last_stop = start_ticks + (self.ONE_MINUTE * 5)

        # create currentRun directory
        cur_start = start_ticks + (self.ONE_MINUTE * 6)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 10)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage(re.compile(r"No data found between \d+ and \d+"))

        if HsRSyncFiles.DEBUG_EMPTY:
            hsr.i3socket.addDebugEMail(hsr.shorthost)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_case5(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_case5_link_fail(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_hardlink()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # TODO: should compute these file values instead of hardcoding them
        firstfile = 575
        numfiles = 5

        # add all expected log messages
        for num in xrange(firstfile, firstfile + numfiles):
            self.expectLogMessage("failed to link HitSpool-%d.dat to tmp dir:"
                                  " Fake Hardlink Error" % num)
        self.expectLogMessage("No relevant files found")

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast,
                                  fail_links=True)
        tstrun.add_expected_files(start_ticks, curfile, numcur,
                                  fail_links=True)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_alert_in_future(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)

        # create currentRun directory
        cur_start = start_ticks - (self.ONE_MINUTE * 5)
        cur_stop = stop_ticks - (self.ONE_MINUTE * 2)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage(re.compile(r"No data found between \d+ and \d+"))

        if HsRSyncFiles.DEBUG_EMPTY:
            hsr.i3socket.addDebugEMail(hsr.shorthost)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_penultimate(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + (self.TICKS_PER_SECOND * 3)

        # create lastRun directory
        last_start = start_ticks + (self.TICKS_PER_SECOND * 1)
        last_stop = start_ticks + (self.TICKS_PER_SECOND * 2)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 4)
        cur_stop = start_ticks + (self.TICKS_PER_SECOND * 6)

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_works(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, lastfile, numlast)
        tstrun.add_expected_files(start_ticks, curfile, numcur)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        tstrun.run(start_ticks, stop_ticks)

        hsr.validate()

    def test_extract(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # fill info database
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks, last_start,
                                        last_stop, self.INTERVAL,
                                        create_files=True)
        (curfile, numcur) = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                                      cur_start, cur_stop,
                                                      self.INTERVAL,
                                                      create_files=True)

        # add all expected files being transferred
        destdir = os.path.join(self.HUB_DIR, HsRSyncFiles.DEFAULT_SPOOL_NAME)
        tstrun.add_expected_files(start_ticks, lastfile, numlast,
                                  destdir=destdir)
        tstrun.add_expected_files(start_ticks, curfile, numcur,
                                  destdir=destdir)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        tstrun.run(start_ticks, stop_ticks, extract_hits=True)

        hsr.validate()

    def test_extract_fail(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = start_ticks - self.TICKS_PER_SECOND

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        # populate directory with hit files and database
        tstrun.populate(self)

        # add all expected log messages
        self.expectLogMessage(re.compile("No hits found for .*"))

        # fill info database
        offset = (stop_ticks - start_ticks) * 100
        (lastfile, numlast) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                        last_start, last_stop,
                                        self.INTERVAL,
                                        offset=offset,
                                        create_files=True)
        (curfile, numcur) \
            = tstrun.update_hitspool_db(start_ticks, stop_ticks,
                                        cur_start, cur_stop,
                                        self.INTERVAL,
                                        offset=offset,
                                        create_files=True)

        # add all expected files being transferred
        destdir = os.path.join(self.HUB_DIR, HsRSyncFiles.DEFAULT_SPOOL_NAME)
        tstrun.add_expected_files(start_ticks, lastfile, numlast,
                                  destdir=destdir)
        tstrun.add_expected_files(start_ticks, curfile, numcur,
                                  destdir=destdir)

        tstrun.run(start_ticks, stop_ticks, extract_hits=True)

        hsr.validate()
