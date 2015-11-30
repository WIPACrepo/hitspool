#!/usr/bin/env python


import logging
import os
import re
import shutil

import HsTestUtil

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

        self.expectLogMessage("Destination parsing failed for \"%s\":\n"
                              "Abort request." % None)

        # test parser
        try:
            hsr.request_parser(None, None, None, sleep_secs=0)
        except TypeError, terr:
            self.assertEquals(str(terr), "expected string or buffer",
                              "Unexpected %s exception: %s" %
                              (type(terr), terr))

    def test_initial_old(self):
        # create the worker object
        hsr = self.real_object

        # initialize copy path
        copyuser = "me"
        copyhost = "xxx"
        copypath = "/one/two/three"
        copydir = "%s@%s:%s" % (copyuser, copyhost, copypath)

        # initialize currentRun/info.txt path
        cur_info = os.path.join(hsr.TEST_HUB_DIR, "currentRun", "info.txt")

        # check all log messages
        self.setLogLevel(0)

        self.expectLogMessage("HS COPY SSH ACCESS: %s@%s" %
                              (copyuser, copyhost))
        self.expectLogMessage("HS COPYDIR = %s" % copypath)
        self.expectLogMessage("HS DESTINATION HOST: %s" % copyhost)
        self.expectLogMessage("HsInterface running on: %s" % hsr.cluster)
        self.expectLogMessage("%s reading/parsing failed" % cur_info)

        # test parser
        try:
            hsr.request_parser(HsTestUtil.get_time(1),
                               HsTestUtil.get_time(2),
                               copydir, sleep_secs=0)
        except TypeError, terr:
            self.assertEquals(str(terr), "expected string or buffer",
                              "Unexpected %s exception: %s" %
                              (type(terr), terr))

    def test_no_info_txt_old(self):
        # create the worker object
        hsr = self.real_object
        hsr.TEST_HUB_DIR = "/this/directory/does/not/exist"

        # initialize currentRun/info.txt path
        cur_info = os.path.join(hsr.TEST_HUB_DIR, "currentRun", "info.txt")

        # add all expected log messages
        self.expectLogMessage("%s reading/parsing failed" % cur_info)

        copydir = "me@host:/a/b/c"

        # test parser
        hsr.request_parser(HsTestUtil.get_time(1),
                           HsTestUtil.get_time(2),
                           copydir, sleep_secs=0)

    def test_bad_last_run_old(self):
        # create the worker object
        hsr = self.real_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)
        tstrun.make_bad_last()

        tstrun.populate(self, use_db=use_db)

        # initialize lasttRun/info.txt path
        last_info = os.path.join(hsr.TEST_HUB_DIR, "lastRun", "info.txt")

        # add all expected log messages
        self.expectLogMessage("%s reading/parsing failed" % last_info)

        tstrun.run(start_ticks, stop_ticks)

    def test_bad_current_run_old(self):
        # create the worker object
        hsr = self.real_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)
        tstrun.make_bad_current()

        tstrun.populate(self, use_db=use_db)

        # initialize currentRun/info.txt path
        cur_info = os.path.join(hsr.TEST_HUB_DIR, "currentRun", "info.txt")

        # add all expected log messages
        self.expectLogMessage("%s reading/parsing failed" % cur_info)

        tstrun.run(start_ticks, stop_ticks)

    def test_not_first_current_file_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)
        tstrun.set_current_interval(self.INTERVAL / 100)

        tstrun.populate(self, use_db=use_db)

        # TODO: should compute these file values instead of hardcoding them
        tstrun.add_expected_links(start_ticks, "lastRun", 4, 5)

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)
        tstrun.set_current_interval(self.INTERVAL / 100)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

    def test_not_first_last_file_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)
        tstrun.set_last_interval(self.INTERVAL / 100)

        tstrun.populate(self, use_db=use_db)

        # TODO: should compute these file values instead of hardcoding them
        tstrun.add_expected_links(start_ticks, "currentRun", 4, 5)

        tstrun.run(start_ticks, stop_ticks)

    def test_not_first_last_file(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)
        tstrun.set_last_interval(self.INTERVAL / 100)

        tstrun.populate(self, use_db=use_db)

        # TODO: should compute these file values instead of hardcoding them
        tstrun.add_expected_links(start_ticks, "currentRun", 4, 5)

        tstrun.run(start_ticks, stop_ticks)

    def test_bad_alert_range_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("sn_start & sn_stop time-stamps inverted."
                              " Abort request.")

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("sn_start & sn_stop time-stamps inverted."
                              " Abort request.")

        tstrun.run(start_ticks, stop_ticks)

    def test_partial_current_front_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # TODO: should compute these file values instead of hardcoding them
        tstrun.add_expected_links(start_ticks, "currentRun", 0, 4)

        # add all expected log messages
        self.expectLogMessage("Sn_start doesn't exist in currentRun buffer"
                              " anymore! Start with oldest possible data: "
                              "HitSpool-0")

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

    def test_between_runs_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("Requested data doesn't exist in HitSpool"
                              " Buffer anymore! Abort request.")

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("No data found between %s and %s" %
                              (HsTestUtil.get_time(start_ticks),
                               HsTestUtil.get_time(stop_ticks)))

        tstrun.run(start_ticks, stop_ticks)

    def test_partial_last_front_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # TODO: should compute these file values instead of hardcoding them
        tstrun.add_expected_links(start_ticks, "lastRun", 0, 4)

        # add all expected log messages
        self.expectLogMessage("sn_start doesn't exist in lastRun buffer"
                              " anymore! Start with oldest possible data:"
                              " HitSpool-0")

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

    def test_partial_last_end_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # TODO should compute the expected number of files
        tstrun.add_expected_links(start_ticks, "lastRun", 20, 1)

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

    def test_span_time_gap_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # TODO should compute the expected number of files
        tstrun.add_expected_links(start_ticks, "lastRun", 20, 1)
        tstrun.add_expected_links(start_ticks, "currentRun", 0, 1)

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

    def test_span_link_fail_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # TODO: should compute these file values instead of hardcoding them
        firstfile = 0
        numfiles = 1

        # add all expected log messages
        self.expectLogMessage("failed to link HitSpool-20.dat to tmp dir:"
                              " Fake Hardlink Error")
        for num in xrange(firstfile, firstfile + numfiles):
            self.expectLogMessage("failed to link HitSpool-%d.dat to tmp dir:"
                                  " Fake Hardlink Error" % num)
        self.expectLogMessage("No relevant files found")

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("failed to link HitSpool-575.dat to tmp dir:"
                              " Fake Hardlink Error")
        self.expectLogMessage("failed to link HitSpool-576.dat to tmp dir:"
                              " Fake Hardlink Error")
        self.expectLogMessage("failed to link HitSpool-579.dat to tmp dir:"
                              " Fake Hardlink Error")
        self.expectLogMessage("No relevant files found")

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, fail_links=True,
                                  use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, fail_links=True, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

    def test_before_last_start_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("Requested data doesn't exist in HitSpool"
                              " Buffer anymore! Abort request.")

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("No data found between %s and %s" %
                              (HsTestUtil.get_time(start_ticks),
                               HsTestUtil.get_time(stop_ticks)))

        tstrun.run(start_ticks, stop_ticks)

    def test_case5_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # TODO should compute the expected number of files
        tstrun.add_expected_links(start_ticks, "currentRun", 4, 5)

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

    def test_case5_link_fail_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # TODO: should compute these file values instead of hardcoding them
        firstfile = 4
        numfiles = 5

        # add all expected log messages
        for num in xrange(firstfile, firstfile + numfiles):
            self.expectLogMessage("failed to link HitSpool-%d.dat to tmp dir:"
                                  " Fake Hardlink Error" % num)
        self.expectLogMessage("No relevant files found")

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # TODO: should compute these file values instead of hardcoding them
        firstfile = 575
        numfiles = 5

        # add all expected log messages
        for num in xrange(firstfile, firstfile + numfiles):
            self.expectLogMessage("failed to link HitSpool-%d.dat to tmp dir:"
                                  " Fake Hardlink Error" % num)
        self.expectLogMessage("No relevant files found")

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, fail_links=True,
                                  use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, fail_links=True, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

    def test_alert_in_future_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("alert_start is in the FUTURE ?!")

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("No data found between %s and %s" %
                              (HsTestUtil.get_time(start_ticks),
                               HsTestUtil.get_time(stop_ticks)))

        tstrun.run(start_ticks, stop_ticks)

    def test_penultimate_old(self):
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

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("alert_start < lastRun < alert_stop < currentRun."
                              " Assign: all HS data of lastRun instead.")

        # TODO: should compute these file values instead of hardcoding them
        tstrun.add_expected_links(start_ticks, "lastRun", 0, 1)

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

    def test_fail_rsync_old(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_rsync()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE

        # use DB or old info.txt?
        use_db = False

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("failed rsync process:\nFakeFail")

        # TODO: should compute these file values instead of hardcoding them
        tstrun.add_expected_links(start_ticks, "currentRun", 4, 5)

        tstrun.run(start_ticks, stop_ticks)

    def test_fail_rsync(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_rsync()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage("failed rsync process:\nFakeFail")

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db)

        tstrun.run(start_ticks, stop_ticks)

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

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected files being transferred
        destdir = os.path.join(self.HUB_DIR, "hitspool")
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db,
                                  destdir=destdir)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db,
                                  destdir=destdir)

        tstrun.run(start_ticks, stop_ticks, extract_hits=True)

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
        cur_stop = stop_ticks + self.ONE_MINUTE

        # use DB or old info.txt?
        use_db = True

        tstrun = HsTestUtil.HsTestRunner(hsr, last_start, last_stop, cur_start,
                                         cur_stop, interval=self.INTERVAL)

        tstrun.populate(self, use_db=use_db)

        # add all expected log messages
        self.expectLogMessage(re.compile("No hits found for .*"))

        # add all expected files being transferred
        destdir = os.path.join(self.HUB_DIR, "hitspool")
        tstrun.add_expected_files(start_ticks, stop_ticks, last_start,
                                  last_stop, self.INTERVAL, use_db=use_db,
                                  destdir=destdir, fail_extract=True)
        tstrun.add_expected_files(start_ticks, stop_ticks, cur_start, cur_stop,
                                  self.INTERVAL, use_db=use_db,
                                  destdir=destdir, fail_extract=True)

        tstrun.run(start_ticks, stop_ticks, extract_hits=True)
