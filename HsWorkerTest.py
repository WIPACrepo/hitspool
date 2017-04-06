#!/usr/bin/env python

import logging
import os
import re
import unittest

from collections import namedtuple

import HsWorker
import HsTestUtil

from HsException import HsException
from HsRSyncFiles import HsRSyncFiles
from LoggingTestCase import LoggingTestCase


class MockSenderSocket(HsTestUtil.Mock0MQSocket):
    def __init__(self):
        super(MockSenderSocket, self).__init__("Sender")

    def send_json(self, jobj):
        if isinstance(jobj, str) or isinstance(jobj, unicode):
            raise Exception("Got string JSON object %s<%s>" %
                            (jobj, type(jobj)))

        super(MockSenderSocket, self).send_json(jobj)


class MockSocket(object):
    def __init__(self):
        self.__input = []

    def add_input(self, msg):
        self.__input.append(msg)

    def recv_json(self):
        if len(self.__input) == 0:
            raise Exception("No more inputs")

        return self.__input.pop(0)


class MyWorker(HsWorker.Worker):
    def __init__(self):
        self.__snd_sock = None
        self.__sub_sock = None

        super(MyWorker, self).__init__("HsWorker", host="tstwrk", is_test=True)

        self.__link_paths = []
        self.__fail_hardlink = False
        self.__fail_rsync = False

        # don't sleep during unit tests
        self.MIN_DELAY = 0.0

    @classmethod
    def __timetag(cls, starttime):
        return starttime.strftime("%Y%m%d_%H%M%S")

    def add_expected_links(self, start_utc, firstnum, numfiles,
                           i3socket=None, finaldir=None):
        timetag = self.__timetag(start_utc)
        for i in xrange(firstnum, firstnum + numfiles):
            frompath = os.path.join(self.TEST_HUB_DIR, self.DEFAULT_SPOOL_NAME,
                                    "HitSpool-%d.dat" % i)
            self.__link_paths.append((frompath, self.TEST_HUB_DIR, timetag))
            if i3socket is not None:
                errmsg = "linked %s to tmp dir" % frompath
                i3socket.addExpectedValue(errmsg)
        if i3socket is not None and finaldir is not None:
            i3socket.addExpectedValue(" TBD [MB] HS data transferred to %s " %
                                      finaldir, prio=1)

    def check_for_unused_links(self):
        llen = len(self.__link_paths)
        if llen > 0:
            raise Exception("Found %d extra link%s" %
                            (llen, "" if llen == 1 else "s"))

    def create_i3socket(self, host):
        return HsTestUtil.MockI3Socket("HsWorker@%s" % self.shorthost)

    def create_sender_socket(self, host):
        if self.__snd_sock is None:
            self.__snd_sock = MockSenderSocket()
        return self.__snd_sock

    def create_subscriber_socket(self, host):
        if self.__sub_sock is None:
            self.__sub_sock = HsTestUtil.Mock0MQSocket("Subscriber")
        return self.__sub_sock

    def fail_hardlink(self):
        self.__fail_hardlink = True

    def fail_rsync(self):
        self.__fail_rsync = True

    def hardlink(self, filename, targetdir):
        if self.__fail_hardlink:
            raise HsException("Fake Hardlink Error")

        if len(self.__link_paths) == 0:
            raise Exception("Unexpected hardlink from \"%s\" to \"%s\"" %
                            (filename, targetdir))

        expfile, expdir, exptag = self.__link_paths.pop(0)
        if not targetdir.startswith(expdir) or \
           not targetdir.endswith(exptag):
            if filename != expfile:
                raise Exception("Expected to link \"%s\" to \"%s\", not"
                                " \"%s/*/%s\" to \"%s\"" %
                                (expfile, expdir, exptag, filename, targetdir))
            raise Exception("Expected to link \"%s\" to \"%s/*/%s\", not to"
                            " \"%s\"" % (expfile, expdir, exptag, targetdir))
        elif filename != expfile:
            raise Exception("Expected to link \"%s\" to \"%s/*/%s\", not"
                            " \"%s\"" % (expfile, expdir, exptag, filename))

        return 0

    def send_files(self, req, source_list, rsync_user, rsync_host, rsync_dir,
                   timetag_dir, use_daemon, update_status=None, bwlimit=None,
                   log_format=None, relative=True):
        if self.__fail_rsync:
            raise HsException("FakeFail")
        return True

    def validate(self):
        if self.__snd_sock is not None:
            self.__snd_sock.validate()
        if self.__sub_sock is not None:
            self.__sub_sock.validate()
        self.check_for_unused_links()


class HsWorkerTest(LoggingTestCase):
    # pylint: disable=too-many-public-methods
    # Really?!?!  In a test class?!?!  Shut up, pylint!

    TICKS_PER_SECOND = 10000000000
    INTERVAL = 15 * TICKS_PER_SECOND
    ONE_MINUTE = 60 * TICKS_PER_SECOND

    def __dump_hsdb(self, hsr, spooldir):
        "Debugging code to dump the contents of the SQLite3 database"
        import sqlite3
        hsdbpath = os.path.join(hsr.TEST_HUB_DIR, spooldir,
                                HsRSyncFiles.DEFAULT_SPOOL_DB)
        print ":: %s" % hsdbpath
        conn = sqlite3.connect(hsdbpath)

        try:
            cursor = conn.cursor()

            print "=== %s" % hsdbpath
            for row in cursor.execute("select filename,start_tick,stop_tick"
                                      " from hitspool"):
                print "%s: %d - %d" % row
        finally:
            conn.close()

    @classmethod
    def make_alert_object(cls, xdict):
        if "prefix" in xdict:
            mydict = xdict
        else:
            mydict = xdict.copy()
            mydict["prefix"] = None
        newobj = namedtuple("TestAlert", mydict.keys())(**mydict)
        return newobj

    @property
    def real_object(self):
        return HsWorker.Worker("Worker")

    @property
    def wrapped_object(self):
        return MyWorker()

    def setUp(self):
        super(HsWorkerTest, self).setUp()

        # by default, don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

    def tearDown(self):
        try:
            super(HsWorkerTest, self).tearDown()
        finally:
            HsTestUtil.MockHitspool.destroy()

    def test_receive_nothing(self):
        # create the worker object
        hsr = self.real_object

        # no alert
        alert = None

        # check all log messages
        self.setLogLevel(0)

        # set up input source
        sock = MockSocket()
        sock.add_input(alert)

        # test input method
        try:
            hsr.receive_request(sock)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("JSON message should be a dict") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_receive_str(self):
        # create the worker object
        hsr = self.wrapped_object

        # bad alert
        alert = "foo"

        # check all log messages
        self.setLogLevel(0)

        # set up input source
        sock = MockSocket()
        sock.add_input(alert)

        # test input method
        try:
            hsr.receive_request(sock)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("JSON message should be a dict") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_receive_empty(self):
        # create the worker object
        hsr = self.wrapped_object

        # empty alert
        alert = {}

        # check all log messages
        self.setLogLevel(0)

        # set up input source
        sock = MockSocket()
        sock.add_input(alert)

        # test input method
        try:
            hsr.receive_request(sock)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("Missing fields ") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_alert_start_none(self):
        # create the worker object
        hsr = self.wrapped_object

        # create the alert
        alert = {
            'start_time': None,
            'stop_time': None,
            'destination_dir': None,
            'extract': False,
        }

        # initialize i3socket

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(self.make_alert_object(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("No date/time specified") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_alert_start_bad(self):
        # create the worker object
        hsr = self.wrapped_object

        # create the alert
        alert = {
            'start_time': "ABC",
            'stop_time': None,
            'destination_dir': None,
            'extract': False,
        }

        # initialize i3socket

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(self.make_alert_object(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("Problem with the time-stamp format") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_alert_stop_none(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 1234567890

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': None,
            'destination_dir': None,
            'extract': False,
        }

        # initialize i3socket

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(self.make_alert_object(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("No date/time specified") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_alert_stop_bad(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 1234567890

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': "ABC",
            'destination_dir': None,
            'extract': False,
        }

        # initialize i3socket

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(self.make_alert_object(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("Problem with the time-stamp format") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_alert_bad_copy(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = 157886964643994920

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': None,
            'extract': False,
        }

        # add all expected log messages
        self.expectLogMessage("Destination parsing failed for \"%s\":\n"
                              "Abort request." % alert["destination_dir"])

        # initialize remaining values
        logfile = None

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

    def test_huge_time_range(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.TICKS_PER_SECOND * \
            (hsr.MAX_REQUEST_SECONDS + 1)

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        errmsg = "Request for %.2fs exceeds limit of allowed data time range" \
                 " of %.2fs. Abort request..." % \
                 (hsr.MAX_REQUEST_SECONDS + 1,
                  hsr.MAX_REQUEST_SECONDS)

        # add all expected log messages
        self.expectLogMessage(errmsg)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue("ERROR: " + errmsg)

        # initialize remaining values
        logfile = None

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

    def test_not_first_current_file(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - self.ONE_MINUTE
        #last_stop = stop_ticks + (self.ONE_MINUTE * 5)
        cur_stop = start_ticks + (self.ONE_MINUTE * 60)
        #cur_start = start_ticks + (self.ONE_MINUTE * 70)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 575, 5, i3socket=hsr.i3socket,
                               finaldir=alert['destination_dir'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_not_first_last_file(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, last_stop,
                                          interval=self.INTERVAL)
        HsTestUtil.MockHitspool.add_files(hspath, cur_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 575, 5, i3socket=hsr.i3socket,
                               finaldir=alert['destination_dir'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_bad_alert_range(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks - self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        #last_stop = start_ticks - (self.ONE_MINUTE * 60)
        #cur_start = start_ticks + (self.TICKS_PER_SECOND * 5)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("sn_start & sn_stop time-stamps inverted."
                              " Abort request.")
        self.expectLogMessage("Request failed")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("alert_stop < alert_start."
                                      " Abort request.")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

    def test_partial_current_front(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 5)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, last_stop,
                                          interval=self.INTERVAL)
        HsTestUtil.MockHitspool.add_files(hspath, cur_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)
        #self.expectLogMessage("Sn_start doesn't exist in currentRun buffer"
        #                      " anymore! Start with oldest possible data: "
        #                      "HitSpool-0")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 576, 4, i3socket=hsr.i3socket,
                               finaldir=alert['destination_dir'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_between_runs(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        cur_start = start_ticks + (self.ONE_MINUTE * 5)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 10)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, last_stop,
                                          interval=self.INTERVAL)
        HsTestUtil.MockHitspool.add_files(hspath, cur_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        hsr.DEBUG_EMPTY = False

        # add all expected log messages
        self.expectLogMessage(re.compile(r"No data found between \d+ and \d+"))
        self.expectLogMessage("Request failed")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("Requested data doesn't exist anymore"
                                      " in HsBuffer. Abort request.")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

    def test_partial_last_front(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks + (self.TICKS_PER_SECOND * 5)
        last_stop = stop_ticks + (self.ONE_MINUTE * 5)
        cur_start = start_ticks + (self.ONE_MINUTE * 10)
        cur_stop = start_ticks + (self.ONE_MINUTE * 15)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, last_stop,
                                          interval=self.INTERVAL)
        HsTestUtil.MockHitspool.add_files(hspath, cur_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 576, 4, i3socket=hsr.i3socket,
                               finaldir=alert['destination_dir'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_partial_last_end(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 5)
        #last_stop = start_ticks + (self.TICKS_PER_SECOND * 5)
        #cur_start = start_ticks + self.ONE_MINUTE + self.TICKS_PER_SECOND
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 575, 5, i3socket=hsr.i3socket,
                               finaldir=alert['destination_dir'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_span_time_gap(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 5)
        #last_stop = start_ticks + (self.TICKS_PER_SECOND * 5)
        #cur_start = start_ticks + (self.TICKS_PER_SECOND * 50)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 575, 5, i3socket=hsr.i3socket,
                               finaldir=None)

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_span_link_fail(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_hardlink()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 5)
        #last_stop = start_ticks + (self.TICKS_PER_SECOND * 5)
        #cur_start = start_ticks + (self.TICKS_PER_SECOND * 50)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # TODO: should compute these file values instead of hardcoding them
        firstfile = 575
        numfiles = 5

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        for num in xrange(firstfile, firstfile + numfiles):
            self.expectLogMessage("failed to link HitSpool-%d.dat to tmp dir:"
                                  " Fake Hardlink Error" % num)
        self.expectLogMessage("No relevant files found")
        self.expectLogMessage("Request failed")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("ERROR: linking HitSpool-%d.dat to"
                                      " tmp dir failed" % 20)
        for num in xrange(firstfile, firstfile + numfiles):
            errmsg = "ERROR: linking HitSpool-%d.dat to tmp dir failed" % num
            hsr.i3socket.addExpectedValue(errmsg)
        hsr.i3socket.addExpectedValue(" TBD [MB] HS data transferred to %s " %
                                      alert['destination_dir'], prio=1)

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

    def test_before_last_start(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks + (self.ONE_MINUTE * 2)
        #last_stop = start_ticks + (self.ONE_MINUTE * 5)
        #cur_start = start_ticks + (self.ONE_MINUTE * 6)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 10)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        hsr.DEBUG_EMPTY = False

        # add all expected log messages
        self.expectLogMessage(re.compile(r"No data found between \d+ and \d+"))
        self.expectLogMessage("Request failed")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("Requested data doesn't exist anymore"
                                      " in HsBuffer. Abort request.")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

    def test_debug_empty(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks + (self.ONE_MINUTE * 2)
        #last_stop = start_ticks + (self.ONE_MINUTE * 5)
        #cur_start = start_ticks + (self.ONE_MINUTE * 6)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 10)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage(re.compile(r"No data found between \d+ and \d+"))
        self.expectLogMessage("Request failed")

        # add all expected I3Live messages
        notify_hdr = re.compile(r"Query for \[\d+-\d+\] failed on %s$" %
                                hsr.shorthost)
        notify_txt = re.compile(r"DB contains \d+ entries from .* to .*$")
        notifies = []
        for email in HsRSyncFiles.DEBUG_EMAIL:
            notifies.append({
                'notifies_txt': notify_txt,
                'notifies_header': notify_hdr,
                'receiver': email,
            })

        hsr.i3socket.addExpectedAlert({
            'condition': notify_hdr,
            'desc': "HsInterface Data Request",
            'short_subject': "true",
            'quiet': "true",
            'notifies': notifies
        }, prio=2)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("Requested data doesn't exist anymore"
                                      " in HsBuffer. Abort request.")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

    def test_case5(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        #last_stop = start_ticks - (self.ONE_MINUTE * 60)
        #cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 575, 5, i3socket=hsr.i3socket,
                               finaldir=alert['destination_dir'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_case5_link_fail(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_hardlink()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        #last_stop = start_ticks - (self.ONE_MINUTE * 60)
        #cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # TODO: should compute these file values instead of hardcoding them
        firstfile = 575
        numfiles = 5

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        for num in xrange(firstfile, firstfile + numfiles):
            self.expectLogMessage("failed to link HitSpool-%d.dat to tmp dir:"
                                  " Fake Hardlink Error" % num)
        self.expectLogMessage("No relevant files found")
        self.expectLogMessage("Request failed")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        for num in xrange(firstfile, firstfile + numfiles):
            errmsg = "ERROR: linking HitSpool-%d.dat to tmp dir failed" % num
            hsr.i3socket.addExpectedValue(errmsg)
        hsr.i3socket.addExpectedValue(" TBD [MB] HS data transferred to %s " %
                                      alert['destination_dir'], prio=1)

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

    def test_alert_in_future(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        #last_stop = start_ticks - (self.ONE_MINUTE * 6)
        #cur_start = start_ticks - (self.ONE_MINUTE * 5)
        cur_stop = stop_ticks - (self.ONE_MINUTE * 2)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        hsr.DEBUG_EMPTY = False

        # add all expected log messages
        self.expectLogMessage(re.compile(r"No data found between \d+ and \d+"))
        self.expectLogMessage("Request failed")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("Requested data is younger than most"
                                      " recent HS data. Abort request.")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

    def test_penultimate(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + (self.TICKS_PER_SECOND * 3)

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks + (self.TICKS_PER_SECOND * 1)
        #last_stop = start_ticks + (self.TICKS_PER_SECOND * 2)
        #cur_start = start_ticks + (self.TICKS_PER_SECOND * 4)
        cur_stop = start_ticks + (self.TICKS_PER_SECOND * 4)
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 575, 1, i3socket=hsr.i3socket,
                               finaldir=alert['destination_dir'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_fail_rsync(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_rsync()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        #last_stop = start_ticks - (self.ONE_MINUTE * 6)
        #cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)
        self.expectLogMessage("Request failed")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 575, 5, i3socket=hsr.i3socket,
                               finaldir=None)

        hsr.i3socket.addExpectedValue("ERROR in rsync. Keep tmp dir.")
        hsr.i3socket.addExpectedValue("FakeFail")

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_works(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = {
            'start_time': str(start_ticks / 10),
            'stop_time': str(stop_ticks / 10),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        #last_stop = start_ticks - (self.ONE_MINUTE * 6)
        #cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 575, 5, i3socket=hsr.i3socket,
                               finaldir=alert['destination_dir'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()

    def test_works_datestring(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643990000
        stop_ticks = start_ticks + self.ONE_MINUTE

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # create the alert
        alert = {
            'start_time': str(utcstart),
            'stop_time': str(utcstop),
            'destination_dir': "/foo/bar",
            'extract': False,
        }

        # create hitspool directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        #last_stop = start_ticks - (self.ONE_MINUTE * 6)
        #cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE
        hspath = HsTestUtil.MockHitspool.create(hsr)
        HsTestUtil.MockHitspool.add_files(hspath, last_start, cur_stop,
                                          interval=self.INTERVAL)

        # create copy directory
        HsTestUtil.MockHitspool.create_copy_dir(hsr)

        # set log file name
        logfile = "unknown.log"

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, 575, 5, i3socket=hsr.i3socket,
                               finaldir=alert['destination_dir'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # test parser
        hsr.alert_parser(self.make_alert_object(alert), logfile,
                         delay_rsync=False)

        hsr.validate()


if __name__ == '__main__':
    unittest.main()
