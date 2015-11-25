#!/usr/bin/env python

import json
import logging
import os
import re
import shutil
import sys
import tempfile
import unittest

import HsWorker
import HsTestUtil

from HsException import HsException
from LoggingTestCase import LoggingTestCase


class MockSenderSocket(HsTestUtil.Mock0MQSocket):
    def __init__(self):
        super(MockSenderSocket, self).__init__("Sender")

    def send_json(self, jstr):
        if not isinstance(jstr, str):
            raise Exception("Got non-string JSON object %s<%s>" %
                            (jstr, type(jstr)))

        super(MockSenderSocket, self).send_json(json.loads(jstr))


class MyWorker(HsWorker.Worker):
    def __init__(self):
        super(MyWorker, self).__init__("HsWorker", is_test=True)

        self.__link_paths = []
        self.__fail_hardlink = False
        self.__fail_rsync = False

    @classmethod
    def __timetag(cls, starttime):
        return starttime.strftime("%Y%m%d_%H%M%S")

    def add_expected_links(self, start_utc, rundir, firstnum, numfiles,
                           i3socket=None, finaldir=None):
        timetag = self.__timetag(start_utc)
        for i in xrange(firstnum, firstnum + numfiles):
            frompath = os.path.join(self.TEST_HUB_DIR, rundir,
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
        return MockSenderSocket()

    def create_subscriber_socket(self, host):
        return HsTestUtil.Mock0MQSocket("Subscriber")

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

    def rsync(self, source, target, bwlimit=None, log_format=None,
              relative=True):
        if self.__fail_rsync:
            return ([], "FakeFail")
        return (["", ], "")


class HsWorkerTest(LoggingTestCase):
    # pylint: disable=too-many-public-methods
    # Really?!?!  In a test class?!?!  Shut up, pylint!

    TICKS_PER_SECOND = 10000000000
    INTERVAL = 15 * TICKS_PER_SECOND
    ONE_MINUTE = 60 * TICKS_PER_SECOND

    HUB_DIR = None
    COPY_DIR = None

    JAN1 = None

    RCV_REQ_PAT = re.compile(r"received request at \S+ \S+")

    def __create_copydir(self, hsr):
        if self.COPY_DIR is None:
            # create temporary copy directory and set in HsWorker
            self.COPY_DIR = tempfile.mkdtemp()
            hsr.TEST_COPY_DIR = self.COPY_DIR

    def __populate_run(self, hsr, subdir, t0, t_cur, interval=INTERVAL,
                       max_f=1000, make_bad=False, debug=False):
        if self.HUB_DIR is None:
            # create temporary hub directory and set in HsRSyncFiles
            self.HUB_DIR = tempfile.mkdtemp()
            hsr.TEST_HUB_DIR = self.HUB_DIR

        # create subdir if necessary
        path = os.path.join(self.HUB_DIR, subdir)
        if not os.path.exists(path):
            os.makedirs(path)

        # compute "current file"
        cur_f = int((t_cur - t0) / interval)

        # create info.txt
        infopath = os.path.join(path, "info.txt")
        with open(infopath, "w") as fout:
            if not make_bad:
                print >>fout, "T0 %d" % t0
            print >>fout, "CURT %d" % t_cur
            print >>fout, "IVAL %d" % interval
            print >>fout, "CURF %d" % cur_f
            print >>fout, "MAXF %d" % max_f

        if debug:
            print >>sys.stderr, "=== %s" % infopath
            print >>sys.stderr, "=== start %d :: %s" % \
                (t0, HsTestUtil.get_time(t0))
            print >>sys.stderr, "=== stop  %d :: %s" % \
                (t_cur, HsTestUtil.get_time(t_cur))
            with open(infopath, "r") as fin:
                for line in fin:
                    print >>sys.stderr, line,

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
            if self.COPY_DIR is not None:
                # clear lingering files
                shutil.rmtree(self.COPY_DIR)
            if self.HUB_DIR is not None:
                # clear lingering files
                shutil.rmtree(self.HUB_DIR)

    def test_nothing(self):
        # create the worker object
        hsr = self.real_object

        # no alert
        alert = None

        # initialize remaining values
        logfile = None

        # check all log messages
        self.setLogLevel(0)

        # test parser
        try:
            hsr.alert_parser(json.dumps(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("TypeError: ") >= 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_bad_alert_str(self):
        # create the worker object
        hsr = self.wrapped_object

        # bad alert
        alert = "foo"

        # check all log messages
        self.setLogLevel(0)

        # add all expected log messages
        self.expectLogMessage("Ignoring all but first of %d alerts" %
                              len(alert))

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(json.dumps(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("ValueError: ") >= 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_bad_alert_empty(self):
        # create the worker object
        hsr = self.wrapped_object

        # empty alert
        alert = []

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(json.dumps(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("IndexError: ") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_bad_alert_dict(self):
        # create the worker object
        hsr = self.wrapped_object

        # bad alert
        alert = {}

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(json.dumps(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("KeyError: ") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_alert_start_none(self):
        # create the worker object
        hsr = self.wrapped_object

        # create the alert
        alert = [{
            'start': None,
            'stop': None,
            'copy': None,
        },]

        # initialize i3socket

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(json.dumps(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("No date/time specified") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_alert_stop_none(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 1234567890

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': None,
            'copy': None,
        },]

        # initialize i3socket

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(json.dumps(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("No date/time specified") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_alert_start_bad(self):
        # create the worker object
        hsr = self.wrapped_object

        # create the alert
        alert = [{
            'start': "ABC",
            'stop': None,
            'copy': None,
        },]

        # initialize i3socket

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(json.dumps(alert), logfile)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("Problem with the time-stamp format") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_alert_stop_bad(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 1234567890

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': "ABC",
            'copy': None,
        },]

        # initialize i3socket

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(json.dumps(alert), logfile)
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
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': None,
        },]

        # initialize i3socket

        # check all log messages
        self.setLogLevel(0)

        # initialize remaining values
        logfile = None

        # test parser
        try:
            hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)
            self.fail("This method should fail")
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("Copy directory must be set") < 0:
                self.fail("Unexpected exception: " + hsestr)

    def test_huge_time_range(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.TICKS_PER_SECOND * \
                     (HsWorker.MAX_REQUEST_SECONDS + 1)

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        errmsg = "Request for %.2fs exceeds limit of allowed data time range" \
                 " of %.2fs. Abort request..." % \
                 (HsWorker.MAX_REQUEST_SECONDS + 1,
                  HsWorker.MAX_REQUEST_SECONDS)

        # add all expected log messages
        self.expectLogMessage(errmsg)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(re.compile(r"received request at"
                                                 r" \S+ \S+"))
        hsr.i3socket.addExpectedValue("ERROR: " + errmsg)

        # initialize remaining values
        logfile = None

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_no_info_txt(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)
        self.expectLogMessage("CurrentRun info.txt reading/parsing failed")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue('ERROR: Current Run info.txt'
                                      ' reading/parsing failed')

        # initialize remaining values
        logfile = None

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_bad_last_run(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        self.__populate_run(hsr, "lastRun", last_start, last_stop,
                            make_bad=True)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)
        self.expectLogMessage("LastRun info.txt reading/parsing failed")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue('ERROR: Last Run info.txt'
                                      ' reading/parsing failed')

        # initialize remaining values
        logfile = "unknown.log"

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_bad_current_run(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop,
                            make_bad=True)

        # create copy directory
        self.__create_copydir(hsr)

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)
        self.expectLogMessage("CurrentRun info.txt reading/parsing failed")


        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue('ERROR: Current Run info.txt'
                                      ' reading/parsing failed')

        # initialize remaining values
        logfile = "unknown.log"

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_not_first_current_file(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - self.ONE_MINUTE
        last_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_stop = start_ticks + (self.ONE_MINUTE * 60)
        cur_start = start_ticks + (self.ONE_MINUTE * 70)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop,
                            interval=self.INTERVAL / 100)

        # create copy directory
        self.__create_copydir(hsr)

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
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "lastRun", 4, 5,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_not_first_last_file(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        self.__populate_run(hsr, "lastRun", last_start, last_stop,
                            interval=self.INTERVAL / 100)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "currentRun", 4, 5,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_bad_alert_range(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks - self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 5)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

        # set log file name
        logfile = "unknown.log"

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("sn_start & sn_stop time-stamps inverted."
                              " Abort request.")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("alert_stop < alert_start."
                                      " Abort request.")

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_partial_current_front(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 5)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        self.expectLogMessage("Sn_start doesn't exist in currentRun buffer"
                              " anymore! Start with oldest possible data: "
                              "HitSpool-0")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "currentRun", 0, 4,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_between_runs(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks + (self.ONE_MINUTE * 5)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 10)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        self.expectLogMessage("Requested data doesn't exist in HitSpool"
                              " Buffer anymore! Abort request.")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("Requested data doesn't exist anymore"
                                      " in HsBuffer. Abort request.")

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_partial_last_front(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks + (self.TICKS_PER_SECOND * 5)
        last_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks + (self.ONE_MINUTE * 10)
        cur_stop = start_ticks + (self.ONE_MINUTE * 15)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        self.expectLogMessage("sn_start doesn't exist in lastRun buffer"
                              " anymore! Start with oldest possible data:"
                              " HitSpool-0")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "lastRun", 0, 4,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_partial_last_end(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 5)
        last_stop = start_ticks + (self.TICKS_PER_SECOND * 5)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks + self.ONE_MINUTE + self.TICKS_PER_SECOND
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "lastRun", 20, 1,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_span_time_gap(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 5)
        last_stop = start_ticks + (self.TICKS_PER_SECOND * 5)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 50)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "lastRun", 20, 1,
                               i3socket=hsr.i3socket,
                               finaldir=None)
        hsr.add_expected_links(utcstart, "currentRun", 0, 1,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_span_link_fail(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_hardlink()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        }]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 5)
        last_stop = start_ticks + (self.TICKS_PER_SECOND * 5)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 50)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

        # set log file name
        logfile = "unknown.log"

        # TODO: should compute these file values instead of hardcoding them
        firstfile = 0
        numfiles = 1

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)
        self.expectLogMessage("failed to link HitSpool-20.dat to tmp dir:"
                              " Fake Hardlink Error")
        for num in xrange(firstfile, firstfile + numfiles):
            self.expectLogMessage("failed to link HitSpool-%d.dat to tmp dir:"
                                  " Fake Hardlink Error" % num)
        self.expectLogMessage("No relevant files found")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
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
        hsr.i3socket.addExpectedValue(" TBD [MB] HS data transferred"
                                      " to %s " % alert[0]['copy'], prio=1)

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_before_last_start(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks + (self.ONE_MINUTE * 2)
        last_stop = start_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks + (self.ONE_MINUTE * 6)
        cur_stop = stop_ticks + (self.ONE_MINUTE * 10)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        self.expectLogMessage("Requested data doesn't exist in HitSpool"
                              " Buffer anymore! Abort request.")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("Requested data doesn't exist anymore"
                                      " in HsBuffer. Abort request.")

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_case5(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "currentRun", 4, 5,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_case5_link_fail(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_hardlink()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 70)
        last_stop = start_ticks - (self.ONE_MINUTE * 60)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + (self.ONE_MINUTE * 5)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

        # set log file name
        logfile = "unknown.log"

        # TODO: should compute these file values instead of hardcoding them
        firstfile = 4
        numfiles = 5

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)
        for num in xrange(firstfile, firstfile + numfiles):
            self.expectLogMessage("failed to link HitSpool-%d.dat to tmp dir:"
                                  " Fake Hardlink Error" % num)
        self.expectLogMessage("No relevant files found")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        for num in xrange(firstfile, firstfile + numfiles):
            errmsg = "ERROR: linking HitSpool-%d.dat to tmp dir failed" % num
            hsr.i3socket.addExpectedValue(errmsg)
        hsr.i3socket.addExpectedValue(" TBD [MB] HS data transferred"
                                      " to %s " % alert[0]['copy'], prio=1)

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_alert_in_future(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks - (self.ONE_MINUTE * 5)
        cur_stop = stop_ticks - (self.ONE_MINUTE * 2)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        self.expectLogMessage("alert_start is in the FUTURE ?!")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })
        hsr.i3socket.addExpectedValue("Requested data is younger than most"
                                      " recent HS data. Abort request.")

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

    def test_penultimate(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + (self.TICKS_PER_SECOND * 3)

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks + (self.TICKS_PER_SECOND * 1)
        last_stop = start_ticks + (self.TICKS_PER_SECOND * 2)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks + (self.TICKS_PER_SECOND * 4)
        cur_stop = start_ticks + (self.TICKS_PER_SECOND * 4)
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        self.expectLogMessage("alert_start < lastRun < alert_stop < currentRun."
                              " Assign: all HS data of lastRun instead.")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "lastRun", 0, 1,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_fail_rsync(self):
        # create the worker object
        hsr = self.wrapped_object
        hsr.fail_rsync()

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        self.expectLogMessage("failed rsync process:\nFakeFail")

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "currentRun", 4, 5,
                               i3socket=hsr.i3socket,
                               finaldir=None)

        hsr.i3socket.addExpectedValue("ERROR in rsync. Keep tmp dir.")
        hsr.i3socket.addExpectedValue("FakeFail")

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": 0,
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_works(self):
        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643994920
        stop_ticks = start_ticks + self.ONE_MINUTE

        # create the alert
        alert = [{
            'start': str(start_ticks / 10),
            'stop': str(stop_ticks / 10),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

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
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "currentRun", 4, 5,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()

    def test_works_datestring(self):
        #self.setVerbose()
        #self.setLogLevel(logging.INFO)
        #ANY_LOG = re.compile(r"^.*$")
        #for i in range(100):
        #    self.expectLogMessage(ANY_LOG)

        # create the worker object
        hsr = self.wrapped_object

        # define alert times
        start_ticks = 157886364643990000
        stop_ticks = start_ticks + self.ONE_MINUTE

        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # create the alert
        alert = [{
            'start': str(utcstart),
            'stop': str(utcstop),
            'copy': "/foo/bar",
        },]

        # create lastRun directory
        last_start = start_ticks - (self.ONE_MINUTE * 10)
        last_stop = start_ticks - (self.ONE_MINUTE * 6)
        self.__populate_run(hsr, "lastRun", last_start, last_stop)

        # create currentRun directory
        cur_start = start_ticks - self.ONE_MINUTE
        cur_stop = stop_ticks + self.ONE_MINUTE
        self.__populate_run(hsr, "currentRun", cur_start, cur_stop)

        # create copy directory
        self.__create_copydir(hsr)

        # set log file name
        logfile = "unknown.log"

        # add all expected log messages
        self.expectLogMessage("Requested HS data copy destination differs"
                              " from default!")
        self.expectLogMessage("data will be sent to default destination: %s" %
                              hsr.TEST_COPY_DIR)

        # add all expected I3Live messages
        hsr.i3socket.addExpectedValue(self.RCV_REQ_PAT)
        hsr.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        # TODO should compute the expected number of files
        hsr.add_expected_links(utcstart, "currentRun", 4, 5,
                               i3socket=hsr.i3socket,
                               finaldir=alert[0]['copy'])

        # reformat time string for file names
        timetag = utcstart.strftime("%Y%m%d_%H%M%S")

        # build copy/log directories passed to HsSender
        copydir = os.path.join(self.COPY_DIR,
                               "ANON_%s_%s" % (timetag, hsr.fullhost))
        logfiledir = os.path.join(self.COPY_DIR, "logs")

        # initialize hsr.sender.socket and add all expected HsSender messages
        hsr.sender.addExpected({"hubname": hsr.shorthost,
                                "dataload": "TBD",
                                "datastart": str(utcstart),
                                "alertid": timetag,
                                "datastop": str(utcstop),
                                "msgtype": "rsync_sum",
                                "copydir_user": alert[0]['copy'],
                                "copydir": copydir,
                               })
        hsr.sender.addExpected({"msgtype": "log_done",
                                "logfile_hsworker": logfile,
                                "hubname": hsr.shorthost,
                                "logfiledir": logfiledir,
                                "alertid": timetag,
                               })

        # test parser
        hsr.alert_parser(json.dumps(alert), logfile, sleep_secs=0)

        hsr.check_for_unused_links()


if __name__ == '__main__':
    unittest.main()
