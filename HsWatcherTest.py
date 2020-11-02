#!/usr/bin/env python
"""
Test the HsWatcher class
"""

import logging
import re
import unittest

from datetime import datetime

import HsConstants
import HsTestUtil
import HsWatcher

from HsException import HsException
from LoggingTestCase import LoggingTestCase


class MyWatchee(HsWatcher.Watchee):
    "Extend Watchee and/or stub out some methods for unit tests"

    def __init__(self, basename):
        self.__pid = None
        self.__daemon_pid = None
        self.__extra_pid = None

        super(MyWatchee, self).__init__(basename)

    def check_executable(self, basename, executable):
        pass

    def daemonize(self, stdin=None, stdout=None, stderr=None):
        if self.__daemon_pid is not None:
            self.__pid = self.__daemon_pid
            self.__daemon_pid = None

    def list_processes(self):
        for pid in (self.__pid, self.__extra_pid):
            if pid is not None:
                yield (pid, self.basename, None)

    def set_daemon_pid(self, pid):
        self.__daemon_pid = pid

    def set_extra_pid(self, pid):
        self.__extra_pid = pid

    def set_pid(self, pid):
        self.__pid = pid


class MyWatcher(HsWatcher.HsWatcher):
    "Extend HsWatcher and/or stub out some methods for unit tests"

    def __init__(self, host=None):
        self.__i3_sock = None
        self.__loglines = []
        self.__watchee = None

        super(MyWatcher, self).__init__(host=host)

    def add_log_status(self, msg, time=None):
        if self.__loglines is None:
            self.__loglines = []
        if time is None:
            time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.__loglines.append("%s INFO Status: %s" % (time, msg))

    def create_i3socket(self, host):
        if self.__i3_sock is not None:
            raise Exception("Cannot create multiple I3 sockets")

        self.__i3_sock = HsTestUtil.MockI3Socket('HsPublisher')
        return self.__i3_sock

    def create_watchee(self, basename):
        if self.__watchee is None:
            self.__watchee = MyWatchee(basename)

        return self.__watchee

    def logline(self, idx):
        if self.__loglines is None or len(self.__loglines) <= idx:
            raise HsException("Bad log index %d (%d available)" %
                              (idx, 0 if self.__loglines is None else
                               len(self.__loglines)))
        return self.__loglines[idx]

    def set_watchee(self, watchee):
        self.__watchee = watchee

    def tail(self, _, lines=None, search_string=None):
        return self.__loglines

    def validate(self):
        self.close_all()

        for sock in (self.__i3_sock, ):
            if sock is not None:
                sock.validate()


class HsWatcherTest(LoggingTestCase):
    "Test the HsWatcher class"

    @classmethod
    def __add_alert(cls, watcher, program, alert_type, desc, alertmsg):
        notify_hdr = '%s HsInterface Alert: %s@%s' % \
            (alert_type, program, watcher.shorthost)

        watcher.i3socket.add_generic_email(HsConstants.ALERT_EMAIL_DEV,
                                           notify_hdr, alertmsg,
                                           description=desc)

    def setUp(self):
        super(HsWatcherTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)

    def tearDown(self):
        try:
            super(HsWatcherTest, self).tearDown()
        finally:
            pass

    def test_bad_host(self):
        watcher = MyWatcher(host="badhost")

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        try:
            watcher.get_watchee()
            self.fail("Bad host should fail")
        except HsException:
            pass

    def test_sender_simple(self):
        watcher = MyWatcher(host="fake_2ndbuild")
        watcher.get_watchee().set_pid(1234)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        watcher.check("/bad/path/to/log", sleep_secs=0.0)

        watcher.validate()

    def test_publisher_simple(self):
        watcher = MyWatcher(host="fake_expcont")
        watcher.get_watchee().set_pid(1234)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        watcher.check("/bad/path/to/log", sleep_secs=0.0)

        watcher.validate()

    def test_worker_simple(self):
        watcher = MyWatcher(host="fake_hub")
        watcher.get_watchee().set_pid(1234)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        watcher.check("/bad/path/to/log", sleep_secs=0.0)

        watcher.validate()

    def test_worker_started(self):
        host = "fake_hub"
        program = "HsXXX"

        watcher = MyWatcher(host=host)

        watchee = MyWatchee(program)
        watchee.set_daemon_pid(666)

        watcher.set_watchee(watchee)

        watcher.add_log_status(watcher.STATUS_STARTED)

        desc = "HsInterface service recovery notice"
        alertmsg = "%s@%s recovered by HsWatcher:\n%s" % \
            (program, host, watcher.logline(0))
        self.__add_alert(watcher, program, "RECOVERY", desc, alertmsg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        watcher.check("/bad/path/to/log", sleep_secs=0.0)

        watcher.validate()

    def test_worker_recovery(self):
        host = "fake_hub"
        program = "HsXXX"

        watcher = MyWatcher(host=host)

        watchee = MyWatchee(program)
        watchee.set_daemon_pid(666)

        watcher.set_watchee(watchee)

        watcher.add_log_status(watcher.STATUS_STOPPED)
        watcher.add_log_status(watcher.STATUS_STARTED)

        desc = "HsInterface service recovery notice"
        alertmsg = "%s@%s recovered by HsWatcher:\n%s\n%s" % \
            (program, host, watcher.logline(0), watcher.logline(1))
        self.__add_alert(watcher, program, "RECOVERY", desc, alertmsg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        watcher.check("/bad/path/to/log", sleep_secs=0.0)

        watcher.validate()

    def test_worker_stopped(self):
        host = "fake_hub"
        program = "HsXXX"

        watcher = MyWatcher(host=host)

        watchee = MyWatchee(program)

        watcher.set_watchee(watchee)

        watcher.add_log_status(watcher.STATUS_STOPPED, time="XXX")
        watcher.add_log_status(watcher.STATUS_STOPPED, time="XXX")

        desc = "HsInterface service stopped"
        alertmsg = "%s@%s is STOPPED" % (program, host)
        self.__add_alert(watcher, program, "STOPPED", desc, alertmsg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        watcher.check("/bad/path/to/log", sleep_secs=0.0)

        watcher.validate()

    def test_worker_halted_bad(self):
        host = "fake_hub"
        program = "HsXXX"

        watcher = MyWatcher(host=host)

        watchee = MyWatchee(program)

        watcher.set_watchee(watchee)

        for _ in range(4):
            watcher.add_log_status(watcher.STATUS_STOPPED, time="XXX")

        desc = "HsInterface service halted"
        alertmsg = "%s@%s in STOPPED state more than 1h.\n" \
            "Last seen running before %s" % (program, host, "???")
        self.__add_alert(watcher, program, "HALTED", desc, alertmsg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        watcher.check("/bad/path/to/log", sleep_secs=0.0)

        watcher.validate()

    def test_worker_halted_good(self):
        host = "fake_hub"
        program = "HsXXX"

        watcher = MyWatcher(host=host)

        watchee = MyWatchee(program)

        watcher.set_watchee(watchee)

        for _ in range(4):
            watcher.add_log_status(watcher.STATUS_STOPPED)

        desc = "HsInterface service halted"
        alertmsg = re.compile(r"%s@%s in STOPPED state more than 1h.\n"
                              r"Last seen running before .*" %
                              (program, host))
        self.__add_alert(watcher, program, "HALTED", desc, alertmsg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        watcher.check("/bad/path/to/log", sleep_secs=0.0)

        watcher.validate()

    def test_worker_multistart(self):
        host = "fake_hub"
        program = "HsXXX"

        watcher = MyWatcher(host=host)

        watchee = MyWatchee(program)
        watchee.set_pid(1234)
        watchee.set_daemon_pid(666)
        watchee.set_extra_pid(2345)

        watcher.set_watchee(watchee)

        watcher.add_log_status(watcher.STATUS_STARTED)

        self.expect_log_message("Found multiple copies of %s;"
                                " killing everything!" % program)
        self.expect_log_message("Found multiple copies of %s after starting" %
                                program)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        desc = "HsInterface service error notice"
        alertmsg = "Found multiple copies of %s after starting" % program
        self.__add_alert(watcher, program, "ERROR", desc, alertmsg)

        watcher.check("/bad/path/to/log", sleep_secs=0.0)

        watcher.validate()

    # pylint: disable=no-self-use
    def test_kill_none(self):
        watchee = MyWatchee("HsNope")
        watchee.kill_all()


if __name__ == '__main__':
    unittest.main()
