#!/usr/bin/env python

import json
import logging
import unittest

import HsGrabber
import HsTestUtil

from LoggingTestCase import LoggingTestCase


class MyGrabber(HsGrabber.HsGrabber):
    def __init__(self):
        super(MyGrabber, self).__init__()

    def create_grabber(self, host):
        return HsTestUtil.Mock0MQSocket("Grabber")

    def create_poller(self, grabber):
        return HsTestUtil.Mock0MQSocket("Poller")

    def create_i3socket(self, host):
        return HsTestUtil.MockI3Socket("HsGrabber")

    def validate(self):
        self.grabber().validate()
        self.poller().validate()
        self.i3socket().validate()

class HsGrabberTest(LoggingTestCase):
    def setUp(self):
        super(HsGrabberTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)

    def tearDown(self):
        try:
            super(HsGrabberTest, self).tearDown()
        finally:
            pass

    def test_negative_time(self):
        timeout = 1
        alert_start_sn = 1578900679602462
        alert_stop_sn = alert_start_sn - 1000000000
        copydir = None

        secrange = (alert_stop_sn - alert_start_sn) / 1E9

        # create the grabber object
        hsg = MyGrabber()

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Requesting negative time range (%.2f)."
                              " Try another time window." % secrange)

        # run it!
        hsg.send_alert(timeout, alert_start_sn, alert_stop_sn, copydir,
                       print_dots=False)

        hsg.validate()

    def test_huge_time(self):
        timeout = 1
        alert_start_sn = 1578900679602462
        alert_stop_sn = alert_start_sn + \
                        10000000000 * (HsGrabber.MAX_SECONDS + 1)
        copydir = None

        secrange = (alert_stop_sn - alert_start_sn) / 1E9

        # create the grabber object
        hsg = MyGrabber()

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Request for %.2f seconds is too huge.\n"
                              "HsWorker processes request only up to %d sec.\n"
                              "Try a smaller time window." %
                              (secrange, HsGrabber.MAX_SECONDS))

        # run it!
        hsg.send_alert(timeout, alert_start_sn, alert_stop_sn, copydir,
                       print_dots=False)

        hsg.validate()

    def test_timeout(self):
        timeout = 1
        alert_start_sn = 1578900679602462
        alert_stop_sn = 1578906679602462
        copydir = None

        # create the alert
        alert = {"start": alert_start_sn,
                 "stop": alert_stop_sn,
                 "copy": copydir}

        # create the grabber object
        hsg = MyGrabber()

        # add all JSON and response messages
        hsg.grabber().addExpected(json.dumps(alert))

        # add grabber to poller socket
        hsg.poller().addPollResult(None)
        hsg.poller().addPollResult(None)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("no connection to expcont's HsPublisher"
                              " within %s seconds.\nAbort request." % timeout)

        # run it!
        try:
            hsg.send_alert(timeout, alert_start_sn, alert_stop_sn, copydir,
                           print_dots=False)
            self.fail("Expected alert to call sys.exit(1)")
        except SystemExit:
            pass

        hsg.validate()

    def test_working(self):
        timeout = 1
        alert_start_sn = 1578900679602462
        alert_stop_sn = 1578906679602462
        copydir = None

        # create the alert
        alert = {"start": alert_start_sn,
                 "stop": alert_stop_sn,
                 "copy": copydir}

        # create the grabber object
        hsg = MyGrabber()

        # add all JSON and response messages
        hsg.grabber().addExpected(json.dumps(alert))
        hsg.grabber().addIncoming("DONE\0")

        # add grabber to poller socket
        hsg.poller().addPollResult(hsg.grabber())

        # add all expected I3Live messages
        hsg.i3socket().addExpectedValue("pushed request")

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        try:
            hsg.send_alert(timeout, alert_start_sn, alert_stop_sn, copydir,
                           print_dots=False)
            self.fail("Expected alert to call sys.exit(1)")
        except SystemExit:
            pass

        hsg.validate()


if __name__ == '__main__':
    unittest.main()
