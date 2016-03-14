#!/usr/bin/env python

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

    def validate(self):
        self.close_all()
        val = self.grabber.validate()
        val |= self.poller.validate()
        return val


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
        alert_start_ns = 1578900679602462
        alert_start_utc = HsTestUtil.get_time(alert_start_ns, is_sn_ns=True)
        alert_stop_ns = alert_start_ns - 1000000000
        alert_stop_utc = HsTestUtil.get_time(alert_stop_ns, is_sn_ns=True)
        copydir = None

        # create the grabber object
        hsg = MyGrabber()

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        msg = "Requesting negative time range (%.2f).\n" \
              "Try another time window." % \
              ((alert_stop_ns - alert_start_ns) / 1E9)
        self.expectLogMessage(msg)

        # run it!
        hsg.send_alert(alert_start_ns, alert_start_utc, alert_stop_ns,
                       alert_stop_utc, copydir, print_to_console=False)

        hsg.validate()

    def test_nonstandard_time(self):
        alert_start_ns = 1578900679602462
        alert_start_utc = HsTestUtil.get_time(alert_start_ns, is_sn_ns=True)
        alert_stop_ns = alert_start_ns + \
            1E9 * (HsGrabber.STD_SECONDS + 1)
        alert_stop_utc = HsTestUtil.get_time(alert_stop_ns, is_sn_ns=True)
        copydir = "/not/valid/path"

        secrange = (alert_stop_ns - alert_start_ns) / 1E9

        # create the alert
        alert = {"start": alert_start_ns,
                 "stop": alert_stop_ns,
                 "copy": copydir}

        # create the grabber object
        hsg = MyGrabber()

        # add all JSON and response messages
        hsg.grabber.addExpected(alert)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        msg = "Warning: You are requesting %.2f seconds of data\n" \
              "Normal requests are %d seconds or less" % \
              (secrange, HsGrabber.STD_SECONDS)
        self.expectLogMessage(msg)

        # run it!
        hsg.send_alert(alert_start_ns, alert_start_utc, alert_stop_ns,
                       alert_stop_utc, copydir, print_to_console=False)

        hsg.validate()

        # add grabber to poller socket
        hsg.poller.addPollResult(hsg.grabber)

        # add final grabber response
        hsg.grabber.addIncoming("DONE\0")

        timeout = 1
        hsg.wait_for_response(timeout=timeout, print_to_console=False)

    def test_huge_time(self):
        alert_start_ns = 1578900679602462
        alert_start_utc = HsTestUtil.get_time(alert_start_ns, is_sn_ns=True)
        alert_stop_ns = alert_start_ns + \
            1E9 * (HsGrabber.MAX_SECONDS + 1)
        alert_stop_utc = HsTestUtil.get_time(alert_stop_ns, is_sn_ns=True)
        copydir = None

        secrange = (alert_stop_ns - alert_start_ns) / 1E9

        # create the grabber object
        hsg = MyGrabber()

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        msg = "Request for %.2f seconds is too huge.\n" \
              "HsWorker processes request only up to %d seconds.\n" \
              "Try a smaller time window." % \
              (secrange, HsGrabber.MAX_SECONDS)
        self.expectLogMessage(msg)

        # run it!
        hsg.send_alert(alert_start_ns, alert_start_utc, alert_stop_ns,
                       alert_stop_utc, copydir, print_to_console=False)

        hsg.validate()

    def test_timeout(self):
        # create the grabber object
        hsg = MyGrabber()

        # add grabber to poller socket
        hsg.poller.addPollResult(None)
        hsg.poller.addPollResult(None)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # change the timeout
        timeout = 1

        # add all expected log messages
        self.expectLogMessage("no connection to expcont's HsPublisher"
                              " within %s seconds.\nAbort request." % timeout)

        # run it!
        hsg.wait_for_response(timeout=timeout, print_to_console=False)

        hsg.validate()

    def test_working(self):
        alert_start_ns = 1578900679602462
        alert_start_utc = HsTestUtil.get_time(alert_start_ns, is_sn_ns=True)
        alert_stop_ns = 1578906679602462
        alert_stop_utc = HsTestUtil.get_time(alert_stop_ns, is_sn_ns=True)
        copydir = "/somewhere/else"

        # create the alert
        alert = {"start": alert_start_ns,
                 "stop": alert_stop_ns,
                 "copy": copydir}

        # create the grabber object
        hsg = MyGrabber()

        # add all JSON and response messages
        hsg.grabber.addExpected(alert)
        hsg.grabber.addIncoming("DONE\0")

        # add grabber to poller socket
        hsg.poller.addPollResult(hsg.grabber)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        hsg.send_alert(alert_start_ns, alert_start_utc, alert_stop_ns,
                       alert_stop_utc, copydir, print_to_console=False)

        timeout = 1
        hsg.wait_for_response(timeout=timeout, print_to_console=False)
        hsg.validate()


if __name__ == '__main__':
    unittest.main()
