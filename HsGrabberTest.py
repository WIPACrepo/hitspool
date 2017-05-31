#!/usr/bin/env python

import getpass
import logging
import re
import unittest

import HsBase
import HsGrabber
import HsMessage
import HsTestUtil

from HsPrefix import HsPrefix

from LoggingTestCase import LoggingTestCase


class MyGrabber(HsGrabber.HsGrabber):
    def __init__(self):
        super(MyGrabber, self).__init__()

    def create_poller(self, sockets):
        return HsTestUtil.Mock0MQPoller("Poller")

    def create_publisher(self, host):
        return HsTestUtil.Mock0MQSocket("Publisher")

    def create_sender(self, host):
        return HsTestUtil.Mock0MQSocket("Sender")

    def validate(self):
        self.close_all()
        val = self.publisher.validate()
        val = self.sender.validate()
        val |= self.poller.validate()
        return val


class HsGrabberTest(LoggingTestCase):
    MATCH_ANY = re.compile(r"^.*$")

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
        start_time = HsBase.DAQTime(15789006796024620)
        stop_time = HsBase.DAQTime(start_time.ticks - 1000000000)
        copydir = None

        # create the grabber object
        hsg = MyGrabber()

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        msg = "Requesting negative time range (%.2f).\n" \
              "Try another time window." % \
              ((stop_time.ticks - start_time.ticks) / 1E10)
        self.expectLogMessage(msg)

        # run it!
        hsg.send_alert(start_time, stop_time, copydir, print_to_console=False)

        hsg.validate()

    def test_nonstandard_time(self):
        start_time = HsBase.DAQTime(15789006796024620)
        stop_ticks = start_time.ticks + 1E10 * (HsGrabber.WARN_SECONDS + 1)
        stop_time = HsBase.DAQTime(stop_ticks)
        copydir = "/not/valid/path"

        secrange = (stop_time.ticks - start_time.ticks) / 1E10

        # create the grabber object
        hsg = MyGrabber()

        # add all JSON and response messages
        expected = {
            "start_time": start_time.ticks,
            "stop_time": stop_time.ticks,
            "destination_dir": copydir,
            "prefix": HsPrefix.ANON,
            "request_id": self.MATCH_ANY,
            "msgtype": HsMessage.MESSAGE_INITIAL,
            "version": HsMessage.DEFAULT_VERSION,
            "username": getpass.getuser(),
            "host": self.MATCH_ANY,
            "extract": False,
            "copy_dir": None,
        }
        hsg.sender.addExpected(expected)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        msg = "Warning: You are requesting %.2f seconds of data\n" \
              "Normal requests are %d seconds or less" % \
              (secrange, HsGrabber.WARN_SECONDS)
        self.expectLogMessage(msg)

        # run it!
        hsg.send_alert(start_time, stop_time, copydir, print_to_console=False)

        hsg.validate()

        # add grabber to poller socket
        hsg.poller.addPollResult(hsg.sender)

        # add final grabber response
        hsg.sender.addIncoming("DONE\0")

        timeout = 1
        hsg.wait_for_response(timeout=timeout, print_to_console=False)

    def test_huge_time(self):
        start_time = HsBase.DAQTime(15789006796024620)
        stop_ticks = start_time.ticks + \
            1E10 * (HsBase.HsBase.MAX_REQUEST_SECONDS + 1)
        stop_time = HsBase.DAQTime(stop_ticks)
        copydir = None

        secrange = (stop_time.ticks - start_time.ticks) / 1E10

        # create the grabber object
        hsg = MyGrabber()

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        msg = "Request for %.2f seconds is too huge.\n" \
              "HsWorker processes request only up to %d seconds.\n" \
              "Try a smaller time window." % \
              (secrange, HsBase.HsBase.MAX_REQUEST_SECONDS)
        self.expectLogMessage(msg)

        # run it!
        hsg.send_alert(start_time, stop_time, copydir, print_to_console=False)

        hsg.validate()

    def test_timeout(self):
        # create the grabber object
        hsg = MyGrabber()

        # add grabber to poller socket
        hsg.poller.addPollResult(None)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # change the timeout
        timeout = 1

        # add all expected log messages
        self.expectLogMessage("No response from expcont's HsPublisher"
                              " within %s seconds.\nAbort request." % timeout)

        # run it!
        hsg.wait_for_response(timeout=timeout, print_to_console=False)

        hsg.validate()

    def test_working(self):
        start_time = HsBase.DAQTime(15789006796024620)
        stop_time = HsBase.DAQTime(15789066796024620)
        copydir = "/somewhere/else"

        # create the grabber object
        hsg = MyGrabber()

        # add all JSON and response messages
        expected = {
            "start_time": start_time.ticks,
            "stop_time": stop_time.ticks,
            "destination_dir": copydir,
            "prefix": HsPrefix.ANON,
            "request_id": self.MATCH_ANY,
            "msgtype": HsMessage.MESSAGE_INITIAL,
            "version": HsMessage.DEFAULT_VERSION,
            "username": getpass.getuser(),
            "host": self.MATCH_ANY,
            "extract": False,
            "copy_dir": None,
        }
        hsg.sender.addExpected(expected)
        hsg.sender.addIncoming("DONE\0")

        # add grabber to poller socket
        hsg.poller.addPollResult(hsg.sender)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        hsg.send_alert(start_time, stop_time, copydir, print_to_console=False)

        timeout = 1
        hsg.wait_for_response(timeout=timeout, print_to_console=False)
        hsg.validate()


if __name__ == '__main__':
    unittest.main()
