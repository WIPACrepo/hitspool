#!/usr/bin/env python
"""
Test HsGrabber code
"""

import getpass
import logging
import re
import unittest

import HsGrabber
import HsMessage
import HsTestUtil

from HsBase import HsBase
from HsPrefix import HsPrefix

from LoggingTestCase import LoggingTestCase


class MyGrabber(HsGrabber.HsGrabber):
    "Test wrapper around HsGrabber"

    def __init__(self):
        self.__poller = None
        self.__publisher = None
        self.__sender = None

        super(MyGrabber, self).__init__()

    def create_poller(self, sockets):
        "Create mock poller"
        if self.__poller is not None:
            raise Exception("Cannot create multiple poller sockets")

        self.__poller = HsTestUtil.Mock0MQPoller("Poller")
        return self.__poller

    def create_publisher(self, host):
        "Create mock publisher"
        if self.__publisher is not None:
            raise Exception("Cannot create multiple publisher sockets")

        self.__publisher = HsTestUtil.Mock0MQSocket("Publisher")
        return self.__publisher

    def create_sender(self, host):
        "Create mock sender"
        if self.__sender is not None:
            raise Exception("Cannot create multiple sender sockets")

        self.__sender = HsTestUtil.Mock0MQSocket("Sender")
        return self.__sender

    def validate(self):
        "Validate all mock sockets"
        self.close_all()

        for sock in (self.__publisher, self.__sender, self.__poller):
            if sock is not None:
                sock.validate()


class HsGrabberTest(LoggingTestCase):
    "Test HsGrabber code"

    MATCH_ANY = re.compile(r"^.*$")

    def setUp(self):
        "Set log level to see all log messages"
        super(HsGrabberTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)

    def test_negative_time(self):
        "Test request with negative time window"

        start_ticks = 15789006796024620
        stop_ticks = start_ticks - 1000000000
        copydir = None

        # create the grabber object
        hsg = MyGrabber()

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        msg = "Requesting negative time range (%.2f).\n" \
              "Try another time window." % \
              ((stop_ticks - start_ticks) / 1E10)
        self.expect_log_message(msg)

        # run it!
        hsg.send_alert(start_ticks, stop_ticks, copydir,
                       print_to_console=False)

        hsg.validate()

    def test_nonstandard_time(self):
        "Test request with large time window"

        start_ticks = 15789006796024620
        stop_ticks = start_ticks + int(1E10 * (HsGrabber.WARN_SECONDS + 1))
        copydir = "/not/valid/path"

        secrange = (stop_ticks - start_ticks) / 1E10

        # create the grabber object
        hsg = MyGrabber()

        # add all JSON and response messages
        expected = {
            "start_ticks": start_ticks,
            "stop_ticks": stop_ticks,
            "destination_dir": copydir,
            "prefix": HsPrefix.ANON,
            "request_id": self.MATCH_ANY,
            "msgtype": HsMessage.INITIAL,
            "version": HsMessage.CURRENT_VERSION,
            "username": getpass.getuser(),
            "host": self.MATCH_ANY,
            "extract": False,
            "copy_dir": None,
            "hubs": None,
        }
        hsg.sender.add_expected(expected)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        msg = "Warning: You are requesting %.2f seconds of data\n" \
              "Normal requests are %d seconds or less" % \
              (secrange, HsGrabber.WARN_SECONDS)
        self.expect_log_message(msg)

        # run it!
        hsg.send_alert(start_ticks, stop_ticks, copydir,
                       print_to_console=False)

        hsg.validate()

        # add grabber to poller socket
        hsg.poller.add_poll_result(hsg.sender)

        # add final grabber response
        hsg.sender.add_incoming("DONE\0")

        timeout = 1
        hsg.wait_for_response(timeout=timeout, print_to_console=False)

    def test_huge_time(self):
        "Test request with huge time window"

        start_ticks = 15789006796024620
        stop_ticks = start_ticks + \
                     int(1E10 * (HsBase.MAX_REQUEST_SECONDS + 1))
        copydir = None

        secrange = (stop_ticks - start_ticks) / 1E10

        # create the grabber object
        hsg = MyGrabber()

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        msg = "Request for %.2f seconds is too huge.\n" \
              "HsWorker processes request only up to %d seconds.\n" \
              "Try a smaller time window." % \
              (secrange, HsBase.MAX_REQUEST_SECONDS)
        self.expect_log_message(msg)

        # run it!
        hsg.send_alert(start_ticks, stop_ticks, copydir,
                       print_to_console=False)

        hsg.validate()

    def test_timeout(self):
        "Test publisher timeout"

        # create the grabber object
        hsg = MyGrabber()

        # add grabber to poller socket
        hsg.poller.add_poll_result(None)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # change the timeout
        timeout = 1

        # add all expected log messages
        self.expect_log_message("No response from expcont's HsPublisher"
                                " within %s seconds.\nAbort request." %
                                (timeout, ))

        # run it!
        hsg.wait_for_response(timeout=timeout, print_to_console=False)

        hsg.validate()

    def test_working(self):
        "Test that everything works"
        start_ticks = 15789006796024620
        stop_ticks = 15789066796024620
        copydir = "/somewhere/else"

        # create the grabber object
        hsg = MyGrabber()

        # add all JSON and response messages
        expected = {
            "start_ticks": start_ticks,
            "stop_ticks": stop_ticks,
            "destination_dir": copydir,
            "prefix": HsPrefix.ANON,
            "request_id": self.MATCH_ANY,
            "msgtype": HsMessage.INITIAL,
            "version": HsMessage.CURRENT_VERSION,
            "username": getpass.getuser(),
            "host": self.MATCH_ANY,
            "extract": False,
            "copy_dir": None,
            "hubs": None,
        }
        hsg.sender.add_expected(expected)
        hsg.sender.add_incoming("DONE\0")

        # add grabber to poller socket
        hsg.poller.add_poll_result(hsg.sender)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        hsg.send_alert(start_ticks, stop_ticks, copydir,
                       print_to_console=False)

        timeout = 1
        hsg.wait_for_response(timeout=timeout, print_to_console=False)
        hsg.validate()


if __name__ == '__main__':
    unittest.main()
