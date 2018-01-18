#!/usr/bin/env python
"""
Test HsDelete
"""

import logging
import re
import unittest

import HsDelete
import HsMessage
import HsTestUtil

from HsPrefix import HsPrefix

from LoggingTestCase import LoggingTestCase


class MyDelete(HsDelete.HsDelete):
    "Test wrapper around HsDelete"

    def __init__(self):
        self.__poller = None
        self.__sender = None

        super(MyDelete, self).__init__()

    def create_poller(self, sockets):
        "Create mock poller"
        if self.__poller is not None:
            raise Exception("Cannot create multiple poller sockets")

        self.__poller = HsTestUtil.Mock0MQPoller("Poller")
        return self.__poller

    def create_sender(self, host):
        "Create mock sender"
        if self.__sender is not None:
            raise Exception("Cannot create multiple sender sockets")

        self.__sender = HsTestUtil.Mock0MQSocket("Sender")
        return self.__sender

    def validate(self):
        "Validate all mock sockets"
        self.close_all()

        for sock in (self.__sender, self.__poller):
            if sock is not None:
                sock.validate()


class HsDeleteTest(LoggingTestCase):
    "Test HsDelete"

    MATCH_ANY = re.compile(r"^.*$")

    def setUp(self):
        "Set log level to see all log messages"
        super(HsDeleteTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)

    def test_timeout(self):
        "Test timeout"
        # create the grabber object
        hsg = MyDelete()

        # add grabber to poller socket
        hsg.poller.addPollResult(None)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # change the timeout
        timeout = 1

        # add all expected log messages
        self.expectLogMessage("No response within %s seconds.\n"
                              "Abort request." % timeout)

        # run it!
        hsg.wait_for_response(timeout=timeout, print_to_console=False)

        hsg.validate()

    def test_working(self):
        "Test deletion"
        req_id = "123456789abcdef"
        username = "WOPR"

        # create the grabber object
        hsg = MyDelete()

        # add all JSON and response messages
        expected = {
            "start_ticks": 0L,
            "stop_ticks": 0L,
            "destination_dir": "/dev/null",
            "prefix": HsPrefix.ANON,
            "request_id": req_id,
            "msgtype": HsMessage.DELETE,
            "version": HsMessage.CURRENT_VERSION,
            "username": username,
            "host": self.MATCH_ANY,
            "extract": False,
            "copy_dir": None,
            "hubs": None,
        }
        hsg.sender.addExpected(expected)
        hsg.sender.addIncoming("DONE\0")

        # add grabber to poller socket
        hsg.poller.addPollResult(hsg.sender)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        hsg.send_alert(req_id, username, print_to_console=False)

        timeout = 1
        hsg.wait_for_response(timeout=timeout, print_to_console=False)
        hsg.validate()


if __name__ == '__main__':
    unittest.main()
