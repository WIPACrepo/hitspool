#!/usr/bin/env python
"""
Convenience class for testing classes which issue log messages
"""

from __future__ import absolute_import

import logging

from unittest import TestCase

from MockLoggingHandler import MockLoggingHandler


class LoggingTestCase(TestCase):
    "Convenience class for testing classes which issue log messages"

    _my_log_handler = None

    @classmethod
    def __init_handler(cls):
        mylog = logging.getLogger()
        mylog.setLevel(0)

        ooo = cls.allow_out_of_order()
        cls._my_log_handler = MockLoggingHandler(out_of_order=ooo)
        mylog.addHandler(cls._my_log_handler)

    @classmethod
    def allow_out_of_order(cls):
        "Return True if this logger accepts out-of-order log messages"
        return False

    @classmethod
    def setUpClass(cls):
        super(LoggingTestCase, cls).setUpClass()
        cls.__init_handler()

    def setUp(self):
        if self._my_log_handler is None:
            self.__init_handler()

    def tearDown(self):
        super(LoggingTestCase, self).tearDown()
        try:
            self._my_log_handler.validate()
        finally:
            self._my_log_handler.reset()

    # pylint: disable=invalid-name
    # match other test methods
    def expect_log_message(self, msg):
        "Add an expected log message"
        self._my_log_handler.add_expected(msg)

    # pylint: disable=invalid-name
    # match other test methods
    @classmethod
    def setLogLevel(cls, level):
        "Set the log level"
        mylog = logging.getLogger()
        mylog.setLevel(level)

    # pylint: disable=invalid-name
    # match other test methods
    def setVerbose(self, value=True):
        "If value is unspecified or True, set verbose logging"
        self._my_log_handler.setVerbose(value)
