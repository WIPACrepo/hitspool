#!/usr/bin/env python


import logging
import unittest

from MockLoggingHandler import MockLoggingHandler

try:
    from unittest import skip as xxx
    from unittest import TestCase as TestCasePlus
except ImportError:
    from unittest import TestCase
    class TestCasePlus(TestCase):
        def assertIsNone(self, var, msg=None):
            self.assertTrue(var is None, msg)
        def assertIsNotNone(self, var, msg=None):
            self.assertTrue(var is not None, msg)


class LoggingTestCase(TestCasePlus):
    _my_log_handler = None

    @classmethod
    def __init_handler(cls):
        mylog = logging.getLogger()
        mylog.setLevel(0)
        cls._my_log_handler = MockLoggingHandler()
        mylog.addHandler(cls._my_log_handler)
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
    def expectLogMessage(self, msg):
        self._my_log_handler.addExpected(msg)

    # pylint: disable=invalid-name
    # match other test methods
    def setLogLevel(self, level):
        mylog = logging.getLogger()
        mylog.setLevel(level)