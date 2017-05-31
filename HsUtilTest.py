#!/usr/bin/env python

import datetime
import unittest

from HsBase import DAQTime

try:
    # pylint: disable=unused-import
    # test for modern (post 2.6) unittest function
    from unittest import skip as xxx
    # if we made it here, we don't need to augment the <=2.6 TestCase
    from unittest import TestCase as TestCasePlus
except ImportError:
    # add assertIsNone() and assertIsNotNone() to older TestCase
    from unittest import TestCase

    class TestCasePlus(TestCase):
        def assertIsNone(self, var, msg=None):
            self.assertTrue(var is None, msg)

        def assertIsNotNone(self, var, msg=None):
            self.assertTrue(var is not None, msg)


class HsUtilTest(TestCasePlus):
    # map of year to datetime(year, 1, 2)
    TICKS_PER_SECOND = 10000000000

    def __check_dt(self, start_ticks, use_start, stop_ticks, use_stop,
                   is_ns=False):
        modifier = 10 if is_ns else 1

        good_start = DAQTime(start_ticks / modifier, is_ns=is_ns)
        exp_start = int(start_ticks / modifier) * modifier
        self.assertEquals(good_start.ticks, exp_start,
                          "Start tick changed from %d to %d" %
                          (start_ticks, good_start.ticks))

        good_stop = DAQTime(stop_ticks / modifier, is_ns=is_ns)
        exp_stop = int(stop_ticks / modifier) * modifier
        self.assertEquals(good_stop.ticks, exp_stop,
                          "Stop tick changed from %d to %d" %
                          (stop_ticks, good_stop.ticks))

    def test_fix_d_t_daq(self):
        start_tick = 15789006796024623
        stop_tick = 15789066798765432

        for i in xrange(4):
            self.__check_dt(start_tick, i & 1 == 1, stop_tick, i & 2 == 2,
                            is_ns=False)

    def test_fix_d_t_sn(self):
        start_tick = 15789006796024623
        stop_tick = 15789066798765432

        for i in xrange(4):
            self.__check_dt(start_tick, i & 1 == 1, stop_tick, i & 2 == 2,
                            is_ns=True)


if __name__ == '__main__':
    unittest.main()
