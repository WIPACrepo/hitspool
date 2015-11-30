#!/usr/bin/env python

import unittest

import HsUtil

import HsTestUtil

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
    TICKS_PER_SECOND = 10000000000

    def __check_dt(self, good_tk_start, use_start, good_tk_stop, use_stop,
                   is_sn_ns=False):
        good_tm_start = HsTestUtil.get_time(good_tk_start, is_sn_ns=is_sn_ns)
        good_tm_stop = HsTestUtil.get_time(good_tk_stop, is_sn_ns=is_sn_ns)

        (tk_start, tm_start) \
            = HsUtil.fix_date_or_timestamp(good_tk_start if use_start
                                           else None,
                                           good_tm_start if not use_start
                                           else None,
                                           is_sn_ns=is_sn_ns)

        (tk_stop, tm_stop) \
            = HsUtil.fix_date_or_timestamp(good_tk_stop if use_stop
                                           else None,
                                           good_tm_stop if not use_stop
                                           else None,
                                           is_sn_ns=is_sn_ns)

        if is_sn_ns:
            ms_mult = 1000
        else:
            ms_mult = 10000

        if use_start:
            exp_tk_start = good_tk_start
        else:
            exp_tk_start = int((good_tk_start + (ms_mult / 2)) / ms_mult) * \
                         ms_mult
        self.assertEquals(tk_start, exp_tk_start,
                          "Start tick changed from %d to %d" %
                          (exp_tk_start, tk_start))

        if use_stop:
            exp_tk_stop = good_tk_stop
        else:
            exp_tk_stop = int((good_tk_stop + (ms_mult / 2)) / ms_mult) * \
                          ms_mult
        self.assertEquals(tk_stop, exp_tk_stop,
                          "Stop tick changed from %d to %d" %
                          (exp_tk_stop, tk_stop))

        exp_tm_start = good_tm_start
        self.assertEquals(tm_start, exp_tm_start,
                          "Expected start time \"%s\", got \"%s\" (%s)" %
                          (exp_tm_start, tm_start, tm_start - exp_tm_start))

        exp_tm_stop = good_tm_stop
        self.assertEquals(tm_stop, exp_tm_stop,
                          "Expected stop time \"%s\", got \"%s\" (%s)" %
                          (exp_tm_stop, tm_stop, tm_stop - exp_tm_stop))

    def test_fix_d_t_none(self):
        for i in xrange(2):
            # initialize everything to None
            start_tick = None
            start_time = None
            stop_tick = None
            stop_time = None

            # set one of the four values
            if i == 0:
                stop_tick = 15789066796024623
            elif i == 1:
                start_tick = 15789006796024623
            elif i == 2:
                stop_time = HsTestUtil.get_time(15789066796024623)
            elif i == 3:
                start_time = HsTestUtil.get_time(15789006796024623)

            (start_tick, start_time) \
                = HsUtil.fix_date_or_timestamp(start_tick, start_time)
            (stop_tick, stop_time) \
                = HsUtil.fix_date_or_timestamp(stop_tick, stop_time)

            if i == 0 or i == 2:
                self.assertIsNone(start_tick, "start_tick should not be set")
                self.assertIsNone(start_time, "start_time should not be set")
                self.assertIsNotNone(stop_tick, "stop_tick should be set")
                self.assertIsNotNone(stop_time, "stop_time should be set")
            else:
                self.assertIsNotNone(start_tick, "start_tick should be set")
                self.assertIsNotNone(start_time, "start_time should be set")
                self.assertIsNone(stop_tick, "stop_tick should not be set")
                self.assertIsNone(stop_time, "stop_time should not be set")

    def test_fix_d_tdaq(self):
        start_tick = 15789006796024623
        stop_tick = 15789066798765432

        for i in xrange(4):
            self.__check_dt(start_tick, i & 1 == 1, stop_tick, i & 2 == 2,
                            is_sn_ns=False)

    def test_fix_d_tsn(self):
        start_tick = 15789006796024623
        stop_tick = 15789066798765432

        for i in xrange(4):
            self.__check_dt(int(start_tick / 10), i & 1 == 1,
                            int(stop_tick / 10), i & 2 == 2,
                            is_sn_ns=True)


if __name__ == '__main__':
    unittest.main()
