#!/usr/bin/env python

import unittest

import hsrsync
import HsRSyncTestCase

from HsException import HsException


class MyHsRSync(hsrsync.HsRSync):
    def __init__(self, is_test=False):
        super(MyHsRSync, self).__init__(is_test=is_test)

        self.__fail_hardlink = False
        self.__fail_rsync = False

    def add_expected_links(self, start_utc, rundir, firstnum, numfiles):
        pass

    def check_for_unused_links(self):
        pass

    def fail_hardlink(self):
        self.__fail_hardlink = True

    def fail_rsync(self):
        self.__fail_rsync = True

    def hardlink(self, filename, targetdir):
        if self.__fail_hardlink:
            raise HsException("Fake Hardlink Error")

    def rsync(self, source, target, bwlimit=None, log_format=None,
              relative=True):
        #print >>sys.stderr, "Ignoring rsync %s -> %s" % (source, target)
        if self.__fail_rsync:
            return ([], "FakeFail")
        return (["", ], "")


class HsRSyncTest(HsRSyncTestCase.HsRSyncTestCase):
    @property
    def real_object(self):
        return hsrsync.HsRSync(is_test=True)

    @property
    def wrapped_object(self):
        return MyHsRSync(is_test=True)

    def setUp(self):
        super(HsRSyncTest, self).setUp()

    def tearDown(self):
        super(HsRSyncTest, self).tearDown()


if __name__ == '__main__':
    unittest.main()
