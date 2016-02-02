#!/usr/bin/env python

import os
import unittest

import hsrsyncfab
import HsRSyncTestCase
import HsTestUtil

from HsException import HsException


class MyHsRSyncFab(hsrsyncfab.HsRSyncFab):
    def __init__(self, is_test=False):
        self.__i3_sock = None

        super(MyHsRSyncFab, self).__init__(is_test=is_test)

        self.__fail_hardlink = False
        self.__fail_rsync = False

    def add_expected_links(self, start_utc, rundir, firstnum, numfiles):
        pass

    def check_for_unused_links(self):
        pass

    def create_i3socket(self, host):
        if self.__i3_sock is not None:
            raise Exception("Cannot create multiple I3 sockets")

        self.__i3_sock = HsTestUtil.MockI3Socket('HsRSyncFiles')
        return self.__i3_sock

    def fail_hardlink(self):
        self.__fail_hardlink = True

    def fail_rsync(self):
        self.__fail_rsync = True

    def hardlink(self, filename, targetdir):
        if self.__fail_hardlink:
            raise HsException("Fake Hardlink Error")

    def mkdir(self, host, new_dir):
        if not self.is_cluster_local:
            raise Exception("Can only simulate LOCALHOST cluster")
        os.makedirs(new_dir)

    def rsync(self, source, target, bwlimit=None, log_format=None,
              relative=True):
        if self.__fail_rsync:
            raise HsException("FakeFail")
        return ("", )


class HsRSyncFabTest(HsRSyncTestCase.HsRSyncTestCase):
    @property
    def real_object(self):
        return hsrsyncfab.HsRSyncFab(is_test=True)

    @property
    def wrapped_object(self):
        return MyHsRSyncFab(is_test=True)

    def setUp(self):
        super(HsRSyncFabTest, self).setUp()

    def tearDown(self):
        super(HsRSyncFabTest, self).tearDown()


if __name__ == '__main__':
    unittest.main()
