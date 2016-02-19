#!/usr/bin/env python

import os
import unittest

import HsRSyncFiles
import HsRSyncTestCase
import HsTestUtil

from HsException import HsException


class MyHsRSyncFiles(HsRSyncFiles.HsRSyncFiles):
    def __init__(self, is_test=False):
        self.__i3_sock = None

        super(MyHsRSyncFiles, self).__init__(is_test=is_test)

        self.__link_paths = []
        self.__fail_hardlink = False
        self.__fail_rsync = False

    @classmethod
    def __timetag(cls, starttime):
        return starttime.strftime("%Y%m%d+%H%M%S")

    def add_expected_links(self, start_utc, rundir, firstnum, numfiles):
        timetag = self.__timetag(start_utc)
        for i in xrange(firstnum, firstnum + numfiles):
            frompath = os.path.join(self.TEST_HUB_DIR, rundir,
                                    "HitSpool-%d.dat" % i)
            self.__link_paths.append((frompath, self.TEST_HUB_DIR, timetag))

    def check_for_unused_links(self):
        llen = len(self.__link_paths)
        if llen > 0:
            raise Exception("Found %d extra link%s (%s)" %
                            (llen, "" if llen == 1 else "s",
                             self.__link_paths))

    def create_i3socket(self, host):
        if self.__i3_sock is not None:
            raise Exception("Cannot create multiple I3 sockets")

        self.__i3_sock = HsTestUtil.MockI3Socket('HsRSyncFiles')
        return self.__i3_sock

    def fail_hardlink(self):
        self.__fail_hardlink = True

    def fail_rsync(self):
        self.__fail_rsync = True

    def get_timetag_tuple(self, prefix, hs_copydir, starttime):
        return "TestHS", self.__timetag(starttime)

    def hardlink(self, filename, targetdir):
        if self.__fail_hardlink:
            raise HsException("Fake Hardlink Error")

        if len(self.__link_paths) == 0:
            raise Exception("Unexpected hardlink from \"%s\" to \"%s\"" %
                            (filename, targetdir))

        expfile, expdir, exptag = self.__link_paths.pop(0)
        if not targetdir.startswith(expdir) or \
           not targetdir.endswith(exptag):
            if filename != expfile:
                raise Exception("Expected to link \"%s\" to \"%s\", not"
                                " \"%s/*/%s\" to \"%s\"" %
                                (expfile, expdir, exptag, filename, targetdir))
            raise Exception("Expected to link \"%s\" to \"%s/*/%s\", not to"
                            " \"%s\"" % (expfile, expdir, exptag, targetdir))
        elif filename != expfile:
            raise Exception("Expected to link \"%s\" to \"%s/*/%s\", not"
                            " \"%s\"" % (expfile, expdir, exptag, filename))

        return 0

    def rsync(self, source, target, bwlimit=None, log_format=None,
              relative=True):
        if self.__fail_rsync:
            raise HsException("FakeFail")
        return ("", )


class HsRSyncFilesTest(HsRSyncTestCase.HsRSyncTestCase):
    @property
    def real_object(self):
        return HsRSyncFiles.HsRSyncFiles(is_test=True)

    @property
    def wrapped_object(self):
        return MyHsRSyncFiles(is_test=True)

    def setUp(self):
        super(HsRSyncFilesTest, self).setUp()

    def tearDown(self):
        super(HsRSyncFilesTest, self).tearDown()


if __name__ == '__main__':
    unittest.main()
