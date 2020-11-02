#!/usr/bin/env python
"""
Test HsCopier
"""

from __future__ import print_function

import getpass
import os
import shutil
import socket
import unittest

from HsCopier import CopyUsingRSync, CopyUsingSCP


class HsCopierTest(unittest.TestCase):
    "Test HsCopier"

    SOURCE_DIR = "/tmp/testsrc"
    TARGET_DIR = "/tmp/testtgt"

    @classmethod
    def __create_files(cls, num, topdir=None):
        if topdir is None:
            raise Exception("Top directory has not been set")

        if not os.path.exists(topdir):
            os.mkdir(topdir, 0o755)

        created = []
        for idx in range(num):
            path = os.path.join(topdir, "file%d" % idx)
            created.append(path)

            if os.path.exists(path):
                continue

            with open(path, "w") as fout:
                for cnt in range(100):
                    print("x"*cnt, file=fout)

        return created

    @classmethod
    def __file_size(cls, files):
        return sum([os.path.getsize(x) for x in files])

    @property
    def __is_access(self):
        fullname = socket.gethostname()
        return "access" in fullname

    def tearDown(self):
        if os.path.exists(self.SOURCE_DIR):
            shutil.rmtree(self.SOURCE_DIR)
        if os.path.exists(self.TARGET_DIR):
            shutil.rmtree(self.TARGET_DIR)

    def test_rsync(self):
        "Test 'rsync' copy"
        source_list = self.__create_files(3, topdir=self.SOURCE_DIR)
        source_size = self.__file_size(source_list)

        copier = CopyUsingRSync(getpass.getuser(), "localhost",
                                self.TARGET_DIR, "ignored")
        copier.copy(source_list)

        self.assertEqual(copier.size, source_size,
                         "Expected size %s, not %s" %
                         (source_size, copier.size))

    def test_scp(self):
        "Test 'scp' copy"
        if self.__is_access:
            return

        source_list = self.__create_files(3, topdir=self.SOURCE_DIR)
        source_size = self.__file_size(source_list)

        copier = CopyUsingSCP(getpass.getuser(), "localhost", self.TARGET_DIR,
                              "ignored")
        copier.copy(source_list)

        self.assertEqual(copier.size, source_size,
                         "Expected size %s, not %s" %
                         (source_size, copier.size))


if __name__ == '__main__':
    unittest.main()
