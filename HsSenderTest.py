#!/usr/bin/env python

import json
import logging
import os
import shutil
import tarfile
import tempfile
import unittest

import HsSender

from HsException import HsException
from HsTestUtil import Mock0MQSocket, MockI3Socket
from LoggingTestCase import LoggingTestCase


class MySender(HsSender.HsSender):
    """
    Use mock 0MQ sockets for testing
    """
    def __init__(self):
        super(MySender, self).__init__()

    def create_reporter(self):
        return Mock0MQSocket("Reporter")

    def create_i3socket(self, host):
        return MockI3Socket("HsSender@%s" % self.shorthost)

    def validate(self):
        """
        Check that all expected messages were received by mock sockets
        """
        self.reporter.validate()
        self.i3socket.validate()


class FailableSender(MySender):
    """
    Override crucial methods to test failure modes
    """
    def __init__(self):
        super(FailableSender, self).__init__()

        self.__fail_move_file = False
        self.__fail_touch_file = False
        self.__fail_tar_file = False
        self.__moved_files = False

    def fail_create_sem_file(self):
        self.__fail_touch_file = True

    def fail_create_tar_file(self):
        self.__fail_tar_file = True

    def fail_move_file(self):
        self.__fail_move_file = True

    def move_file(self, src, dst):
        if self.__fail_move_file:
            raise HsException("Fake Move Error")

    def moved_files(self):
        return self.__moved_files

    def movefiles(self, copydir, targetdir):
        self.__moved_files = True
        return True

    def touch_file(self, name, times=None):
        if self.__fail_touch_file:
            raise HsException("Fake Touch Error")

    def write_tarfile(self, sourcedir, sourcefiles, tarname):
        if self.__fail_tar_file > 0:
            self.__fail_tar_file -= 1
            raise HsException("Fake Tar Error")


class HsSenderTest(LoggingTestCase):
    # pylint: disable=too-many-public-methods
    # Really?!?!  In a test class?!?!  Shut up, pylint!

    COPY_DIR = None
    SENDER = None

    def __check_hitspool_file_list(self, flist, firstnum, numfiles):
        flist.sort()

        for fnum in xrange(firstnum, firstnum + numfiles):
            fname = "HitSpool-%d" % fnum
            if len(flist) == 0:
                self.fail("Not all files were copied (found %d of %d)" %
                          (fnum - firstnum, numfiles))

            self.assertEquals(fname, flist[0],
                              "Expected file #%d to be \"%s\" not \"%s\"" %
                              (fnum - firstnum, fname, flist[0]))
            del flist[0]
        if len(flist) != 0:
            self.fail("%d extra files were copied (%s)", (len(flist), flist))

    def __create_copydir(self, real_stuff=False):
        """create temporary copy directory"""
        if self.COPY_DIR is None:
            if real_stuff:
                self.COPY_DIR = tempfile.mkdtemp()
            else:
                self.COPY_DIR = "/cloud/cuckoo/land"
        return self.COPY_DIR

    def __create_hitspool_copy(self, prefix, timetag, host, startnum,
                               numfiles, real_stuff=False):
        """create copy directory and fill with fake hitspool files"""
        copydir = self.__create_copydir(real_stuff=real_stuff)

        # create copy directory
        path = os.path.join(copydir, "%s_%s_%s" % (prefix, timetag, host))

        # if caller wants actual directory and files, create them
        if real_stuff:
            if not os.path.exists(path):
                os.makedirs(path)

            # create all fake hitspool files
            for num in xrange(startnum, startnum + numfiles):
                fpath = os.path.join(path, "HitSpool-%d" % num)
                with open(fpath, "w") as fout:
                    print >>fout, "Fake#%d" % num

        return path

    def setUp(self):
        super(HsSenderTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)

    def tearDown(self):
        try:
            super(HsSenderTest, self).tearDown()
        finally:
            if self.COPY_DIR is not None and os.path.exists(self.COPY_DIR):
                # clear lingering files
                try:
                    shutil.rmtree(self.COPY_DIR)
                except:
                    pass
            if self.SENDER is not None:
                try:
                    self.SENDER.close_all()
                except:
                    pass
                self.SENDER = None

    def test_bad_dir_name(self):
        sender = FailableSender()
        self.SENDER = sender

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create fake directory paths
        hsdir = self.__create_hitspool_copy("XXX", "12345678_987654", "ichub01",
                                            firstnum, numfiles,
                                            real_stuff=True)
        copydir = self.__create_copydir(real_stuff=True)
        usrdir = os.path.join(copydir, "UserCopy")

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Naming scheme validation failed.")
        self.expectLogMessage("Please put the data manually in the"
                              " desired location: %s" % usrdir)

        # run it!
        sender.hs_data_location_check(hsdir, usrdir)

        # make sure no files moved
        self.assertFalse(sender.moved_files(), "Should not have moved files")

        # make sure 0MQ communications checked out
        sender.validate()

    def test_no_move(self):
        sender = FailableSender()
        self.SENDER = sender

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create fake directory paths
        hsdir = self.__create_hitspool_copy("ANON", "12345678_987654",
                                            "ichub01", firstnum, numfiles,
                                            real_stuff=True)
        if hsdir.endswith('/'):
            usrdir = os.path.dirname(hsdir[:-1])
        else:
            usrdir = os.path.dirname(hsdir)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        sender.hs_data_location_check(hsdir, usrdir)

        # make sure no files moved
        self.assertFalse(sender.moved_files(), "Should not have moved files")

        # make sure 0MQ communications checked out
        sender.validate()

    def test_copy_sn_alert(self):
        sender = FailableSender()
        self.SENDER = sender

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create fake directory paths
        hsdir = self.__create_hitspool_copy("HESE", "12345678_987654",
                                            "ichub01", firstnum, numfiles,
                                            real_stuff=True)
        copydir = self.__create_copydir(real_stuff=True)
        usrdir = os.path.join(copydir, "UserCopy")

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        sender.hs_data_location_check(hsdir, usrdir)

        # files should have been moved!
        self.assertTrue(sender.moved_files(), "Should have moved files")

        # make sure 0MQ communications checked out
        sender.validate()

    def test_real_copy_sn_alert(self):
        sender = MySender()
        self.SENDER = sender

        # initialize directory parts
        category = "SNALERT"
        timetag = "12345678_987654"
        host = "ichub01"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create real directories
        hsdir = self.__create_hitspool_copy(category, timetag, host, firstnum,
                                            numfiles, real_stuff=True)
        copydir = self.__create_copydir(real_stuff=True)
        usrdir = os.path.join(copydir, "UserCopy")

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        sender.hs_data_location_check(hsdir, usrdir)

        self.assertFalse(os.path.exists(hsdir),
                         "HitSpool directory \"%s\" was not moved" % hsdir)
        self.assertTrue(os.path.exists(usrdir),
                        "User directory \"%s\" does not exist" % usrdir)

        base = os.path.basename(hsdir)
        if base == "":
            base = os.path.basename(os.path.dirname(hsdir))
            if base == "":
                self.fail("Cannot find basename from %s" % hsdir)
        subdir = os.path.join(usrdir, base)
        self.assertTrue(os.path.exists(subdir),
                        "Moved directory \"%s\" does not exist" % subdir)

        # I'm not sure why, but there's a dual level of subdirectories
        subsub = os.path.join(subdir, base)
        self.assertTrue(os.path.exists(subsub),
                        "Moved subdirectory \"%s\" does not exist" % subsub)

        flist = []
        for entry in os.listdir(subsub):
            flist.append(entry)
        self.__check_hitspool_file_list(flist, firstnum, numfiles)

        # make sure 0MQ communications checked out
        sender.validate()

    def test_spade_data_bad_copy_dir(self):
        sender = FailableSender()
        self.SENDER = sender

        # initialize directory parts
        category = "XXX"
        timetag = "12345678_987654"
        host = "ichub01"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create bad directory name
        hsdir = self.__create_hitspool_copy(category, timetag, host, firstnum,
                                            numfiles, real_stuff=False)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Naming scheme validation failed.")
        self.expectLogMessage("Please put the data manually in the SPADE"
                              " directory. Use HsSpader.py, for example.")

        # run it!
        (tarname, semname) \
            = sender.spade_pickup_data(hsdir, "/foo/bar")

        self.assertIsNone(tarname, "spade_pickup_data() should return None,"
                          " not tar name \"%s\"" % tarname)
        self.assertIsNone(semname, "spade_pickup_data() should return None,"
                          " not semaphore name \"%s\"" % semname)

        # make sure 0MQ communications checked out
        sender.validate()

    def test_spade_data_fail_tar(self):
        sender = FailableSender()
        self.SENDER = sender

        sender.fail_create_tar_file()

        # initialize directory parts
        category = "SNALERT"
        timetag = "12345678_987654"
        host = "ichub07"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create bad directory name
        hsdir = self.__create_hitspool_copy(category, timetag, host, firstnum,
                                            numfiles, real_stuff=False)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Fake Tar Error")
        self.expectLogMessage("Please put the data manually in the SPADE"
                              " directory. Use HsSpader.py, for example.")

        # run it!
        (tarname, semname) \
            = sender.spade_pickup_data(hsdir, "/foo/bar")

        self.assertIsNone(tarname, "spade_pickup_data() should return None,"
                          " not tar name \"%s\"" % tarname)
        self.assertIsNone(semname, "spade_pickup_data() should return None,"
                          " not semaphore name \"%s\"" % semname)

        # make sure 0MQ communications checked out
        sender.validate()

    def test_spade_data_fail_move(self):
        sender = FailableSender()
        self.SENDER = sender

        sender.fail_move_file()

        # initialize directory parts
        category = "SNALERT"
        timetag = "12345678_987654"
        host = "ichub07"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create bad directory name
        hsdir = self.__create_hitspool_copy(category, timetag, host, firstnum,
                                            numfiles, real_stuff=False)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Fake Move Error")
        self.expectLogMessage("Please put the data manually in the SPADE"
                              " directory. Use HsSpader.py, for example.")

        # run it!
        (tarname, semname) \
            = sender.spade_pickup_data(hsdir, "/foo/bar")

        self.assertIsNone(tarname, "spade_pickup_data() should return None,"
                          " not tar name \"%s\"" % tarname)
        self.assertIsNone(semname, "spade_pickup_data() should return None,"
                          " not semaphore name \"%s\"" % semname)

        # make sure 0MQ communications checked out
        sender.validate()

    def test_spade_data_fail_sem(self):
        sender = FailableSender()
        self.SENDER = sender

        sender.fail_create_sem_file()

        # initialize directory parts
        category = "SNALERT"
        timetag = "12345678_987654"
        host = "ichub07"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create bad directory name
        hsdir = self.__create_hitspool_copy(category, timetag, host, firstnum,
                                            numfiles, real_stuff=False)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Fake Touch Error")
        self.expectLogMessage("Please put the data manually in the SPADE"
                              " directory. Use HsSpader.py, for example.")

        # run it!
        (tarname, semname) \
            = sender.spade_pickup_data(hsdir, "/foo/bar")

        self.assertIsNone(tarname, "spade_pickup_data() should return None,"
                          " not tar name \"%s\"" % tarname)
        self.assertIsNone(semname, "spade_pickup_data() should return None,"
                          " not semaphore name \"%s\"" % semname)

        # make sure 0MQ communications checked out
        sender.validate()

    def test_spade_pickup_data(self):
        sender = MySender()
        self.SENDER = sender

        # initialize directory parts
        category = "SNALERT"
        timetag = "12345678_987654"
        host = "ichub01"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create real directories
        hsdir = self.__create_hitspool_copy(category, timetag, host, firstnum,
                                            numfiles, real_stuff=True)

        # add all expected I3Live messages
        sender.i3socket.addExpectedValue("SPADE-ing of %s done" % hsdir)

        # set SPADE path to something which exists everywhere
        sender.HS_SPADE_DIR = "/tmp"

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        mybase = "%s_%s_%s" % (category, timetag, host)
        mytar = "HS_%s.dat.tar.bz2" % mybase
        mysem = "HS_%s.sem" % mybase

        # clean up test files
        for fnm in (mytar, mysem):
            tmppath = os.path.join(sender.HS_SPADE_DIR, fnm)
            if os.path.exists(tmppath):
                os.unlink(tmppath)

        # run it!
        (tarname, semname) \
            = sender.spade_pickup_data(hsdir, "/foo/bar")

        self.assertEquals(mysem, semname,
                          "Expected semaphore to be named \"%s\" not \"%s\"" %
                          (mysem, semname))
        self.assertEquals(mytar, tarname,
                          "Expected tarfile to be named \"%s\" not \"%s\"" %
                          (mytar, tarname))

        sempath = os.path.join(sender.HS_SPADE_DIR, semname)
        self.assertTrue(os.path.exists(sempath),
                        "Semaphore file %s was not created" % sempath)

        tarpath = os.path.join(sender.HS_SPADE_DIR, tarname)
        self.assertTrue(tarfile.is_tarfile(tarpath),
                        "Tar file %s was not created" % tarpath)

        # read in contents of tarfile
        tar = tarfile.open(tarpath)
        names = []
        for fnm in tar.getnames():
            if fnm == mybase:
                continue
            if fnm.startswith(mybase):
                fnm = fnm[len(mybase)+1:]
            names.append(fnm)
        tar.close()

        # validate the list
        self.__check_hitspool_file_list(names, firstnum, numfiles)

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_no_msg(self):
        sender = MySender()
        self.SENDER = sender

        # initialize message
        msg = None

        # add all expected JSON messages
        sender.reporter.addIncoming(msg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Cannot load JSON message \"%s\"" % msg)

        # run it!
        sender.mainloop()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_str_msg(self):
        sender = MySender()
        self.SENDER = sender

        # initialize message
        msg = json.dumps("abc")

        # add all expected JSON messages
        sender.reporter.addIncoming(msg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        rcv_msg = json.loads(msg)
        self.expectLogMessage("Ignoring bad message \"%s\"<%s>" %
                              (rcv_msg, type(rcv_msg)))

        # run it!
        sender.mainloop()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_bad_msg(self):
        sender = MySender()
        self.SENDER = sender

        # initialize message
        msg = json.dumps({"abc": 123})

        # add all expected JSON messages
        sender.reporter.addIncoming(msg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        rcv_msg = json.loads(msg)
        self.expectLogMessage("Ignoring bad message \"%s\"<%s>" %
                              (rcv_msg, type(rcv_msg)))

        # run it!
        sender.mainloop()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_incomplete_msg(self):
        sender = MySender()
        self.SENDER = sender

        # initialize message
        msg = json.dumps({"msgtype": "rsync_sum"})

        # add all expected JSON messages
        sender.reporter.addIncoming(msg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        rcv_msg = json.loads(msg)
        self.expectLogMessage("Ignoring incomplete message \"%s\"" % rcv_msg)

        # run it!
        sender.mainloop()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_unknown_msg(self):
        sender = MySender()
        self.SENDER = sender

        # initialize message
        msg = json.dumps({"msgtype": "xxx",
                          "copydir": "yyy",
                          "copydir_user": "zzz"})

        # add all expected JSON messages
        sender.reporter.addIncoming(msg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        rcv_msg = json.loads(msg)
        self.expectLogMessage("Ignoring message type \"%s\"" %
                              rcv_msg["msgtype"])

        # run it!
        sender.mainloop()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop(self):
        sender = MySender()
        self.SENDER = sender

        # initialize directory parts
        category = "SNALERT"
        timetag = "12345678_987654"
        host = "ichub01"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create real directories
        hsdir = self.__create_hitspool_copy(category, timetag, host, firstnum,
                                            numfiles, real_stuff=True)
        copydir = self.__create_copydir(real_stuff=True)
        usrdir = os.path.join(copydir, "UserCopy")

        # initialize message
        msg = {"msgtype": "rsync_sum",
               "copydir": hsdir,
               "copydir_user": usrdir,
              }

        # add all expected JSON messages
        sender.reporter.addIncoming(json.dumps(msg))

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        sender.mainloop()

        self.assertFalse(os.path.exists(hsdir),
                         "HitSpool directory \"%s\" was not moved" % hsdir)
        self.assertTrue(os.path.exists(usrdir),
                        "User directory \"%s\" does not exist" % usrdir)

        base = os.path.basename(hsdir)
        if base == "":
            base = os.path.basename(os.path.dirname(hsdir))
            if base == "":
                self.fail("Cannot find basename from %s" % hsdir)
        subdir = os.path.join(usrdir, base)
        self.assertTrue(os.path.exists(subdir),
                        "Moved directory \"%s\" does not exist" % subdir)

        # I'm not sure why, but there's a dual level of subdirectories
        subsub = os.path.join(subdir, base)
        self.assertTrue(os.path.exists(subsub),
                        "Moved subdirectory \"%s\" does not exist" % subsub)

        flist = []
        for entry in os.listdir(subsub):
            flist.append(entry)
        self.__check_hitspool_file_list(flist, firstnum, numfiles)

        # make sure 0MQ communications checked out
        sender.validate()


if __name__ == '__main__':
    unittest.main()
