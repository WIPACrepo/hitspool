#!/usr/bin/env python


import json
import logging
import os
import shutil
import tarfile
import tempfile
import unittest

import HsSender
import HsMessage
import HsUtil

from HsException import HsException
from HsTestUtil import Mock0MQSocket, MockHitspool, MockI3Socket, TIME_PAT, \
    get_time
from LoggingTestCase import LoggingTestCase


class MySender(HsSender.HsSender):
    """
    Use mock 0MQ sockets for testing
    """
    def __init__(self):
        super(MySender, self).__init__(host="tstsnd", is_test=True)

    def create_reporter(self):
        return Mock0MQSocket("Reporter")

    def create_i3socket(self, host):
        return MockI3Socket("HsSender@%s" % self.shorthost)

    def validate(self):
        """
        Check that all expected messages were received by mock sockets
        """
        val = self.reporter.validate()
        val |= self.i3socket.validate()
        return val


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

    def remove_tree(self, path):
        pass

    def write_meta_xml(self, spadedir, basename, start_time, stop_time):
        if self.__fail_touch_file:
            raise HsException("Fake Touch Error")
        return basename + HsSender.HsSender.META_SUFFIX

    def write_sem(self, spadedir, basename):
        if self.__fail_touch_file:
            raise HsException("Fake Touch Error")
        return basename + HsSender.HsSender.SEM_SUFFIX

    def write_tarfile(self, sourcedir, sourcefiles, tarname):
        if self.__fail_tar_file > 0:
            self.__fail_tar_file -= 1
            raise HsException("Fake Tar Error")


class TestException(Exception):
    pass


class MockRequestBuilder(object):
    SNDAQ = 1
    HESE = 2
    ANON = 3

    USRDIR = None

    def __init__(self, req_id, req_type, start_utc, stop_utc, timetag, host,
                 firstfile, numfiles):
        self.__req_id = req_id
        self.__start_utc = start_utc
        self.__stop_utc = stop_utc
        self.__host = host
        self.__firstfile = firstfile
        self.__numfiles = numfiles

        # create initial directory
        category = self.__get_category(req_type)
        self.__hsdir = MockHitspool.create_copy_files(category, timetag, host,
                                                      firstfile, numfiles,
                                                      real_stuff=True)

        # build final directory path
        if self.USRDIR is None:
            self.create_user_dir()
        self.__destdir = os.path.join(self.USRDIR,
                                      "%s_%s_%s" % (category, timetag, host))

    def __get_category(self, reqtype):
        if reqtype == self.SNDAQ:
            return "SNALERT"
        if reqtype == self.HESE:
            return "HESE"
        if reqtype == self.ANON:
            return "ANON"
        if isinstance(reqtype, str):
            return reqtype
        raise NotImplementedError("Unknown request type #%s" % reqtype)

    def add_i3live_message(self, i3socket, status, success=None, failed=None):
        # build I3Live success message
        value = {
            'status': status,
            'request_id': self.__req_id,
            'username': None,
            'start_time': str(self.__start_utc),
            'stop_time': str(self.__stop_utc),
            'destination_dir': self.__destdir,
            'prefix': None,
            'update_time': TIME_PAT,
        }

        if success is not None:
            value["success"] = success
        if failed is not None:
            value["success"] = failed

        # add all expected I3Live messages
        i3socket.addExpectedMessage(value, service="hitspool",
                                    varname="hsrequest_info", time=TIME_PAT,
                                    prio=1)

    @classmethod
    def add_reporter_request(cls, reporter, msgtype, req_id, start_time,
                             stop_time, host, hsdir, destdir, success=None,
                             failed=None):
        # initialize message
        rcv_msg = {
            "msgtype": msgtype,
            "request_id": req_id,
            "username": None,
            "start_time": start_time,
            "stop_time": stop_time,
            "copy_dir": hsdir,
            "destination_dir": destdir,
            "prefix": None,
            "extract": None,
            "host": host,
        }

        if success is not None:
            rcv_msg["success"] = success
        if failed is not None:
            rcv_msg["failed"] = failed

        # add all expected JSON messages
        reporter.addIncoming(rcv_msg)

    def add_request(self, reporter, msgtype, success=None, failed=None):
        self.add_reporter_request(reporter, msgtype, self.__req_id,
                                  self.__start_utc, self.__stop_utc,
                                  self.__host, self.__hsdir, self.__destdir,
                                  success=success, failed=failed)

    def check_files(self):
        if os.path.exists(self.__hsdir):
            raise TestException("HitSpool directory \"%s\" was not moved" %
                                self.__hsdir)
        if not os.path.exists(self.USRDIR):
            raise TestException("User directory \"%s\" does not exist" %
                                self.USRDIR)

        base = os.path.basename(self.__hsdir)
        if base == "":
            base = os.path.basename(os.path.dirname(self.__hsdir))
            if base == "":
                raise TestException("Cannot find basename from %s" %
                                    self.__hsdir)

        if os.path.basename(self.__destdir) == base:
            subdir = self.__destdir
        else:
            subdir = os.path.join(self.__destdir, base)

        if not os.path.exists(subdir):
            raise TestException("Moved directory \"%s\" does not exist" %
                                subdir)

        flist = []
        for entry in os.listdir(subdir):
            flist.append(entry)

        self.check_hitspool_file_list(flist, self.__firstfile, self.__numfiles)

    @classmethod
    def check_hitspool_file_list(cls, flist, firstfile, numfiles):
        flist.sort()

        for fnum in xrange(firstfile, firstfile + numfiles):
            fname = "HitSpool-%d" % fnum
            if len(flist) == 0:
                raise TestException("Not all files were copied"
                                    " (found %d of %d)" %
                                    (fnum - firstfile, numfiles))

            if fname != flist[0]:
                raise TestException("Expected file #%d to be \"%s\""
                                    " not \"%s\"" %
                                    (fnum - firstfile, fname, flist[0]))

            del flist[0]

        if len(flist) != 0:
            raise TestException("%d extra files were copied (%s)" %
                                (len(flist), flist))

    @classmethod
    def create_user_dir(cls):
        usrdir = cls.get_user_dir()
        if os.path.exists(usrdir):
            raise TestException("UserDir %s already exists" % str(usrdir))
        os.makedirs(usrdir)
        return usrdir

    @property
    def destdir(self):
        return self.__destdir

    @classmethod
    def destroy(cls):
        if cls.USRDIR is not None:
            # clear lingering files
            try:
                shutil.rmtree(cls.USRDIR)
            except:
                pass
            cls.USRDIR = None

    @classmethod
    def get_user_dir(cls):
        if cls.USRDIR is None:
            if MockHitspool.COPY_DIR is not None:
                cls.USRDIR = os.path.join(MockHitspool.COPY_DIR, "UserCopy")
        return cls.USRDIR

    @property
    def host(self):
        return self.__host

    @property
    def hsdir(self):
        return self.__hsdir

    @property
    def req_id(self):
        return self.__req_id


class HsSenderTest(LoggingTestCase):
    # pylint: disable=too-many-public-methods
    # Really?!?!  In a test class?!?!  Shut up, pylint!

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

    def setUp(self):
        super(HsSenderTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)

        # get rid of HsSender's state database
        dbpath = HsSender.HsSender.get_db_path()
        if os.path.exists(dbpath):
            os.unlink(dbpath)

    def tearDown(self):
        try:
            super(HsSenderTest, self).tearDown()
        finally:
            found_error = False

            # clear lingering files
            try:
                MockHitspool.destroy()
            except:
                import traceback
                traceback.print_exc()
                found_error = True

            try:
                MockRequestBuilder.destroy()
            except:
                import traceback
                traceback.print_exc()
                found_error = True

            # close all sockets
            if self.SENDER is not None:
                if not self.SENDER.has_monitor:
                    logging.error("Sender monitor has died")
                    found_error = True
                try:
                    self.SENDER.close_all()
                except:
                    import traceback
                    traceback.print_exc()
                    found_error = True
                self.SENDER = None

            # get rid of HsSender's state database
            dbpath = HsSender.HsSender.get_db_path()
            if os.path.exists(dbpath):
                try:
                    os.unlink(dbpath)
                except:
                    import traceback
                    traceback.print_exc()
                    found_error = True

            if found_error:
                self.fail("Found one or more errors during tear-down")

    def test_bad_dir_name(self):
        sender = FailableSender()
        self.SENDER = sender

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create fake directory paths
        hsdir = MockHitspool.create_copy_files("XXX", "12345678_987654",
                                               "ichub01", firstnum, numfiles,
                                               real_stuff=True)
        usrdir = os.path.join(MockHitspool.COPY_DIR, "UserCopy")

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        datadir = os.path.basename(hsdir)

        # run it!
        sender.move_to_destination_dir(hsdir, usrdir)

        # files should have been moved!
        self.assertTrue(sender.moved_files(), "Should have moved files")

        # make sure 0MQ communications checked out
        sender.validate()

    def test_no_move(self):
        sender = FailableSender()
        self.SENDER = sender

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        # create fake directory paths
        hsdir = MockHitspool.create_copy_files("ANON", "12345678_987654",
                                               "ichub01", firstnum, numfiles,
                                               real_stuff=True)
        if hsdir.endswith('/'):
            usrdir = os.path.dirname(hsdir[:-1])
        else:
            usrdir = os.path.dirname(hsdir)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        sender.move_to_destination_dir(hsdir, usrdir)

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
        hsdir = MockHitspool.create_copy_files("HESE", "12345678_987654",
                                               "ichub01", firstnum, numfiles,
                                               real_stuff=True)
        usrdir = os.path.join(MockHitspool.COPY_DIR, "UserCopy")

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        sender.move_to_destination_dir(hsdir, usrdir)

        # files should have been moved!
        self.assertTrue(sender.moved_files(), "Should have moved files")

        # make sure 0MQ communications checked out
        sender.validate()

    def test_real_copy_sn_alert(self):
        sender = MySender()
        self.SENDER = sender

        req = MockRequestBuilder(None, MockRequestBuilder.SNDAQ, None, None,
                                 "12345678_987654", "ichub01", 11, 3)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        sender.move_to_destination_dir(req.hsdir, req.destdir)

        req.check_files()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_spade_data_nonstandard_prefix(self):
        sender = FailableSender()
        self.SENDER = sender

        # initialize directory parts
        category = "XXX"
        timetag = "12345678_987654"
        host = "ichub01"

        # initialize HitSpool file parameters
        firstnum = 11
        numfiles = 3

        req = MockRequestBuilder(None, category, None, None, timetag, host,
                                 firstnum, numfiles)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        mybase = "%s_%s_%s" % (category, timetag, host)
        mytar = "%s%s" % (mybase, HsSender.HsSender.TAR_SUFFIX)
        if HsSender.HsSender.WRITE_META_XML:
            mysem = "%s%s" % (mybase, HsSender.HsSender.META_SUFFIX)
        else:
            mysem = "%s%s" % (mybase, HsSender.HsSender.SEM_SUFFIX)

        # create real directories
        hsdir = MockHitspool.create_copy_files(category, timetag, host,
                                               firstnum, numfiles,
                                               real_stuff=True)

        # clean up test files
        for fnm in (mytar, mysem):
            tmppath = os.path.join(sender.HS_SPADE_DIR, fnm)
            if os.path.exists(tmppath):
                os.unlink(tmppath)

        # run it!
        (tarname, semname) \
            = sender.spade_pickup_data(hsdir, mybase, prefix=category)
        self.assertEquals(mytar, tarname,
                          "Expected tarfile to be named \"%s\" not \"%s\"" %
                          (mytar, tarname))
        self.assertEquals(mysem, semname,
                          "Expected semaphore to be named \"%s\" not \"%s\"" %
                          (mysem, semname))

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
        hsdir = MockHitspool.create_copy_files(category, timetag, host,
                                               firstnum, numfiles,
                                               real_stuff=False)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Fake Tar Error")
        self.expectLogMessage("Please put the data manually in the SPADE"
                              " directory. Use HsSpader.py, for example.")

        # run it!
        result = sender.spade_pickup_data(hsdir, "ignored", prefix=category)
        self.assertIsNone(result, "spade_pickup_data() should return None,"
                          " not %s" % str(result))

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
        hsdir = MockHitspool.create_copy_files(category, timetag, host,
                                               firstnum, numfiles,
                                               real_stuff=False)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Fake Move Error")
        self.expectLogMessage("Please put the data manually in the SPADE"
                              " directory. Use HsSpader.py, for example.")

        # run it!
        result = sender.spade_pickup_data(hsdir, "ignored", prefix=category)
        self.assertIsNone(result, "spade_pickup_data() should return None,"
                          " not %s" % str(result))

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
        hsdir = MockHitspool.create_copy_files(category, timetag, host,
                                               firstnum, numfiles,
                                               real_stuff=False)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Fake Touch Error")
        self.expectLogMessage("Please put the data manually in the SPADE"
                              " directory. Use HsSpader.py, for example.")

        # run it!
        result = sender.spade_pickup_data(hsdir, "ignored", prefix=category)
        self.assertIsNone(result, "spade_pickup_data() should return None,"
                          " not %s" % str(result))

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
        hsdir = MockHitspool.create_copy_files(category, timetag, host,
                                               firstnum, numfiles,
                                               real_stuff=True)

        # set SPADE path to something which exists everywhere
        sender.HS_SPADE_DIR = tempfile.mkdtemp(prefix="SPADE_")

        mybase = "%s_%s_%s" % (category, timetag, host)
        mytar = "HS_%s%s" % (mybase, HsSender.HsSender.TAR_SUFFIX)
        if HsSender.HsSender.WRITE_META_XML:
            mysem = "HS_%s%s" % (mybase, HsSender.HsSender.META_SUFFIX)
        else:
            mysem = "HS_%s%s" % (mybase, HsSender.HsSender.SEM_SUFFIX)

        # create intermediate directory
        movetop = tempfile.mkdtemp(prefix="Intermediate_")
        movedir = os.path.join(movetop, mybase)
        os.makedirs(movedir)

        # copy hitspool files to intermediate directory
        shutil.copytree(hsdir, os.path.join(movedir,
                                            os.path.basename(hsdir)))

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages

        # clean up test files
        for fnm in (mytar, mysem):
            tmppath = os.path.join(sender.HS_SPADE_DIR, fnm)
            if os.path.exists(tmppath):
                os.unlink(tmppath)

        # run it!
        (tarname, semname) = sender.spade_pickup_data(movedir, mybase,
                                                      prefix=category)
        self.assertEquals(mytar, tarname,
                          "Expected tarfile to be named \"%s\" not \"%s\"" %
                          (mytar, tarname))
        self.assertEquals(mysem, semname,
                          "Expected semaphore to be named \"%s\" not \"%s\"" %
                          (mysem, semname))

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
        MockRequestBuilder.check_hitspool_file_list(names, firstnum, numfiles)

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_no_msg(self):
        sender = MySender()
        self.SENDER = sender

        # initialize message
        no_msg = None

        # add all expected JSON messages
        sender.reporter.addIncoming(no_msg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        sender.mainloop()

        # wait for message to be processed
        sender.wait_for_idle()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_str_msg(self):
        sender = MySender()
        self.SENDER = sender

        # initialize message
        msg = "abc"
        snd_msg = json.dumps("abc")

        # add all expected JSON messages
        sender.reporter.addIncoming(snd_msg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        try:
            sender.mainloop()
        except HsException, hse:
            hsestr = str(hse)
            expstr = "Message is not a dictionary: \"\"%s\"\"<%s>" % \
                     (msg, type(msg))
            if hsestr.find(expstr) < 0:
                self.fail("Unexpected exception: " + hsestr)

        # wait for message to be processed
        sender.wait_for_idle()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_incomplete_msg(self):
        sender = MySender()
        self.SENDER = sender

        # initialize message
        rcv_msg = {"msgtype": "rsync_sum"}

        # add all expected JSON messages
        sender.reporter.addIncoming(rcv_msg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        try:
            sender.mainloop()
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("Missing fields ") < 0:
                self.fail("Unexpected exception: " + hsestr)

        # wait for message to be processed
        sender.wait_for_idle()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_unknown_msg(self):
        sender = MySender()
        self.SENDER = sender

        # initialize message
        rcv_msg = {
            "msgtype": "xxx",
            "request_id": None,
            "username": None,
            "start_time": None,
            "stop_time": None,
            "copy_dir": None,
            "destination_dir": None,
            "prefix": None,
            "extract": None,
            "host": None,
        }

        # add all expected JSON messages
        sender.reporter.addIncoming(rcv_msg)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # run it!
        try:
            sender.mainloop()
        except HsException, hse:
            hsestr = str(hse)
            if hsestr.find("No date/time specified") < 0:
                self.fail("Unexpected exception: " + hsestr)

        # wait for message to be processed
        sender.wait_for_idle()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_no_init_just_success(self):
        sender = MySender()
        self.SENDER = sender

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        start_utc = get_time(start_ticks)
        stop_utc = get_time(stop_ticks)

        req = MockRequestBuilder("1234abcd", MockRequestBuilder.SNDAQ,
                                 start_utc, stop_utc, "12345678_987654",
                                 "ichub01", 11, 3)

        msgtype = HsMessage.MESSAGE_DONE
        req.add_request(sender.reporter, msgtype)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add all expected log messages
        self.expectLogMessage("Request %s was not initialized (received %s"
                              " from %s)" % (req.req_id, msgtype, req.host))
        self.expectLogMessage("Saw %s message for request %s host %s without"
                              " a START message" % (msgtype, req.req_id,
                                                    req.host))

        req.add_i3live_message(sender.i3socket, HsUtil.STATUS_SUCCESS,
                               success="1")

        # run it!
        sender.mainloop()

        # wait for message to be processed
        sender.wait_for_idle()

        # make sure expected files were copied
        req.check_files()

        # make sure 0MQ communications checked out
        sender.validate()

    def test_main_loop_multi_request(self):
        sender = MySender()
        self.SENDER = sender

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        start_utc = get_time(start_ticks)
        stop_utc = get_time(stop_ticks)

        # request details
        req_id = "1234abcd"
        req_type = MockRequestBuilder.SNDAQ
        timetag = "12345678_987654"

        # create two requests
        req1 = MockRequestBuilder(req_id, req_type, start_utc, stop_utc,
                                  timetag, "ichub01", 11, 3)
        req2 = MockRequestBuilder(req_id, req_type, start_utc, stop_utc,
                                  timetag, "ichub86", 11, 3)

        # add initial message
        req1.add_request(sender.reporter, HsMessage.MESSAGE_INITIAL)

        # add initial message for Live
        req1.add_i3live_message(sender.i3socket, HsUtil.STATUS_QUEUED)

        # add start messages
        msgtype = HsMessage.MESSAGE_STARTED
        req1.add_request(sender.reporter, msgtype)
        req2.add_request(sender.reporter, msgtype)

        # add in-progress message for Live
        req1.add_i3live_message(sender.i3socket, HsUtil.STATUS_IN_PROGRESS)

        # add done messages
        msgtype = HsMessage.MESSAGE_DONE
        req1.add_request(sender.reporter, msgtype)
        req2.add_request(sender.reporter, msgtype)

        # don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # add final message for Live
        req2.add_i3live_message(sender.i3socket, HsUtil.STATUS_SUCCESS,
                                success="1,86")

        # run it!
        while sender.reporter.has_input:
            sender.mainloop()

        # wait for message to be processed
        sender.wait_for_idle()

        req1.check_files()
        req2.check_files()

        # make sure 0MQ communications checked out
        sender.validate()


if __name__ == '__main__':
    unittest.main()
