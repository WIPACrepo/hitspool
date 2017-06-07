#!/usr/bin/env python


import datetime
import json
import logging
import os
import re
import threading
import time
import unittest

import HsConstants
import HsMessage
import HsPublisher
import HsRSyncFiles
import HsSender
import HsWorker
import HsTestUtil
import HsUtil

from HsBase import DAQTime
from HsPrefix import HsPrefix
from LoggingTestCase import LoggingTestCase
from RequestMonitor import RequestMonitor

class MockSenderSocket(HsTestUtil.Mock0MQSocket):
    def __init__(self, name="Sender", verbose=False):
        super(MockSenderSocket, self).__init__(name, verbose=verbose)

    def send_json(self, jstr):
        if isinstance(jstr, str):
            raise Exception("Got string JSON object %s<%s>" %
                            (jstr, type(jstr)))

        super(MockSenderSocket, self).send_json(jstr)


class MockPubSocket(object):
    def __init__(self, name, pubsub, verbose=False):
        self.__name = name
        self.__pubsub = pubsub
        self.__closed = False
        self.__verbose = verbose

    def __str__(self):
        cstr = "[CLOSED]" if self.__closed else ""
        return "%s(%s%s)" % (type(self).__name__, self.__name, cstr)

    def close(self):
        self.__closed = True

    @property
    def name(self):
        return self.__name

    def send(self, msg):
        if self.__verbose:
            print "%s(%s) -> %s" % (type(self).__name__, self.__name, str(msg))
        self.__pubsub.send_to_subs(msg)

    def send_json(self, msg):
        if self.__verbose:
            print "%s(%s) -> %s" % (type(self).__name__, self.__name, str(msg))
        self.__pubsub.send_to_subs(msg)

    def validate(self):
        if not self.__closed:
            raise Exception("Publisher socket was not closed")


class MockPubSubSocket(object):
    def __init__(self, name, verbose=False):
        self.__pub = MockPubSocket(name, self, verbose=verbose)
        self.__subs = []
        self.__verbose = verbose
        self.__next_num = 0

    def close(self):
        self.__pub.close()
        for sub in self.__subs:
            sub.close()

    @property
    def publisher(self):
        return self.__pub

    def subscribe(self):
        name = "%s#%d" % (self.__pub.name, self.__next_num)
        self.__next_num += 1
        sub = MockSubSocket(name, verbose=self.__verbose)
        self.__subs.append(sub)
        return sub

    def send_to_subs(self, msg):
        for sub in self.__subs:
            sub.recv_from_pub(msg)

    def validate(self):
        pass


class MockPullSocket(HsTestUtil.MockPollableSocket):
    def __init__(self, name, verbose=False):
        self.__name = name
        self.__queueLock = threading.Condition()
        self.__outqueue = []
        self.__expLock = threading.Condition()
        self.__expected = []
        self.__closed = False
        self.__verbose = verbose

    def __str__(self):
        cstr = "[CLOSED]" if self.__closed else ""
        explen = len(self.__expected)
        xstr = "" if explen == 0 else ", %d expected" % explen
        return "%s(%s%s%s)#%d" % \
            (type(self).__name__, self.__name, cstr, xstr,
             len(self.__outqueue))

    def addExpected(self, jdict):
        with self.__expLock:
            self.__expected.append(jdict)

    def close(self):
        with self.__queueLock:
            self.__closed = True
            self.__queueLock.notifyAll()

    @property
    def has_input(self):
        with self.__queueLock:
            return not self.__closed or len(self.__outqueue) > 0

    def recv_json(self):
        with self.__queueLock:
            while True:
                if len(self.__outqueue) > 0:
                    return self.__outqueue.pop(0)
                if self.__closed:
                    break
                self.__queueLock.wait()

    def send_json(self, msgstr):
        if len(self.__expected) == 0:
            raise HsTestUtil.CompareException("Unexpected %s message: %s" %
                                              (self.__name, msgstr))

        try:
            msgjson = json.loads(msgstr)
        except:
            msgjson = msgstr

        with self.__expLock:
            found = None
            for i in range(len(self.__expected)):
                if i >= len(self.__expected):
                    break
                expjson = self.__expected[i]
                try:
                    HsTestUtil.CompareObjects(self.__name, msgjson, expjson)
                    found = i
                    break
                except:
                    continue

            # if the message was unknown, throw a CompareException
            if found is None:
                if len(self.__expected) == 0:
                    xstr = ""
                else:
                    xstr = "\n\t(exp %s)" % str(self.__expected[0])

                raise HsTestUtil.CompareException("Unexpected %s(%s) message"
                                                  " (of %d): %s%s" %
                                                  (type(self).__name__,
                                                   self.__name,
                                                   len(self.__expected),
                                                   msgjson, xstr))

            # we received an expected message, delete it
            expjson = self.__expected[found]
            del self.__expected[found]

            if self.__verbose:
                explen = len(self.__expected)
                print "%s(%s) <- %s (exp %s)" % \
                    (type(self).__name__, self.__name, msgjson, expjson)
                print "%s(%s) expect %d more message%s" % \
                    (type(self).__name__, self.__name, explen,
                     "s" if explen != 1 else "")

        with self.__queueLock:
            if self.__closed:
                raise Exception("Cannot send from closed %s socket" %
                                str(self.__name))

            self.__outqueue.append(msgjson)
            self.__queueLock.notify()

    def set_verbose(self, value=True):
        self.__verbose = True

    def validate(self):
        if not self.__closed:
            raise Exception("%s<%s> was not closed" %
                            (self.__name, type(self).__name__))
        if len(self.__outqueue) > 0:
            raise Exception("%s<%s> queue contains %s entries (%s)" % \
                            (self.__name, type(self).__name__,
                             len(self.__outqueue), self.__outqueue))
        with self.__queueLock:
            self.__queueLock.notify()


class MockPushPullSocket(object):
    def __init__(self, name, verbose=False):
        self.__name = name
        self.__pushers = []
        self.__puller = MockPullSocket(self.__name + "Pull", verbose=verbose)
        self.__verbose = verbose

    def __str__(self):
        pushstr = None
        for psh in self.__pushers:
            if pushstr is None:
                pushstr = ""
                add_slash = True
            else:
                pushstr += " / "
            pushstr += str(psh)

        return "%s(%s -> %s)" % (type(self).__name__, pushstr, self.__puller)

    def close_pusher(self, pusher):
        found = False
        for i in range(len(self.__pushers)):
            if self.__pushers[i] == pusher:
                del self.__pushers[i]
                found = True
                break

        if not found:
            raise Exception("Cannot find pusher %s" % str(pusher))
        if len(self.__pushers) == 0:
            self.__puller.close()

    def create_pusher(self, name):
        pusher = MockPushSocket(self, name, verbose=self.__verbose)
        self.__pushers.append(pusher)
        return pusher

    @property
    def puller(self):
        return self.__puller

    def send_json(self, msg):
        self.__puller.send_json(msg)


class MockPushSocket(object):
    def __init__(self, parent, name, verbose=False):
        self.__parent = parent
        self.__name = name
        self.__closed = False
        self.__verbose = verbose

    def __str__(self):
        cstr = "[CLOSED]" if self.__closed else ""
        return "%s(%s%s)" % (type(self).__name__, self.__name, cstr)

    def addExpected(self, msg):
        pass

    def close(self):
        self.__closed = True
        self.__parent.close_pusher(self)

    def send_json(self, msg):
        if self.__verbose:
            print "%s <- %s(%s): %s" % (self.__parent, type(self).__name__,
                                        self.__name, str(msg))
        self.__parent.send_json(msg)

    def set_verbose(self, value=True):
        self.__verbose = (value is True)

    def validate(self):
        if not self.__closed:
            raise Exception("%s<%s> was not closed" %
                            (self.__name, type(self).__name__))


class MockSubSocket(object):
    def __init__(self, name, verbose=False):
        self.__name = name
        self.__queueLock = threading.Condition()
        self.__outqueue = []
        self.__closed = False
        self.__verbose = verbose

    def close(self):
        self.__closed = True

    @property
    def has_input(self):
        with self.__queueLock:
            return len(self.__outqueue) > 0

    def recv_from_pub(self, msg):
        with self.__queueLock:
            self.__outqueue.append(msg)
            self.__queueLock.notify()

    def recv_json(self):
        with self.__queueLock:
            while True:
                if len(self.__outqueue) == 0:
                    self.__queueLock.wait()
                if len(self.__outqueue) > 0:
                    return self.__outqueue.pop(0)

    def validate(self):
        if not self.__closed:
            raise Exception("%s<%s> was not closed" %
                            (self.__name, type(self).__name__))
        if len(self.__outqueue) > 0:
            raise Exception("%s<%s> queue contains %s entries (%s)" %
                            (self.__name, type(self).__name__,
                             len(self.__outqueue), self.__outqueue))
        with self.__queueLock:
            self.__queueLock.notify()


class MyPublisher(HsPublisher.Receiver):
    def __init__(self, snd_sock, verbose=False):
        self.__snd_sock = snd_sock
        self.__alert_sock = None
        self.__i3_sock = None

        super(MyPublisher, self).__init__(host="mypublisher", is_test=True)

        if verbose:
            self.alert_socket.set_verbose()
            self.i3socket.set_verbose()
            self.sender.set_verbose()

    def create_alert_socket(self):
        if self.__alert_sock is not None:
            raise Exception("Cannot create multiple alert sockets")

        self.__alert_sock = HsTestUtil.Mock0MQSocket("AlertSocket")
        return self.__alert_sock

    def create_i3socket(self, host):
        if self.__i3_sock is not None:
            raise Exception("Cannot create multiple I3 sockets")

        self.__i3_sock = HsTestUtil.MockI3Socket("Pub2Live")
        return self.__i3_sock

    def create_sender_socket(self, host):
        if self.__snd_sock is None:
            self.__snd_sock = HsTestUtil.Mock0MQSocket("Pub2Sndr")
        return self.__snd_sock

    def create_workers_socket(self):
        raise NotImplementedError

    @property
    def name(self):
        return "Publisher"

    def run_test(self):
        if self.__alert_sock is None:
            raise Exception("Alert socket does not exist")
        while True:
            self.reply_request()
            if not self.__alert_sock.has_input:
                break
            time.sleep(0.1)
        self.close_all()

    def validate(self):
        for sock in (self.__alert_sock, self.__i3_sock, self.__snd_sock):
            if sock is not None:
                sock.validate()


class MySender(HsSender.HsSender):
    def __init__(self, reporter, workers, verbose=False):
        self.__msg_sock = reporter
        self.__wrk_sock = workers
        self.__verbose = verbose

        super(MySender, self).__init__(host="mysender", is_test=True)

    def __str__(self):
        return "MySender@%s" % self.shorthost

    def create_i3socket(self, host):
        sock = HsTestUtil.MockI3Socket("Snd2Live", verbose=self.__verbose)
        return sock

    def create_poller(self):
        return HsTestUtil.Mock0MQPoller("Poller")

    def create_reporter(self):
        return self.__msg_sock

    def create_workers_socket(self):
        return self.__wrk_sock

    def run_test(self):
        while True:
            if not self.process_one_message():
                pass
            if not self.__msg_sock.has_input:
                break
            time.sleep(0.1)
        self.close_all()

    def validate(self):
        """
        Check that all expected messages were received by mock sockets
        """
        self.reporter.validate()
        self.i3socket.validate()


class MyWorker(HsWorker.Worker):
    def __init__(self, num, host, sub_sock, sender_sock):
        self.__num = num
        self.__sub_sock = sub_sock

        self.__i3_sock = None
        self.__sender_sock = sender_sock

        self.__link_paths = []

        super(MyWorker, self).__init__(self.name, host=host, is_test=True)

        # don't sleep during unit tests
        self.MIN_DELAY = 0.0

    @classmethod
    def __timetag(cls, starttime):
        return starttime.strftime("%Y%m%d_%H%M%S")

    def add_expected_links(self, base_utc, rundir, firstnum, numfiles,
                           i3socket=None, finaldir=None):
        timetag = self.__timetag(base_utc)
        srcdir = self.TEST_HUB_DIR
        for i in xrange(firstnum, firstnum + numfiles):
            frompath = os.path.join(srcdir, rundir, "HitSpool-%d.dat" % i)
            self.__link_paths.append((frompath, srcdir, timetag))

    def check_for_unused_links(self):
        llen = len(self.__link_paths)
        if llen > 0:
            raise Exception("Found %d extra link%s" %
                            (llen, "" if llen == 1 else "s"))

    def create_i3socket(self, host):
        if self.__i3_sock is not None:
            raise Exception("Cannot create multiple I3 sockets")

        self.__i3_sock = HsTestUtil.MockI3Socket("%s@%s" %
                                                 (self.name, self.shorthost))
        return self.__i3_sock

    def create_sender_socket(self, host):
        if self.__sender_sock is None:
            self.__sender_sock = MockSenderSocket("%s->Sender" %
                                                  self.shorthost)
        return self.__sender_sock

    def create_subscriber_socket(self, host):
        if self.__sub_sock is None:
            raise Exception("Subscriber socket has not been set")
        return self.__sub_sock

    def hardlink(self, filename, targetdir):
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
            raise Exception("Expected to link \"%s\" to \"%s/*%s\", not to"
                            " target \"%s\"" %
                            (expfile, expdir, exptag, targetdir))
        elif filename != expfile:
            raise Exception("Expected to link \"%s\" not \"%s\"" %
                            (expfile, filename))

        # create empty file if it does not exist
        if not os.path.exists(filename):
            fdir = os.path.dirname(filename)
            if not os.path.exists(fdir):
                os.makedirs(fdir)
            open(filename, "w").close()

        super(MyWorker, self).hardlink(filename, targetdir)

    @property
    def name(self):
        return "Worker#%d" % self.__num

    def run_test(self):
        if self.__sub_sock is None:
            raise Exception("Subscriber socket does not exist")
        while True:
            # create logfile
            logfile = "/tmp/%s.log" % self.shorthost
            open(logfile, "w").close()

            self.mainloop(logfile, fail_sleep=0.001)
            if not self.__sub_sock.has_input:
                break
        self.close_all()

    def validate(self):
        for sock in (self.__i3_sock, self.__sender_sock, self.__sub_sock):
            if sock is not None:
                sock.validate()


class IntegrationTest(LoggingTestCase):
    TICKS_PER_SECOND = 10000000000
    INTERVAL = 15 * TICKS_PER_SECOND

    MATCH_ANY = re.compile(r"^.*$")

    def __create_hsdir(self, workers, spoolname, start_ticks, stop_ticks):
        HsTestUtil.MockHitspool.create_copy_dir(workers[0])
        hspath = HsTestUtil.MockHitspool.create(workers[0], spoolname)
        HsTestUtil.MockHitspool.add_files(hspath,
                                          start_ticks - self.TICKS_PER_SECOND,
                                          stop_ticks + self.TICKS_PER_SECOND,
                                          self.INTERVAL, create_files=True)
        for i in range(1, len(workers)):
            workers[i].TEST_COPY_DIR = HsTestUtil.MockHitspool.COPY_DIR
            workers[i].TEST_HUB_DIR = HsTestUtil.MockHitspool.HUB_DIR

    def __create_publisher_thread(self, pub):
        thrd = threading.Thread(name="Publisher", target=pub.run_test, args=())
        thrd.setDaemon(True)
        return thrd

    def __create_sender_thread(self, snd):
        thrd = threading.Thread(name="Sender", target=snd.run_test, args=())
        thrd.setDaemon(True)
        return thrd

    def __create_worker_thread(self, wrk):
        thrd = threading.Thread(name=wrk.name, target=wrk.run_test, args=())
        thrd.setDaemon(True)
        return thrd

    def __delete_state(self):
        try:
            HsTestUtil.MockHitspool.destroy()
        except:
            import traceback
            traceback.print_exc()

        # get rid of HsSender's state database
        dbpath = RequestMonitor.get_db_path()
        if os.path.exists(dbpath):
            try:
                os.unlink(dbpath)
            except:
                import traceback
                traceback.print_exc()

    def __init_publisher(self, publisher, req_id, username, prefix,
                         start_ticks, stop_ticks, copydir):
        start_utc = HsTestUtil.get_time(start_ticks)
        stop_utc = HsTestUtil.get_time(stop_ticks)

        # request message
        alertdict = {
            "start": start_ticks / 10,
            "stop": stop_ticks / 10,
            "copy": copydir,
            "request_id": req_id,
            "username": username,
        }

        # initialize incoming socket and add expected message(s)
        publisher.alert_socket.addIncoming(alertdict)
        publisher.alert_socket.addExpected("DONE\0")

    def __init_sender(self, sender, workers, req_id, username, prefix, destdir,
                      start_ticks, stop_ticks, success=None):
        start_time = DAQTime(start_ticks)
        stop_time = DAQTime(stop_ticks)

        # I3Live status message value
        status_queued = {
            "request_id": req_id,
            "username": username,
            "prefix": prefix,
            "start_time": HsTestUtil.TIME_PAT,
            "stop_time": HsTestUtil.TIME_PAT,
            "destination_dir": destdir,
            "status": HsUtil.STATUS_QUEUED,
            "update_time": HsTestUtil.TIME_PAT,
        }
        sender.i3socket.addExpectedMessage(status_queued, service="hitspool",
                                           varname="hsrequest_info",
                                           time=self.MATCH_ANY, prio=1)

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % sender.cluster
        notify_lines = [
            'Start: %s' % start_time.utc,
            'Stop: %s' % stop_time.utc,
            '(no possible leapseconds applied)',
        ]
        notify_pat = re.compile(r".*" + re.escape("\n".join(notify_lines)),
                                flags=re.MULTILINE)

        address_list = HsConstants.ALERT_EMAIL_DEV[:]
        if prefix == HsPrefix.SNALERT:
            address_list += HsConstants.ALERT_EMAIL_SN

        sender.i3socket.addGenericEMail(address_list, notify_hdr, notify_pat,
                                        prio=1)

        status_in_progress = status_queued.copy()
        status_in_progress["status"] = HsUtil.STATUS_IN_PROGRESS
        sender.i3socket.addExpectedMessage(status_in_progress,
                                           service="hitspool",
                                           varname="hsrequest_info",
                                           time=self.MATCH_ANY, prio=1)

        status_success = status_queued.copy()
        status_success["status"] = HsUtil.STATUS_SUCCESS
        status_success["success"] = "1,66"
        sender.i3socket.addExpectedMessage(status_success,
                                           service="hitspool",
                                           varname="hsrequest_info",
                                           time=self.MATCH_ANY, prio=1)

        # initial request from publisher
        msg_initial = {
            "request_id": req_id,
            "username": username,
            "prefix": prefix,
            "start_time": start_ticks,
            "stop_time": stop_ticks,
            "destination_dir": destdir,
            "msgtype": HsMessage.INITIAL,
            "extract": False,
            "host": "mypublisher",
            "hubs": None,
            "version": HsMessage.DEFAULT_VERSION,
            "copy_dir": None,
        }
        sender.reporter.addExpected(msg_initial)

        for wrk in workers:
            msg_started = msg_initial.copy()
            msg_started["msgtype"] = HsMessage.STARTED
            msg_started["host"] = wrk.shorthost
            sender.reporter.addExpected(msg_started)

            msg_working = msg_started.copy()
            msg_working["msgtype"] = HsMessage.WORKING
            sender.reporter.addExpected(msg_working)
            # hard-coded to only expect 2 file transfers
            sender.reporter.addExpected(msg_working)

            msg_done = msg_started.copy()
            msg_done["msgtype"] = HsMessage.DONE
            msg_done["copy_dir"] = re.compile(os.path.join(destdir, prefix) +
                                              r"_\d+_\d+_" +
                                              wrk.shorthost)
            sender.reporter.addExpected(msg_done)

    def __init_worker(self, worker, req_id, username, prefix, spoolname,
                      start_ticks, stop_ticks, destdir):

        msg_started = {
            "request_id": req_id,
            "username": username,
            "prefix": prefix,
            "start_time": start_ticks,
            "stop_time": stop_ticks,
            "destination_dir": destdir,
            "msgtype": HsMessage.STARTED,
            "extract": False,
            "host": worker.shorthost,
            "hubs": None,
            "version": HsMessage.DEFAULT_VERSION,
            "copy_dir": None,
        }
        worker.sender.addExpected(msg_started)

        msg_working = msg_started.copy()
        msg_working["msgtype"] = HsMessage.WORKING
        worker.sender.addExpected(msg_working)
        worker.sender.addExpected(msg_working)

        msg_done = msg_started.copy()
        msg_done["msgtype"] = HsMessage.DONE
        # build a copy directory path which substitutes wildcards for date/time
        msg_done["copy_dir"] = re.compile(os.path.join(destdir, prefix) +
                                          r"_\d+_\d+_" +
                                          worker.shorthost)
        worker.sender.addExpected(msg_done)

        # build timetag used to construct final destination
        utcstart = HsTestUtil.get_time(start_ticks)
        plus30 = utcstart + datetime.timedelta(0, 30)

        # reformat time string for file names
        timetag = plus30.strftime("%Y%m%d_%H%M%S")

        # TODO should compute the expected number of files
        worker.add_expected_links(plus30, spoolname, 658, 2,
                                  i3socket=worker.i3socket,
                                  finaldir=HsTestUtil.MockHitspool.COPY_DIR)

    @classmethod
    def allow_out_of_order(cls):
        return True

    def setUp(self):
        super(IntegrationTest, self).setUp()

        HsSender.RequestMonitor.EXPIRE_SECONDS = 15.0

        # by default, don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

        # point the RequestMonitor at a temporary state file for tests
        HsTestUtil.set_state_db_path()

        self.__delete_state()

        from DumpThreads import DumpThreadsOnSignal
        DumpThreadsOnSignal()

    def tearDown(self):
        try:
            super(IntegrationTest, self).tearDown()
        finally:
            self.__delete_state()

    def test_publisher_to_worker(self):
        verbose = False

        snd2wrk = MockPubSubSocket("Snd2Wrk", verbose=verbose)

        push2snd = MockPushPullSocket("SndrMsg", verbose=verbose)

        publisher = MyPublisher(push2snd.create_pusher("publisher"),
                                verbose=verbose)

        workers = (MyWorker(1, "ichub01.usap.gov", snd2wrk.subscribe(),
                            push2snd.create_pusher("ichub01")),
                   MyWorker(2, "ichub66.usap.gov", snd2wrk.subscribe(),
                            push2snd.create_pusher("ichub66")),
                   )

        sender = MySender(push2snd.puller, snd2wrk.publisher, verbose=verbose)

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000

        spooldir = HsRSyncFiles.HsRSyncFiles.DEFAULT_SPOOL_NAME
        self.__create_hsdir(workers, spooldir, start_ticks, stop_ticks)

        # set up request fields
        req_id = "FAKE_ID"
        username = "test_pdaq"
        prefix = HsPrefix.SNALERT
        destdir = workers[0].TEST_COPY_DIR

        # initialize HS services
        self.__init_publisher(publisher, req_id, username, prefix, start_ticks,
                              stop_ticks, destdir)
        for wrk in workers:
            self.__init_worker(wrk, req_id, username, prefix, spooldir,
                               start_ticks, stop_ticks, destdir)
        self.__init_sender(sender, workers, req_id, username, prefix, destdir,
                           start_ticks, stop_ticks, success="1,66")

        # create HS service threads
        thrds = [self.__create_publisher_thread(publisher), ]
        for wrk in workers:
            thrds.append(self.__create_worker_thread(wrk))
        thrds.append(self.__create_sender_thread(sender))

        # start all threads
        for thrd in reversed(thrds):
            thrd.start()

        # wait for threads to finish
        for _ in range(25):
            alive = False
            for thrd in thrds:
                if thrd.isAlive():
                    thrd.join(1)
                    if thrd.isAlive():
                        alive = True
                        break
            if not alive:
                break
            time.sleep(0.1)

        # validate services
        progs = [publisher, ]
        progs += workers
        progs.append(sender)
        failed = 0
        for prg in progs:
            try:
                prg.validate()
            except Exception, cex:
                import traceback
                traceback.print_exc()
                failed = 1
        if failed > 0:
            raise HsTestUtil.CompareException("Failed to validate"
                                              " %d programs" % failed)

if __name__ == '__main__':
    unittest.main()
