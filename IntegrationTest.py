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
import HsPublisher
import HsSender
import HsWorker
import HsTestUtil

from LoggingTestCase import LoggingTestCase


class MockSenderSocket(HsTestUtil.Mock0MQSocket):
    def __init__(self, name="Sender"):
        super(MockSenderSocket, self).__init__(name)

    def send_json(self, jstr):
        if not isinstance(jstr, str):
            raise Exception("Got non-string JSON object %s<%s>" %
                            (jstr, type(jstr)))

        super(MockSenderSocket, self).send_json(json.loads(jstr))


class MockPubSocket(object):
    def __init__(self, pubsub):
        self.__pubsub = pubsub
        self.__closed = False

    def close(self):
        self.__closed = True

    def send(self, msg):
        self.__pubsub.send_to_subs(msg)

    def validate(self):
        rtnval = True
        if not self.__closed:
            rtnval = False
        return rtnval


class MockPubSubSocket(object):
    def __init__(self):
        self.__pub = MockPubSocket(self)
        self.__subs = []

    @property
    def publisher(self):
        return self.__pub

    def subscribe(self):
        sub = MockSubSocket()
        self.__subs.append(sub)
        return sub

    def send_to_subs(self, msg):
        for sub in self.__subs:
            sub.recv_from_pub(msg)

    def validate(self):
        return True


class MockPullSocket(object):
    def __init__(self):
        self.__queueLock = threading.Condition()
        self.__queue = []
        self.__closed = False

    def close(self):
        self.__closed = True

    @property
    def has_input(self):
        with self.__queueLock:
            return not self.__closed or len(self.__queue) > 0

    def recv_json(self):
        with self.__queueLock:
            while True:
                if len(self.__queue) == 0:
                    self.__queueLock.wait()
                if len(self.__queue) > 0:
                    return self.__queue.pop(0)

    def send_json(self, json):
        with self.__queueLock:
            self.__queue.append(json)
            self.__queueLock.notify()

    def validate(self):
        rtnval = True
        if not self.__closed:
            import sys
            print >>sys.stderr, "MockPullSocket was not closed"
            rtnval = False
        if len(self.__queue) > 0:
            import sys
            print >>sys.stderr, \
                "MockPullSocket queue contains %s entries (%s)" % \
                (len(self.__queue), self.__queue)
        with self.__queueLock:
            self.__queueLock.notify()
        return rtnval


class MockPushPullSocket(object):
    def __init__(self):
        self.__pushers = []
        self.__puller = MockPullSocket()

    def close_pusher(self, pusher):
        found = False
        for i in range(len(self.__pushers)):
            if self.__pushers[i] == pusher:
                del self.__pushers[i]
                found = True
                break

        if not found:
            raise Exception("Cannot find pusher %s" % pusher)
        if len(self.__pushers) == 0:
            self.__puller.close()

    def create_pusher(self):
        pusher = MockPushSocket(self)
        self.__pushers.append(pusher)
        return pusher

    @property
    def puller(self):
        return self.__puller

    def send_json(self, msg):
        self.__puller.send_json(msg)


class MockPushSocket(object):
    def __init__(self, parent):
        self.__parent = parent
        self.__closed = False

    def close(self):
        self.__closed = True
        self.__parent.close_pusher(self)

    def send_json(self, json):
        self.__parent.send_json(json)

    def validate(self):
        rtnval = True
        if not self.__closed:
            import sys
            print >>sys.stderr, "MockPushSocket was not closed"
            rtnval = False
        return rtnval


class MockSubSocket(object):
    def __init__(self):
        self.__queueLock = threading.Condition()
        self.__queue = []
        self.__closed = False

    def close(self):
        self.__closed = True

    @property
    def has_input(self):
        with self.__queueLock:
            return len(self.__queue) > 0

    def recv_from_pub(self, msg):
        with self.__queueLock:
            self.__queue.append(msg)
            self.__queueLock.notify()

    def recv(self):
        with self.__queueLock:
            while True:
                if len(self.__queue) == 0:
                    self.__queueLock.wait()
                if len(self.__queue) > 0:
                    return self.__queue.pop(0)

    def validate(self):
        rtnval = True
        if not self.__closed:
            rtnval = False
        if len(self.__queue) > 0:
            print >>sys.stderr, \
                "MockSubSocket queue contains %s entries (%s)" % \
                (len(self.__queue), self.__queue)
        with self.__queueLock:
            self.__queueLock.notify()
        return rtnval


class MyPublisher(HsPublisher.Receiver):
    def __init__(self, pub_sock):
        self.__pub_sock = pub_sock
        self.__alert_sock = None
        self.__i3_sock = None

        super(MyPublisher, self).__init__(is_test=True)

    def create_alert_socket(self):
        if self.__alert_sock is not None:
            raise Exception("Cannot create multiple alert sockets")

        self.__alert_sock = HsTestUtil.Mock0MQSocket("AlertSocket")
        return self.__alert_sock

    def create_i3socket(self, host):
        if self.__i3_sock is not None:
            raise Exception("Cannot create multiple I3 sockets")

        self.__i3_sock = HsTestUtil.MockI3Socket('HsPublisher')
        return self.__i3_sock

    def create_publisher(self):
        if self.__pub_sock is None:
            self.__pub_sock = HsTestUtil.Mock0MQSocket("Publisher")
        return self.__pub_sock

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
        self.close_all()

    def validate(self):
        for sock in (self.__alert_sock, self.__i3_sock, self.__pub_sock):
            if sock is not None:
                sock.validate()


class MySender(HsSender.HsSender):
    def __init__(self, reporter):
        self.__rpt_sock = reporter

        super(MySender, self).__init__()

    def create_reporter(self):
        if self.__rpt_sock is None:
            self.__rpt_sock = HsTestUtil.Mock0MQSocket("Reporter")
        return self.__rpt_sock

    def create_i3socket(self, host):
        return HsTestUtil.MockI3Socket("HsSender@%s" % self.shorthost)

    def run_test(self):
        while True:
            self.mainloop()
            if not self.__rpt_sock.has_input:
                break
        self.close_all()

    def validate(self):
        """
        Check that all expected messages were received by mock sockets
        """
        self.reporter.validate()
        self.i3socket.validate()


class MyWorker(HsWorker.Worker):
    def __init__(self, num, sub_sock, sender_sock):
        self.__num = num
        self.__sub_sock = sub_sock

        self.__i3_sock = None
        self.__sender_sock = sender_sock

        self.__link_paths = []

        super(MyWorker, self).__init__(self.name, is_test=True)

    @classmethod
    def __timetag(cls, starttime):
        return starttime.strftime("%Y%m%d_%H%M%S")

    def add_expected_links(self, base_utc, rundir, firstnum, numfiles,
                           i3socket=None, ssh_access=None):
        timetag = self.__timetag(base_utc)
        srcdir = self.TEST_HUB_DIR
        for i in xrange(firstnum, firstnum + numfiles):
            frompath = os.path.join(srcdir, rundir, "HitSpool-%d.dat" % i)
            self.__link_paths.append((frompath, srcdir, timetag))
            if i3socket is not None:
                errmsg = "linked %s to tmp dir" % frompath
                i3socket.addExpectedValue(errmsg)
        if i3socket is not None and ssh_access is not None:
            i3socket.addExpectedValue(" 0.0 [MB] HS data transferred"
                                      " to %s " % ssh_access, prio=1)

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
            self.__sender_sock = MockSenderSocket("Sender#%d" % self.__num)
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
                os.makedirs()
            open(filename, "w").close()

        super(MyWorker, self).hardlink(filename, targetdir)

    @property
    def name(self):
        return "Worker#%d" % self.__num

    def run_test(self, logfile):
        if self.__sub_sock is None:
            raise Exception("Subscriber socket does not exist")
        while True:
            self.mainloop(logfile)
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

    def __create_hsdir(self, workers, start_ticks, stop_ticks):
        HsTestUtil.MockHitspool.create_copy_dir(workers[0])
        HsTestUtil.MockHitspool.create(workers[0], "currentRun",
                                       start_ticks - self.TICKS_PER_SECOND,
                                       stop_ticks + self.TICKS_PER_SECOND,
                                       self.INTERVAL)
        HsTestUtil.MockHitspool.create(workers[0], "lastRun",
                                       start_ticks - self.INTERVAL,
                                       start_ticks -
                                       (self.TICKS_PER_SECOND * 2),
                                       self.INTERVAL)
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

    def __create_worker_thread(self, wrk, logfile):
        thrd = threading.Thread(name=wrk.name, target=wrk.run_test,
                                args=(logfile, ))
        thrd.setDaemon(True)
        return thrd

    def __init_publisher(self, publisher, start_ticks, stop_ticks, copydir):
        start_utc = HsTestUtil.get_time(start_ticks)
        stop_utc = HsTestUtil.get_time(stop_ticks)

        # request message
        req_str = "{\"start\": %d, \"stop\": %d, \"copy\": \"%s\"}" % \
                  (start_ticks / 10, stop_ticks / 10, copydir)

        # initialize incoming socket and add expected message(s)
        publisher.alert_socket.addIncoming(req_str)
        publisher.alert_socket.addExpected("DONE\0")

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % publisher.cluster
        notify_lines = [
            req_str,
            'start in UTC : %s' % start_utc,
            'stop  in UTC : %s' % stop_utc,
            '(no possible leapseconds applied)',
        ]

        # initialize I3Live socket and add all expected I3Live messages
        publisher.i3socket.addExpectedValue("Received data request for [%s ,"
                                            " %s] " % (start_utc, stop_utc))
        publisher.i3socket.addExpectedValue("Published request to HsWorkers")
        publisher.i3socket.addExpectedAlert({
            'condition': notify_hdr,
            'desc': 'HsInterface Data Request',
            'notifies': [
                {
                    'notifies_txt': "\n".join(notify_lines),
                    'notifies_header': notify_hdr,
                    'receiver': HsConstants.ALERT_EMAIL_DEV,
                },
                {
                    'notifies_txt': "\n".join(notify_lines),
                    'notifies_header': notify_hdr,
                    'receiver': HsConstants.ALERT_EMAIL_SN,
                },
            ],
            'short_subject': 'true',
            'quiet': 'true',
        })

        # add all expected log messages
        # self.expectLogMessage("Publisher published: %s" % req_str)
        # self.expectLogMessage("send confirmation back to requester: DONE")
        self.expectLogMessage("Quitting: Incoming message queue is empty")

    def __init_sender(self, sender):
        # add all expected log messages
        self.expectLogMessage("Ignoring message type \"log_done\"")
        self.expectLogMessage("Ignoring message type \"log_done\"")

    def __init_worker(self, worker, start_ticks, stop_ticks, copydir, logfile):
        # initialize formatted start/stop times
        utcstart = HsTestUtil.get_time(start_ticks)
        utcstop = HsTestUtil.get_time(stop_ticks)

        # add all expected I3Live messages
        worker.i3socket.addExpectedValue({
            'START': int(start_ticks / 10),
            'UTCSTART': str(utcstart),
            'STOP': int(stop_ticks / 10),
            'UTCSTOP': str(utcstop),
        })

        plus30 = utcstart + datetime.timedelta(0, 30)

        # reformat time string for file names
        timetag = plus30.strftime("%Y%m%d_%H%M%S")

        # directory where HsWorker copies results
        finaldir = HsTestUtil.MockHitspool.COPY_DIR
        finaluser = os.path.join(finaldir, "SNALERT_%s_%s" %
                                 (timetag, worker.fullhost))

        logfiledir = os.path.join(finaldir, "logs")

        # TODO should compute the expected number of files
        worker.add_expected_links(plus30, "currentRun", 0, 1,
                                  i3socket=worker.i3socket,
                                  ssh_access=finaldir)

        # add all expected HsSender messages
        # worker.sender.addExpected({
        #     "hubname": worker.shorthost,
        #     "dataload": "TBD",
        #     "datastart": str(utcstart),
        #     "alertid": timetag,
        #     "datastop": str(utcstop),
        #     "msgtype": "rsync_sum",
        #     "copydir_user": finaldir,
        #     "copydir": finaluser,
        # })
        # worker.sender.addExpected({
        #     "msgtype": "log_done",
        #     "logfile_hsworker": logfile,
        #     "hubname": worker.shorthost,
        #     "logfiledir": logfiledir,
        #     "alertid": timetag,
        # })

        # add all expected log messages
        # self.expectLogMessage("ready for new alert...")
        # self.expectLogMessage("HsWorker received alert message:\n"
        #                       "[{\"start\": %d, \"stop\": %d, \"copy\":"
        #                       " \"%s\"}]\nfrom Publisher" %
        #                       (start_ticks / 10, stop_ticks / 10, copydir))
        # self.expectLogMessage("Requested HS data copy destination differs"
        #                       " from default!")
        # self.expectLogMessage("data will be sent to default"
        #                       " destination: %s" % worker.TEST_COPY_DIR)
        # self.expectLogMessage("start processing alert...")

    @classmethod
    def allow_out_of_order(cls):
        return True

    def setUp(self):
        super(IntegrationTest, self).setUp()

        # by default, don't check DEBUG/INFO log messages
        self.setLogLevel(logging.WARN)

    def tearDown(self):
        try:
            super(IntegrationTest, self).tearDown()
        finally:
            HsTestUtil.MockHitspool.destroy()

    def test_publisher_to_worker(self):
        pub2wrk = MockPubSubSocket()

        wrk2snd = MockPushPullSocket()

        publisher = MyPublisher(pub2wrk.publisher)

        workers = (MyWorker(1, pub2wrk.subscribe(), wrk2snd.create_pusher()),
                   MyWorker(2, pub2wrk.subscribe(), wrk2snd.create_pusher()))

        sender = MySender(wrk2snd.puller)

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        copydir = "foo@localhost:/tmp/bogus"

        # create logfile
        logfile = "/tmp/XXX.log"
        open(logfile, "w").close()

        # self.setVerbose(True) # XXX

        self.__create_hsdir(workers, start_ticks, stop_ticks)

        # initialize HS services
        self.__init_publisher(publisher, start_ticks, stop_ticks,
                              workers[0].TEST_COPY_DIR)
        for wrk in workers:
            self.__init_worker(wrk, start_ticks, stop_ticks, copydir, logfile)
        self.__init_sender(sender)

        # create HS service threads
        thrds = [self.__create_publisher_thread(publisher), ]
        for wrk in workers:
            thrds.append(self.__create_worker_thread(wrk, logfile))
        thrds.append(self.__create_sender_thread(sender))

        # start all threads
        for thrd in reversed(thrds):
            thrd.start()

        # wait for threads to finish
        for n in range(10):
            alive = False
            for thrd in thrds:
                if thrd.isAlive():
                    thrd.join(1)
                    alive |= thrd.isAlive()
            if not alive:
                break

        # validate services
        progs = [publisher, ]
        progs += workers
        progs.append(sender)
        for prg in progs:
            prg.validate()

if __name__ == '__main__':
    unittest.main()
