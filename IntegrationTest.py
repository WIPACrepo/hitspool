#!/usr/bin/env python


import datetime
import json
import logging
import os
import re
import threading
import unittest

import HsConstants
import HsPublisher
import HsSender
import HsWorker
import HsTestUtil
import HsUtil

from HsPrefix import HsPrefix
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

    def send_json(self, msg):
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
        with self.__queueLock:
            self.__closed = True

    @property
    def has_input(self):
        with self.__queueLock:
            return not self.__closed or len(self.__queue) > 0

    def recv_json(self):
        with self.__queueLock:
            while True:
                if self.__closed:
                    break
                if len(self.__queue) == 0:
                    self.__queueLock.wait()
                if len(self.__queue) > 0:
                    return self.__queue.pop(0)

    def send_json(self, obj):
        with self.__queueLock:
            if self.__closed:
                raise Exception("Cannot send from closed socket")
            self.__queue.append(obj)
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

    def create_pusher(self, name):
        pusher = MockPushSocket(self, name)
        self.__pushers.append(pusher)
        return pusher

    @property
    def puller(self):
        return self.__puller

    def send_json(self, msg):
        self.__puller.send_json(msg)


class MockPushSocket(object):
    def __init__(self, parent, name):
        self.__parent = parent
        self.__name = name
        self.__closed = False
        self.__verbose = False

    def close(self):
        self.__closed = True
        self.__parent.close_pusher(self)

    def send_json(self, json):
        if self.__verbose:
            print "PushSocket(%s) <- %s" % (self.__name, str(json))
        self.__parent.send_json(json)

    def set_verbose(self, value=True):
        self.__verbose = (value is True)

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

    def recv_json(self):
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
            import sys
            print >>sys.stderr, \
                "MockSubSocket queue contains %s entries (%s)" % \
                (len(self.__queue), self.__queue)
        with self.__queueLock:
            self.__queueLock.notify()
        return rtnval


class MyPublisher(HsPublisher.Receiver):
    def __init__(self, pub_sock, snd_sock):
        self.__pub_sock = pub_sock
        self.__snd_sock = snd_sock
        self.__alert_sock = None
        self.__i3_sock = None

        super(MyPublisher, self).__init__(host="mypublisher", is_test=True)

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

    def create_sender_socket(self, host):
        if self.__snd_sock is None:
            self.__snd_sock = HsTestUtil.Mock0MQSocket("Sender")
        return self.__snd_sock

    def create_workers_socket(self):
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
        val = True
        for sock in (self.__alert_sock, self.__i3_sock, self.__pub_sock,
                     self.__snd_sock):
            if sock is not None:
                val |= sock.validate()
        return val


class MySender(HsSender.HsSender):
    def __init__(self, reporter):
        self.__rpt_sock = reporter

        super(MySender, self).__init__(host="mysender", is_test=True)

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
        val = self.reporter.validate()
        val |= self.i3socket.validate()
        return val


class MyWorker(HsWorker.Worker):
    def __init__(self, num, host, sub_sock, sender_sock):
        self.__num = num
        self.__sub_sock = sub_sock

        self.__i3_sock = None
        self.__sender_sock = sender_sock

        self.__link_paths = []

        super(MyWorker, self).__init__(self.name, host=host, is_test=True)

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

            self.mainloop(logfile)
            if not self.__sub_sock.has_input:
                break
        self.close_all()

    def validate(self):
        val = True
        for sock in (self.__i3_sock, self.__sender_sock, self.__sub_sock):
            if sock is not None:
                val |= sock.validate()
        return val


class IntegrationTest(LoggingTestCase):
    TICKS_PER_SECOND = 10000000000
    INTERVAL = 15 * TICKS_PER_SECOND

    MATCH_ANY = re.compile(r"^.*$")

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

    def __create_worker_thread(self, wrk):
        thrd = threading.Thread(name=wrk.name, target=wrk.run_test, args=())
        thrd.setDaemon(True)
        return thrd

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

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % publisher.cluster
        notify_lines = [
            '',
            'start in UTC : %s' % start_utc,
            'stop  in UTC : %s' % stop_utc,
            '(no possible leapseconds applied)',
        ]
        notify_pat = re.compile(r".*" + re.escape("\n".join(notify_lines)),
                                flags=re.MULTILINE)

        # fill in defaults for worker request
        stddict = alertdict.copy()
        stddict["prefix"] = prefix

        notifies = []
        for addr in HsConstants.ALERT_EMAIL_DEV:
            notifies.append(
                {
                    'notifies_txt': notify_pat,
                    'notifies_header': notify_hdr,
                    'receiver': addr,
                })

        # initialize I3Live socket and add all expected I3Live messages
        publisher.i3socket.addExpectedAlert({
            'condition': notify_hdr,
            'desc': 'HsInterface Data Request',
            'notifies': notifies,
            'short_subject': 'true',
            'quiet': 'true',
        })

    def __init_sender(self, sender, req_id, username, prefix, destdir):
        # I3Live status message value
        status_queued = {
            "request_id": req_id,
            "username": username,
            "prefix": prefix,
            "start_time": HsTestUtil.TIME_PAT,
            "stop_time": HsTestUtil.TIME_PAT,
            "update_time": self.MATCH_ANY,
            "destination_dir": destdir,
            "status": HsUtil.STATUS_QUEUED,
        }
        sender.i3socket.addExpectedMessage(status_queued, service="hitspool",
                                           varname="hsrequest_info",
                                           time=self.MATCH_ANY)

        status_started = status_queued.copy()
        status_started["status"] = HsUtil.STATUS_IN_PROGRESS
        sender.i3socket.addExpectedMessage(status_started, service="hitspool",
                                           varname="hsrequest_info",
                                           time=self.MATCH_ANY)

        status_success = status_queued.copy()
        status_success["status"] = HsUtil.STATUS_SUCCESS
        sender.i3socket.addExpectedMessage(status_success, service="hitspool",
                                           varname="hsrequest_info",
                                           time=self.MATCH_ANY)

    def __init_worker(self, worker, start_ticks, stop_ticks, copydir):
        # build timetag used to construct final destination
        utcstart = HsTestUtil.get_time(start_ticks)
        plus30 = utcstart + datetime.timedelta(0, 30)

        # reformat time string for file names
        timetag = plus30.strftime("%Y%m%d_%H%M%S")

        # TODO should compute the expected number of files
        worker.add_expected_links(plus30, "currentRun", 0, 1,
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

    def tearDown(self):
        try:
            super(IntegrationTest, self).tearDown()
        finally:
            try:
                HsTestUtil.MockHitspool.destroy()
            except:
                import traceback
                traceback.print_exc()

            # get rid of HsSender's state database
            dbpath = HsSender.HsSender.get_db_path()
            if os.path.exists(dbpath):
                try:
                    os.unlink(dbpath)
                except:
                    import traceback
                    traceback.print_exc()

    def test_publisher_to_worker(self):
        pub2wrk = MockPubSubSocket()

        push2snd = MockPushPullSocket()

        publisher = MyPublisher(pub2wrk.publisher,
                                push2snd.create_pusher("publisher"))

        workers = (MyWorker(1, "ichub01.usap.gov", pub2wrk.subscribe(),
                            push2snd.create_pusher("ichub01")),
                   MyWorker(2, "ichub66.usap.gov", pub2wrk.subscribe(),
                            push2snd.create_pusher("ichub66")))

        sender = MySender(push2snd.puller)

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        copydir = "foo@localhost:/tmp/bogus"

        self.__create_hsdir(workers, start_ticks, stop_ticks)

        # set up request fields
        req_id = "FAKE_ID"
        username = "test_pdaq"
        prefix = HsPrefix.SNALERT

        # initialize HS services
        self.__init_publisher(publisher, req_id, username, prefix, start_ticks,
                              stop_ticks, workers[0].TEST_COPY_DIR)
        for wrk in workers:
            self.__init_worker(wrk, start_ticks, stop_ticks, copydir)
        self.__init_sender(sender, req_id, username, prefix,
                           workers[0].TEST_COPY_DIR)

        # create HS service threads
        thrds = [self.__create_publisher_thread(publisher), ]
        for wrk in workers:
            thrds.append(self.__create_worker_thread(wrk))
        thrds.append(self.__create_sender_thread(sender))

        # start all threads
        for thrd in reversed(thrds):
            thrd.start()

        # wait for threads to finish
        for _ in range(10):
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
