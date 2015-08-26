#!/usr/bin/env python

import datetime
import unittest

import HsConstants
import HsPublisher

from HsTestUtil import Mock0MQSocket, MockI3Socket
from LoggingTestCase import LoggingTestCase


class MyReceiver(HsPublisher.Receiver):
    def __init__(self):
        super(MyReceiver, self).__init__(is_test=True)

    def create_alert_socket(self):
        return Mock0MQSocket("Socket")

    def create_i3socket(self, host):
        return MockI3Socket('HsPublisher')

    def create_publisher(self):
        return Mock0MQSocket("Publisher")


class HsPublisherTest(LoggingTestCase):
    TICKS_PER_SECOND = 10000000000

    JAN1 = None

    @classmethod
    def utc(cls, daq_ticks):
        if cls.JAN1 is None:
            now = datetime.datetime.utcnow()
            cls.JAN1 = datetime.datetime(now.year, 1, 1)

        secs = int(daq_ticks / cls.TICKS_PER_SECOND)
        msecs = int(daq_ticks / 10000 - secs * 1000000)
        return cls.JAN1 + datetime.timedelta(seconds=secs, microseconds=msecs)

    def setUp(self):
        super(HsPublisherTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)

    def tearDown(self):
        try:
            super(HsPublisherTest, self).tearDown()
        finally:
            pass

    def test_empty_request(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_utc = "TBD"
        stop_utc = "TBD"

        # request message
        req_str = ""

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket().addIncoming(req_str)
        rcvr.alert_socket().addExpected("DONE\0")

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [req_str,
                        'start in UTC : %s' % start_utc,
                        'stop  in UTC : %s' % stop_utc,
                        '(no possible leapseconds applied)',
                       ]

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedValue("Received data request for"
                                       " [%s , %s] " % (start_utc, stop_utc))
        rcvr.i3socket.addExpectedValue("Published request to HsWorkers")
        rcvr.i3socket.addExpectedAlert(
            {'condition': notify_hdr,
             'desc': 'HsInterface Data Request',
             'notifies': [
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_DEV,
                 },
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_SN,
                 }
             ],
             'short_subject': 'true',
             'quiet': 'true',
            })

        # initialize outgoing socket and add all expected messages
        rcvr.publisher.addExpected("[%s]" % req_str)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Publisher published: %s" % req_str)
        self.expectLogMessage("send confirmation back to requester: DONE")
        self.expectLogMessage("Quitting: Incoming message queue is empty")

        # run it!
        rcvr.reply_request()

    def test_bad_msg(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_utc = "TBD"
        stop_utc = "TBD"

        # request message
        req_str = "XXX"

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket().addIncoming(req_str)
        rcvr.alert_socket().addExpected("DONE\0")

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [req_str,
                        'start in UTC : %s' % start_utc,
                        'stop  in UTC : %s' % stop_utc,
                        '(no possible leapseconds applied)',
                       ]

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedValue("Received data request for [TBD , TBD] ")
        rcvr.i3socket.addExpectedValue("Published request to HsWorkers")
        rcvr.i3socket.addExpectedAlert(
            {'condition': notify_hdr,
             'desc': 'HsInterface Data Request',
             'notifies': [
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_DEV,
                 },
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_SN,
                 }
             ],
             'short_subject': 'true',
             'quiet': 'true',
            })

        # initialize outgoing socket and add all expected messages
        rcvr.publisher.addExpected("[%s]" % req_str)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Publisher published: %s" % req_str)
        self.expectLogMessage("send confirmation back to requester: DONE")
        self.expectLogMessage("Quitting: Incoming message queue is empty")

        # run it!
        rcvr.reply_request()

    def test_bad_start(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_utc = "TBD"
        stop_utc = "TBD"

        # request message
        req_str = "{'start': \"XXX\", 'stop': \"%s\"}" % stop_utc

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket().addIncoming(req_str)
        rcvr.alert_socket().addExpected("DONE\0")

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [req_str,
                        'start in UTC : %s' % start_utc,
                        'stop  in UTC : %s' % stop_utc,
                        '(no possible leapseconds applied)',
                       ]

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedValue("Received data request for"
                                       " [%s , %s] " % (start_utc, stop_utc))
        rcvr.i3socket.addExpectedValue("Published request to HsWorkers")
        rcvr.i3socket.addExpectedAlert(
            {'condition': notify_hdr,
             'desc': 'HsInterface Data Request',
             'notifies': [
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_DEV,
                 },
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_SN,
                 }
             ],
             'short_subject': 'true',
             'quiet': 'true',
            })

        # initialize outgoing socket and add all expected messages
        rcvr.publisher.addExpected("[%s]" % req_str)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Publisher published: %s" % req_str)
        self.expectLogMessage("send confirmation back to requester: DONE")
        self.expectLogMessage("Quitting: Incoming message queue is empty")

        # run it!
        rcvr.reply_request()

    def test_bad_stop(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_ticks = 98765432101234
        start_utc = "TBD"
        stop_utc = "TBD"

        # request message
        req_str = "{'start': %d, 'stop': \"%s\"}" % (start_ticks / 10, stop_utc)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket().addIncoming(req_str)
        rcvr.alert_socket().addExpected("DONE\0")

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [req_str,
                        'start in UTC : %s' % start_utc,
                        'stop  in UTC : %s' % stop_utc,
                        '(no possible leapseconds applied)',
                       ]

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedValue("Received data request for"
                                       " [%s , %s] " % (start_utc, stop_utc))
        rcvr.i3socket.addExpectedValue("Published request to HsWorkers")
        rcvr.i3socket.addExpectedAlert(
            {'condition': notify_hdr,
             'desc': 'HsInterface Data Request',
             'notifies': [
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_DEV,
                 },
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_SN,
                 }
             ],
             'short_subject': 'true',
             'quiet': 'true',
            })

        # initialize outgoing socket and add all expected messages
        rcvr.publisher.addExpected("[%s]" % req_str)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Publisher published: %s" % req_str)
        self.expectLogMessage("send confirmation back to requester: DONE")
        self.expectLogMessage("Quitting: Incoming message queue is empty")

        # run it!
        rcvr.reply_request()

    def test_done_fail(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        start_utc = self.utc(start_ticks)
        stop_utc = self.utc(stop_ticks)

        # request message
        req_str = "{'start': %d, 'stop': %d}" % \
                 (start_ticks / 10, stop_ticks / 10)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket().addIncoming(req_str)
        rcvr.alert_socket().addExpected("DONE\0", answer="XXX")

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [req_str,
                        'start in UTC : %s' % start_utc,
                        'stop  in UTC : %s' % stop_utc,
                        '(no possible leapseconds applied)',
                       ]

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedValue("Received data request for"
                                       " [%s , %s] " % (start_utc, stop_utc))
        rcvr.i3socket.addExpectedValue("Published request to HsWorkers")
        rcvr.i3socket.addExpectedAlert(
            {'condition': notify_hdr,
             'desc': 'HsInterface Data Request',
             'notifies': [
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_DEV,
                 },
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_SN,
                 }
             ],
             'short_subject': 'true',
             'quiet': 'true',
            })

        # initialize outgoing socket and add all expected messages
        rcvr.publisher.addExpected("[%s]" % req_str)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Publisher published: %s" % req_str)
        self.expectLogMessage("failed sending confirmation to requester")
        self.expectLogMessage("Quitting: Incoming message queue is empty")

        # run it!
        rcvr.reply_request()

    def test_good(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        start_utc = self.utc(start_ticks)
        stop_utc = self.utc(stop_ticks)

        # request message
        req_str = "{'start': %d, 'stop': %d}" % \
                 (start_ticks / 10, stop_ticks / 10)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket().addIncoming(req_str)
        rcvr.alert_socket().addExpected("DONE\0")

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [req_str,
                        'start in UTC : %s' % start_utc,
                        'stop  in UTC : %s' % stop_utc,
                        '(no possible leapseconds applied)',
                       ]

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedValue("Received data request for"
                                       " [%s , %s] " % (start_utc, stop_utc))
        rcvr.i3socket.addExpectedValue("Published request to HsWorkers")
        rcvr.i3socket.addExpectedAlert(
            {'condition': notify_hdr,
             'desc': 'HsInterface Data Request',
             'notifies': [
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_DEV,
                 },
                 {'notifies_txt': "\n".join(notify_lines),
                  'notifies_header': notify_hdr,
                  'receiver': HsConstants.ALERT_EMAIL_SN,
                 }
             ],
             'short_subject': 'true',
             'quiet': 'true',
            })

        # initialize outgoing socket and add all expected messages
        rcvr.publisher.addExpected("[%s]" % req_str)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Publisher published: %s" % req_str)
        self.expectLogMessage("send confirmation back to requester: DONE")
        self.expectLogMessage("Quitting: Incoming message queue is empty")

        # run it!
        rcvr.reply_request()

if __name__ == '__main__':
    unittest.main()
