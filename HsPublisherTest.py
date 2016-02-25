#!/usr/bin/env python

import json
import re
import unittest

import HsConstants
import HsMessage
import HsPublisher
import HsTestUtil

from HsPrefix import HsPrefix
from LoggingTestCase import LoggingTestCase


class MyReceiver(HsPublisher.Receiver):
    def __init__(self):
        self.__alert_sock = None
        self.__i3_sock = None
        self.__pub_sock = None
        self.__sender_sock = None

        super(MyReceiver, self).__init__(host="tstpub", is_test=True)

    def create_alert_socket(self):
        if self.__alert_sock is not None:
            raise Exception("Cannot create multiple alert sockets")

        self.__alert_sock = HsTestUtil.Mock0MQSocket("Socket")
        return self.__alert_sock

    def create_i3socket(self, host):
        if self.__i3_sock is not None:
            raise Exception("Cannot create multiple I3 sockets")

        self.__i3_sock = HsTestUtil.MockI3Socket('HsPublisher')
        return self.__i3_sock

    def create_sender_socket(self, host):
        if self.__sender_sock is not None:
            raise Exception("Cannot create multiple Sender sockets")

        self.__sender_sock = HsTestUtil.Mock0MQSocket("Sender")
        return self.__sender_sock

    def create_workers_socket(self):
        if self.__pub_sock is not None:
            raise Exception("Cannot create multiple Publisher sockets")

        self.__pub_sock = HsTestUtil.Mock0MQSocket("Publisher")
        return self.__pub_sock

    def validate(self):
        val = True
        for sock in (self.__alert_sock, self.__i3_sock, self.__pub_sock,
                     self.__sender_sock):
            if sock is not None:
                val |= sock.validate()
        return val


class HsPublisherTest(LoggingTestCase):
    MATCH_ANY = re.compile(r"^.*$")

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
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [
            '',
            'start in UTC : %s' % start_utc,
            'stop  in UTC : %s' % stop_utc,
            '(no possible leapseconds applied)',
        ]
        notify_pat = re.compile(r".*" + re.escape("\n".join(notify_lines)),
                                flags=re.MULTILINE)

        notifies = []
        for addr in HsConstants.ALERT_EMAIL_DEV:
            notifies.append(
                {
                    'notifies_txt': notify_pat,
                    'notifies_header': notify_hdr,
                    'receiver': addr,
                })

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedAlert({
            'condition': notify_hdr,
            'desc': 'HsInterface Data Request',
            'notifies': notifies,
            'short_subject': 'true',
            'quiet': 'true',
        })

        # initialize outgoing socket and add all expected messages
        rcvr.workers.addExpected("[]")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Cannot decode %s" % req_str)
        self.expectLogMessage("Ignoring bad request: %s" % req_str)
        self.expectLogMessage("Sent response back to requester: ERROR")

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
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [
            '',
            'start in UTC : %s' % start_utc,
            'stop  in UTC : %s' % stop_utc,
            '(no possible leapseconds applied)',
        ]
        notify_pat = re.compile(r".*" + re.escape("\n".join(notify_lines)),
                                flags=re.MULTILINE)

        notifies = []
        for addr in HsConstants.ALERT_EMAIL_DEV:
            notifies.append(
                {
                    'notifies_txt': notify_pat,
                    'notifies_header': notify_hdr,
                    'receiver': addr,
                })

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedAlert({
            'condition': notify_hdr,
            'desc': 'HsInterface Data Request',
            'notifies': notifies,
            'short_subject': 'true',
            'quiet': 'true',
        })

        # initialize outgoing socket and add all expected messages
        rcvr.workers.addExpected("XXX")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Cannot decode %s" % req_str)
        self.expectLogMessage("Ignoring bad request: %s" % req_str)
        self.expectLogMessage("Sent response back to requester: ERROR")

        # run it!
        rcvr.reply_request()

    def test_bad_start(self):
        rcvr = MyReceiver()

        # expected start/stop times
        stop_utc = "TBD"

        # request message
        req_str = "{\"start\": \"XXX\", \"stop\": \"%s\"}" % stop_utc

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Bad start time \"XXX\"")
        self.expectLogMessage(re.compile("Request failed: .*"))
        self.expectLogMessage("Sent response back to requester: ERROR")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_bad_stop(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_ticks = 98765432101234
        stop_utc = "TBD"
        copydir = "/bad/copy/path"

        # build initial message
        alertdict = {
            "start": start_ticks / 10,
            "stop": stop_utc,
            "copy": copydir,
        }
        req_str = json.dumps(alertdict)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Bad stop time \"%s\"" % stop_utc)
        self.expectLogMessage(re.compile("Request failed: .*"))
        self.expectLogMessage("Sent response back to requester: ERROR")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_missing_copydir(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_ticks = 98765432101234
        stop_ticks = 98899889980000

        # request message
        req_str = "{'start': %d, 'stop': %s}" % \
                  (start_ticks / 10, stop_ticks / 10)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Request did not contain a copy directory: %s" %
                              req_str)
        self.expectLogMessage("Sent response back to requester: ERROR")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_done_fail(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        start_utc = HsTestUtil.get_time(start_ticks)
        stop_utc = HsTestUtil.get_time(stop_ticks)
        copydir = "/bad/copy/path"

        # build initial message
        alertdict = {
            "start": start_ticks / 10,
            "stop": stop_ticks / 10,
            "copy": copydir,
        }
        req_str = json.dumps(alertdict)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("DONE\0", answer="XXX")

        # fill in defaults for worker request
        stddict = {
            "msgtype": HsMessage.MESSAGE_INITIAL,
            "request_id": self.MATCH_ANY,
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_time": start_ticks / 10,
            "stop_time": stop_ticks / 10,
            "copy_dir": None,
            "destination_dir": alertdict["copy"],
            "extract": False,
            "host": rcvr.shorthost,
        }

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [
            '',
            'start in UTC : %s' % start_utc,
            'stop  in UTC : %s' % stop_utc,
            '(no possible leapseconds applied)',
        ]
        notify_pat = re.compile(r".*" + re.escape("\n".join(notify_lines)),
                                flags=re.MULTILINE)

        notifies = []
        for addr in HsConstants.ALERT_EMAIL_DEV:
            notifies.append(
                {
                    'notifies_txt': notify_pat,
                    'notifies_header': notify_hdr,
                    'receiver': addr,
                })

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedAlert({
            'condition': notify_hdr,
            'desc': 'HsInterface Data Request',
            'notifies': notifies,
            'short_subject': 'true',
            'quiet': 'true',
        })

        # build sender status message
        send_msg = stddict.copy()
        send_msg["msgtype"] = HsMessage.MESSAGE_INITIAL
        send_msg["start_time"] = str(start_utc)
        send_msg["stop_time"] = str(stop_utc)

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add expected worker request
        rcvr.workers.addExpected(stddict)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Failed sending DONE to requester")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_oldgood(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        start_utc = HsTestUtil.get_time(start_ticks)
        stop_utc = HsTestUtil.get_time(stop_ticks)
        copydir = "/bad/copy/path"

        # request message
        alertdict = {
            "start": start_ticks / 10,
            "stop": stop_ticks / 10,
            "copy": copydir,
        }
        req_str = json.dumps(alertdict)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("DONE\0")

        # fill in defaults for worker request
        stddict = {
            "msgtype": HsMessage.MESSAGE_INITIAL,
            "request_id": self.MATCH_ANY,
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_time": start_ticks / 10,
            "stop_time": stop_ticks / 10,
            "copy_dir": None,
            "destination_dir": alertdict["copy"],
            "extract": False,
            "host": rcvr.shorthost,
        }

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_lines = [
            '',
            'start in UTC : %s' % start_utc,
            'stop  in UTC : %s' % stop_utc,
            '(no possible leapseconds applied)',
        ]
        notify_pat = re.compile(r".*" + re.escape("\n".join(notify_lines)),
                                flags=re.MULTILINE)

        notifies = []
        for addr in HsConstants.ALERT_EMAIL_DEV:
            notifies.append(
                {
                    'notifies_txt': notify_pat,
                    'notifies_header': notify_hdr,
                    'receiver': addr,
                })

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedAlert({
            'condition': notify_hdr,
            'desc': 'HsInterface Data Request',
            'notifies': notifies,
            'short_subject': 'true',
            'quiet': 'true',
        })

        # build sender status message
        send_msg = stddict.copy()
        send_msg["msgtype"] = HsMessage.MESSAGE_INITIAL
        send_msg["start_time"] = str(start_utc)
        send_msg["stop_time"] = str(stop_utc)

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add expected worker request
        rcvr.workers.addExpected(stddict)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Sent response back to requester: DONE")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_newgood(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        start_utc = HsTestUtil.get_time(start_ticks)
        stop_utc = HsTestUtil.get_time(stop_ticks)
        copydir = "/bad/copy/path"

        # request message
        alertdict = {
            "start_time": start_ticks / 10,
            "stop_time": stop_ticks / 10,
            "destination_dir": copydir,
            "prefix": HsPrefix.ANON,
            "request_id": "NO ID",
        }
        req_str = json.dumps(alertdict)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("DONE\0")

        # fill in defaults for worker request
        stddict = {
            "msgtype": HsMessage.MESSAGE_INITIAL,
            "request_id": alertdict["request_id"],
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_time": start_ticks / 10,
            "stop_time": stop_ticks / 10,
            "copy_dir": None,
            "destination_dir": alertdict["destination_dir"],
            "extract": False,
            "host": rcvr.shorthost,
        }

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_desc = 'HsInterface Data Request'
        notify_lines = [
            '',
            'start in UTC : %s' % start_utc,
            'stop  in UTC : %s' % stop_utc,
            '(no possible leapseconds applied)',
        ]
        notify_pat = re.compile(r".*" + re.escape("\n".join(notify_lines)),
                                flags=re.MULTILINE)

        notifies = []
        for addr in HsConstants.ALERT_EMAIL_DEV:
            notifies.append(
                {
                    'notifies_txt': notify_pat,
                    'notifies_header': notify_hdr,
                    'receiver': addr,
                })

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedAlert({
            'condition': notify_hdr,
            'desc': notify_desc,
            'notifies': notifies,
            'short_subject': 'true',
            'quiet': 'true',
        })

        # build sender status message
        send_msg = stddict.copy()
        send_msg["msgtype"] = HsMessage.MESSAGE_INITIAL
        send_msg["start_time"] = str(start_utc)
        send_msg["stop_time"] = str(stop_utc)

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add expected worker request
        rcvr.workers.addExpected(stddict)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Sent response back to requester: DONE")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_req_datetime(self):
        rcvr = MyReceiver()

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        start_utc = HsTestUtil.get_time(start_ticks)
        stop_utc = HsTestUtil.get_time(stop_ticks)
        copydir = "/bad/copy/path"

        # request message
        alertdict = {
            "start_time": str(start_utc),
            "stop_time": str(stop_utc),
            "destination_dir": copydir,
            "prefix": HsPrefix.ANON,
            "request_id": "NO ID",
        }
        req_str = json.dumps(alertdict)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("DONE\0")

        # fill in defaults for worker request
        stddict = {
            "msgtype": HsMessage.MESSAGE_INITIAL,
            "request_id": alertdict["request_id"],
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_time": start_ticks / 10,
            "stop_time": stop_ticks / 10,
            "copy_dir": None,
            "destination_dir": alertdict["destination_dir"],
            "extract": False,
            "host": rcvr.shorthost,
        }

        # notification message strings
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % rcvr.cluster
        notify_desc = 'HsInterface Data Request'
        notify_lines = [
            '',
            'start in UTC : %s' % start_utc,
            'stop  in UTC : %s' % stop_utc,
            '(no possible leapseconds applied)',
        ]
        notify_pat = re.compile(r".*" + re.escape("\n".join(notify_lines)),
                                flags=re.MULTILINE)

        notifies = []
        for addr in HsConstants.ALERT_EMAIL_DEV:
            notifies.append(
                {
                    'notifies_txt': notify_pat,
                    'notifies_header': notify_hdr,
                    'receiver': addr,
                })

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedAlert({
            'condition': notify_hdr,
            'desc': notify_desc,
            'notifies': notifies,
            'short_subject': 'true',
            'quiet': 'true',
        })

        # build sender status message
        send_msg = stddict.copy()
        send_msg["msgtype"] = HsMessage.MESSAGE_INITIAL
        send_msg["start_time"] = str(start_utc)
        send_msg["stop_time"] = str(stop_utc)

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add expected worker request
        rcvr.workers.addExpected(stddict)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Sent response back to requester: DONE")

        # run it!
        rcvr.reply_request()

        rcvr.validate()


if __name__ == '__main__':
    unittest.main()
