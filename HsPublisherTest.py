#!/usr/bin/env python

import json
import re
import unittest

import HsMessage
import HsPublisher
import HsTestUtil
import HsUtil

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

    def validate(self):
        for sock in (self.__alert_sock, self.__i3_sock, self.__pub_sock,
                     self.__sender_sock):
            if sock is not None:
                sock.validate()


class HsPublisherTest(LoggingTestCase):
    MATCH_ANY = re.compile(r"^.*$")
    RECEIVER = None

    def setUp(self):
        super(HsPublisherTest, self).setUp()
        # by default, check all log messages
        self.setLogLevel(0)
        self.RECEIVER = None

    def tearDown(self):
        try:
            super(HsPublisherTest, self).tearDown()
        finally:
            found_error = False
            if self.RECEIVER is not None:
                try:
                    self.RECEIVER.close_all()
                except:
                    import traceback
                    traceback.print_exc()
                    found_error = True
                self.RECEIVER = None

            if found_error:
                self.fail("Found one or more errors during tear-down")

    def test_empty_request(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # request message
        req_str = ""

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Cannot decode %s" % req_str)
        self.expectLogMessage("Ignoring bad request: %s" % req_str)
        self.expectLogMessage("Sent response back to requester: ERROR")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_bad_msg(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # request message
        req_str = "XXX"

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Cannot decode %s" % req_str)
        self.expectLogMessage("Ignoring bad request: %s" % req_str)
        self.expectLogMessage("Sent response back to requester: ERROR")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_bad_start(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_utc = "XXX"
        stop_utc = "TBD"
        copydir = "XXX"

        # request message
        req_str = "{\"start\": \"%s\", \"stop\": \"%s\", \"copy\": \"%s\"}" % \
                  (start_utc, stop_utc, copydir)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Bad start time \"%s\"" % start_utc)
        self.expectLogMessage(re.compile(r"Could not find start/stop time"
                                         r" in request:\n.*"))
        self.expectLogMessage("Sent response back to requester: ERROR")

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedMessage({
            'username': HsPublisher.Receiver.DEFAULT_USERNAME,
            'status': HsUtil.STATUS_REQUEST_ERROR,
            'prefix': HsPrefix.ANON,
            'request_id': self.MATCH_ANY,
            'start_time': self.MATCH_ANY,
            'stop_time': self.MATCH_ANY,
            'destination_dir': copydir,
            'update_time': self.MATCH_ANY,
        }, service="hitspool", varname="hsrequest_info", prio=1,
                                         time=self.MATCH_ANY)

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_bad_stop(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

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
        self.expectLogMessage(re.compile(r"Could not find start/stop time"
                                         r" in request:\n.*"))
        self.expectLogMessage("Sent response back to requester: ERROR")

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedMessage({
            'username': HsPublisher.Receiver.DEFAULT_USERNAME,
            'status': HsUtil.STATUS_REQUEST_ERROR,
            'prefix': HsPrefix.ANON,
            'request_id': self.MATCH_ANY,
            'start_time': self.MATCH_ANY,
            'stop_time': self.MATCH_ANY,
            'destination_dir': copydir,
            'update_time': self.MATCH_ANY,
        }, service="hitspool", varname="hsrequest_info", prio=1,
                                         time=self.MATCH_ANY)

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_missing_copydir(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

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
        self.expectLogMessage("Request did not specify a destination"
                              " directory")
        self.expectLogMessage("Sent response back to requester: ERROR")

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedMessage({
            'username': HsPublisher.Receiver.DEFAULT_USERNAME,
            'status': HsUtil.STATUS_REQUEST_ERROR,
            'prefix': HsPrefix.ANON,
            'request_id': self.MATCH_ANY,
            'start_time': self.MATCH_ANY,
            'stop_time': self.MATCH_ANY,
            'destination_dir': rcvr.BAD_DESTINATION,
            'update_time': self.MATCH_ANY,
        }, service="hitspool", varname="hsrequest_info", prio=1,
                                         time=self.MATCH_ANY)

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_null_copydir(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_ticks = 98765432101234
        stop_ticks = 98899889980000

        # request message
        req_str = '{"start": %d, "stop": %s, "copy": null}' % \
                  (start_ticks / 10, stop_ticks / 10)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("Request did not specify a destination"
                              " directory")
        self.expectLogMessage("Sent response back to requester: ERROR")

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedMessage({
            'username': HsPublisher.Receiver.DEFAULT_USERNAME,
            'status': HsUtil.STATUS_REQUEST_ERROR,
            'prefix': HsPrefix.ANON,
            'request_id': self.MATCH_ANY,
            'start_time': self.MATCH_ANY,
            'stop_time': self.MATCH_ANY,
            'destination_dir': rcvr.BAD_DESTINATION,
            'update_time': self.MATCH_ANY,
        }, service="hitspool", varname="hsrequest_info", prio=1,
                                         time=self.MATCH_ANY)

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_bad_copydir_user(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_ticks = 98765432101234
        stop_ticks = 98899889980000

        copy_user = "xxx"
        copy_host = "xxxhost"
        copy_path = "/not/really"
        copydir = "%s@%s:%s" % (copy_user, copy_host, copy_path)

        # request message
        req_str = "{'start': %d, 'stop': %s, 'copy': '%s'}" % \
                  (start_ticks / 10, stop_ticks / 10, copydir)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("rsync user must be %s, not %s (from \"%s\")" %
                              (rcvr.rsync_user, copy_user, copydir))
        self.expectLogMessage("Sent response back to requester: ERROR")

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedMessage({
            'username': HsPublisher.Receiver.DEFAULT_USERNAME,
            'status': HsUtil.STATUS_REQUEST_ERROR,
            'prefix': HsPrefix.ANON,
            'request_id': self.MATCH_ANY,
            'start_time': self.MATCH_ANY,
            'stop_time': self.MATCH_ANY,
            'destination_dir': copydir,
            'update_time': self.MATCH_ANY,
        }, service="hitspool", varname="hsrequest_info", prio=1,
                                         time=self.MATCH_ANY)

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_bad_copydir_host(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_ticks = 98765432101234
        stop_ticks = 98899889980000

        copy_user = rcvr.rsync_user
        copy_host = "xxxhost"
        copy_path = "/not/really"
        copydir = "%s@%s:%s" % (copy_user, copy_host, copy_path)

        # request message
        req_str = "{'start': %d, 'stop': %s, 'copy': '%s'}" % \
                  (start_ticks / 10, stop_ticks / 10, copydir)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("ERROR\0")

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage("rsync host must be %s, not %s (from \"%s\")" %
                              (rcvr.rsync_host, copy_host, copydir))
        self.expectLogMessage("Sent response back to requester: ERROR")

        # initialize I3Live socket and add all expected I3Live messages
        rcvr.i3socket.addExpectedMessage({
            'username': HsPublisher.Receiver.DEFAULT_USERNAME,
            'status': HsUtil.STATUS_REQUEST_ERROR,
            'prefix': HsPrefix.ANON,
            'request_id': self.MATCH_ANY,
            'start_time': self.MATCH_ANY,
            'stop_time': self.MATCH_ANY,
            'destination_dir': copydir,
            'update_time': self.MATCH_ANY,
        }, service="hitspool", varname="hsrequest_info", prio=1,
                                         time=self.MATCH_ANY)

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_done_fail(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        copydir = "/bad/copy/path"

        # build initial message
        alertdict = {
            "start": start_ticks / 10,
            "stop": stop_ticks / 10,
            "copy": copydir,
        }
        req_str = json.dumps(alertdict)
        answer = "BadAnswer"

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("DONE\0", answer=answer)

        # build sender status message
        send_msg = {
            "msgtype": HsMessage.INITIAL,
            "request_id": self.MATCH_ANY,
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_ticks": start_ticks,
            "stop_ticks": stop_ticks,
            "copy_dir": None,
            "destination_dir": copydir,
            "extract": False,
            "host": rcvr.shorthost,
            "hubs": None,
            "version": HsMessage.CURRENT_VERSION,
        }

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Failed sending DONE to requester: %s" % answer)

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_oldgood(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
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
        send_msg = {
            "msgtype": HsMessage.INITIAL,
            "request_id": self.MATCH_ANY,
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_ticks": start_ticks,
            "stop_ticks": stop_ticks,
            "copy_dir": None,
            "destination_dir": copydir,
            "extract": False,
            "host": rcvr.shorthost,
            "hubs": None,
            "version": HsMessage.CURRENT_VERSION,
        }

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Sent response back to requester: DONE")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_newgood(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
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

        # build sender status message
        send_msg = {
            "msgtype": HsMessage.INITIAL,
            "request_id": alertdict["request_id"],
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_ticks": start_ticks,
            "stop_ticks": stop_ticks,
            "copy_dir": None,
            "destination_dir": copydir,
            "extract": False,
            "host": rcvr.shorthost,
            "hubs": None,
            "version": HsMessage.CURRENT_VERSION,
        }

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Sent response back to requester: DONE")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_req_datetime(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        copydir = "/bad/copy/path"

        # request message
        alertdict = {
            "start_time": str(HsTestUtil.get_time(start_ticks)),
            "stop_time": str(HsTestUtil.get_time(stop_ticks)),
            "destination_dir": copydir,
            "prefix": HsPrefix.ANON,
            "request_id": "NO ID",
        }
        req_str = json.dumps(alertdict)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("DONE\0")

        # build sender status message
        send_msg = {
            "msgtype": HsMessage.INITIAL,
            "request_id": alertdict["request_id"],
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_ticks": start_ticks,
            "stop_ticks": stop_ticks,
            "copy_dir": None,
            "destination_dir": copydir,
            "extract": False,
            "host": rcvr.shorthost,
            "hubs": None,
            "version": HsMessage.CURRENT_VERSION,
        }

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Sent response back to requester: DONE")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_hese(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        copydir = "localhost:/tmp"

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

        _, request_dir = copydir.split(":")

        # build sender status message
        send_msg = {
            "msgtype": HsMessage.INITIAL,
            "request_id": alertdict["request_id"],
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_ticks": start_ticks,
            "stop_ticks": stop_ticks,
            "copy_dir": None,
            "destination_dir": request_dir,
            "extract": False,
            "host": rcvr.shorthost,
            "hubs": None,
            "version": HsMessage.CURRENT_VERSION,
        }

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Sent response back to requester: DONE")

        # run it!
        rcvr.reply_request()

        rcvr.validate()

    def test_date_requests(self):
        rcvr = MyReceiver()
        self.RECEIVER = rcvr

        # expected start/stop times
        start_ticks = 98765432100000
        stop_ticks = 98899889980000
        copydir = "localhost:/tmp"

        # request message
        alertdict = {
            "start_time": str(HsTestUtil.get_time(start_ticks)),
            "stop_time": str(HsTestUtil.get_time(stop_ticks)),
            "destination_dir": copydir,
            "prefix": HsPrefix.ANON,
            "request_id": "NO ID",
        }
        req_str = json.dumps(alertdict)

        # initialize incoming socket and add expected message(s)
        rcvr.alert_socket.addIncoming(req_str)
        rcvr.alert_socket.addExpected("DONE\0")

        _, request_dir = copydir.split(":")

        # build sender status message
        send_msg = {
            "msgtype": HsMessage.INITIAL,
            "request_id": alertdict["request_id"],
            "username": HsPublisher.Receiver.DEFAULT_USERNAME,
            "prefix": HsPrefix.ANON,
            "start_ticks": start_ticks,
            "stop_ticks": stop_ticks,
            "copy_dir": None,
            "destination_dir": request_dir,
            "extract": False,
            "host": rcvr.shorthost,
            "hubs": None,
            "version": HsMessage.CURRENT_VERSION,
        }

        # add expected sender message
        rcvr.sender.addExpected(send_msg)

        # add all expected log messages
        self.expectLogMessage("received request:\n%s" % req_str)
        self.expectLogMessage(re.compile("Publisher published: .*"))
        self.expectLogMessage("Sent response back to requester: DONE")

        # run it!
        rcvr.reply_request()

        rcvr.validate()


if __name__ == '__main__':
    unittest.main()
