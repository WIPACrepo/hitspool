#!/usr/bin/env python

import numbers
import os
import re
import time
import unittest

import DAQTime
import HsConstants
import HsMessage
import HsUtil

from HsException import HsException
from HsSender import HsSender
from HsTestUtil import Mock0MQSocket, MockI3Socket, TIME_PAT, \
    set_state_db_path
from RequestMonitor import RequestMonitor

from LoggingTestCase import LoggingTestCase


CLUSTER_LOCAL = "local"
CLUSTER_SPS = "sps"
CLUSTER_SPTS = "spts"


class MockSender(object):
    def __init__(self, cluster=CLUSTER_LOCAL):
        self.__cluster = cluster

        self.__workers = Mock0MQSocket("Workers")
        self.__i3socket = MockI3Socket("I3Socket")

    @property
    def cluster(self):
        return "TEST"

    @property
    def i3socket(self):
        return self.__i3socket

    @property
    def is_cluster_sps(self):
        return self.__cluster == CLUSTER_SPS

    @property
    def is_cluster_spts(self):
        return self.__cluster == CLUSTER_SPTS

    def move_to_destination_dir(self, copydir, copydir_user, prefix=None,
                                force_spade=False):
        return "/dest/dir", True

    @property
    def shorthost(self):
        return "localhost"

    @property
    def workers(self):
        return self.__workers


class RequestMonitorTest(LoggingTestCase):
    # cached RequestMonitor object (used in tearDown() cleanup code)
    REQUEST_MONITOR = None

    # temporary request state database
    TEMP_STATE_DB = None

    # standard format for date/time strings
    TIMEFMT = "%Y-%m-%d %H:%M:%S"

    # used in comparison code as a wildcard for unpredictable values
    MATCH_ANY = re.compile(r"^.*$")

    @classmethod
    def __build_state_lists(cls, pairs, active):
        in_progress = []
        done = []
        error = []
        for hub, final_state in pairs:
            hub_id = HsUtil.hub_name_to_id(hub)
            if hub in active:
                in_progress.append(hub_id)
            elif final_state == HsMessage.DONE:
                done.append(hub_id)
            elif final_state == HsMessage.FAILED:
                error.append(hub_id)
        return in_progress, error, done

    def __check_reqmon_state(self, rmon, req_id, exp_progress, exp_error,
                             exp_done):
        """
        Check the RequestMonitor state.
        'exp_*' are lists of hub IDs
        """
        states = rmon.request_state(req_id)
        if states is None:
            self.fail("Expected some sort of state")

        in_progress, error, done = states
        failmsg = None
        for name, exp, got_tuple in (
                ("in_progress", exp_progress, in_progress),
                ("error", exp_error, error),
                ("done", exp_done, done),
        ):
            got = list(got_tuple)
            explen = 0 if exp is None else len(exp)
            gotlen = 0 if got is None else len(got)

            if explen != gotlen:
                tmpmsg = "%d %s hosts, not %s" % (explen, name, gotlen)
                if failmsg is None:
                    failmsg = tmpmsg
                else:
                    failmsg += "; " + tmpmsg
                continue

            if explen > 0:
                for expval in exp:
                    found = None
                    for idx in range(gotlen):
                        if expval == got[idx]:
                            del got[idx]
                            gotlen -= 1
                            found = idx
                            break
                    if found is None:
                        self.fail("Could not find %s value %s" %
                                  (name, expval))
        if failmsg is not None:
            self.fail("Expected " + failmsg)

    @classmethod
    def __convert_message_dict(cls, olddict, live_msg=False):
        mdict = olddict.copy()

        # fix time-based fields
        for prefix in ("start", "stop"):
            key = prefix + "_ticks"
            if key not in mdict:
                raise HsException("Message is missing \"%s_ticks\"" %
                                  (prefix, prefix))

            if not isinstance(mdict[key], numbers.Number):
                raise HsException("Message field %s type is %s, not number" %
                                  (key, type(mdict[key]).__name__))

            if live_msg:
                if key.endswith("_ticks"):
                    del mdict[key]
                mdict[prefix + "_time"] \
                    = str(DAQTime.ticks_to_utc(mdict[key]))

        # return new dictionary
        return mdict

    @classmethod
    def __create_message(cls, msgtype=HsMessage.INITIAL,
                         request_id="default", username="foo",
                         prefix="bar", start_ticks=int(10E10),
                         stop_ticks=int(11E10), dest_dir="/tmp/rmtst",
                         host="localhost", hubs=None, version=2,
                         live_msg=False):
        mdict = {}
        mdict["msgtype"] = msgtype
        mdict["request_id"] = request_id
        mdict["username"] = username
        mdict["prefix"] = prefix
        mdict["start_ticks"] = start_ticks
        mdict["stop_ticks"] = stop_ticks
        mdict["destination_dir"] = dest_dir
        mdict["copy_dir"] = None
        mdict["host"] = host
        mdict["hubs"] = hubs
        mdict["version"] = version

        return HsMessage.fix_message_dict(mdict)

    @classmethod
    def __create_monitor(cls, sender):
        if cls.REQUEST_MONITOR is not None:
            raise ValueError("REQUEST_MONITOR was already initialized")

        rmon = RequestMonitor(sender)
        cls.REQUEST_MONITOR = rmon
        cls.__start_thread(rmon)
        return rmon

    def __expect_live_status(self, sender, mdict, status=None, success=None,
                             failed=None):
        # if no status was specified, use success/failed to derive it
        if status is None:
            if success is not None:
                if failed is not None:
                    status = HsUtil.STATUS_PARTIAL
                else:
                    status = HsUtil.STATUS_SUCCESS
            elif failed is not None:
                status = HsUtil.STATUS_FAIL
            else:
                raise HsException("Must specify status or success/failed")

        livedict = mdict.copy()
        # delete non-Live fields
        for fld in ("copy_dir", "host", "version", "msgtype", "extract",
                    "hubs"):
            if fld in livedict:
                del livedict[fld]

        # fix time-based fields
        for prefix in ("start", "stop"):
            tick_key = prefix + "_ticks"
            time_key = prefix + "_time"
            if tick_key in livedict:
                utc = DAQTime.ticks_to_utc(livedict[tick_key])
                livedict[time_key] = str(utc)
                del livedict[tick_key]
            elif time_key not in livedict:
                raise ValueError("Message has neither \"%s_ticks\" nor"
                                 " \"%s_time\"" % (prefix, prefix))

        # add a few Live-specific fields
        livedict["status"] = status
        livedict["update_time"] = TIME_PAT
        if success is not None:
            livedict["success"] = success
        if failed is not None:
            livedict["failed"] = failed

        sender.i3socket.addExpectedMessage(livedict, service="hitspool",
                                           varname="hsrequest_info", prio=1,
                                           time=TIME_PAT)

    def __send_message(self, rmon, template_dict, msgtype=None, host=None,
                       wait_for_receipt=True):
        mdict = template_dict.copy()
        if msgtype is not None:
            mdict["msgtype"] = msgtype
        if host is not None:
            mdict["host"] = host
        rmon.add_message(HsMessage.dict_to_message(mdict), False)

        if wait_for_receipt:
            self.__wait_for_message(rmon)

    @classmethod
    def __start_thread(cls, rmon, max_count=10):
        cls.REQUEST_MONITOR = rmon
        rmon.start()

        count = 0
        while count < max_count and not rmon.is_started:
            count += 1
            time.sleep(0.01)

        if not rmon.is_started:
            raise Exception("Cannot start %s" % rmon)

    @classmethod
    def __stop_thread(cls, rmon, max_count=10):
        rmon.stop()

        count = 0
        while count < max_count and rmon.is_started:
            count += 1
            time.sleep(0.01)

        if rmon == cls.REQUEST_MONITOR:
            cls.REQUEST_MONITOR = None

        if rmon.is_started:
            raise Exception("Cannot stop %s" % rmon)

    def __standard_hub_test(self, req_id, hub_pairs, restart=False):
        sender = MockSender()
        rmon = self.__create_monitor(sender)

        # build hub lists
        hubs = []
        hub_ids = []
        for hub, state in hub_pairs:
            hubs.append(hub)
            hub_ids.append(HsUtil.hub_name_to_id(hub))

        # build text list of hubs for initial request
        hubstr = ",".join([pair[0] for pair in hub_pairs
                           if pair[1] != HsMessage.IGNORED])

        mdict = self.__create_message(request_id=req_id, hubs=hubstr)

        # tell workers to expect the request
        sender.workers.addExpected(mdict.copy())

        # tell Live to expect a status message from the sender
        self.__expect_live_status(sender, mdict, status=HsUtil.STATUS_QUEUED)

        if restart:
            self.__stop_thread(rmon)
            rmon = self.__create_monitor(sender)

        # add request notification email
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % sender.cluster
        notify_msg = re.compile(r'Start: .*\nStop: .*\n' +
                                r'\(no possible leapseconds applied\)',
                                flags=re.MULTILINE)
        sender.i3socket.addGenericEMail(HsConstants.ALERT_EMAIL_DEV,
                                        notify_hdr, notify_msg, prio=1)

        # send initial message to RequestMonitor
        self.__send_message(rmon, mdict.copy())

        # shouldn't have any worker state yet
        self.__check_reqmon_state(rmon, req_id, None, None, None)

        if restart:
            self.__stop_thread(rmon)
            rmon = self.__create_monitor(sender)

        # sender will notify Live that the request is being processed
        self.__expect_live_status(sender, mdict,
                                  status=HsUtil.STATUS_IN_PROGRESS)

        # send simulated hub STARTED messages to RequestMonitor
        for pair in hub_pairs:
            self.__send_message(rmon, mdict,
                                msgtype=HsMessage.STARTED,
                                host=pair[0])
            if restart:
                self.__stop_thread(rmon)
                rmon = self.__create_monitor(sender)

        self.__check_reqmon_state(rmon, req_id, hub_ids[:], None, None)

        # add WORKING messages to RequestMonitor
        for hub, final_state in hub_pairs:
            if final_state != HsMessage.IGNORED:
                self.__send_message(rmon, mdict,
                                    msgtype=HsMessage.WORKING,
                                    host=hub)
                if restart:
                    self.__stop_thread(rmon)
                    rmon = self.__create_monitor(sender)

        # add IGNORED messages to RequestMonitor
        for hub, final_state in hub_pairs:
            if final_state == HsMessage.IGNORED:
                self.__send_message(rmon, mdict,
                                    msgtype=HsMessage.IGNORED,
                                    host=hub)
                if restart:
                    self.__stop_thread(rmon)
                    rmon = self.__create_monitor(sender)

        # remove IGNORED hubs from ID list
        for hub, state in hub_pairs:
            if state == HsMessage.IGNORED:
                hub_id = HsUtil.hub_name_to_id(hub)
                del hub_ids[hub_ids.index(hub_id)]

        self.__check_reqmon_state(rmon, req_id, hub_ids[:], None, None)

        active = [pair[0] for pair in hub_pairs
                  if pair[1] != HsMessage.IGNORED]

        success = []
        failed = []
        for hub, state in hub_pairs:
            if state == HsMessage.DONE:
                success.append(hub)
            elif state == HsMessage.FAILED:
                failed.append(hub)
            elif state == HsMessage.IGNORED:
                continue
            else:
                raise ValueError("Unknown state \"%s\" for hub %s" %
                                 (state, hub))

            del active[active.index(hub)]

            if len(active) == 0:
                # determine final status of request
                if len(success) == 0:
                    final_state = "FAIL"
                elif len(failed) == 0:
                    final_state = "SUCCESS"
                else:
                    final_state = "PARTIAL"

                # build lists of hubs
                if len(success) == 0:
                    shubs = None
                    sstr = ""
                else:
                    shubs = HsUtil.hubs_to_string(success)
                    sstr = " success=%s" % shubs
                if len(failed) == 0:
                    fhubs = None
                    fstr = ""
                else:
                    fhubs = HsUtil.hubs_to_string(failed)
                    fstr = " failed=%s" % fhubs

                # add expected summary log message
                self.expectLogMessage("Req#%s %s%s%s" %
                                      (req_id, final_state, sstr, fstr))

                # send summary message to Live
                self.__expect_live_status(sender, mdict, success=shubs,
                                          failed=fhubs)

            # send hub status message to RequestMonitor
            self.__send_message(rmon, mdict, msgtype=state, host=hub)

            if restart:
                self.__stop_thread(rmon)
                rmon = self.__create_monitor(sender)

            if len(active) > 0:
                state_lists = self.__build_state_lists(hub_pairs, active)
                self.__check_reqmon_state(rmon, req_id, state_lists[0],
                                          state_lists[1], state_lists[2])

        result = rmon.request_state(req_id)
        self.assertTrue(result is None,
                        "Did not expect in_progress/error/done list")

    def __wait_for_idle(self, rmon, max_count=10):
        count = 0
        while count < max_count and not rmon.is_idle:
            count += 1
            time.sleep(0.01)

        if not rmon.is_idle:
            self.fail("%s is still busy" % rmon)

    def __wait_for_message(self, rmon, max_count=10):
        count = 0
        while count < max_count and rmon.has_message:
            count += 1
            time.sleep(0.01)
        if rmon.has_message:
            self.fail("ReqMon has not fetched message")

    def setUp(self):
        super(RequestMonitorTest, self).setUp()

        # point the RequestMonitor at a temporary state file for tests
        set_state_db_path()

        # remove old DB file
        path = RequestMonitor.get_db_path()
        if os.path.exists(path):
            os.unlink(path)

    def tearDown(self):
        if self.REQUEST_MONITOR is not None:
            self.__stop_thread(self.REQUEST_MONITOR)
            self.REQUEST_MONITOR = None

        super(RequestMonitorTest, self).tearDown()

    def test_only_msg_started(self):
        sender = MockSender()
        rmon = self.__create_monitor(sender)

        req_id = "OnlyStarted"
        mdict = self.__create_message(request_id=req_id)

        self.__expect_live_status(sender, mdict,
                                  status=HsUtil.STATUS_IN_PROGRESS)

        hubname = "ichub11"

        self.expectLogMessage("Received unexpected STARTED message from %s"
                              " for Req#%s (no active request)" %
                              (hubname, req_id))
        self.expectLogMessage("Request %s was not initialized (received"
                              " START from %s)" % (req_id, hubname))

        self.__send_message(rmon, mdict.copy(), msgtype=HsMessage.STARTED,
                            host=hubname)

        self.__check_reqmon_state(rmon, req_id, (11, ), None, None)

    def test_only_msg_in_progress(self):
        sender = MockSender()
        rmon = self.__create_monitor(sender)

        req_id = "OnlyInProg"
        mdict = self.__create_message(request_id=req_id)

        self.__expect_live_status(sender, mdict,
                                  status=HsUtil.STATUS_IN_PROGRESS)

        hubname = "ichub11"

        self.expectLogMessage("Received unexpected WORKING message from %s"
                              " for Req#%s (no active request)" %
                              (hubname, req_id))
        self.expectLogMessage("Request %s was not initialized (received"
                              " WORKING from %s)" % (req_id, hubname))
        self.expectLogMessage("Saw WORKING message for request %s host %s"
                              " but host was unknown" %
                              (req_id, hubname))

        self.__send_message(rmon, mdict.copy(), msgtype=HsMessage.WORKING,
                            host=hubname)

        self.__check_reqmon_state(rmon, req_id, None, None, None)

    def test_only_msg_done(self):
        sender = MockSender()
        rmon = self.__create_monitor(sender)

        req_id = "OnlyDone"
        mdict = self.__create_message(request_id=req_id)

        self.__expect_live_status(sender, mdict, success="11")

        hubname = "ichub11"

        self.expectLogMessage("Received unexpected DONE message from %s"
                              " for Req#%s (no active request)" %
                              (hubname, req_id))
        self.expectLogMessage("Request %s was not initialized (received"
                              " DONE from %s)" % (req_id, hubname))
        self.expectLogMessage("Saw DONE message for request %s host %s"
                              " without a START message" %
                              (req_id, hubname))

        self.expectLogMessage("Req#%s SUCCESS success=%s" % (req_id, 11))

        self.__send_message(rmon, mdict.copy(), msgtype=HsMessage.DONE,
                            host=hubname)

        result = rmon.request_state(req_id)
        self.assertTrue(result is None,
                        "Did not expect in_progress/error/done list")

    def test_only_msg_failed(self):
        sender = MockSender()
        rmon = self.__create_monitor(sender)

        req_id = "OnlyFailed"
        mdict = self.__create_message(request_id=req_id)

        hubname = "ichub11"

        self.__expect_live_status(sender, mdict, failed="11")

        self.expectLogMessage("Received unexpected FAILED message from %s"
                              " for Req#%s (no active request)" %
                              (hubname, req_id))
        self.expectLogMessage("Request %s was not initialized (received"
                              " FAILED from %s)" % (req_id, hubname))
        self.expectLogMessage("Saw FAILED message for request %s host %s"
                              " without a START message" %
                              (req_id, hubname))

        self.expectLogMessage("Req#%s FAIL failed=%s" % (req_id, 11))

        self.__send_message(rmon, mdict.copy(), msgtype=HsMessage.FAILED,
                            host=hubname)

        result = rmon.request_state(req_id)
        self.assertTrue(result is None,
                        "Did not expect in_progress/error/done list")

    def test_standard_stuff(self):
        pairs = (("ichub11", HsMessage.DONE),
                 ("scube", HsMessage.FAILED))

        self.__standard_hub_test("StdStuff", hub_pairs=pairs)

    def test_standard_stuff_restart(self):
        pairs = (("ichub11", HsMessage.DONE),
                 ("scube", HsMessage.FAILED))

        self.__standard_hub_test("RestartStd", hub_pairs=pairs,
                                 restart=True)

    def test_ignored_hub(self):
        pairs = (("ichub11", HsMessage.DONE),
                 ("ithub05", HsMessage.IGNORED),
                 ("ichub85", HsMessage.DONE))

        self.__standard_hub_test("IgnoreHub", hub_pairs=pairs)

    def test_ignored_hub_restart(self):
        pairs = (("ichub11", HsMessage.DONE),
                 ("ithub05", HsMessage.IGNORED),
                 ("ichub85", HsMessage.DONE))

        self.__standard_hub_test("RestartIgn", hub_pairs=pairs,
                                 restart=True)

    def test_failed(self):
        pairs = (("ichub11", HsMessage.FAILED),
                 ("ithub05", HsMessage.IGNORED),
                 ("ichub85", HsMessage.FAILED))

        self.__standard_hub_test("FailedHubs", hub_pairs=pairs)

    def test_failed_restart(self):
        pairs = (("ichub11", HsMessage.FAILED),
                 ("ithub05", HsMessage.IGNORED),
                 ("ichub85", HsMessage.FAILED))

        self.__standard_hub_test("FailedHubs", hub_pairs=pairs)

    def test_interleaved_requests(self):
        sender = MockSender()
        rmon = self.__create_monitor(sender)

        req_id1 = "ReqOne"
        mdict1 = self.__create_message(request_id=req_id1)

        test_msg1 = mdict1.copy()

        # workers receive the first request
        sender.workers.addExpected(test_msg1)

        # sender notifies Live of first request
        self.__expect_live_status(sender, mdict1, status=HsUtil.STATUS_QUEUED)

        # add first request notification email
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % sender.cluster
        notify_msg = re.compile(r'Start: .*\nStop: .*\n' +
                                r'\(no possible leapseconds applied\)',
                                flags=re.MULTILINE)
        sender.i3socket.addGenericEMail(HsConstants.ALERT_EMAIL_DEV,
                                        notify_hdr, notify_msg, prio=1)

        # send first request to RequestMonitor
        self.__send_message(rmon, test_msg1)

        req_id2 = "ReqTwo"
        mdict2 = self.__create_message(request_id=req_id2,
                                       start_ticks=int(12E10),
                                       stop_ticks=int(13E10))

        test_msg2 = mdict2.copy()

        # workers receive the second request
        sender.workers.addExpected(test_msg2)

        # sender notifies Live of second request
        self.__expect_live_status(sender, mdict2, status=HsUtil.STATUS_QUEUED)

        # add second request notification email
        notify_hdr = 'DATA REQUEST HsInterface Alert: %s' % sender.cluster
        notify_msg = re.compile(r'Start: .*\nStop: .*\n' +
                                r'\(no possible leapseconds applied\)',
                                flags=re.MULTILINE)
        sender.i3socket.addGenericEMail(HsConstants.ALERT_EMAIL_DEV,
                                        notify_hdr, notify_msg, prio=1)

        # send second request to RequestMonitor
        self.__send_message(rmon, test_msg2)

        # sender notifies Live that first request is being processed
        self.__expect_live_status(sender, mdict1,
                                  status=HsUtil.STATUS_IN_PROGRESS)

        # send hub STARTED messages to RequestMonitor
        self.__send_message(rmon, mdict1, msgtype=HsMessage.STARTED,
                            host="ichub11")
        self.__send_message(rmon, mdict1, msgtype=HsMessage.STARTED,
                            host="scube", wait_for_receipt=False)

        # add first ichub11 WORKING messages to RequestMonitor
        self.__send_message(rmon, mdict1, msgtype=HsMessage.WORKING,
                            host="ichub11")

        self.__check_reqmon_state(rmon, req_id1, (11, 99), None, None)

        # send first ichub11 DONE message to RequestMonitor
        self.__send_message(rmon, mdict1, msgtype=HsMessage.DONE,
                            host="ichub11")

        self.__check_reqmon_state(rmon, req_id1, (99, ), None, (11, ))

        # sender notifies Live that second request is being processed
        self.__expect_live_status(sender, mdict2,
                                  status=HsUtil.STATUS_IN_PROGRESS)

        # add expected error messages
        self.expectLogMessage("Received unexpected STARTED message from"
                              " %s for Req#%s (Req#%s is active)" %
                              ("ichub11", req_id2, req_id1))

        self.expectLogMessage("Received unexpected STARTED message from"
                              " %s for Req#%s (Req#%s is active)" %
                              ("ithub05", req_id2, req_id1))

        # send second "worker" STARTED messages to RequestMonitor
        self.__send_message(rmon, mdict2, msgtype=HsMessage.STARTED,
                            host="ichub11")
        self.__send_message(rmon, mdict2, msgtype=HsMessage.STARTED,
                            host="ithub05")

        self.__check_reqmon_state(rmon, req_id2, (11, 205), None, None)

        self.expectLogMessage("Received unexpected DONE message from"
                              " %s for Req#%s (Req#%s is active)" %
                              ("ichub11", req_id2, req_id1))

        # send second ichub11 DONE message to RequestMonitor
        self.__send_message(rmon, mdict2, msgtype=HsMessage.DONE,
                            host="ichub11")

        self.__check_reqmon_state(rmon, req_id2, (205, ), None, (11, ))

        # add second expected summary log message
        self.expectLogMessage("Req#%s PARTIAL success=%s failed=%s" %
                              (req_id1, 11, 99))

        # send second summary message to Live
        self.__expect_live_status(sender, mdict1, success="11", failed="99")

        # send second scube DONE message to RequestMonitor
        self.__send_message(rmon, mdict1, msgtype=HsMessage.FAILED,
                            host="scube")

        result = rmon.request_state(req_id1)
        self.assertTrue(result is None,
                        "Did not expect in_progress/error/done list")

        self.expectLogMessage("Received unexpected FAILED message from"
                              " %s for Req#%s (no active request)" %
                              ("ithub05", req_id2))

        # add second expected summary log message
        self.expectLogMessage("Req#%s PARTIAL success=%s failed=%s" %
                              (req_id2, 11, 205))

        # send second summary message to Live
        self.__expect_live_status(sender, mdict2, success="11", failed="205")

        # send second hub's DONE message to RequestMonitor
        self.__send_message(rmon, mdict2, msgtype=HsMessage.FAILED,
                            host="ithub05")

        result = rmon.request_state(req_id2)
        self.assertTrue(result is None,
                        "Did not expect in_progress/error/done list")


if __name__ == '__main__':
    unittest.main()
