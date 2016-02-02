#!/usr/bin/env python

import logging
import os
import shutil
import sqlite3
import sys
import time
import traceback
import zmq

import HsConstants
import HsUtil

from HsException import HsException
from HsGrabber import HsGrabber
from HsPrefix import HsPrefix
from HsSender import HsSender
from HsTestUtil import TICKS_PER_SECOND, create_hits, create_hitspool_db


# Root testbed directory
ROOTDIR = "/tmp/TESTCLUSTER"

class HsEnvironment(object):
    def __init__(self, rootdir):
        self.__hubroot = os.path.join(rootdir, "testhub")
        self.__copysrc = os.path.join(rootdir, "copysrc")
        self.__copydst = os.path.join(rootdir, "HsDataCopy")
        self.__explog = os.path.join(rootdir, "expcont", "logs")
        self.__hubtmp = os.path.join(self.__hubroot, "tmp")
        self.__hubspool = os.path.join(self.__hubroot, "hitspool")

    def __create_files(self, cursor, first_time, last_time, hits_per_file):
        file_interval = 15 * TICKS_PER_SECOND
        file_num = 1

        cur_time = first_time
        while cur_time < last_time:
            if cur_time + file_interval < last_time:
                timespan = file_interval
            else:
                timespan = last_time - cur_time

            # how many hits can fit in this file?
            if timespan < hits_per_file:
                num_hits = timespan
            else:
                num_hits = hits_per_file

            # fill file with fake hits
            filename = "HitSpool-%d.dat" % file_num
            path = os.path.join(self.__hubspool, filename)
            create_hits(path, cur_time, cur_time + timespan - 1,
                        timespan / num_hits)

            cursor.execute("insert or replace"
                           " into hitspool(filename, start_tick, stop_tick)"
                           " values (?,?,?)", (filename, cur_time,
                                               cur_time + timespan - 1))

            # onto the next file?
            file_num += 1
            cur_time += timespan

    @property
    def copydst(self):
        "Final destination"
        return self.__copydst

    @property
    def copysrc(self):
        return self.__copysrc

    def create(self, first_time, last_time, hits_per_file):
        for path in (self.__hubroot, self.__copysrc, self.__copydst,
                     self.__explog, self.__hubtmp, self.__hubspool):
            if not os.path.exists(path):
                os.makedirs(path)

        dbpath = create_hitspool_db(self.__hubspool)

        conn = sqlite3.connect(dbpath)
        cursor = conn.cursor()

        try:
            self.__create_files(cursor, first_time, last_time, hits_per_file)
            conn.commit()
        finally:
            conn.close()

        # delete cached request database
        statedb = HsSender.get_db_path()
        if os.path.exists(statedb):
            os.unlink(statedb)

    @property
    def hubroot(self):
        return self.__hubroot

    @property
    def hubspool(self):
        return self.__hubspool

class Request(object):
    def __init__(self, env, succeed, start_time, stop_time, expected_hubs,
                 expected_files, request_id=None, username=None, prefix=None,
                 copydir=None, extract=False, send_json=False):
        # get both SnDAQ timestamp (in ns) and UTC datetime
        (self.__start_sn, self.__start_utc) \
            = HsUtil.parse_sntime(start_time)
        (self.__stop_sn, self.__stop_utc) \
            = HsUtil.parse_sntime(stop_time)

        if copydir is None:
            copydir = os.path.join(ROOTDIR, "HsDataCopy")

        self.__expected_result = succeed
        self.__expected_hubs = expected_hubs[:]
        if expected_files is None:
            self.__expected_files = None
        else:
            self.__expected_files = expected_files[:]
        self.__req_id = request_id
        self.__username = username
        self.__prefix = prefix
        self.__copydir = copydir
        self.__extract = extract
        self.__send_json = send_json

    def __str__(self):
        secs = (self.__stop_sn - self.__start_sn) / 1E9

        if self.__req_id is None:
            rstr = ""
        else:
            rstr = "Request %s " % str(self.__req_id)

        if self.__username is None:
            ustr = ""
        else:
            ustr = " by %s" % self.__username

        if self.__prefix is None:
            pstr = ""
        else:
            pstr = " for %s" % str(self.__prefix)

        if self.__extract:
            estr = ", extract to file"
        else:
            estr = ""
        if self.__send_json:
            jstr = " as JSON"
        else:
            jstr = ""

        return "%s%.2f secs%s%s to %s%s%s\n\t[%s :: %s]" % \
            (rstr, secs, ustr, pstr, self.__copydir, estr, jstr,
             self.__start_utc, self.__stop_utc)

    def __check_destination(self, destination):
        if not os.path.isdir(destination):
            raise HsException("Destination directory %s does not exist" %
                              destination)

        # get the name of the single subdirectory in the destination directory
        found = []
        for entry in os.listdir(destination):
            found.append(entry)
        if len(found) == 0:
            raise HsException("Destination directory %s is empty" %
                              destination)
        elif len(found) != 1:
            raise HsException("Found multiple entries under %s: %s" %
                              (destination, found))

        # gather all expected subdirectory pieces
        if self.__prefix is not None:
            exp_prefix = self.__prefix
        else:
            exp_prefix = HsPrefix.guess_from_dir(destination)
        exp_yymmdd = self.__start_utc.strftime("%Y%m%d")

        # make sure the subdirectory has all the expected pieces
        subdir = found[0]
        subpieces = subdir.split("_")
        if len(subpieces) != 4:
            raise HsException("Subdirectory \"%s\" doesn't have enough"
                              " underbar-separated pieces" % subdir)
        if subpieces[0] != exp_prefix:
            raise HsException("Expected subdirectory prefix \"%s\","
                              " not \"%s\" (from %s)" %
                              (exp_prefix, subpieces[0], subdir))
        if subpieces[1] != exp_yymmdd:
            raise HsException("Expected subdirectory year/month/day \"%s\","
                              " not \"%s\" (from %s)" %
                              (exp_yymmdd, subpieces[1], subdir))
        if subpieces[3] not in self.__expected_hubs:
            raise HsException("Subdirectory \"%s\" ends with unexpected"
                              " hub \"%s\"" % (subdir, subpieces[3]))

        # build the full path to the subdirectory
        subpath = os.path.join(destination, subdir)

        # get a list of all unexpected files
        badfiles = []
        for entry in os.listdir(subpath):
            if entry == subdir:
                raise HsException("Found %s subdirectory under %s" %
                                  (subdir, subpath))
            if not entry in self.__expected_files:
                badfiles.append(entry)

        # complain about unexpected files
        if len(badfiles) > 0:
            raise HsException("Found unexpected files under %s: %s" %
                                  (subdir, badfiles))

        exp_num = len(self.__expected_files)
        print "Destination directory %s looks good (found %d file%s)" % \
            (subdir, exp_num, "" if exp_num == 1 else "s")

    def __check_empty(self, destination):
        if not os.path.isdir(destination):
            return

        found = []
        for entry in os.listdir(destination):
            found.append(entry)
        if len(found) > 0:
            raise HsException("Found files under %s: %s" %
                              (destination, found))

    @property
    def copydir(self):
        return self.__copydir

    def run(self, requester):
        return requester.send_alert(self.__start_sn, self.__start_utc,
                                    self.__stop_sn, self.__stop_utc,
                                    self.__copydir, request_id=self.__req_id,
                                    username=self.__username,
                                    prefix=self.__prefix,
                                    extract_hits=self.__extract,
                                    send_json=self.__send_json)

    @property
    def should_succeed(self):
        return self.__expected_result

    def update_copydir(self, user, host, path):
        self.__copydir = "%s@%s:%s" % (user, host, path)

    def validate(self, destination):
        if self.__expected_result:
            self.__check_destination(destination)
        else:
            self.__check_empty(destination)

        return True

class Processor(object):
    # list of top-level fields in Live messages
    REQUIRED_FIELDS = (
        "service", "varname", "value",
    )
    # list of Live status message fields
    STATUS_FIELDS = (
        "request_id",
        "username",
        "prefix",
        "start_time",
        "stop_time",
        "destination_dir",
        "update_time",
        "status",
    )

    def __init__(self):
        # set up pseudo-Live socket
        #
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.PULL)
        self.__socket.bind("tcp://127.0.0.1:%d" % HsConstants.I3LIVE_PORT)

        # create object to submit requests
        self.__requester = HsGrabber()

        # create dictionary to track requests
        self.__requests = {}

        # initialize logging
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                            level=logging.INFO,
                            datefmt='%Y-%m-%d %H:%M:%S',
                            stream=sys.stderr)

    def __clear_destination(self, path):
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.unlink(path)

    def __find_request(self, message):
        if not message.request_id in self.__requests:
            logging.error("Found %s message for unknown request %s",
                          message.status, message.request_id)
            return False
        return True

    def __process_alert(self, value_dict):
        if not isinstance(value_dict, dict):
            logging.error("Alert value should be 'dict', not '%s' (in %s)",
                          type(value_dict), value_dict)
            return

        if not "condition" in value_dict:
            logging.error("Alert value does not contain 'condition' (in %s)",
                          value_dict)
            return

        logging.info("LiveAlert:\n\tCondition %s", value_dict["condition"])

    def __process_responses(self, request, destination):
        while True:
            rawmsg = self.__socket.recv_json()
            if not isinstance(rawmsg, dict):
                logging.error("Expected 'dict', not '%s' for %s" %
                              (type(rawmsg), rawmsg))
                continue

            badtop = False
            for exp in self.REQUIRED_FIELDS:
                if not exp in rawmsg:
                    logging.error("Missing '%s' in Live message %s" %
                                  (exp, rawmsg))
                    badtop = True
                    break
            if badtop:
                continue

            if rawmsg["service"] == "hitspool" and \
               rawmsg["varname"] == "hsrequest":
                rtnval = self.__process_status(rawmsg["value"],
                                               request.should_succeed)
                if rtnval is not None:
                    # got success/failure status
                    try:
                        request.validate(destination)
                    except HsException, hsex:
                        logging.error("Could not validate %s" % request,
                                      exc_info=True)
                        rtnval = False
                    return rtnval

                # keep looking
                continue

            if rawmsg["service"] == "HSiface" and \
               rawmsg["varname"] == "alert":
                self.__process_alert(rawmsg["value"])
                continue

            logging.error("Bad service/varname pair \"%s/%s\" for %s" %
                          (rawmsg["service"], rawmsg["varname"], rawmsg))
            continue

    def __process_status(self, value_dict, should_succeed):
        message = HsUtil.dict_to_object(value_dict, self.STATUS_FIELDS,
                                        "LiveMessage")

        print "::: Req %s LiveStatus %s" % (message.request_id, message.status)

        if message.status == HsUtil.STATUS_QUEUED:
            if message.request_id in self.__requests:
                logging.error("Found %s message for existing request %s",
                              message.status, message.request_id)
            else:
                self.__requests[message.request_id] = message.status
        elif message.status == HsUtil.STATUS_IN_PROGRESS:
            if self.__find_request(message):
                if self.__requests[message.request_id] != HsUtil.STATUS_QUEUED:
                    logging.error("Expected request %s status %s, not %s",
                                  message.request_id, HsUtil.STATUS_QUEUED,
                                  self.__requests[message.request_id])
                self.__requests[message.request_id] = message.status
        elif message.status == HsUtil.STATUS_FAIL:
            if self.__find_request(message):
                return self.__report_result(message, should_succeed, False)
        elif message.status == HsUtil.STATUS_SUCCESS:
            if self.__find_request(message):
                return self.__report_result(message, should_succeed, True)
        else:
            logging.error("Unknown status %s for request %s (%s)",
                          message.status, message.request_id, message)

        return None

    def __report_result(self, message, expected, actual):
        del self.__requests[message.request_id]

        if expected == actual:
            rstr = "succeeded" if expected else "failed (as expected)"
        elif expected:
            rstr = "succeeded (but should have FAILED!)"
        else:
            rstr = "FAILED!"

        logging.info("Request %s %s", message.request_id, rstr)

        return expected == actual

    def __submit(self, request):
        print "::: Submit %s" % str(request)

        try:
            result = request.run(self.__requester)
        except:
            logging.exception("problem with request %s", request)
            return False

        if not result:
            logging.error("Request %s failed", request)
            return False

        try:
            result = self.__requester.wait_for_response()
        except:
            logging.exception("Problem with request %s response", request)
            return False

        if not result:
            logging.error("Request %s response failed", request)
            return False

        return True

    def run(self, request):
        (user, host, path) = self.__requester.split_rsync_path(request.copydir)
        request.update_copydir(user, host, path)

        self.__clear_destination(path)

        try:
            if not self.__submit(request):
                return False
            return self.__process_responses(request, path)
        finally:
            self.__clear_destination(path)


if __name__ == "__main__":
    import subprocess

    from contextlib import contextmanager


    def find_open_requests():
        num_open = 0

        conn = sqlite3.connect(HsSender.get_db_path())
        try:
            cursor = conn.cursor()
            for row in cursor.execute("select id, count(id) from requests"
                                      " group by id"):
                num_open += 1
        finally:
            conn.close()

        return num_open

    def main():
        first_ticks = 157890067960246236
        last_ticks = first_ticks + 75 * TICKS_PER_SECOND
        hits_per_file = 40

        env = HsEnvironment(ROOTDIR)
        env.create(first_ticks, last_ticks, hits_per_file)

        single_file_start = (first_ticks + TICKS_PER_SECOND) / 10
        single_file_stop = (first_ticks + 6 * TICKS_PER_SECOND) / 10
        multi_file_stop = (first_ticks + 65 * TICKS_PER_SECOND) / 10

        hubs = ("ichub01", )

        # list of requests
        requests = (
            Request(env, True, single_file_start, single_file_stop, hubs,
                    ("HitSpool-1.dat", )),
            Request(env, True, single_file_start, multi_file_stop, hubs,
                    ("HitSpool-1.dat", "HitSpool-2.dat", "HitSpool-3.dat",
                     "HitSpool-4.dat", "HitSpool-5.dat", )),
            Request(env, True, single_file_start, single_file_stop, hubs,
                    ("HitSpool-1.dat", ), prefix=HsPrefix.SNALERT,
                    copydir=env.copydst, extract=False, send_json=False),
            Request(env, False, first_ticks - 6 * TICKS_PER_SECOND,
                    first_ticks - 100, hubs, None, prefix=HsPrefix.SNALERT),
            Request(env, True, single_file_start, single_file_stop, hubs,
                    ("hits_157890077960249984_157890127960249984.dat", ),
                    copydir=os.path.join(ROOTDIR, "hese_hs"),
                    extract=True),
            Request(env, True, single_file_start, single_file_stop, hubs,
                    ("hits_157890077960249984_157890127960249984.dat", ),
                    prefix=HsPrefix.HESE, copydir=os.path.join(ROOTDIR, "xxx"),
                    extract=True),
            Request(env, True, single_file_start, single_file_stop, hubs,
                    ("hits_157890077960249984_157890127960249984.dat", ),
                    request_id="ABC123", prefix=HsPrefix.ANON,
                    copydir=os.path.join(ROOTDIR, "anonymous"),
                    extract=True),
            Request(env, True, single_file_start, single_file_stop, hubs,
                    ("hits_157890077960249984_157890127960249984.dat", ),
                    request_id="AliveOrDead", prefix=HsPrefix.LIVE,
                    username="mfrere",
                    copydir=os.path.join(ROOTDIR, "live_and_let_die"),
                    extract=True),
            Request(env, True, single_file_start, single_file_stop, hubs,
                    ("HitSpool-1.dat", ), prefix=HsPrefix.HESE,
                    copydir=env.copydst, extract=False, send_json=False),
        )

        if len(hubs) != 1:
            raise HsException("Expected 1 hub, not %d" % len(hubs))

        with run_and_terminate(("python", "HsPublisher.py",
                                "-l", "/tmp/publish.log")):
            with run_and_terminate(("python", "HsWorker.py",
                                    "-l", "/tmp/worker.log",
                                    "-C", env.copysrc,
                                    "-H", hubs[0],
                                    "-R", env.hubroot)):
                with run_and_terminate(("python", "HsSender.py",
                                        "-l", "/tmp/sender.log")):
                    # give everything a chance to start up
                    time.sleep(5)
                    process_requests(requests)


    def process_requests(requests):
        processor = Processor()

        failed = 0

        first = True
        for request in requests:
            try:
                if not processor.run(request):
                    failed += 1
            except:
                logging.exception("Request failed")
                failed += 1

            # print a separator so it's easy to see different requests
            print >>sys.stderr, "="*75

        open_reqs = find_open_requests()

        if failed == 0 and open_reqs == 0:
            print "No problems found"
        else:
            print >>sys.stderr, "Found problems with %d requests!" % failed
            if open_reqs > 0 and open_reqs != failed:
                print >>sys.stderr, "Found %d open requests in state DB" % \
                    open_reqs


    @contextmanager
    def run_and_terminate(*args, **kwargs):
        p = None
        try:
            p = subprocess.Popen(*args, **kwargs)
            yield p
        finally:
            if p is not None:
                p.terminate() # send sigterm, or ...
                p.kill()      # send sigkill

    main()