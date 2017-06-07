#!/usr/bin/env python

import logging
import os
import shutil
import sqlite3
import sys
import tarfile
import time
import traceback
import zmq

# make sure HsConstant is available
current = os.path.dirname(os.path.realpath(__file__))
if not os.path.exists(os.path.join(current, "HsConstants.py")):
    parent = os.path.dirname(current)
    if not os.path.exists(os.path.join(parent, "HsConstants.py")):
        raise System.exit("Cannot find HsConstants.py")
    sys.path.insert(0, parent)

import HsConstants
import HsUtil

from lxml import etree, objectify

from HsBase import DAQTime
from HsException import HsException
from HsGrabber import HsGrabber
from HsPrefix import HsPrefix
from HsSender import HsSender
from HsTestUtil import TICKS_PER_SECOND, create_hits, create_hitspool_db, \
    set_state_db_path
from RequestMonitor import RequestMonitor


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
        self.__spadequeue = os.path.join(rootdir, "SpadeQueue")

        # use a temporary copy of the hitspool DB
        set_state_db_path()

        self.__hsdbpath = None

    def __clear_db(self, conn):
        conn.execute("delete from hitspool")

    def __create_files(self, conn, first_time, last_time, hits_per_file):
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

            # update hitspool DB
            conn.execute("insert or replace"
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
                     self.__explog, self.__hubtmp, self.__hubspool,
                     self.__spadequeue):
            if not os.path.exists(path):
                os.makedirs(path)

        self.__hsdbpath = create_hitspool_db(self.__hubspool)
        conn = sqlite3.connect(self.__hsdbpath)
        try:
            self.__clear_db(conn)
            self.__create_files(conn, first_time, last_time, hits_per_file)
            conn.commit()
        finally:
            conn.close()

        # delete cached request database
        statedb = RequestMonitor.get_db_path()
        if os.path.exists(statedb):
            os.unlink(statedb)

    def files_in_range(self, start_time, stop_time):
        if self.__hsdbpath is None:
            raise HsException("Hitspool DB has not been initialized!")

        conn = sqlite3.connect(self.__hsdbpath)

        files = []
        try:
            cursor = conn.cursor()
            cursor.execute("select filename from hitspool"
                           " where start_tick<=? and stop_tick>=?",
                           (stop_time, start_time))
            files = [row[0].encode("ascii") for row in cursor.fetchall()]
            files.sort()
            return files
        finally:
            conn.close()

    @property
    def hubroot(self):
        return self.__hubroot

    @property
    def hubspool(self):
        return self.__hubspool

    @property
    def spadequeue(self):
        "SPADE queue directory"
        return self.__spadequeue

class Request(object):
    SCHEMA_PATH = "testbed/schema/IceCubeDIFPlus.xsd"
    NEXT_NUM = 1

    def __init__(self, env, succeed, start_time, stop_time, expected_hubs,
                 expected_files=None, request_id=None, username=None,
                 prefix=None, copydir=None, extract=False, send_old=False,
                 number=None, ignored=None):
        # check parameters
        if not isinstance(start_time, DAQTime):
            raise TypeError("Start time %s is <%s>, not <DAQTime>" %
                            (start_time, type(start_time)))
        if not isinstance(stop_time, DAQTime):
            raise TypeError("Stop time %s is <%s>, not <DAQTime>" %
                            (stop_time, type(stop_time)))

        # if present, validate list of ignored hubs
        if ignored is not None:
            # ensure 'ignored' is a list/tuple
            if isinstance(ignored, str):
                ignored = (ignored, )
            for hub in ignored:
                try:
                    expected_hubs.index(hub)
                except:
                    raise TypeError("Cannot ignore unknown host \"%s\""
                                    " (known hosts are %s)" %
                                    (hub, expected_hubs))
            if send_old:
                raise TypeError("Cannot send old request with ignored hubs")

        # if no request number was specified, get the next available
        if number is not None:
            self.__number = number
        else:
            self.__number = self.get_next_number()

        self.__start_time = start_time
        self.__stop_time = stop_time

        if extract:
            if expected_files is None:
                raise HsException("No expected files specified!")
        else:
            if expected_files is not None:
                raise HsException("Expected files should not be specified!")
            expected_files = env.files_in_range(start_time.ticks,
                                                stop_time.ticks)

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
        self.__send_old = send_old
        self.__spadequeue = env.spadequeue
        self.__ignored = ignored

    def __str__(self):
        secs = (self.__stop_time.ticks - self.__start_time.ticks) / 1E10

        if self.__req_id is None:
            rstr = ""
        else:
            rstr = " (ID=%s)" % str(self.__req_id)

        if self.__username is None:
            ustr = ""
        else:
            ustr = " by %s" % self.__username

        if self.__prefix is None:
            pstr = ""
        else:
            pstr = " for %s" % str(self.__prefix)

        if self.__send_old:
            jstr = " (old-style)"
        else:
            jstr = ""

        if self.__extract:
            estr = ", extract to file"
        else:
            estr = ""

        return "Request #%s%s: %.2f secs%s%s to %s%s%s\n\t[%s :: %s]" % \
            (self.__number, rstr, secs, ustr, pstr, self.__copydir, jstr,
             estr, self.__start_time.utc, self.__stop_time.utc)

    def __check_destination(self, destination, quiet=False):
        if not os.path.isdir(destination):
            raise HsException("Destination directory %s does not exist" %
                              destination)

        # get the name of the single subdirectory in the destination directory
        found_subdir = False
        extralist = []
        for entry in os.listdir(destination):
            path = os.path.join(destination, entry)
            if os.path.isdir(path) and not found_subdir:
                self.__check_destination_subdir(destination, entry,
                                                quiet=quiet)
                found_subdir = True
                continue

            extralist.append(entry)

        if not found_subdir:
            raise HsException("Destination directory %s didn't contain"
                              " expected subdirectory" % destination)
        if len(extralist) != 0:
            raise HsException("Destination directory %s contained"
                              " unexpected files: %s" %
                              (destination, extralist))

    def __check_destination_subdir(self, destination, subdir, quiet=False):
        # gather all expected subdirectory pieces
        if self.__prefix is not None:
            exp_prefix = self.__prefix
        else:
            exp_prefix = HsPrefix.guess_from_dir(destination)
        exp_yymmdd = self.__start_time.utc.strftime("%Y%m%d")

        # make sure the subdirectory has all the expected pieces
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
        if not quiet:
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

    def __check_spademeta(self, metapath, quiet=False):
        # find the schema first
        schemapath = self.SCHEMA_PATH
        while not os.path.exists(schemapath):
            result = schemapath.split(os.path, 1)
            if len(result) == 1:
                print >>sys.stderr, "WARNING: Not validating %s" % metapath
                return
            schemapath = result[1]

        # read in the metadata XML
        with open(metapath, "r") as xml:
            xmlstr = xml.read()

        # validate the metadata
        try:
            schema = etree.XMLSchema(file=schemapath)
            parser = objectify.makeparser(schema=schema)
            objectify.fromstring(xmlstr, parser)
        except:
            raise HsException("Bad metadata file %s: %s" %
                              (os.path.basename(metapath),
                               traceback.format_exc()))

        if not quiet:
            print "SPADE metadata %s looks good" % os.path.basename(metapath)

    def __check_spadequeue(self, directory=None, quiet=False):
        if directory is None:
            directory = self.__spadequeue

        tarfiles = []
        semfiles = []
        extralist = []
        for entry in os.listdir(directory):
            if HsSender.WRITE_META_XML:
                if entry.endswith(HsSender.META_SUFFIX):
                    semfiles.append(entry)
                    continue
            elif entry.endswith(HsSender.SEM_SUFFIX):
                semfiles.append(entry)
                continue
            if entry.endswith(HsSender.TAR_SUFFIX):
                tarfiles.append(entry)
                continue

            extralist.append(entry)

        if len(extralist) > 0:
            raise HsException("Found extra files in SPADE queue %s: %s" %
                              (directory, extralist))
        if len(semfiles) == 0:
            if len(tarfiles) == 0:
                raise HsException("No files found in SPADE queue")
            raise HsException("Found tar file(s) %s without semaphore file" %
                              (tarfiles, ))
        elif len(tarfiles) == 0:
            raise HsException("Found semaphore(s) %s without tar file" %
                              (semfiles, ))

        for tarname in tarfiles:
            # not strictly necessary, but never hurts to be paranoid in tests
            if not tarname.endswith(HsSender.TAR_SUFFIX):
                raise HsException("Weird, tar name \"%s\" doesn't end with"
                                  " \"%s\"" % (tarname, HsSender.TAR_SUFFIX))

            # find corresponding semaphore file
            basename = tarname[:-len(HsSender.TAR_SUFFIX)]
            semname = None
            for sem in semfiles:
                if sem.startswith(basename):
                    semname = sem
                    break
            if semname is None:
                raise HsException("No semaphore file found for \"%s\"" %
                                  (tarname, ))

            tarpath = os.path.join(directory, tarname)
            sempath = os.path.join(directory, semname)

            try:
                self.__check_spadetar(tarpath, quiet=quiet)
                if HsSender.WRITE_META_XML:
                    self.__check_spademeta(sempath, quiet=quiet)
            finally:
                if os.path.exists(tarpath):
                    os.unlink(tarpath)
                if os.path.exists(sempath):
                    os.unlink(sempath)

    def __check_spadetar(self, tarpath, quiet=False):
        try:
            tar = tarfile.open(tarpath, "r")
        except StandardError, err:
            raise HsException("Cannot read %s: %s" %
                              (tarpath, err))

        unknown = []
        try:
            subdir = None
            count = 0
            for info in tar:
                if info.isdir() and subdir is None:
                    subdir = info.name
                    continue

                found = False
                for name in self.__expected_files:
                    if info.name.endswith(name):
                        count += 1
                        found = True
                        break
                if not found:
                    unknown.append(info.name)
        finally:
            tar.close()

        if len(unknown) > 0:
            raise HsException("Found %d unknown entr%s in tar file %s: %s"
                              "\n\t(expected %s)" %
                              (len(unknown),
                               "y" if len(unknown) == 1 else "ies",
                               os.path.basename(tarpath), unknown,
                               self.__expected_files))
        num_exp = len(self.__expected_files)
        if num_exp != count:
            raise HsException("Expected %d file%s in %s, found %d" %
                              (num_exp, "" if num_exp == 1 else "s",
                               os.path.basename(tarpath), count))

        if not quiet:
            print "SPADE tarfile %s looks good (found %d file%s)" % \
                (os.path.basename(tarpath), count, "" if count == 1 else "s")

    @classmethod
    def get_next_number(cls):
        val = cls.NEXT_NUM
        cls.NEXT_NUM += 1
        return val

    @property
    def copydir(self):
        return self.__copydir

    @property
    def number(self):
        return self.__number

    def run(self, requester, quiet=False):
        if self.__send_old:
            send_method = requester.send_old_alert
        else:
            send_method = requester.send_alert

        # if one or more hubs are ignored, build a list of desired hubs
        hubs = None
        if self.__ignored is not None:
            for hub in self.__expected_hubs:
                if hub in self.__ignored:
                    continue
                if hubs is None:
                    hubs = hub
                else:
                    hubs += "," + hub

        return send_method(self.__start_time, self.__stop_time, self.__copydir,
                           request_id=self.__req_id, username=self.__username,
                           prefix=self.__prefix, extract_hits=self.__extract,
                           hubs=hubs)

    def set_number(self, number):
        self.__number = number

    @property
    def should_succeed(self):
        return self.__expected_result

    @property
    def spadequeue(self):
        return self.__spadequeue

    def update_copydir(self, user, host, path):
        self.__copydir = "%s@%s:%s" % (user, host, path)

    def validate(self, destination, quiet=False):
        if self.__prefix is not None:
            exp_prefix = self.__prefix
        else:
            exp_prefix = HsPrefix.guess_from_dir(destination)

        if self.__expected_result:
            if exp_prefix == HsPrefix.SNALERT or \
               exp_prefix == HsPrefix.HESE:
                self.__check_empty(destination)
                self.__check_spadequeue(quiet=quiet)
            else:
                self.__check_spadequeue(directory=destination, quiet=quiet)
        else:
            self.__check_empty(destination)
            self.__check_empty(self.__spadequeue)

        return True


class MultiRequest(object):
    def __init__(self, *args):
        self.__number = Request.get_next_number()
        self.__requests = args

        alpha = "abcdefghijklmnopqrstuvwxyz"

        self.__copydir = None
        self.__spadequeue = None

        for idx, req in enumerate(self.__requests):
            self.__requests[idx].set_number("%s%s" %
                                            (self.__number, alpha[idx]))

            self.__copydir = self.__check_and_assign(self.__copydir,
                                                     req.copydir)
            self.__spadequeue = self.__check_and_assign(self.__spadequeue,
                                                        req.spadequeue)

    def __str__(self):
        mstr = None
        for req in self.__requests:
            if mstr is None:
                mstr = str(req)
            else:
                mstr += "\n" + str(req)
        return mstr

    @classmethod
    def __check_and_assign(cls, oldval, newval):
        if oldval is not None and oldval != newval:
            raise HsException("%s mismatch (%s != %s)" % (oldval, newval))
        return newval

    @property
    def number(self):
        return self.__number

    @property
    def requests(self):
        return self.__requests[:]


class Twirly(object):
    CHARS = "-/|\\"

    def __init__(self, quiet=False):
        self.__quiet = quiet
        self.__idx = 0

    def print_next(self):
        sys.stdout.write(self.CHARS[self.__idx] + "\b")
        sys.stdout.flush()
        self.__idx = (self.__idx + 1) % len(self.CHARS)


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

    # error conditions which can occur during runs
    #
    RUN_ERR_UNKNOWN = "UNKNOWN"
    RUN_ERR_MISSING = "MISSING"
    RUN_ERR_ORDER = "OUT_OF_ORDER"
    RUN_ERR_MSGCHG = "MESSAGE CHANGED"
    RUN_ERR_ERROR = "ERROR"
    RUN_ERR_EXPECTED = "EXPECTED"

    def __init__(self, quiet=False):
        # set up pseudo-Live socket
        #
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.PULL)
        self.__socket.bind("tcp://127.0.0.1:%d" % HsConstants.I3LIVE_PORT)

        # create object to submit requests
        self.__requester = HsGrabber(is_test=True)

        # create dictionary to track requests
        self.__requests = {}

        # animated progress character for 'quiet' mode
        self.__twirly = Twirly(quiet=quiet)

        # initialize logging
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                            level=logging.INFO,
                            datefmt='%Y-%m-%d %H:%M:%S',
                            stream=sys.stderr)

    def __check_for_changes(self, oldmsg, newmsg):
        if oldmsg.request_id != newmsg.request_id:
            logging.error("Unexpected request ID #%s (should be #%s)",
                          oldmsg.request_id, newmsg.request_id)
            return self.RUN_ERR_MSGCHG

        if oldmsg.username != newmsg.username:
            logging.error("Request ID #%s username changed"
                          " from \"%s\" (for %s) to \"%s\" (for %s)",
                          newmsg.request_id, oldmsg.username, oldmsg.status,
                          newmsg.username, newmsg.status)
            return self.RUN_ERR_MSGCHG

        if oldmsg.start_time != newmsg.start_time:
            logging.error("Request ID #%s start time changed"
                          " from \"%s\" (for %s) to \"%s\" (for %s)",
                          newmsg.request_id, oldmsg.start_time, oldmsg.status,
                          newmsg.start_time, newmsg.status)
            return self.RUN_ERR_MSGCHG

        if oldmsg.stop_time != newmsg.stop_time:
            logging.error("Request ID #%s stop time changed"
                          " from \"%s\" (for %s) to \"%s\" (for %s)",
                          newmsg.request_id, oldmsg.stop_time, oldmsg.status,
                          newmsg.stop_time, newmsg.status)
            return self.RUN_ERR_MSGCHG

        return None

    def __clear_destination(self, path):
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.unlink(path)

    def __find_request(self, message):
        if message.request_id in self.__requests:
            return self.__requests[message.request_id]

        logging.error("Found %s message for unknown request %s",
                      message.status, message.request_id)
        return None

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

    def __process_responses(self, request, destination, quiet=False):
        saw_error = False
        while True:
            if quiet:
                self.__twirly.print_next()

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

            if quiet:
                self.__twirly.print_next()

            if rawmsg["service"] == "hitspool" and \
               rawmsg["varname"].startswith("hsrequest_info"):
                runstatus = self.__process_status(rawmsg["value"],
                                                  request.should_succeed,
                                                  quiet=quiet)
                if (runstatus == self.RUN_ERR_EXPECTED or \
                    runstatus == self.RUN_ERR_ERROR):
                    # got success/failure status
                    rtnval = runstatus == self.RUN_ERR_EXPECTED
                    try:
                        request.validate(destination, quiet=quiet)
                    except HsException, hsex:
                        logging.error("Could not validate %s" % request,
                                      exc_info=True)
                        rtnval = False

                    return rtnval and not saw_error

                # any status other than None indicates an error
                if runstatus is not None:
                    saw_error = True

                # keep looking
                continue

            if rawmsg["service"] == "HSiface" and \
               rawmsg["varname"] == "alert":
                self.__process_alert(rawmsg["value"])
                continue

            logging.error("Bad service/varname pair \"%s/%s\" for %s" %
                          (rawmsg["service"], rawmsg["varname"], rawmsg))
            continue

    def __process_status(self, value_dict, should_succeed, quiet=False):
        message = HsUtil.dict_to_object(value_dict, self.STATUS_FIELDS,
                                        "LiveMessage")

        if not quiet:
            print "::: Req %s LiveStatus %s" % \
                (message.request_id, message.status)

        if message.status == HsUtil.STATUS_QUEUED:
            if message.request_id in self.__requests:
                logging.error("Found %s message for existing request %s",
                              message.status, message.request_id)
                return self.RUN_ERR_ORDER

            self.__requests[message.request_id] = message
            return None
        elif message.status == HsUtil.STATUS_IN_PROGRESS:
            oldmsg = self.__find_request(message)
            if oldmsg is None:
                return self.RUN_ERR_MISSING

            self.__requests[message.request_id] = message
            if oldmsg.status != HsUtil.STATUS_QUEUED:
                logging.error("Expected request %s status %s, not %s",
                              message.request_id, HsUtil.STATUS_QUEUED,
                              oldmsg.status)
                return self.RUN_ERR_ORDER

            return self.__check_for_changes(oldmsg, message)
        elif message.status == HsUtil.STATUS_FAIL:
            oldmsg = self.__find_request(message)
            if oldmsg is None:
                return self.RUN_ERR_MISSING

            return self.__report_result(oldmsg, message, should_succeed, False)
        elif message.status == HsUtil.STATUS_SUCCESS:
            oldmsg = self.__find_request(message)
            if oldmsg is None:
                return self.RUN_ERR_MISSING

            return self.__report_result(oldmsg, message, should_succeed, True)

        logging.error("Unknown status %s for request %s (%s)",
                      message.status, message.request_id, message)
        return self.RUN_ERR_UNKNOWN

    def __report_result(self, oldmsg, message, expected, actual):
        changed = self.__check_for_changes(oldmsg, message)

        del self.__requests[message.request_id]

        if expected == actual:
            rstr = "succeeded" if expected else "failed (as expected)"
        elif expected:
            rstr = "FAILED!"
        else:
            rstr = "succeeded (but should have FAILED!)"

        logging.info("Request %s %s", message.request_id, rstr)

        if changed is not None:
            return self.RUN_ERR_ERROR

        return self.RUN_ERR_EXPECTED if expected == actual else \
            self.RUN_ERR_ERROR

    def __submit(self, request, quiet=False):
        if not quiet:
            print "::: Submit %s" % str(request)

        try:
            result = request.run(self.__requester, quiet=quiet)
        except:
            if not quiet:
                logging.exception("problem with request %s", request)
            return False

        if not result:
            if not quiet:
                logging.error("%s failed", request)
            return False

        try:
            result = self.__requester.wait_for_response()
        except:
            if not quiet:
                logging.exception("Problem with %s response", request)
            return False

        if not result:
            if not quiet:
                logging.error("%s response failed", request)
            return False

        return True

    def run(self, target, quiet=False):
        if isinstance(target, MultiRequest):
            requests = target.requests
        else:
            requests = (target, )

        for request in requests:
            (user, host, path) \
                = self.__requester.split_rsync_path(request.copydir)
            request.update_copydir(user, host, path)

            self.__clear_destination(path)
            self.__clear_destination(request.spadequeue)
            if not os.path.exists(request.spadequeue):
                os.makedirs(request.spadequeue)

        try:
            for request in requests:
                if not self.__submit(request, quiet=quiet):
                    return False
            for request in requests:
                if not self.__process_responses(request, path, quiet=quiet):
                    return False
            return True
        finally:
            self.__clear_destination(path)
            self.__clear_destination(request.spadequeue)


if __name__ == "__main__":
    import argparse
    import subprocess

    from contextlib import contextmanager


    def build_requests(env, hubs, first_ticks=None):
        if first_ticks is None:
            first_ticks = 123450067960246236L
            # this should be extracted from the file, not hard-coded!
            first_extracted_hit = 123450077960246236L

        last_ticks = first_ticks + 65 * TICKS_PER_SECOND
        hits_per_file = 40

        env.create(first_ticks, last_ticks, hits_per_file)

        start_time = DAQTime(first_ticks + TICKS_PER_SECOND)
        stop_time = DAQTime(first_ticks + 6 * TICKS_PER_SECOND)
        second_start_time = DAQTime(first_ticks + 7 * TICKS_PER_SECOND)
        second_stop_time = DAQTime(first_ticks + 12 * TICKS_PER_SECOND)
        third_start_time = DAQTime(first_ticks + 13 * TICKS_PER_SECOND)
        third_stop_time = DAQTime(first_ticks + 18 * TICKS_PER_SECOND)
        final_stop_time = DAQTime(last_ticks - 1 * TICKS_PER_SECOND)

        extracted_hits_filename = "hits_%d_%d.dat" % \
                                  (first_extracted_hit,
                                   first_extracted_hit + 50000000000L)

        # list of requests
        return (
            Request(env, True, start_time, stop_time, hubs),
            Request(env, True, start_time, final_stop_time, hubs),
            Request(env, True, start_time, stop_time, hubs,
                    prefix=HsPrefix.SNALERT, copydir=env.copydst),
            Request(env, False,
                    DAQTime((first_ticks - 6 * TICKS_PER_SECOND) / 10),
                    DAQTime((first_ticks - 1 * TICKS_PER_SECOND) / 10),
                    hubs, prefix=HsPrefix.SNALERT),
            Request(env, True, start_time, stop_time, hubs,
                    expected_files=(extracted_hits_filename, ),
                     copydir=os.path.join(ROOTDIR, "hese_hs"), extract=True),
            Request(env, True, start_time, stop_time, hubs,
                    expected_files=(extracted_hits_filename, ),
                    prefix=HsPrefix.HESE, copydir=os.path.join(ROOTDIR, "xxx"),
                    extract=True),
            Request(env, True, start_time, stop_time, hubs,
                    expected_files=(extracted_hits_filename, ),
                    request_id="ABC123", prefix=HsPrefix.ANON,
                    copydir=os.path.join(ROOTDIR, "anonymous"),
                    extract=True),
            Request(env, True, start_time, stop_time, hubs,
                    expected_files=(extracted_hits_filename, ),
                    request_id="AliveOrDead", prefix=HsPrefix.LIVE,
                    username="mfrere",
                    copydir=os.path.join(ROOTDIR, "live_and_let_die"),
                    extract=True),
            Request(env, True, start_time, stop_time, hubs,
                    prefix=HsPrefix.HESE, copydir=env.copydst),
            Request(env, True, start_time, stop_time, hubs,
                    prefix=HsPrefix.ANON, copydir=env.copydst),
            Request(env, True, start_time, stop_time, hubs,
                    prefix="UNOFFICIAL", copydir=env.copydst),
            Request(env, True, start_time, stop_time, hubs,
                    prefix=HsPrefix.SNALERT, copydir=env.copydst,
                    send_old=True),
            MultiRequest(
                Request(env, True, start_time, stop_time, hubs, number="???"),
                Request(env, True, second_start_time, second_stop_time, hubs,
                        number="???"),
                Request(env, True, third_start_time, third_stop_time, hubs,
                        number="???"),
            ),
            Request(env, True, start_time, stop_time, hubs,
                    prefix=HsPrefix.SNALERT, copydir=env.copydst,
                    ignored=hubs[1]),
        )

    def find_open_requests():
        num_open = 0

        conn = sqlite3.connect(RequestMonitor.get_db_path())
        try:
            cursor = conn.cursor()
            for row in cursor.execute("select id, count(id) from requests"
                                      " group by id"):
                num_open += 1
        finally:
            conn.close()

        return num_open

    def main():
        argp = argparse.ArgumentParser()
        argp.add_argument("-q", "--quiet", dest="quiet",
                          action="store_true", default=False,
                          help="Print the absolute minimum")
        argp.add_argument("-r", "--request", dest="request_list",
                          type=int, action="append",
                          help="List of specific requests to run")

        args = argp.parse_args()

        # if we're being quiet, disable log message output
        if args.quiet:
            logging.disable(logging.CRITICAL)

        env = HsEnvironment(ROOTDIR)
        hubs = ("ichub01", "ithub11", "ichub82")
        requests = build_requests(env, hubs)

        if args.request_list is not None and len(args.request_list) > 0:
            # extract only the selected requests
            newlist = []
            prev = None
            for num in sorted(args.request_list):
                if num == prev:
                    continue
                found = None
                for req in requests:
                    if req.number == num:
                        found = req
                        break
                if found is None:
                    raise SystemExit("Unknown request #%d (of %d requests)" %
                                     (num, len(requests)))
                newlist.append(found)
                prev = num

            # replace main list with extracted subset
            requests = newlist

        success = True
        with run_and_terminate(("python", "HsPublisher.py",
                                "-l", "/tmp/publish.log",
                                "-T")):
            with run_hubs_and_terminate(hubs, env):
                with run_and_terminate(("python", "HsSender.py",
                                        "-l", "/tmp/sender.log",
                                        "-D", RequestMonitor.STATE_DB_PATH,
                                        "-F",
                                        "-S", env.spadequeue,
                                        "-T")):
                    # give everything a chance to start up
                    time.sleep(5)
                    if not process_requests(requests, quiet=args.quiet):
                        success = False

        return success


    def process_requests(requests, quiet=False):
        processor = Processor(quiet=quiet)

        failed = []

        first = True
        for request in requests:
            if first:
                first = False
            elif not quiet:
                # print a separator so it's easy to see different requests
                print >>sys.stderr, "="*75

            # keep track of request numbers to make debugging easier
            if not quiet:
                print "::: Request #%d" % request.number

            result = None
            try:
                result = processor.run(request, quiet=quiet)
                if not result:
                    failed.append(request.number)
            except:
                if not quiet:
                    logging.exception("Request failed")
                failed.append(request.number)
                result = False

            if quiet:
                sys.stdout.write("." if result else "!")
                sys.stdout.flush()

        if quiet:
            sys.stdout.write("\n")
            sys.stdout.flush()

        open_reqs = find_open_requests()

        rtnval = True
        if len(failed) == 0 and open_reqs == 0:
            print "No problems found"
        else:
            if len(failed) > 0:
                print >>sys.stderr, "Found problems with %d request%s: %s" % \
                    (len(failed), "" if len(failed) == 1 else "s", failed)
                rtnval = False
            if open_reqs > 0 and open_reqs != len(failed):
                print >>sys.stderr, "Found %d open request%s in state DB" % \
                    (open_reqs, "" if open_reqs == 1 else "s")
                rtnval = False

        return rtnval


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

    @contextmanager
    def run_hubs_and_terminate(hubs, env):
        hproc = {}
        try:
            for hub in hubs:
                hproc[hub] = subprocess.Popen(("python", "HsWorker.py",
                                               "-l", "/tmp/%s.log" % hub,
                                               "-C", env.copysrc,
                                               "-H", hub,
                                               "-R", env.hubroot,
                                               "-T"))
            yield hproc
        finally:
            failed = None
            for hub, proc in hproc.iteritems():
                try:
                    proc.terminate() # send sigterm, or ...
                    proc.kill()      # send sigkill
                except:
                    if failed is None:
                        failed = [hub, ]
                    else:
                        failed.append(hub)
            if failed is not None:
                raise HsException("Failed to stop " + ", ".join(failed))


    if not main():
        raise SystemExit(1)
