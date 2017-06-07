#!/usr/bin/env python


import datetime
import json
import os
import re
import shutil
import sqlite3
import struct
import tempfile
import threading
import zmq

from HsBase import DAQTime
from HsRSyncFiles import HsRSyncFiles
from RequestMonitor import RequestMonitor


# January 1 of this year
JAN1 = None
# DAQ ticks per second (0.1 ns)
TICKS_PER_SECOND = 10000000000
# match Python date/time string
TIME_PAT = re.compile(r"\d+-\d+-\d+ +\d+:\d+:\d+(.\d+)?")
# location of test-only version of RequestMonitor state database
TEMP_STATE_DB = None


def create_hits(filename, start_tick, stop_tick, interval):
    hit_type = 3
    hit_len = 54
    ignored = 0L
    mbid = 0x1234567890123

    filler = [i for i in range((hit_len - 32) / 2)]
    # byte-order word must be 1
    filler[0] = 1

    fout = open(filename, "wb")
    try:
        tick = start_tick
        while tick <= stop_tick:
            buf = struct.pack(">IIQQQ%sH" % len(filler), hit_len, hit_type,
                              ignored, mbid, tick, *filler)
            fout.write(buf)
            if tick == stop_tick:
                break
            tick += interval
            if tick > stop_tick:
                tick = stop_tick
    finally:
        fout.close()


def create_hitspool_db(spooldir):
    # create database
    dbpath = os.path.join(spooldir, HsRSyncFiles.DEFAULT_SPOOL_DB)

    conn = sqlite3.connect(dbpath)
    try:
        cursor = conn.cursor()
        cursor.execute("create table if not exists hitspool("
                       "filename text primary key not null," +
                       "start_tick integer, stop_tick integer)")
        conn.commit()
    finally:
        conn.close()

    return dbpath


def dump_dir(path, title=None, indent=""):
    import sys
    if title is not None:
        print >>sys.stderr, "%s=== %s" % (indent, title)
    if path is None:
        print >>sys.stderr, "%s(path is None)" % indent
    elif not os.path.exists(path):
        print >>sys.stderr, "%s%s (does not exist)" % (indent, path)
    else:
        for entry in os.listdir(path):
            full = os.path.join(path, entry)
            if not os.path.isdir(full):
                print >>sys.stderr, "%s%s" % (indent, entry)
            else:
                print >>sys.stderr, "%s%s/" % (indent, entry)
                dump_dir(full, indent=indent + "  ")


def get_time(tick, is_ns=False):
    """
    Convert a DAQ tick to a Python `datetime`
    NOTE: this conversion does not include leapseconds!!!
    """
    global TICKS_PER_SECOND

    ticks_per_sec = TICKS_PER_SECOND
    if is_ns:
        ticks_per_sec /= 10
    ticks_per_ms = ticks_per_sec / 1000000

    secs = int(tick / ticks_per_sec)
    msecs = int(((tick - secs * ticks_per_sec) + (ticks_per_ms / 2)) /
                ticks_per_ms)
    return jan1() + datetime.timedelta(seconds=secs, microseconds=msecs)


def jan1():
    """
    Date/time info for January 1 00:00:00 of this year
    """
    global JAN1

    if JAN1 is None:
        now = datetime.datetime.utcnow()
        JAN1 = datetime.datetime(now.year, 1, 1)

    return JAN1

def set_state_db_path():
    "Point RequestMonitor at a temporary state DB for testing"

    global TEMP_STATE_DB

    if TEMP_STATE_DB is None:
        tmptuple = tempfile.mkstemp(suffix="db", prefix="hsstate")
        os.close(tmptuple[0])
        TEMP_STATE_DB = tmptuple[1]

    if RequestMonitor.STATE_DB_PATH is None:
        RequestMonitor.STATE_DB_PATH = TEMP_STATE_DB
    elif RequestMonitor.STATE_DB_PATH != TEMP_STATE_DB:
        raise ValueError("Request state database is already set to \"%s\"" %
                         (RequestMonitor.STATE_DB_PATH, ))


def update_hitspool_db(spooldir, alert_start, alert_stop, run_start, run_stop,
                       interval, max_files=1000, offset=0, create_files=False):
    """
    Create entries in the hitspool database based on the supplied times.
    If "create_files" is True, create hit files as well.
    Return the first file number and the total number of files.
    """

    dbpath = os.path.join(spooldir, HsRSyncFiles.DEFAULT_SPOOL_DB)

    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()

    firstfile = None
    numfiles = None

    first_time = int(run_start / interval) * interval
    for tick in range(first_time, run_stop, interval):
        ival_num = int(tick / interval)
        filenum = ival_num % max_files
        start_tick = ival_num * interval
        stop_tick = start_tick + (interval - 1)

        if stop_tick >= alert_start and start_tick <= alert_stop:
            if firstfile is None:
                firstfile = filenum
                numfiles = 1
                # first interval can be a partial one
                start_tick = first_time
            else:
                numfiles += 1

        filename = "HitSpool-%d.dat" % filenum
        cursor.execute("insert or replace"
                       " into hitspool(filename, start_tick, stop_tick)"
                       " values (?,?,?)", (filename, start_tick, stop_tick))

        if create_files:
            # how many hits should the file contain?
            num_hits_per_file = 20
            tick_ival = interval / num_hits_per_file
            if tick_ival == 0:
                tick_ival = 1

            # create the file and fill it with hits
            hitfile = os.path.join(spooldir, filename)
            create_hits(hitfile, start_tick + offset, stop_tick + offset,
                        tick_ival)

    conn.commit()

    return (firstfile, numfiles)


class CompareException(Exception):
    pass


class CompareObjects(object):
    def __init__(self, name, obj, exp):
        self.__compare_objects(name, obj, exp)

    def __compare_dicts(self, name, jmsg, jexp):
        if not isinstance(jmsg, dict):
            raise CompareException("%sExpected dict %s<%s>, not %s<%s>" %
                                   (self.__namestr(name), jexp, type(jexp),
                                    jmsg, type(jmsg)))

        extra = {}
        badval = {}
        for key, val in jmsg.iteritems():
            val = self.__unicode_to_ascii(val)

            if key not in jexp:
                extra[key] = val
            else:
                expval = self.__unicode_to_ascii(jexp[key])

                try:
                    self.__compare_objects(None, val, expval)
                except CompareException, ce:
                    badval[key] = str(ce)

                del jexp[key]

        errstr = None
        for pair in ((jexp, "missing"), (badval, "bad values"),
                     (extra, "extra values")):
            if len(pair[0]) > 0:
                if errstr is None:
                    errstr = self.__namestr(name)
                errstr += "\n\t"
                errstr += "%s %s" % (pair[1], pair[0])

        debug = False
        if errstr is not None:
            if debug: print "*** Error: %s" % errstr
            raise CompareException("Message %s: %s" % (jmsg, errstr))
        else:
            if debug: print "+++ Valid message"

    def __compare_lists(self, name, obj, exp):
        if not isinstance(obj, list):
            raise CompareException("%sExpected list %s<%s>, not %s<%s>" %
                                   (self.__namestr(name), exp, type(exp), obj,
                                    type(obj)))

        if len(obj) != len(exp):
            raise CompareException("%sExpected %d list entries,"
                                   " not %d in %s" %
                                   (self.__namestr(name), len(exp), len(obj),
                                    obj))

        for i in range(len(obj)):
            try:
                self.__compare_objects(name, obj[i], exp[i])
            except CompareException, ce:
                raise CompareException("%slist#%d: %s" %
                                       (self.__namestr(name), i, ce))

    def __compare_objects(self, name, obj, exp):
        if isinstance(obj, list) and isinstance(exp, list):
            self.__compare_lists(name, obj[:], exp[:])
        elif isinstance(obj, dict) and isinstance(exp, dict):
            self.__compare_dicts(name, obj.copy(), exp.copy())
        elif hasattr(exp, 'flags') and hasattr(exp, 'pattern'):
            # try to match regular expression
            if exp.match(str(obj)) is None:
                raise CompareException("%s'%s' does not match '%s'" %
                                       (self.__namestr(name), obj,
                                        exp.pattern))

        else:
            expstr = self.__unicode_to_ascii(exp)
            objstr = self.__unicode_to_ascii(obj)
            if isinstance(objstr, type(expstr)):
                if objstr != expstr:
                    raise CompareException("%sExpected str \"%s\"<%s>, not"
                                           " \"%s\"<%s>" %
                                           (self.__namestr(name), expstr,
                                            type(exp), objstr, type(obj)))
            else:
                raise CompareException("%sExpected obj %s<%s> not %s<%s>" %
                                       (self.__namestr(name), exp, type(exp),
                                        obj, type(obj)))

    @classmethod
    def __namestr(cls, name):
        if name is None:
            return ""

        return "%s: " % name

    @classmethod
    def __unicode_to_ascii(cls, xstr):
        if isinstance(xstr, unicode):
            return xstr.encode('ascii', 'ignore')

        return xstr


class MockPollableSocket(object):
    @property
    def has_input(self):
        raise NotImplementedError()


class Mock0MQPoller(object):
    def __init__(self, name, verbose=False):
        self.__name = name
        self.__socks = {}
        self.__pollresult = []

    def addPollResult(self, source, polltype=zmq.POLLIN):
        self.__pollresult.append([(source, polltype)])

    def close(self):
        pass

    @property
    def is_done(self):
        return len(self.__pollresult) > 0

    def poll(self, timeout=None):
        if len(self.__pollresult) != 0:
            return self.__pollresult.pop(0)

        if len(self.__socks) == 0:
            raise Exception("No poll results")

        ready = []
        for sock, event in self.__socks.items():
            if isinstance(sock, zmq.Socket):
                if not sock.poll(0.01, event):
                    continue
            elif isinstance(sock, MockPollableSocket):
                if event != zmq.POLLIN:
                    raise Exception("Not handling POLLOUT for %s<%s>" %
                                    (sock, type(sock).__name__))
                if not sock.has_input:
                    continue
            else:
                raise Exception("Not handling %s<%s>" %
                                (sock, type(sock).__name__))

            # add socket with pending I/O
            ready.append((sock, event))

        return ready

    def register(self, sock, event):
        if sock in self.__socks:
            raise Exception("Socket %s<%s> is already registered" %
                            (sock, type(sock).__name__))

        self.__socks[sock] = event

    def validate(self):
        if len(self.__pollresult) > 0:
            verb = " was" if len(self.__pollresult) == 1 else "s were"
            raise Exception("%s %s message%s not received (%s)" %
                            (len(self.__pollresult), self.__name, verb,
                             self.__pollresult))
        return True


class Mock0MQSocket(MockPollableSocket):
    def __init__(self, name, verbose=False):
        self.__name = name
        self.__outqueue = []
        self.__expected = []
        self.__answer = {}
        self.__verbose = verbose

    def __str__(self):
        vstr = ", verbose" if self.__verbose else ""
        xstr = "" if len(self.__expected) == 0 else \
               ", %d expected" % len(self.__expected)
        return "%s(%s%s%s)" % \
            (type(self).__name__, self.__name, vstr, xstr)

    def addExpected(self, jdict, answer=None):
        self.__expected.append(jdict)
        if answer is not None:
            self.__answer[jdict] = answer

    def addIncoming(self, msg):
        self.__outqueue.append(msg)

    def close(self):
        pass

    @property
    def has_input(self):
        return len(self.__outqueue) > 0

    @property
    def identity(self):
        return self.__name

    @property
    def is_done(self):
        return len(self.__outqueue) > 0 or \
            len(self.__expected) > 0 or \
            len(self.__answer) > 0

    @property
    def num_expected(self):
        return len(self.__expected)

    def recv(self):
        if len(self.__outqueue) == 0:
            raise zmq.ZMQError("Incoming message queue is empty")

        msg = self.__outqueue.pop(0)

        if self.__verbose:
            print "%s(%s) -> %s" % (type(self).__name__, self.__name, str(msg))

        return msg

    def recv_json(self):
        return self.recv()

    def send(self, msgstr):
        try:
            msgjson = json.loads(msgstr)
        except:
            msgjson = msgstr

        return self.send_json(msgjson)

    def send_json(self, msgjson):
        if len(self.__expected) == 0:
            raise Exception("Unexpected %s message: %s" %
                            (self.__name, msgjson))

        found = None
        for i in range(len(self.__expected)):
            expjson = self.__expected[i]
            try:
                CompareObjects(self.__name, msgjson, expjson)
                found = i
                break
            except:
                continue

        # if the message was unknown, throw a CompareException
        if found is None:
            xstr = "\n\t(exp %s)" % str(self.__expected[0])
            raise CompareException("Unexpected %s(%s) message (of %d): %s%s" %
                                   (type(self).__name__, self.__name,
                                    len(self.__expected), msgjson, xstr))

        # we received an expected message, delete it
        expjson = self.__expected[found]
        del self.__expected[found]

        if self.__verbose:
            print "%s(%s) <- %s (exp %s)" % \
                (type(self).__name__, self.__name, str(msgjson), str(expjson))
            print "%s(%s) expect %d more message%s" % \
                (type(self).__name__, self.__name, len(self.__expected),
                 "s" if len(self.__expected) != 1 else "")

        expkey = str(expjson)
        if expkey in self.__answer:
            resp = self.__answer[expkey]
            del self.__answer[expkey]
            return resp

        return None

    def set_verbose(self, value=True):
        self.__verbose = (value is True)

    def validate(self):
        if len(self.__outqueue) > 0:
            verb = " was" if len(self.__outqueue) == 1 else "s were"
            raise Exception("%s %s message%s not received (%s)" %
                            (len(self.__outqueue), self.__name, verb,
                             self.__outqueue))
        if len(self.__expected) > 0:
            plural = "" if len(self.__expected) == 1 else "s"
            raise Exception("Expected %d %s JSON message%s: %s" %
                            (len(self.__expected), self.__name, plural,
                             self.__expected))
        return True


class MockHitspool(object):
    COPY_DIR = None
    HUB_DIR = None
    LOCK = threading.Lock()
    # default maximum number of hitspool files
    MAX_FILES = 1000

    @classmethod
    def create(cls, hsr, subdir=HsRSyncFiles.DEFAULT_SPOOL_NAME):
        with cls.LOCK:
            if cls.HUB_DIR is None:
                # create temporary hub directory and set in HsRSyncFiles
                cls.HUB_DIR = tempfile.mkdtemp(prefix="HubDir_")
                hsr.TEST_HUB_DIR = cls.HUB_DIR

        # create subdir if necessary
        hspath = os.path.join(cls.HUB_DIR, subdir)
        if not os.path.exists(hspath):
            os.makedirs(hspath)

        return hspath

    @classmethod
    def add_files(cls, hspath, t0, t_cur, interval, max_f=None,
                  create_files=True, debug=False):
        if max_f is None:
            max_f = cls.MAX_FILES

        run_start = t0
        run_stop = t_cur
        create_hitspool_db(hspath)
        update_hitspool_db(hspath, t0, t_cur, run_start, run_stop, interval,
                           max_files=max_f, offset=0,
                           create_files=create_files)

        return hspath

    @classmethod
    def create_copy_dir(cls, hsr=None, suffix="_HsDataCopy"):
        with cls.LOCK:
            if cls.COPY_DIR is None:
                # create temporary copy directory and set in HsWorker
                cls.COPY_DIR = tempfile.mkdtemp(suffix=suffix)
                if hsr is not None:
                    # set HsRSyncFiles.TEST_COPY_DIR
                    hsr.TEST_COPY_DIR = cls.COPY_DIR

    @classmethod
    def create_copy_files(cls, prefix, timetag, host, startnum, numfiles,
                          real_stuff=False):
        """create copy directory and fill with fake hitspool files"""
        cls.create_copy_dir()

        # create copy directory
        path = os.path.join(cls.COPY_DIR, "%s_%s_%s" % (prefix, timetag, host))

        # if caller wants actual directory and files, create them
        if real_stuff:
            if not os.path.exists(path):
                os.makedirs(path)

            # create all fake hitspool files
            for num in xrange(startnum, startnum + numfiles):
                fpath = os.path.join(path, "HitSpool-%d" % num)
                with open(fpath, "w") as fout:
                    print >>fout, "Fake#%d" % num

        return path

    @classmethod
    def destroy(cls):
        with cls.LOCK:
            if cls.HUB_DIR is not None:
                # clear lingering files
                try:
                    shutil.rmtree(cls.HUB_DIR)
                except:
                    pass
                cls.HUB_DIR = None
            if cls.COPY_DIR is not None:
                # clear lingering files
                try:
                    shutil.rmtree(cls.COPY_DIR)
                except:
                    pass
                cls.COPY_DIR = None


class MockI3Socket(Mock0MQSocket):
    def __init__(self, varname, verbose=False):
        super(MockI3Socket, self).__init__(varname, verbose=verbose)
        self.__service = "HSiface"
        self.__varname = varname

    def addDebugEMail(self, host=r".*"):
        header = re.compile(r"Query for \[\d+-\d+\] failed on " +
                                host + "$")
        message = re.compile(r"DB contains \d+ entries from .* to .*$")

        self.addGenericEMail(HsRSyncFiles.DEBUG_EMAIL, header, message)

    def addGenericEMail(self, address_list, header, message,
                        description="HsInterface Data Request",
                        prio=2, short_subject=True, quiet=True):
        notifies = []
        for email in address_list:
            notifies.append({
                "receiver": email,
                "notifies_txt": message,
                "notifies_header": header,
            })

        self.addExpectedAlert({
            "condition": header,
            "desc": description,
            "notifies": notifies,
            "short_subject": "true" if short_subject else "false",
            "quiet": "true" if quiet else "false",
        }, prio=prio)

    def addExpectedAlert(self, value, prio=1):
        self.addExpectedMessage(value, service=self.__service,
                                varname="alert", prio=prio, time=TIME_PAT)

    def addExpectedMessage(self, value, service=None, varname=None, prio=None,
                           t=None, time=None):
        if service is None:
            service = self.__service
        if varname is None:
            varname = self.__varname
        edict = {
            'service': service,
            'varname': varname,
            'value': value,
        }

        if prio is not None:
            edict['prio'] = prio
        if t is not None:
            edict['t'] = t
        if time is not None:
            edict['time'] = time

        self.addExpected(edict)

    def addExpectedValue(self, value, prio=None):
        self.addExpectedMessage(value, service=self.__service,
                                varname=self.__varname, prio=prio)


class RunParam(object):
    def __init__(self, start, stop, interval, max_files):
        self.__start = start
        self.__stop = stop
        self.__interval = interval
        self.__max_files = max_files

    def interval(self):
        return self.__interval

    def max_files(self):
        return self.__max_files

    def start(self):
        return self.__start

    def stop(self):
        return self.__stop

    def set_interval(self, val):
        self.__interval = val


class HsTestRunner(object):
    global TICKS_PER_SECOND

    INTERVAL = 15 * TICKS_PER_SECOND

    MAX_FILES = 1000

    SPOOL_PATH = None

    def __init__(self, hsr, last_start, last_stop, cur_start, cur_stop,
                 interval):
        self.__hsr = hsr
        self.__check_links = False

        self.__last_run = RunParam(last_start, last_stop, interval,
                                   self.MAX_FILES)
        self.__cur_run = RunParam(cur_start, cur_stop, interval,
                                  self.MAX_FILES)

    def add_expected_files(self, alert_start, firstfile, numfiles,
                           destdir=None, fail_links=False):
        if firstfile is not None and destdir is None and not fail_links:
            utc = get_time(alert_start)
            self.__hsr.add_expected_links(utc, HsRSyncFiles.DEFAULT_SPOOL_NAME,
                                          firstfile, numfiles)

        self.__check_links = True

    def populate(self, testobj):
        if testobj.HUB_DIR is None:
            # create temporary hub directory and set in HsRSyncFiles
            testobj.HUB_DIR = tempfile.mkdtemp(prefix="HubDir_")
            self.__hsr.TEST_HUB_DIR = testobj.HUB_DIR

        # create subdir if necessary
        path = os.path.join(testobj.HUB_DIR,
                            HsRSyncFiles.DEFAULT_SPOOL_NAME)
        if not os.path.exists(path):
            os.makedirs(path)

        # create the database if necessary
        create_hitspool_db(path)
        self.SPOOL_PATH = path

    def run(self, start_ticks, stop_ticks, copydir="me@host:/a/b/c",
            extract_hits=False):
        self.__hsr.request_parser(None, DAQTime(start_ticks),
                                  DAQTime(stop_ticks), copydir,
                                  extract_hits=extract_hits, delay_rsync=False)

        if self.__check_links:
            self.__hsr.check_for_unused_links()

    def set_current_interval(self, interval):
        self.__cur_run.set_interval(interval)

    def set_last_interval(self, interval):
        self.__last_run.set_interval(interval)

    def update_hitspool_db(self, alert_start, alert_stop, run_start, run_stop,
                           interval, create_files=False, offset=0):
        if self.SPOOL_PATH is None:
            raise Exception("Info DB has not been created")

        return update_hitspool_db(self.SPOOL_PATH, alert_start, alert_stop,
                                  run_start, run_stop, interval,
                                  max_files=self.MAX_FILES,
                                  create_files=create_files,
                                  offset=offset)
