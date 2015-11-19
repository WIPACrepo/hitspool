#!/usr/bin/env python


import datetime
import os
import sqlite3
import struct
import tempfile
import threading
import zmq


JAN1 = None


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


def get_time(tick, is_sn_ns=False):
    """
    Convert a DAQ tick to a Python `datetime`
    NOTE: this conversion does not include leapseconds!!!
    """
    global JAN1

    if JAN1 is None:
        now = datetime.datetime.utcnow()
        JAN1 = datetime.datetime(now.year, 1, 1)

    ticks_per_sec = 1E10
    if is_sn_ns:
        ticks_per_sec /= 10
    ticks_per_ms = ticks_per_sec / 1000000

    secs = int(tick / ticks_per_sec)
    msecs = int(((tick - secs * ticks_per_sec) + (ticks_per_ms / 2)) /
                ticks_per_ms)
    return JAN1 + datetime.timedelta(seconds=secs, microseconds=msecs)


class Mock0MQSocket(object):
    def __init__(self, name):
        self.__name = name
        self.__outqueue = []
        self.__expected = []
        self.__answer = {}
        self.__pollresult = []

    def addExpected(self, jdict, answer=None):
        self.__expected.append(jdict)
        if answer is not None:
            self.__answer[jdict] = answer

    def addPollResult(self, source, polltype=zmq.POLLIN):
        self.__pollresult.append([(source, polltype)])

    def addIncoming(self, msg):
        self.__outqueue.append(msg)

    def close(self):
        pass

    @property
    def has_input(self):
        return len(self.__outqueue) > 0

    def poll(self, _):
        if len(self.__pollresult) == 0:
            raise Exception("No poll results")

        return self.__pollresult.pop(0)

    def recv(self):
        if len(self.__outqueue) == 0:
            raise zmq.ZMQError("Incoming message queue is empty")
        return self.__outqueue.pop(0)

    def recv_json(self):
        return self.recv()

    def send(self, msgstr):
        if len(self.__expected) == 0:
            raise Exception("Unexpected %s message: %s" %
                            (self.__name, msgstr))

        expmsg = self.__expected.pop(0)

        if expmsg != msgstr:
            raise Exception("Expected \"%s\" not \"%s\"" % (expmsg, msgstr))

        if self.__answer.has_key(expmsg):
            resp = self.__answer[expmsg]
            del self.__answer[expmsg]
            return resp

    def send_json(self, json):
        if len(self.__expected) == 0:
            raise Exception("Unexpected %s JSON message: %s" %
                            (self.__name, json))

        jexp = self.__expected.pop(0)

        extra = {}
        badval = {}
        for key, val in json.iteritems():
            if key == "t":
                # ignore times
                continue

            if isinstance(val, unicode):
                val = val.encode('ascii', 'ignore')
            if not jexp.has_key(key):
                extra[key] = val
            else:
                expval = jexp[key]
                if isinstance(expval, unicode):
                    expval = expval.encode('ascii', 'ignore')

                if isinstance(expval, dict) and isinstance(val, dict):
                    badstr = None
                    for xkey in expval:
                        if val[xkey] != expval[xkey]:
                            if badstr is None:
                                badstr = ""
                            else:
                                badstr += ", "
                            badstr += "[%s] %s<%s> != expected %s<%s>" % \
                                      (xkey, val[xkey], type(val[xkey]),
                                       expval[xkey], type(expval[xkey]))
                    if badstr is not None:
                        badval[key] = badstr
                elif isinstance(val, type(expval)):
                    if expval != val:
                        badval[key] = "'%s' != expected '%s'" % (val, expval)
                else:
                    try:
                        if expval.match(val) is None:
                            badval[key] = "'%s' does not match '%s'" % \
                                          (val, expval.pattern)
                    except:
                        badval[key] = "%s<%s> is not expected %s<%s>" % \
                                      (val, type(val), expval, type(expval))

                del jexp[key]

        errstr = None
        if len(jexp) > 0:
            if errstr is None:
                errstr = ""
            else:
                errstr += ","
            errstr += " missing " + str(jexp)
        if len(badval) > 0:
            if errstr is None:
                errstr = ""
            else:
                errstr += ","
            errstr += " bad values " + str(badval)
        if len(extra) > 0:
            if errstr is None:
                errstr = ""
            else:
                errstr += ","
            errstr += " extra values " + str(extra)

        if errstr is not None:
            raise Exception(self.__name + " JSON has" + errstr)

    def validate(self):
        if len(self.__outqueue) > 0:
            verb = " was" if len(self.__outqueue) == 1 else "s were"
            raise Exception("%s message%s not received (%s)" %
                            (len(self.__outqueue), verb, self.__outqueue))
        if len(self.__expected) > 0:
            plural = "" if len(self.__expected) == 1 else "s"
            raise Exception("Expected %d %s JSON message%s: %s" %
                            (len(self.__expected), self.__name, plural,
                             self.__expected))
        if len(self.__pollresult) > 0:
            verb = " was" if len(self.__pollresult) == 1 else "s were"
            raise Exception("%s message%s not received (%s)" %
                            (len(self.__pollresult), verb, self.__pollresult))


class MockHitspool(object):
    COPY_DIR = None
    HUB_DIR = None
    LOCK = threading.Lock()

    @classmethod
    def create(cls, hsr, subdir, t0, t_cur, interval, max_f=1000,
               make_bad=False, debug=False):
        with cls.LOCK:
            if cls.HUB_DIR is None:
                # create temporary hub directory and set in HsRSyncFiles
                cls.HUB_DIR = tempfile.mkdtemp(prefix="HubDir_")
                hsr.TEST_HUB_DIR = cls.HUB_DIR

        # create subdir if necessary
        path = os.path.join(cls.HUB_DIR, subdir)
        if not os.path.exists(path):
            os.makedirs(path)

        # compute "current file"
        cur_f = int((t_cur - t0) / interval)

        # create info.txt
        infopath = os.path.join(path, "info.txt")
        with open(infopath, "w") as fout:
            if not make_bad:
                print >>fout, "T0 %d" % t0
            print >>fout, "CURT %d" % t_cur
            print >>fout, "IVAL %d" % interval
            print >>fout, "CURF %d" % cur_f
            print >>fout, "MAXF %d" % max_f

        if debug:
            print >>sys.stderr, "=== %s" % infopath
            print >>sys.stderr, "=== start %d :: %s" % \
                (t0, HsTestUtil.get_time(t0))
            print >>sys.stderr, "=== stop  %d :: %s" % \
                (t_cur, HsTestUtil.get_time(t_cur))
            with open(infopath, "r") as fin:
                for line in fin:
                    print >>sys.stderr, line,

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
    def __init__(self, varname):
        super(MockI3Socket, self).__init__("I3Socket")
        self.__service = "HSiface"
        self.__varname = varname

    def addExpectedAlert(self, value, prio=1):
        edict = {'service': self.__service,
                 'varname': "alert",
                 'value': value}
        if prio is not None:
            edict['prio'] = prio
        self.addExpected(edict)

    def addExpectedValue(self, value, prio=None):
        edict = {'service': self.__service,
                 'varname': self.__varname,
                 'value': value}
        if prio is not None:
            edict['prio'] = prio
        self.addExpected(edict)


class RunParam(object):
    def __init__(self, start, stop, interval, max_files, make_bad=False):
        self.__start = start
        self.__stop = stop
        self.__interval = interval
        self.__max_files = max_files
        self.__make_bad = make_bad

    def interval(self):
        return self.__interval

    def make_bad(self):
        return self.__make_bad

    def max_files(self):
        return self.__max_files

    def start(self):
        return self.__start

    def stop(self):
        return self.__stop

    def set_interval(self, val):
        self.__interval = val

    def set_make_bad(self, val):
        self.__make_bad = val


class HsTestRunner(object):
    TICKS_PER_SECOND = 10000000000
    INTERVAL = 15 * TICKS_PER_SECOND

    MAX_FILES = 1000

    HUB_DIR = None
    INFODB_PATH = None

    #JAN1 = None

    def __init__(self, hsr, last_start, last_stop, cur_start, cur_stop,
                 interval):
        self.__hsr = hsr
        self.__check_links = False

        self.__last_run = RunParam(last_start, last_stop, interval,
                                   self.MAX_FILES)
        self.__cur_run = RunParam(cur_start, cur_stop, interval, self.MAX_FILES)

    @classmethod
    def __create_info_db(cls, path):
        # create info.db
        infopath = os.path.join(path, "hitspool.db")
        cls.INFODB_PATH = infopath

        conn = sqlite3.connect(infopath)

        cursor = conn.cursor()
        cursor.execute("create table if not exists hitspool("
                       "filename text primary key not null," +
                       "start_tick integer, stop_tick integer)")
        conn.commit()

    @classmethod
    def __create_info_txt(cls, path, t0, t_cur, interval, cur_f, max_f,
                          make_bad=False, debug=False):
        # create info.txt
        infopath = os.path.join(path, "info.txt")
        with open(infopath, "w") as fout:
            if not make_bad:
                print >>fout, "T0 %d" % t0
            print >>fout, "CURT %d" % t_cur
            print >>fout, "IVAL %d" % interval
            print >>fout, "CURF %d" % cur_f
            print >>fout, "MAXF %d" % max_f

        if debug:
            import sys
            print >>sys.stderr, "=== %s" % infopath
            print >>sys.stderr, "=== start %d :: %s" % \
                (t0, get_time(t0))
            print >>sys.stderr, "=== stop  %d :: %s" % \
                (t_cur, get_time(t_cur))
            with open(infopath, "r") as fin:
                for line in fin:
                    print >>sys.stderr, line,

    def __populate_one(self, hubdir, subdir, rundata, use_db=False,
                       debug=False):
        t0 = rundata.start()
        t_cur = rundata.stop()
        interval = rundata.interval()
        max_f = rundata.max_files()
        make_bad = rundata.make_bad()

        # create subdir if necessary
        path = os.path.join(hubdir, subdir)
        if not os.path.exists(path):
            os.makedirs(path)

        # compute "current file"
        cur_f = int((t_cur - t0) / interval)

        if use_db:
            self.__create_info_db(path)
        else:
            self.__create_info_txt(path, t0, t_cur, interval, cur_f, max_f,
                                   make_bad=make_bad, debug=debug)

    def add_expected_files(self, alert_start, alert_stop, run_start, run_stop,
                           interval, destdir=None, fail_links=False,
                           fail_extract=False, use_db=False):
        if not use_db:
            raise Exception("Non-DB tests should use add_expected_links")

        max_files = self.MAX_FILES

        if self.INFODB_PATH is None:
            raise Exception("Info DB has not been created")
        conn = sqlite3.connect(self.INFODB_PATH)
        cursor = conn.cursor()

        firstfile = None
        numfiles = None

        # if extract should fail, all hits will be later than expected
        if not fail_extract:
            offset = 0
        else:
            offset = (alert_stop - alert_start) * 100

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

            if destdir is not None:
                # how many hits should the file contain?
                num_hits_per_file = 20
                tick_ival = interval / num_hits_per_file
                if tick_ival == 0:
                    tick_ival = 1

                # create the file and fill it with hits
                create_hits(os.path.join(destdir, filename),
                            start_tick + offset, stop_tick + offset, tick_ival)

        conn.commit()

        if firstfile is not None and destdir is None and not fail_links:
            utc = get_time(alert_start)
            self.__hsr.add_expected_links(utc, "hitspool", firstfile, numfiles)

        self.__check_links = True

    def add_expected_links(self, start_ticks, subdir, firstfile, numfiles,
                           use_db=False):
        if use_db:
            raise Exception("DB tests should use add_expected_files")

        utc = get_time(start_ticks)
        self.__hsr.add_expected_links(utc, subdir, firstfile, numfiles)

        self.__check_links = True

    def make_bad_current(self):
        self.__cur_run.set_make_bad(True)

    def make_bad_last(self):
        self.__last_run.set_make_bad(True)

    def populate(self, testobj, use_db=False):
        if testobj.HUB_DIR is None:
            # create temporary hub directory and set in HsRSyncFiles
            testobj.HUB_DIR = tempfile.mkdtemp()
            self.__hsr.TEST_HUB_DIR = testobj.HUB_DIR

        for is_last in (False, True):
            if use_db:
                hdir = 'hitspool'
            elif is_last:
                hdir = 'lastRun'
            else:
                hdir = 'currentRun'

            if is_last:
                rundata = self.__last_run
            else:
                rundata = self.__cur_run

            self.__populate_one(testobj.HUB_DIR, hdir, rundata, use_db=use_db)

    def run(self, start_ticks, stop_ticks, copydir="me@host:/a/b/c",
            extract_hits=False):
        if start_ticks is None:
            start_time = None
        else:
            start_time = get_time(start_ticks)
        if stop_ticks is None:
            stop_time = None
        else:
            stop_time = get_time(stop_ticks)

        self.__hsr.request_parser(start_time, stop_time, copydir,
                                  extract_hits=extract_hits, sleep_secs=0)

        if self.__check_links:
            self.__hsr.check_for_unused_links()

    def set_current_interval(self, interval):
        self.__cur_run.set_interval(interval)

    def set_last_interval(self, interval):
        self.__last_run.set_interval(interval)
