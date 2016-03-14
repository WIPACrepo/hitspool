#!/usr/bin/env python


import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
import zmq

from datetime import datetime, timedelta

import HsBase
import HsUtil

from HsConstants import I3LIVE_PORT, PUBLISHER_PORT, SENDER_PORT
from HsException import HsException
from HsPrefix import HsPrefix
from payload import PayloadReader


class RunInfoException(Exception):
    pass


class RunInfo(object):
    # pylint: disable=too-many-instance-attributes
    # class holds a bunch of info from a single file

    """
    Holder for info.txt information
    """

    JAN1 = None

    def __init__(self, hs_sourcedir, rundir, sleep_secs=4):
        # try max 10 times to parse info.txt to dictionary
        retries_max = 10

        filename = os.path.join(hs_sourcedir, rundir, 'info.txt')

        parsed = False
        for _ in range(1, retries_max):
            try:
                fin = open(filename, "r")
                logging.info("read %s", filename)
            except IOError:
                time.sleep(sleep_secs)
            else:
                infodict = {}
                for line in fin:
                    (key, val) = line.split()
                    infodict[str(key)] = int(val)
                fin.close()

                try:
                    startrun = int(infodict['T0'])
                    cur_time = infodict['CURT']
                    interval = infodict['IVAL']
                    self.__ival_sec = interval * 1.0E-10
                    self.__cur_file = infodict['CURF']
                    self.__max_files = infodict['MAXF']
                    parsed = True
                    break
                except KeyError:
                    time.sleep(sleep_secs)

        if not parsed:
            raise RunInfoException("info.txt reading/parsing failed")

        # how long already writing to current file, time in DAQ units
        total_data = int((cur_time - startrun) /
                         (self.__max_files * interval))
        if total_data == 0:
            # file index of oldest in buffer existing file
            self.__oldest_file = 0
            startdata = startrun
        else:
            # oldest, in hit spool buffer existing time stamp in DAQ units
            self.__oldest_file = self.__cur_file + 1
            tfile = (cur_time - startrun) % interval
            startdata = int(cur_time - ((self.__max_files - 1) *
                                        interval) - tfile)

        # convert the times into datetime objects
        # PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        # XXX should use leapsecond to get time
        self.__buffstart = self.jan1() + timedelta(seconds=startdata*1.0E-10)
        self.__buffstop = self.jan1() + \
            timedelta(seconds=cur_time*1.0E-10)

    @property
    def buffer_start_utc(self):
        return self.__buffstart

    @property
    def buffer_stop_utc(self):
        return self.__buffstop

    @property
    def cur_file(self):
        return self.__cur_file

    def file_num(self, alert_time):
        """
        Determine the number of the file holding `alert_time`
        """
        delta = alert_time - self.__buffstart
        seconds = delta.seconds + delta.days * 24 * 3600
        numfiles = seconds / self.__ival_sec
        return int((numfiles + self.__oldest_file) % self.__max_files)

    @classmethod
    def jan1(cls):
        if cls.JAN1 is None:
            utc_now = datetime.utcnow()
            cls.JAN1 = datetime(utc_now.year, 1, 1)

        return cls.JAN1

    @property
    def max_files(self):
        return self.__max_files

    @property
    def oldest_file(self):
        return self.__oldest_file


class HsRSyncFiles(HsBase.HsBase):
    # location of hub directory used for testing HsInterface
    TEST_HUB_DIR = "/home/david/TESTCLUSTER/testhub"
    # number of bytes in a megabyte
    BYTES_PER_MB = 1024.0 * 1024.0
    # regular expression used to get the number of bytes sent by rsync
    RSYNC_TOTAL_PAT = re.compile(r'(?<=total size is )\d+')

    # cached datetime instance representing the first second of the year
    JAN1 = None

    # true if request/DB details should be emailed after no records are found
    DEBUG_EMPTY = True

    def __init__(self, host=None, is_test=False):
        super(HsRSyncFiles, self).__init__(host=host, is_test=is_test)

        if self.is_cluster_sps or self.is_cluster_spts:
            expcont = "expcont"
            sec_bldr = "2ndbuild"
        else:
            expcont = "localhost"
            sec_bldr = "localhost"

        self.__context = zmq.Context()
        self.__sender = self.create_sender_socket(sec_bldr)
        self.__i3socket = self.create_i3socket(expcont)
        self.__subscriber = self.create_subscriber_socket(expcont)

    @staticmethod
    def __build_file_list(src_dir, spoolname, firstnum, lastnum, maxfiles):
        """
        Return a list of tuples containing (spool directory, filename)
        """
        if firstnum < lastnum:
            numfiles = ((lastnum - firstnum) + 1) % maxfiles
        else:
            # mod MAXF in case sn_start & sn_stop are in the same HS file
            numfiles = ((lastnum - firstnum) + maxfiles + 1) % maxfiles
        logging.info("Number of relevant files = %d", numfiles)

        spool_dir = os.path.join(src_dir, spoolname)

        src_tuples_list = []
        for i in range(numfiles):
            filename = "HitSpool-%d.dat" % ((firstnum + i) % maxfiles)
            src_tuples_list.append((spool_dir, filename))

        return src_tuples_list

    def __compute_dataload(self, lines):
        total = None
        for line in lines:
            m = self.RSYNC_TOTAL_PAT.search(line)
            if m is not None:
                if total is None:
                    total = 0.0
                total += float(m.group(0))

        if total is None:
            return "TBD"

        return str(total / self.BYTES_PER_MB)

    def __copy_and_rsync(self, tmp_dir, src_tuples_list, alert_start,
                         start_ticks, alert_stop, stop_ticks, extract_hits,
                         sleep_secs, prefix, hs_copydir, hs_user_machinedir,
                         sender, make_remote_dir=False):
        if not extract_hits:
            # link files to tmp directory
            copy_files_list = self.__link_files(src_tuples_list, tmp_dir)
        else:
            # write hits within the range to a new file in tmp directory
            hitfile = self.__extract_hits(src_tuples_list, start_ticks,
                                          stop_ticks, tmp_dir)
            if hitfile is None:
                logging.error("No hits found for [%s-%s] in %s", alert_start,
                              alert_stop, src_tuples_list)
                return None

            copy_files_list = (hitfile, )

        if len(copy_files_list) == 0:
            logging.error("No relevant files found")
            return None

        logging.info("list of relevant files: %s", copy_files_list)
        return self.__rsync_files(alert_start, alert_stop, prefix,
                                  copy_files_list, sleep_secs, hs_copydir,
                                  hs_user_machinedir, sender,
                                  make_remote_dir=make_remote_dir)

    @staticmethod
    def __db_path(hs_sourcedir):
        return os.path.join(hs_sourcedir, "hitspool", "hitspool.db")

    def __extract_hits(self, src_tuples_list, start_tick, stop_tick, tmp_dir):
        """
        Extract hits within [start_ticks, stop_ticks] to a file in tmp_dir
        and return the new file name
        """
        foutname = None
        fout = None

        try:
            for src_dir, src_name in src_tuples_list:
                with PayloadReader(os.path.join(src_dir, src_name)) as rdr:
                    for pay in rdr:
                        if pay.utime > stop_tick:
                            # if past `stop_tick` it's safe to stop looking
                            break

                        if pay.utime < start_tick:
                            # nothing yet, keep looking
                            continue

                        if fout is None:
                            # if we haven't opened the file yet, do so now
                            basename = "hits_%d_%d" % (start_tick, stop_tick)
                            foutname = self.__make_filename(tmp_dir, basename)
                            fout = open(foutname, "wb")

                        # save this hit
                        fout.write(pay.bytes)
        finally:
            if fout is not None:
                fout.close()

        return foutname

    def __find_requested_files(self, alert_start, alert_stop, hs_sourcedir,
                               sleep_secs):
        """
        Check info.txt files
        """

        # for currentRun:
        try:
            cur_info = RunInfo(hs_sourcedir, 'currentRun',
                               sleep_secs=sleep_secs)
        except RunInfoException:
            logging.error("%s reading/parsing failed",
                          os.path.join(hs_sourcedir, 'currentRun', 'info.txt'))
            self.send_alert("ERROR: Current Run info.txt reading/parsing"
                            " failed")
            return None

        # for lastRun:
        try:
            last_info = RunInfo(hs_sourcedir, 'lastRun', sleep_secs=sleep_secs)
        except RunInfoException:
            logging.error("%s reading/parsing failed",
                          os.path.join(hs_sourcedir, 'lastRun', 'info.txt'))
            self.send_alert("ERROR: Last Run info.txt reading/parsing failed")
            return None

        spoolname = None
        sn_start_file = None
        sn_stop_file = None
        sn_max_files = None

        spoolname2 = None
        sn_start_file2 = None
        sn_stop_file2 = None
        sn_max_files2 = None

        # DETERMINE ALERT DATA LOCATION

        logging.info("Alert start is: %s", alert_start)
        logging.info("Alert stop is: %s", alert_stop)

        # Check if required sn_start / sn_stop data still exists in buffer.
        # If sn request comes in from earlier times, check lastRun directory
        # Which Run directory is the correct one: lastRun or currentRun ?

        # XXX should normalize these tests
        if last_info.buffer_stop_utc < alert_start < \
           cur_info.buffer_start_utc < alert_stop:
            spoolname = 'currentRun'
            sn_start_file = cur_info.oldest_file
            sn_stop_file = cur_info.file_num(alert_stop)
            sn_max_files = cur_info.max_files

            logging.warning("Sn_start doesn't exist in %s buffer anymore!"
                            " Start with oldest possible data: HitSpool-%d",
                            spoolname, cur_info.oldest_file)

        elif (cur_info.buffer_start_utc < alert_start < alert_stop <
              cur_info.buffer_stop_utc):
            spoolname = 'currentRun'
            sn_start_file = cur_info.file_num(alert_start)
            sn_stop_file = cur_info.file_num(alert_stop)
            sn_max_files = cur_info.max_files

        elif (last_info.buffer_stop_utc < alert_start < alert_stop <
              cur_info.buffer_start_utc):
            logging.error("Requested data doesn't exist in HitSpool"
                          " Buffer anymore! Abort request.")

            self.send_alert("Requested data doesn't exist"
                            " anymore in HsBuffer. Abort request.")
            return None

        elif (last_info.buffer_start_utc < alert_start <
              last_info.buffer_stop_utc < alert_stop):
            # tricky case: alert_start in lastRun and alert_stop in currentRun
            spoolname = 'lastRun'
            sn_start_file = last_info.file_num(alert_start)
            sn_stop_file = last_info.cur_file
            sn_max_files = last_info.max_files

            logging.info("Requested data distributed over both"
                         " Run directories")

            # if sn_stop is available from currentRun:
            # define 3 sn_stop files indices to have two data sets:
            # sn_start-sn_stop1 & sn_stop2-sn_top3
            if cur_info.buffer_start_utc < alert_stop:
                logging.info("SN_START & SN_STOP distributed over lastRun"
                             " and currentRun")
                logging.info("add relevant files from currentRun directory...")
                spoolname2 = 'currentRun'
                sn_start_file2 = cur_info.oldest_file
                sn_stop_file2 = cur_info.file_num(alert_stop)
                sn_max_files2 = cur_info.max_files

        elif (last_info.buffer_start_utc < alert_start < alert_stop <
              last_info.buffer_stop_utc):
            spoolname = 'lastRun'
            sn_start_file = last_info.file_num(alert_start)
            sn_stop_file = last_info.file_num(alert_stop)
            sn_max_files = last_info.max_files

        elif (alert_start < last_info.buffer_start_utc < alert_stop <
              last_info.buffer_stop_utc):
            spoolname = 'lastRun'
            sn_start_file = last_info.oldest_file
            sn_stop_file = last_info.file_num(alert_stop)
            sn_max_files = last_info.max_files

            logging.warning("sn_start doesn't exist in %s buffer anymore!"
                            " Start with oldest possible data: HitSpool-%d",
                            spoolname, last_info.oldest_file)

        elif alert_start < alert_stop < last_info.buffer_start_utc:
            logging.error("Requested data doesn't exist in HitSpool Buffer"
                          " anymore! Abort request.")
            self.send_alert("Requested data doesn't exist"
                            " anymore in HsBuffer. Abort request.")
            return None

        elif cur_info.buffer_stop_utc < alert_start:
            logging.error("alert_start is in the FUTURE ?!")
            self.send_alert("Requested data is younger than"
                            " most recent HS data. Abort request.")
            return None

        elif alert_start < last_info.buffer_start_utc < \
            last_info.buffer_stop_utc < alert_stop < \
                cur_info.buffer_start_utc:
            logging.warning("alert_start < lastRun < alert_stop < currentRun."
                            " Assign: all HS data of lastRun instead.")
            spoolname = 'lastRun'
            sn_start_file = last_info.oldest_file
            sn_stop_file = last_info.cur_file
            sn_max_files = last_info.max_files

        elif (last_info.buffer_stop_utc < alert_start <
              cur_info.buffer_start_utc < cur_info.buffer_stop_utc <
              alert_stop):
            logging.warning("lastRun < alert_start < currentRun < alert_stop."
                            " Assign: all HS data of currentRun instead.")
            spoolname = 'currentRun'
            sn_start_file = cur_info.oldest_file
            sn_stop_file = cur_info.cur_file
            sn_max_files = cur_info.max_files
        else:
            logging.error("Unhandled case ?!")
            self.send_alert("Fell off the end of buffer logic."
                            " Abort request.")
            return None

        # ---- HitSpool Data Access and Copy ----:
        # how many files do we have to move and copy:
        logging.info("Start & Stop File: %s and %s", sn_start_file,
                     sn_stop_file)

        src_tuples_list = self.__build_file_list(hs_sourcedir, spoolname,
                                                 sn_start_file, sn_stop_file,
                                                 sn_max_files)

        # in case the alerts data is spread over both lastRun
        # and currentRun directory, there will be two extra variable defined:
        if sn_start_file2 is not None and sn_stop_file2 is not None:
            src_tuples_list += self.__build_file_list(hs_sourcedir, spoolname2,
                                                      sn_start_file2,
                                                      sn_stop_file2,
                                                      sn_max_files2)

        return src_tuples_list

    def __link_files(self, src_tuples_list, tmp_dir):
        """
        Link source files to temporary directory
        """
        copy_files_list = []
        for src_dir, filename in src_tuples_list:
            next_file = os.path.join(src_dir, filename)

            # preserve file to prevent being overwritten on next hs cycle
            try:
                self.hardlink(next_file, tmp_dir)
            except HsException, hsex:
                logging.error("failed to link %s to tmp dir: %s", filename,
                              hsex)
                self.send_alert("ERROR: linking %s to tmp dir failed" %
                                filename)
                continue

            next_tmpfile = os.path.join(tmp_dir, filename)
            copy_files_list.append(next_tmpfile)
            logging.info("linked the file: %s to tmp directory", next_file)
            self.send_alert("linked %s to tmp dir" % next_file)

        return copy_files_list

    @staticmethod
    def __make_filename(dirname, basename, ext=".dat"):
        """
        Generate a unique filename
        dirname - directory where file will be created
        basename - the base filename (will be used unadorned if possible)
        ext - the file extension (defaults to ".dat")
        """
        basepath = os.path.join(dirname, basename)

        filename = basepath + ext
        num = 1
        while True:
            if not os.path.exists(filename):
                return filename
            filename = "%s_%d%s" % (basepath, num, ext)
            num += 1

    def __debug_empty_request(self, cursor, start_ticks, stop_ticks):
        count = None
        for row in cursor.execute("select count(filename) from hitspool"):
            count = row[0]

        first_time = None
        for row in cursor.execute("select start_tick from hitspool"
                                  " order by start_tick asc limit 1"):
            first_time = row[0]

        last_time = None
        for row in cursor.execute("select start_tick from hitspool"
                                  " order by start_tick desc limit 1"):
            last_time = row[0]

        # build email contents
        address_list = ("dglo@icecube.wisc.edu", )
        description = "HsInterface Data Request"
        header = "Query for [%s-%s] failed on %s" % (start_ticks, stop_ticks,
                                                     self.shorthost)
        message = "DB contains %s entries from %s to %s" % \
            (count, first_time, last_time)

        # send email
        debugjson = HsUtil.assemble_email_dict(address_list, header,
                                               description, message)
        self.__i3socket.send_json(debugjson)

    def __query_requested_files(self, start_ticks, stop_ticks, hs_sourcedir):
        """
        Fetch list of files containing requested data from hitspool DB
        """
        hs_dbfile = self.__db_path(hs_sourcedir)
        conn = sqlite3.connect(hs_dbfile)
        try:
            cursor = conn.cursor()

            spooldir = os.path.join(hs_sourcedir, "hitspool")

            src_tuples_list = []
            for row in cursor.execute("select filename from hitspool"
                                      " where stop_tick>=? "
                                      " and start_tick<=?",
                                      (start_ticks, stop_ticks)):
                src_tuples_list.append((spooldir, row[0]))

            if len(src_tuples_list) == 0 and self.DEBUG_EMPTY:
                try:
                    self.__debug_empty_request(cursor, start_ticks, stop_ticks)
                except:
                    logging.exception("Empty request debug failed")

        finally:
            conn.close()

        return src_tuples_list

    def __rsync_files(self, alert_start, alert_stop, prefix, copy_files_list,
                      sleep_secs, hs_copydir, hs_user_machinedir, sender,
                      make_remote_dir=False):
        """
        Copy requested files to remote machine
        """

        (timetag_prefix, timetag) = self.get_timetag_tuple(prefix, hs_copydir,
                                                           alert_start)
        timetag_dir = "_".join((timetag_prefix, timetag, self.shorthost))

        # ---- Rsync the relevant files to DESTINATION ---- #

        if make_remote_dir:
            # mkdir destination copydir
            self.mkdir(self.rsync_host, os.path.join(hs_copydir, timetag_dir))

        hs_copydest = self.get_copy_destination(hs_copydir, timetag_dir)
        logging.info("unique naming for folder: %s", hs_copydest)

        # sleep a random amount so 2ndbuild isn't overwhelmed
        self.add_rsync_delay(sleep_secs)

        # ------- the REAL rsync command for SPS and SPTS:-----#
        # rsync daemon maps hitspool/ to /mnt/data/pdaqlocal/HsDataCopy/
        target = self.rsync_target(hs_user_machinedir, timetag_dir,
                                   hs_copydest)

        try:
            outlines = self.rsync(copy_files_list, target, log_format="%i%n%L")
        except HsException, hsex:
            self.send_alert(str(hsex))
            return None

        logging.info("successful copy of HS data from %s to %s at %s",
                     self.fullhost, hs_copydest, self.rsync_host)

        dataload_mb = self.__compute_dataload(outlines)
        logging.info("dataload of %s in [MB]:\t%s", hs_copydest, dataload_mb)

        if self.__i3socket is not None:
            self.send_alert(" %s [MB] HS data transferred to %s " %
                            (dataload_mb, self.rsync_host), prio=1)

        return hs_copydest

    def __staging_dir(self, timetag):
        if self.is_cluster_sps or self.is_cluster_spts:
            tmpdir = "/mnt/data/pdaqlocal/tmp"
        else:
            tmpdir = os.path.join(self.TEST_HUB_DIR, "tmp")

        if not os.path.exists(tmpdir):
            try:
                os.makedirs(tmpdir)
            except OSError:
                # this should only happen inside unit tests
                pass

        if not os.path.isdir(tmpdir):
            raise HsException("Found non-directory at %s" % tmpdir)

        return tempfile.mkdtemp(suffix=timetag, dir=tmpdir)

    def add_rsync_delay(self, sleep_secs):
        pass

    def close_all(self):
        if self.__i3socket is not None:
            self.__i3socket.close()
        if self.__subscriber is not None:
            self.__subscriber.close()
        if self.__sender is not None:
            self.__sender.close()
        self.__context.term()

    def create_i3socket(self, host):
        if host is None:
            return None

        # Socket for I3Live on expcont
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, I3LIVE_PORT))
        return sock

    def create_sender_socket(self, host):
        if host is None:
            return None

        # Socket to send message to
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, SENDER_PORT))
        return sock

    def create_subscriber_socket(self, host):
        if host is None:
            return None

        # Socket to receive message on:
        sock = self.__context.socket(zmq.SUB)
        sock.setsockopt(zmq.IDENTITY, self.fullhost)
        sock.setsockopt(zmq.SUBSCRIBE, "")
        sock.connect("tcp://%s:%d" % (host, PUBLISHER_PORT))
        return sock

    def get_copy_destination(self, hs_copydir, timetag_dir):
        return os.path.join(hs_copydir, timetag_dir)

    def get_timetag_tuple(self, prefix, hs_copydir, starttime):
        raise NotImplementedError()

    def hardlink(self, filename, targetdir):
        path = os.path.join(targetdir, os.path.basename(filename))
        if os.path.exists(path):
            raise HsException("File \"%s\" already exists" % path)

        try:
            os.link(filename, path)
        except StandardError, err:
            raise HsException("Cannot link \"%s\" to \"%s\": %s" %
                              (filename, targetdir, err))

    @property
    def i3socket(self):
        return self.__i3socket

    @classmethod
    def jan1(cls):
        if cls.JAN1 is None:
            utc_now = datetime.utcnow()
            cls.JAN1 = datetime(utc_now.year, 1, 1)

        return cls.JAN1

    def mkdir(self, _, path):
        os.makedirs(path)

    def request_parser(self, prefix, alert_start, alert_stop,
                       hs_user_machinedir, extract_hits=False, sender=None,
                       sleep_secs=4, make_remote_dir=False):

        # catch bogus requests
        if alert_stop < alert_start:
            logging.error("sn_start & sn_stop time-stamps inverted."
                          " Abort request.")
            self.send_alert("alert_stop < alert_start. Abort request.")
            return None

        # parse destination string
        try:
            hs_ssh_access, hs_copydir \
                = HsUtil.split_rsync_host_and_path(hs_user_machinedir)
            if hs_ssh_access is None:
                raise HsException("Illegal copy directory \"%s\"<%s>",
                                  hs_user_machinedir,
                                  type(hs_user_machinedir))
            if hs_ssh_access != "":
                logging.info("Ignoring rsync user/host \"%s\"", hs_ssh_access)

            logging.info("HS COPYDIR = %s", hs_copydir)

            self.set_default_copydir(hs_copydir)
        except Exception, err:
            self.send_alert("ERROR: destination parsing failed for"
                            " \"%s\". Abort request." % hs_user_machinedir)
            logging.error("Destination parsing failed for \"%s\":\n"
                          "Abort request.", hs_user_machinedir)
            return None

        logging.info("HsInterface running on: %s", self.cluster)

        if self.is_cluster_sps or self.is_cluster_spts:
            hs_sourcedir = '/mnt/data/pdaqlocal'
        else:
            hs_sourcedir = self.TEST_HUB_DIR

        # if no prefix was supplied, guess it from the destination directory
        if prefix is None:
            prefix = HsPrefix.guess_from_dir(hs_copydir)

        # convert start/stop times to DAQ ticks
        start_ticks = HsUtil.get_daq_ticks(self.jan1(), alert_start)
        stop_ticks = HsUtil.get_daq_ticks(self.jan1(), alert_stop)

        hs_dbfile = self.__db_path(hs_sourcedir)
        if not os.path.exists(hs_dbfile):
            src_tuples_list = self.__find_requested_files(alert_start,
                                                          alert_stop,
                                                          hs_sourcedir,
                                                          sleep_secs)
        else:
            src_tuples_list = self.__query_requested_files(start_ticks,
                                                           stop_ticks,
                                                           hs_sourcedir)
            if src_tuples_list is None or len(src_tuples_list) == 0:
                # if it wasn't in the hitspool cache, check the old directories
                curRunDir = os.path.join(hs_sourcedir, "currentRun")
                if os.path.isdir(curRunDir):
                    src_tuples_list = self.__find_requested_files(alert_start,
                                                                  alert_stop,
                                                                  hs_sourcedir,
                                                                  sleep_secs)

                if src_tuples_list is None or len(src_tuples_list) == 0:
                    logging.error("No data found between %s and %s",
                                  alert_start, alert_stop)

        if src_tuples_list is None or len(src_tuples_list) == 0:
            return None

        (_, timetag) = self.get_timetag_tuple(prefix, hs_copydir, alert_start)

        # temporary directory for relevant hs data copy
        tmp_dir = self.__staging_dir(timetag)

        # rsync files to HsSender machine
        hs_copydest = None
        try:
            hs_copydest \
                = self.__copy_and_rsync(tmp_dir, src_tuples_list, alert_start,
                                        start_ticks, alert_stop, stop_ticks,
                                        extract_hits, sleep_secs, prefix,
                                        hs_copydir, hs_user_machinedir, sender,
                                        make_remote_dir=make_remote_dir)
        finally:
            if hs_copydest is None:
                logging.info("KEEP tmp dir with data in: %s", tmp_dir)
                self.send_alert("ERROR in rsync. Keep tmp dir.")
            elif os.path.exists(tmp_dir):
                # remove tmp dir
                try:
                    shutil.rmtree(tmp_dir)
                    logging.info("Deleted tmp dir %s", tmp_dir)
                except StandardError, err:
                    logging.error("failed removing tmp files: %s", err)
                    self.send_alert("ERROR: Deleting tmp dir failed")

        return hs_copydest

    def rsync(self, source_list, target, bwlimit=None, log_format=None,
              relative=True):
        bwstr = "" if bwlimit is None else " --bwlimit=%d" % 300
        logstr = "" if log_format is None \
                 else " --log-format=\"%s\"" % log_format
        relstr = "" if relative else " --no-relative"

        if source_list is None or len(source_list) == 0:
            raise HsException("No source specified")
        if target is None or target == "":
            raise HsException("No target specified")
        if target[-1] != "/":
            # make sure `rsync` knows the target should be a directory
            target += "/"

        source_str = " ".join(source_list)
        rsync_cmd = "nice rsync -avv %s%s%s %s \"%s\"" % \
                    (bwstr, logstr, relstr, source_str, target)

        logging.info("rsync command: %s", rsync_cmd)
        proc = subprocess.Popen(rsync_cmd, shell=True, bufsize=256,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        rsync_out = proc.stdout.readlines()
        err_lines = proc.stderr.readlines()

        proc.stdout.flush()
        proc.stderr.flush()

        if len(err_lines) > 0:
            raise HsException("failed to rsync \"%s\" to \"%s\":\n%s" %
                              (source_str, target, err_lines))

        logging.info("rsync \"%s\" to \"%s\":\n%s", source_str, target,
                     rsync_out)
        return rsync_out

    def rsync_target(self, hs_user_machinedir, timetag_dir, hs_copydest):
        if self.is_cluster_sps or self.is_cluster_spts:
            return os.path.join(hs_user_machinedir, timetag_dir)

        return hs_copydest

    def send_alert(self, value, prio=None):
        pass

    @property
    def sender(self):
        return self.__sender

    def set_default_copydir(self, hs_copydir):
        pass

    @property
    def subscriber(self):
        return self.__subscriber
