#!/usr/bin/env python
"""
Base class for HitSpool workers
"""

import datetime
import logging
import numbers
import os
import random
import re
import shutil
import sqlite3
import tempfile
import time
import zmq

import DAQTime
import HsMessage
import HsUtil

from HsBase import HsBase
from HsConstants import I3LIVE_PORT, PUBLISHER_PORT, SENDER_PORT
from HsCopier import CopyUsingRSync, CopyUsingSCP
from HsException import HsException
from HsPrefix import HsPrefix
from payload import PayloadReader


class HsRSyncFiles(HsBase):
    # location of hub directory used for testing HsInterface
    TEST_HUB_DIR = "/home/david/TESTCLUSTER/testhub"
    # default name of hitspool subdirectory
    DEFAULT_SPOOL_NAME = "hitspool"
    # default name of hitspool file database
    DEFAULT_SPOOL_DB = "hitspool.db"
    # number of bytes in a megabyte
    BYTES_PER_MB = 1024.0 * 1024.0
    # regular expression used to get the number of bytes sent by rsync
    RSYNC_TOTAL_PAT = re.compile(r'(?<=total size is )\d+')

    # cached datetime instance representing the first second of the year
    JAN1 = None

    # rsync bandwidth limit (Kbytes/second)
    BWLIMIT = 1000

    # true if request/DB details should be emailed after no records are found
    DEBUG_EMPTY = True
    # list of email addresses which receive the debugging email
    DEBUG_EMAIL = ["tim.bendfelt@icecube.wisc.edu", ]

    # minimum delay before starting remote copy
    MIN_DELAY = 7.0

    # location of copy directory used for testing HsInterface
    TEST_COPY_PATH = "/home/david/data/HitSpool/copytest"

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
        self.__subscriber = self.create_subscriber_socket(sec_bldr)

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
            mtch = self.RSYNC_TOTAL_PAT.search(line)
            if mtch is not None:
                if total is None:
                    total = 0.0
                total += float(mtch.group(0))

        if total is None:
            return "TBD"

        return str(total / self.BYTES_PER_MB)

    def __copy_and_send(self, req, start_ticks, stop_ticks, hs_copydir,
                        extract_hits, update_status=None, delay_rsync=True,
                        make_remote_dir=False):
        # if no prefix was supplied, guess it from the destination directory
        if req is not None and req.prefix is not None:
            prefix = req.prefix
        else:
            prefix = HsPrefix.guess_from_dir(hs_copydir)

        # get the list of files containing hits within the interval
        src_tuples_list = self.__query_requested_files(start_ticks, stop_ticks)
        if src_tuples_list is None:
            # wait a bit to give other hubs a chance to start
            time.sleep(0.1)
            return None, None

        # get ASCII representation of starting time
        timetag = self.get_timetag_tuple(prefix, start_ticks)

        # create temporary directory for relevant hs data copy
        tmp_dir = self.__make_staging_dir(timetag)

        if not extract_hits:
            # link files to tmp directory
            copy_files_list = self.__link_files(src_tuples_list, tmp_dir)
        else:
            # write hits within the range to a new file in tmp directory
            hitfile = self.__extract_hits(src_tuples_list, start_ticks,
                                          stop_ticks, tmp_dir)
            if hitfile is None:
                logging.error("No hits found for [%s-%s] in %s",
                              DAQTime.ticks_to_utc(start_ticks),
                              DAQTime.ticks_to_utc(stop_ticks),
                              src_tuples_list)
                return None, tmp_dir

            copy_files_list = (hitfile, )

        if len(copy_files_list) == 0:
            logging.error("No relevant files found")
            return None, tmp_dir
        logging.info("Found %d relevant files", len(copy_files_list))

        # build subdirectory name
        timetag_dir = "_".join((prefix, timetag, self.shorthost))

        if make_remote_dir:
            # create destination directory
            self.mkdir(self.rsync_host,
                       os.path.join(hs_copydir, timetag_dir))

        # if we want to delay the rsync, get the maximum delay and then sleep
        if not delay_rsync:
            # always delay a bit to avoid a "thundering herd" DDoS
            max_delay = self.MIN_DELAY
        else:
            max_delay = self.compute_maximum_rsync_delay(copy_files_list)

        start_delay = random.uniform(1.0, max_delay)
        stop_delay = max_delay - start_delay

        if start_delay > 0.0:
            logging.info("Delay rsync start by %d seconds", int(start_delay))
            self.__delay(start_delay, request=req, update_status=update_status)

        rsyncdir = self.__get_rsync_directory(hs_copydir, timetag_dir)
        use_daemon = self.is_cluster_sps or self.is_cluster_spts

        failed = not self.send_files(req, copy_files_list, self.rsync_user,
                                     self.rsync_host, rsyncdir, timetag_dir,
                                     use_daemon, update_status=update_status,
                                     bwlimit=self.BWLIMIT, log_format="%i%n%L")

        if stop_delay > 0.0:
            logging.info("Delay rsync finish by %d seconds", int(stop_delay))
            self.__delay(stop_delay, request=req, update_status=update_status)

        if failed:
            return None, tmp_dir

        return rsyncdir, tmp_dir

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
        header = "Query for [%s-%s] failed on %s" % (start_ticks, stop_ticks,
                                                     self.shorthost)
        message = "DB contains %s entries from %s to %s" % \
            (count, first_time, last_time)

        # send email
        debugjson = HsUtil.assemble_email_dict(self.DEBUG_EMAIL, header,
                                               message)
        if debugjson is not None:
            self.__i3socket.send_json(debugjson)

    @classmethod
    def __delay(cls, delay_time, request=None, update_status=None):
        """
        Delay start/stop by alternately sending a "keepalive" message and then
        sleeping for a bit
        """
        if request is None or update_status is None:
            # since we're missing vital info, we must sleep and then return
            time.sleep(delay_time)
            return

        five_minutes = 300.0
        total_delay = 0.0
        while True:
            # send a "keepalive" message, even if we'll immediately exit
            update_status(request.copy_dir, request.destination_dir,
                          HsMessage.WORKING)

            # if we've waited for the requested time, we're done sleeping
            if total_delay >= delay_time:
                break

            # figure out how long we should sleep
            if delay_time - total_delay > five_minutes:
                sleep_time = five_minutes
            else:
                sleep_time = delay_time - total_delay

            # sleep then update the total delay
            time.sleep(sleep_time)
            total_delay += sleep_time

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

    def __get_rsync_directory(self, hs_copydir, timetag_dir):
        if self.is_cluster_sps or self.is_cluster_spts:
            copydir_dflt = self.DEFAULT_COPY_PATH
        else:
            copydir_dflt = self.TEST_COPY_PATH

        if copydir_dflt != hs_copydir:
            logging.warning("Requested HS data copy destination differs"
                            " from default!")
            logging.warning("data will be sent to default destination: %s",
                            copydir_dflt)
            logging.info("HsSender will redirect it later on to: %s on %s",
                         hs_copydir, HsBase.DEFAULT_RSYNC_HOST)

        return os.path.join(copydir_dflt, timetag_dir)

    @classmethod
    def __get_total_size(cls, files):
        "Get the size of all the files"
        total = 0
        for fnm in files:
            try:
                total += os.path.getsize(fnm)
            except:
                logging.error("Cannot get size of \"%s\"", fnm)
        return total

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
            except HsException as hsex:
                logging.error("failed to link %s to tmp dir: %s", filename,
                              hsex)
                self.send_alert("ERROR: linking %s to tmp dir failed" %
                                filename)
                continue

            next_tmpfile = os.path.join(tmp_dir, filename)
            copy_files_list.append(next_tmpfile)

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

    def __make_staging_dir(self, timetag):
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

    def __query_requested_files(self, start_ticks, stop_ticks):
        """
        Fetch list of files containing requested data from hitspool DB
        """
        if self.is_cluster_sps or self.is_cluster_spts:
            hs_sourcedir = '/mnt/data/pdaqlocal'
        else:
            hs_sourcedir = self.TEST_HUB_DIR

        spooldir = os.path.join(hs_sourcedir, HsRSyncFiles.DEFAULT_SPOOL_NAME)

        hs_dbfile = os.path.join(spooldir, HsRSyncFiles.DEFAULT_SPOOL_DB)
        if not os.path.exists(hs_dbfile):
            logging.error("Cannot find DB file in %s", spooldir)
            return None

        conn = sqlite3.connect(hs_dbfile)
        try:
            cursor = conn.cursor()

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

        if src_tuples_list is None or len(src_tuples_list) == 0:
            logging.error("No data found between %s and %s", start_ticks,
                          stop_ticks)
            return None

        return src_tuples_list

    def close_all(self):
        if self.__i3socket is not None:
            self.__i3socket.close()
        if self.__subscriber is not None:
            self.__subscriber.close()
        if self.__sender is not None:
            self.__sender.close()
        self.__context.term()

    def compute_maximum_rsync_delay(self, copy_file_list):
        """
        Compute the maximum rsync delay for the list of files.
        This is used to spread out rsync requests in order to avoid
        saturating 2ndbuild's disk.
        Simultaneously rsyncing from 97 hubs caused issues in the past
        """
        total_size = self.__get_total_size(copy_file_list)
        if total_size <= 0:
            return 0.0

        fsz = float(total_size) / 1000000.0
        delay = (fsz * fsz) / 7200.0
        if delay < self.MIN_DELAY:
            # we want to randomize a little bit even for tiny rsyncs
            return self.MIN_DELAY

        return delay

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
        sock.identity = (self.fullhost + ">OUT").encode("ascii")
        sock.connect("tcp://%s:%d" % (host, SENDER_PORT))
        return sock

    def create_subscriber_socket(self, host):
        if host is None:
            return None

        # Socket to receive message on:
        sock = self.__context.socket(zmq.SUB)
        sock.identity = (self.fullhost + "<IN").encode("ascii")
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        sock.connect("tcp://%s:%d" % (host, PUBLISHER_PORT))
        return sock

    @classmethod
    def get_timetag_tuple(cls, prefix, ticks):
        if prefix == HsPrefix.SNALERT:
            # this is a SNDAQ request -> SNALERT tag
            # time window around trigger is [-30,+60], so add 30 seconds
            ticks += int(30E10)

        return DAQTime.ticks_to_utc(ticks).strftime("%Y%m%d_%H%M%S")

    @classmethod
    def hardlink(cls, filename, targetdir):
        path = os.path.join(targetdir, os.path.basename(filename))
        if os.path.exists(path):
            raise HsException("File \"%s\" already exists" % path)

        try:
            os.link(filename, path)
        except Exception as err:
            raise HsException("Cannot link \"%s\" to \"%s\": %s" %
                              (filename, targetdir, err))

    @property
    def i3socket(self):
        return self.__i3socket

    @classmethod
    def jan1(cls):
        if cls.JAN1 is None:
            utc_now = datetime.datetime.utcnow()
            cls.JAN1 = datetime.datetime(utc_now.year, 1, 1)

        return cls.JAN1

    @classmethod
    def mkdir(cls, _, path):
        os.makedirs(path)

    def request_parser(self, req, start_ticks, stop_ticks, hs_copydir,
                       extract_hits=False, update_status=None,
                       delay_rsync=True, make_remote_dir=False):

        # catch bogus requests
        if start_ticks is None or stop_ticks is None:
            logging.error("Missing start/stop time(s). Abort request.")
            self.send_alert("Missing start/stop time(s). Abort request.")
            return None
        if not isinstance(start_ticks, numbers.Number):
            logging.error("Starting time %s is %s, not a number",
                          start_ticks, type(start_ticks).__name__)
        if not isinstance(stop_ticks, numbers.Number):
            logging.error("Stopping time %s is %s, not a number",
                          stop_ticks, type(stop_ticks).__name__)
        if start_ticks >= stop_ticks:
            logging.error("Start and stop times are inverted."
                          " Abort request.")
            return None

        logging.info("HsInterface running on: %s", self.cluster)

        # rsync files to HsSender machine
        rsyncdir = None
        tmp_dir = None
        try:
            rsyncdir, tmp_dir \
                = self.__copy_and_send(req, start_ticks, stop_ticks,
                                       hs_copydir, extract_hits,
                                       update_status=update_status,
                                       delay_rsync=delay_rsync,
                                       make_remote_dir=make_remote_dir)
        finally:
            if tmp_dir is not None:
                if rsyncdir is None:
                    logging.info("KEEP tmp dir with data in: %s", tmp_dir)
                    self.send_alert("ERROR in rsync. Keep tmp dir.")
                elif os.path.exists(tmp_dir):
                    # remove tmp dir
                    try:
                        shutil.rmtree(tmp_dir)
                        logging.info("Deleted tmp dir %s", tmp_dir)
                    except Exception as err:
                        logging.error("failed removing tmp files: %s", err)
                        self.send_alert("ERROR: Deleting tmp dir failed")

        return rsyncdir

    def send_alert(self, value, prio=None):
        pass

    def send_files(self, req, source_list, rsync_user, rsync_host, rsync_dir,
                   timetag_dir, use_daemon, update_status=None, bwlimit=None,
                   log_format=None, relative=True):
        exception = None

        files = source_list[:]
        for i in range(2):
            if i == 0:
                 copier = CopyUsingRSync(rsync_user, rsync_host, rsync_dir,
                                        timetag_dir, use_daemon=use_daemon,
                                        bwlimit=bwlimit, log_format=log_format,
                                        relative=relative)
            else:
                copier = CopyUsingSCP(rsync_user, rsync_host, rsync_dir,
                                      timetag_dir, bwlimit=bwlimit,
                                      log_format=log_format, relative=relative)

            try:
                failed = copier.copy(files, request=req,
                                     update_status=update_status)
                if len(failed) == 0:
                    files = None
                    exception = None
                    break

                # if at first we don't succeed...
                files = failed
            except HsException as hsex:
                exception = hsex

        if exception is not None:
            self.send_alert(str(exception))
            return False
        elif files is not None:
            self.send_alert("Failed to copy %d files to \"%s\" (%s)" %
                            (len(failed), copier.target, ", ".join(failed)))
            return False

        logging.info("successful copy of HS data from %s to %s at %s",
                     self.fullhost, timetag_dir, rsync_host)

        if copier.size is None:
            dataload = "(No size found)"
        else:
            try:
                dataload = "%.1f" % (float(copier.size) / self.BYTES_PER_MB)
            except TypeError:
                dataload = "(Bad size \"%s\" type <%s>)" % \
                           (copier.size, type(copier.size).__name__)

        logging.info("dataload of %s in [MB]:\t%s", timetag_dir, dataload)

        if self.__i3socket is not None:
            self.send_alert(" %s [MB] HS data transferred to %s " %
                            (copier.size, rsync_host), prio=1)

        return True

    @property
    def sender(self):
        return self.__sender

    @classmethod
    def set_min_delay(cls, min_delay):
        cls.MIN_DELAY = min_delay

    @property
    def subscriber(self):
        return self.__subscriber
