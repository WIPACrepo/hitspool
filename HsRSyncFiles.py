#!/usr/bin/env python


import json
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

        parsed = False
        for _ in range(1, retries_max):
            try:
                filename = os.path.join(hs_sourcedir, rundir, 'info.txt')
                fin = open(filename, "r")
                logging.info("read %s", filename)
            except IOError:
                #print "couldn't open file, Retry in 4 seconds."
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
        total_data = int((cur_time - startrun) / \
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

    JAN1 = None

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
        #----------- parse info.txt files--------------#

        # for currentRun:
        try:
            cur_info = RunInfo(hs_sourcedir, 'currentRun',
                               sleep_secs=sleep_secs)
        except RunInfoException:
            self.send_alert("ERROR: Current Run info.txt"
                            " reading/parsing failed")
            logging.error("CurrentRun info.txt reading/parsing failed")
            return None

        # for lastRun:
        try:
            last_info = RunInfo(hs_sourcedir, 'lastRun', sleep_secs=sleep_secs)
        except RunInfoException:
            self.send_alert("ERROR: Last Run info.txt"
                            " reading/parsing failed")
            logging.error("LastRun info.txt reading/parsing failed")
            return None

        spoolname = None
        sn_start_file = None
        sn_stop_file = None
        sn_max_files = None

        spoolname2 = None
        sn_start_file2 = None
        sn_stop_file2 = None
        sn_max_files2 = None

        #------ DETERMINE ALERT DATA LOCATION  -----#

        logging.info("Alert start is: %s", alert_start)
        logging.info("Alert stop is: %s", alert_stop)

        # Check if required sn_start / sn_stop data still exists in buffer.
        # If sn request comes in from earlier times -->  Check lastRun directory
        # Which Run directory is the correct one: lastRun or currentRun ?

        ### XXX should normalize these tests
        if last_info.buffer_stop_utc < alert_start < \
           cur_info.buffer_start_utc < alert_stop:
            spoolname = 'currentRun'
            sn_start_file = cur_info.oldest_file
            sn_stop_file = cur_info.file_num(alert_stop)
            sn_max_files = cur_info.max_files

            logging.warning("Sn_start doesn't exist in %s buffer anymore!"
                            " Start with oldest possible data: HitSpool-%d",
                            spoolname, cur_info.oldest_file)

        elif cur_info.buffer_start_utc < alert_start < alert_stop < \
             cur_info.buffer_stop_utc:
            spoolname = 'currentRun'
            sn_start_file = cur_info.file_num(alert_start)
            sn_stop_file = cur_info.file_num(alert_stop)
            sn_max_files = cur_info.max_files

        elif last_info.buffer_stop_utc < alert_start < alert_stop \
             < cur_info.buffer_start_utc:
            logging.error("Requested data doesn't exist in HitSpool"
                          " Buffer anymore! Abort request.")

            self.send_alert("Requested data doesn't exist"
                            " anymore in HsBuffer. Abort request.")
            return None

        elif last_info.buffer_start_utc < alert_start < \
             last_info.buffer_stop_utc < alert_stop:
            # tricky case: alert_start in lastRun and alert_stop in currentRun
            spoolname = 'lastRun'
            sn_start_file = last_info.file_num(alert_start)
            sn_stop_file = last_info.cur_file
            sn_max_files = last_info.max_files

            logging.info("Requested data distributed over both Run directories")

            # if sn_stop is available from currentRun:
            # define 3 sn_stop files indices to have two data sets:
            # sn_start-sn_stop1 & sn_stop2-sn_top3
            if cur_info.buffer_start_utc < alert_stop:
                logging.info("SN_START & SN_STOP distributed over lastRun"
                             " and currentRun")
                logging.info("add relevant files from currentRun directory...")
                #in currentRun:
                spoolname2 = 'currentRun'
                sn_start_file2 = cur_info.oldest_file
                sn_stop_file2 = cur_info.file_num(alert_stop)
                sn_max_files2 = cur_info.max_files

        elif last_info.buffer_start_utc < alert_start < alert_stop < \
             last_info.buffer_stop_utc:
            spoolname = 'lastRun'
            sn_start_file = last_info.file_num(alert_start)
            sn_stop_file = last_info.file_num(alert_stop)
            sn_max_files = last_info.max_files

        elif alert_start < last_info.buffer_start_utc < alert_stop < \
             last_info.buffer_stop_utc:
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

        elif last_info.buffer_stop_utc < alert_start < \
             cur_info.buffer_start_utc < cur_info.buffer_stop_utc < \
             alert_stop:
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
        #how many files do we have to move and copy:
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

            #preserve file to prevent being overwritten on next hs cycle
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

    def __query_requested_files(self, start_ticks, stop_ticks, hs_sourcedir,
                                sleep_secs):
        """
        Fetch list of files containing requested data from hitspool DB
        """
        hs_dbfile = self.__db_path(hs_sourcedir)
        conn = sqlite3.connect(hs_dbfile)
        cursor = conn.cursor()

        spooldir = os.path.join(hs_sourcedir, "hitspool")

        src_tuples_list = []
        for row in cursor.execute("select filename from hitspool" +
                                  " where stop_tick>=? and start_tick<=?",
                                  (start_ticks, stop_ticks)):
            src_tuples_list.append((spooldir, row[0]))

        return src_tuples_list

    def __rsync_files(self, alert_start, alert_stop, copy_files_list,
                      sleep_secs, hs_dest_mchn, hs_copydir, hs_user_machinedir,
                      hs_ssh_access, sender, make_remote_dir=False):
        """
        Copy requested files to remote machine
        """

        (timetag_prefix, timetag) = self.get_timetag_tuple(hs_copydir,
                                                           alert_start)
        timetag_dir = timetag_prefix + "_" + timetag + "_" + self.fullhost

        # ---- Rsync the relevant files to DESTINATION ---- #

        if make_remote_dir:
            # mkdir destination copydir
            self.mkdir(hs_dest_mchn, os.path.join(hs_copydir, timetag_dir))

        hs_copydest = self.get_copy_destination(hs_copydir, timetag_dir)
        logging.info("unique naming for folder: %s", hs_copydest)

        # sleep a random amount so 2ndbuild isn't overwhelmed
        self.add_rsync_delay(sleep_secs)

        # ------- the REAL rsync command for SPS and SPTS:-----#
        # rsync daemon maps hitspool/ to /mnt/data/pdaqlocal/HsDataCopy/
        target = self.rsync_target(hs_user_machinedir, hs_ssh_access,
                                   timetag_dir, hs_copydest)

        copy_files_str = " ".join(copy_files_list)
        (hs_rsync_out, hs_rsync_err) = self.rsync(copy_files_str, target,
                                                  log_format="%i%n%L")

        tmp_dir = self.__staging_dir(timetag)

        # --- catch rsync error --- #
        if len(hs_rsync_err) != 0:
            logging.error("failed rsync process:\n%s", hs_rsync_err)
            logging.info("KEEP tmp dir with data in: %s", tmp_dir)
            self.send_alert("ERROR in rsync. Keep tmp dir.")
            self.send_alert(hs_rsync_err)

            if sender is not None:
                # send msg to HsSender
                report_json = {"hubname": self.shorthost,
                               "alertid": timetag,
                               "dataload": int(0),
                               "datastart": str(alert_start),
                               "datastop": str(alert_stop),
                               "copydir": hs_copydest,
                               "copydir_user": hs_copydir,
                               "msgtype": "rsync_sum"}

                # XXX lose the json.dumps()
                self.__sender.send_json(json.dumps(report_json))
                logging.info("sent rsync report json to HsSender: %s",
                             report_json)

            return None

        logging.info("rsync out:\n%s", hs_rsync_out)
        logging.info("successful copy of HS data from %s to %s at %s",
                     self.fullhost, hs_copydest, hs_ssh_access)

        rsync_dataload = re.search(r'(?<=total size is )[0-9]*',
                                   hs_rsync_out[-1])
        if rsync_dataload is not None:
            bytes_per_mb = 1024.0 * 1024.0
            dataload_mb = str(float(rsync_dataload.group(0)) / bytes_per_mb)
        else:
            dataload_mb = "TBD"
        logging.info("dataload of %s in [MB]:\t%s", hs_copydest, dataload_mb)

        if self.__i3socket is not None:
            self.send_alert(" %s [MB] HS data transferred to %s " %
                            (dataload_mb, hs_ssh_access), prio=1)

        if sender is not None:
            report_json = {"hubname": self.shorthost,
                           "alertid": timetag,
                           "dataload": dataload_mb,
                           "datastart": str(alert_start),
                           "datastop": str(alert_stop),
                           "copydir": hs_copydest,
                           "copydir_user": hs_copydir,
                           "msgtype": "rsync_sum"}

            # XXX lose the json.dumps()
            self.__sender.send_json(json.dumps(report_json))
            logging.info("sent rsync report json to HsSender: %s", report_json)

        # remove tmp dir:
        try:
            shutil.rmtree(tmp_dir)
            logging.info("tmp dir deleted.")
        except StandardError, err:
            logging.error("failed removing tmp files: %s", err)

            self.send_alert("ERROR: Deleting tmp dir failed")

        return timetag

    def __staging_dir(self, timetag):
        if self.is_cluster_sps or self.is_cluster_spts:
            tmpdir = "/mnt/data/pdaqlocal/tmp"
        else:
            tmpdir = os.path.join(self.TEST_HUB_DIR)

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

    def extract_ssh_access(self, hs_user_machinedir):
        return re.sub(r':/[\w+/]*', "", hs_user_machinedir)

    def get_copy_destination(self, hs_copydir, timetag_dir):
        return os.path.join(hs_copydir, timetag_dir)

    def get_timetag_tuple(self, hs_copydir, starttime):
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

    def request_parser(self, alert_start, alert_stop, hs_user_machinedir,
                       extract_hits=False, sender=None, sleep_secs=4,
                       make_remote_dir=False):

        # catch bogus requests
        if alert_stop < alert_start:
            logging.error("sn_start & sn_stop time-stamps inverted."
                          " Abort request.")
            self.send_alert("alert_stop < alert_start. Abort request.")
            return None

        # parse destination string
        try:
            hs_ssh_access \
                = self.extract_ssh_access(hs_user_machinedir)
            logging.info("HS COPY SSH ACCESS: %s", hs_ssh_access)

            hs_copydir = re.sub(hs_ssh_access + ":", '', hs_user_machinedir)
            logging.info("HS COPYDIR = %s", hs_copydir)

            self.set_default_copydir(hs_copydir)

            hs_dest_mchn = re.sub(r'\w+@', '', hs_ssh_access)
            logging.info("HS DESTINATION HOST: %s", hs_dest_mchn)
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

        # convert start/stop times to DAQ ticks
        start_ticks = HsUtil.get_daq_ticks(self.jan1(), alert_start)
        stop_ticks = HsUtil.get_daq_ticks(self.jan1(), alert_stop)

        hs_dbfile = self.__db_path(hs_sourcedir)
        if os.path.exists(hs_dbfile):
            src_tuples_list = self.__query_requested_files(start_ticks,
                                                           stop_ticks,
                                                           hs_sourcedir,
                                                           sleep_secs)
            if src_tuples_list is None:
                logging.error("No data found between %s and %s", alert_start,
                              alert_stop)
        else:
            src_tuples_list = self.__find_requested_files(alert_start,
                                                          alert_stop,
                                                          hs_sourcedir,
                                                          sleep_secs)
        if src_tuples_list is None:
            return None

        (_, timetag) = self.get_timetag_tuple(hs_copydir, alert_start)

        # temporary directory for relevant hs data copy
        tmp_dir = self.__staging_dir(timetag)

        if not os.path.exists(tmp_dir):
            try:
                self.mkdir(self.fullhost, tmp_dir)
                logging.info("created tmp dir for relevant hs files")
            except StandardError, err:
                logging.error("Couldn't create %s: %s", tmp_dir, err)
                return None

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
        return self.__rsync_files(alert_start, alert_stop, copy_files_list,
                                  sleep_secs, hs_dest_mchn, hs_copydir,
                                  hs_user_machinedir, hs_ssh_access, sender,
                                  make_remote_dir=make_remote_dir)

    def rsync(self, source, target, bwlimit=None, log_format=None,
              relative=True):
        bwstr = "" if bwlimit is None else " --bwlimit=%d" % 300
        logstr = "" if log_format is None \
                 else " --log-format=\"%s\"" % log_format
        relstr = "" if relative else " --no-relative"

        if source == "":
            return None, "No source specified"
        elif target == "":
            return None, "No target specified"
        elif target[-1] != "/":
            # make sure `rsync` knows the target should be a directory
            target += "/"

        rsync_cmd = "nice rsync -avv %s%s%s %s \"%s\"" % \
                    (bwstr, logstr, relstr, source, target)

        logging.info("rsync command: %s", rsync_cmd)
        hs_rsync = subprocess.Popen(rsync_cmd, shell=True, bufsize=256,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)

        hs_rsync_out = hs_rsync.stdout.readlines()
        hs_rsync_err = hs_rsync.stderr.readlines()

        hs_rsync.stdout.flush()
        hs_rsync.stderr.flush()

        return (hs_rsync_out, hs_rsync_err)

    def rsync_target(self, hs_user_machinedir, _, timetag_dir, hs_copydest):
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
