#!/usr/bin/env python
"""
Copy files to a remote machine
"""


import logging
import os
import re
import select
import subprocess
import sys
import time

import HsMessage

from HsException import HsException


class Copier(object):
    "Copy files to a remote system"

    def __init__(self, cmd=None, rmt_user=None, rmt_host=None,
                 rmt_dir=None, rmt_subdir=None, make_remote_dir=False):
        if rmt_host is None or rmt_host == "":
            raise HsException("No remote host specified")
        if rmt_dir is None or rmt_dir == "":
            raise HsException("No remote directory specified")

        if make_remote_dir:
            self.make_remote_directory(rmt_user, rmt_host, rmt_dir)

        self.__target = self.build_target(rmt_user, rmt_host, rmt_dir,
                                          rmt_subdir)
        self.__cmd = cmd

        self.__size = None
        self.__unknown_count = 0

        self.__last_chunk_size = None

    def build_target(self, rmt_user, rmt_host, rmt_dir, rmt_subdir):
        raise NotImplementedError()

    def copy(self, source_list, request=None, update_status=None):
        if source_list is None or len(source_list) == 0:
            raise HsException("No source specified")

        failed = []
        for src in source_list:
            rtncode = self.copy_one(src)
            if rtncode != 0:
                logging.error("failed to copy %s to \"%s\" (rtn=%d,"
                              " unknown=%d)", src, self.__target, rtncode,
                              self.__unknown_count)
                failed.append(src)
                self.__unknown_count = 0

            if update_status is not None and request is not None:
                update_status(request.copy_dir, request.destination_dir,
                              HsMessage.WORKING)

        result = self.summarize()
        if result is not None:
            (_, size, _, _, _) = result
            if self.__size is None:
                self.__size = size
            else:
                self.__size += size

        return failed

    def copy_one(self, filename):
        full_cmd = "%s %s \"%s\"" % (self.__cmd, filename, self.__target)

        logging.info("command: %s", full_cmd)
        if sys.version_info < (3, 2):
            # pylint: disable=subprocess-popen-preexec-fn
            proc = subprocess.Popen(full_cmd, shell=True, bufsize=256,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    preexec_fn=os.setsid)
        else:
            # use thread-safe 'start_new_session' to start a new session
            proc = subprocess.Popen(full_cmd, shell=True, bufsize=256,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    start_new_session=True)

        num_err = 0
        while True:
            reads = [proc.stdout.fileno(), proc.stderr.fileno()]
            try:
                ret = select.select(reads, [], [])
            except select.error:
                # ignore a single interrupt
                if num_err > 0:
                    break
                num_err += 1
                continue
            
            for fin in ret[0]:
                if fin == proc.stdout.fileno():
                    self.parse_line(proc.stdout.readline().decode("utf-8"))
                if fin == proc.stderr.fileno():
                    self.parse_line(proc.stderr.readline().decode("utf-8"))

            if proc.poll() is not None:
                    break

        # consume remaining buffered output
        line = proc.stdout.readline().decode("utf-8")
        while line != "":
            self.parse_line(line)
            line = proc.stdout.readline().decode("utf-8")
        line = proc.stderr.readline().decode("utf-8")
        while line != "":
            self.parse_line(line)
            line = proc.stderr.readline().decode("utf-8")


        proc.stdout.close()
        proc.stderr.close()

        return proc.wait()

    def log_unknown_line(self, method, line):
        logging.error("Unknown %s line: %s", method, line.rstrip())
        self.__unknown_count += 1

    @classmethod
    def make_remote_directory(cls, rmt_user, rmt_host, rmt_dir):
        if rmt_user is None:
            rstr = rmt_host
        else:
            rstr = "%s@%s" % (rmt_user, rmt_host)
        cmd = ["ssh", "-o StrictHostKeyChecking=no", "-o UserKnownHostsFile=/dev/null", rstr, "if [ ! -d \"%s\" ]; then mkdir -p \"%s\"; fi" %
               (rmt_dir, rmt_dir)]
        for _ in range(6):
            # this fails occasionally, possibly due to a wave of processes
            #  all trying to create subdirectories in the same directory
            rtncode = subprocess.call(cmd)
            if rtncode == 0:
                return

            # wait for this wave to die down before trying again
            time.sleep(0.3)
        raise HsException("Cannot create %s:%s (rtncode=%d)" %
                          (rstr, rmt_dir, rtncode))

    def parse_line(self, line):
        raise NotImplementedError()

    @property
    def size(self):
        return self.__size

    def summarize(self):
        raise NotImplementedError()

    @property
    def target(self):
        return self.__target


class CopyUsingRSync(Copier):
    "Copy files to the remote system using 'rsync'"

    # regular expression used to get the number of bytes sent by rsync
    SENT_PAT = re.compile(r"sent (\d[\d,]*) bytes\s+received (\d[\d,]*) bytes"
                          r"\s+(\d[\d,]*(\.\d[\d,]*)?) bytes\/sec")
    # regular expression used to get the number of bytes sent by rsync
    TOTAL_PAT = re.compile(r"total size is (\d[\d,]*)\s+"
                           r"speedup is (\d[\d,]*(\.\d[\d,]*)?)")
    FIELDS = ('matches', 'hash_hits', 'false_alarms', 'data')

    def __init__(self, rmt_user, rmt_host, rmt_dir, rmt_subdir,
                 use_daemon=False, bwlimit=None, log_format="%i%n%L",
                 relative=True):
        self.__use_daemon = use_daemon

        self.__filename = None
        self.__size = None
        self.__rcvd = None
        self.__bps = None
        self.__speedup = None
        self.__ssh_error = False

        cmd = self.__build_command(bwlimit, log_format, relative)
        super(CopyUsingRSync, self).__init__(cmd=cmd, rmt_user=rmt_user,
                                             rmt_host=rmt_host,
                                             rmt_dir=rmt_dir,
                                             rmt_subdir=rmt_subdir)

    @classmethod
    def __build_command(cls, bwlimit, log_format, relative):
        # assemble arguments
        bwstr = "" if bwlimit is None or bwlimit <= 0 \
                else " --bwlimit=%d" % bwlimit
        logstr = "" if log_format is None \
                 else " --log-format=\"%s\"" % log_format
        relstr = "" if relative else " --no-relative"

        return "nice rsync -avv %s%s%s" % (bwstr, logstr, relstr)

    def build_target(self, rmt_user, rmt_host, rmt_dir, rmt_subdir):
        if self.__use_daemon:
            # rsync daemon maps hitspool/ to /mnt/data/pdaqlocal/HsDataCopy/
            target = '%s@%s::hitspool/%s/' % \
                     (rmt_user, rmt_host, rmt_subdir)
        else:
            target = rmt_dir
        if target is None or target == "":
            raise HsException("No target specified")
        if target[-1] != "/":
            # make sure `rsync` knows the target should be a directory
            target += "/"
        return target

    def parse_line(self, line):
        if line.endswith("\\n"):
            line = line[:-2]

        if line.startswith("opening tcp connection to 2ndbuild") or \
           line.startswith("sending incremental file list") or \
           line.startswith("sending incremental file list") or \
           line.startswith(".f") or \
           line.startswith(">f") or \
           line.startswith("<f") or \
           line == "":
            return

        if line.startswith("delta-transmission disabled for local"):
            return

        # NOTE: 
        # SL6 rsync (version 3.0.6  protocol version 30)
        #sending daemon args: --server -vvlogDtpre.iLsfxC "--log-format=%i" --bwlimit=1000 . hitspool/TJB_20211005_162545_ichub21/  (6 args)
        #
        # Alma8 rsync (rsync  version 3.1.3  protocol version 31)
        # sending daemon args: --server -vvlogDtpre.iLsfxC "--log-format=%i" --bwlimit=1000 . hitspool/TJB_20211005_162545_ichub29/
        splitidx = None
        if line.startswith("sending daemon args: "):
            splitidx = 21
        elif line.startswith("opening connection using:"):
            splitidx = 26
        if splitidx is not None:
 
            # NOTE:
            # 
            # SL6 rsync (version 3.0.6  protocol version 30)
            # sending daemon args: --server -vvlogDtpre.iLsfxC "--log-format=%i" --bwlimit=1000 . hitspool/TJB_20211005_162545_ichub21/  (6 args)
            #
            # Alma8 rsync (rsync  version 3.1.3  protocol version 31)
            # sending daemon args: --server -vvlogDtpre.iLsfxC "--log-format=%i" --bwlimit=1000 . hitspool/TJB_20211005_162545_ichub29/
            if line.rstrip().endswith("(6 args)"):
                args =line[splitidx:-9].split()
            else:
                args = line[splitidx:].split()
            src = args[-2]
            self.__filename = args[-1]

            if src != ".":
                logging.error("Unexpected source file \"%s\" for"
                              " target \"%s\"", src, self.__filename)
            return

        if line.startswith("created directory "):
            if self.__filename is None:
                if self.target is not None:
                    self.__filename = self.target
                else:
                    logging.error("Saw \"created directory\" before"
                                  " \"sending\"")
                    return

            # get the remote directory path
            if self.__filename.startswith("hitspool/"):
                rmtdir = self.__filename[8:]
            elif not self.__filename.startswith("/"):
                rmtdir = "/" + self.__filename
            else:
                rmtdir = self.__filename
            if rmtdir.endswith("/"):
                rmtdir = rmtdir[:-1]

            args = line.split()
            if args[-1] != rmtdir:
                logging.error("Unexpected remote directory \"%s\""
                              " (should be \"%s\")", args[-1], rmtdir)
            return

        if line.startswith("total: "):
            for pair in line[7:].split():
                (name, vstr) = pair.split('=')
                if name not in self.FIELDS:
                    logging.error("Unknown 'total' field \"%s\"", name)
                    return

                try:
                    value = int(vstr)
                except:
                    logging.error("Bad value \"%s\" for 'total'"
                                  " field \"%s\"", vstr, name)
                    return

                if name != 'data':
                    if value != 0:
                        logging.error("Unexpected value %d for 'total'"
                                      " field \"%s\"", value, name)
                        return

                if value < 0:
                    logging.error("Illegal size \"%s\" for 'total'"
                                  " field \"%s\"", vstr, name)
                    return

                self.__last_chunk_size = value

                if self.__size is None:
                    self.__size = value
                else:
                    self.__size += value
                continue

            # done processing 'total' line
            return

        if line.startswith("sent "):
            mtch = self.SENT_PAT.match(line)
            if mtch is None:
                logging.error("??? %s", line.rstrip())
                return

            try:
                # sentsize = int(m.group(1).replace(",", ""))
                self.__rcvd = int(mtch.group(2).replace(",", ""))
                self.__bps = float(mtch.group(3).replace(",", ""))
            except:
                logging.error("Bad value(s) in %s", line.rstrip())

            return

        if line.startswith("total size is "):
            mtch = self.TOTAL_PAT.match(line)
            if mtch is None:
                logging.error("??? %s", line.rstrip())
                return

            try:
                tsize = int(mtch.group(1).replace(",", ""))
                self.__speedup = float(mtch.group(2).replace(",", ""))
            except:
                logging.error("Bad value(s) in %s", line.rstrip())
                return

            if tsize != self.__last_chunk_size:
                logging.error("Data chunk size was %d, final size is %d",
                              self.__last_chunk_size, tsize)
                return
            return

        if line.startswith("ssh_exchange_identification:"):
            self.__ssh_error = True
            return

        if self.__ssh_error:
            if line.startswith("rsync: connection unexpectedly") or \
               line.startswith("rsync error: unexplained error"):
                return

        if len(line.strip()) > 0:
            self.log_unknown_line("rsync", line)

    def summarize(self):
        if self.__filename is None or self.__size is None:
            if not self.__ssh_error:
                logging.error("Failed to find filename/size")
            return None

        return (self.__filename, self.__size, self.__rcvd, self.__bps,
                self.__speedup)


class CopyUsingSCP(Copier):
    "Copy files to the remote system using 'scp'"

    def __init__(self, rmt_user, rmt_host, rmt_dir, rmt_subdir,
                 bwlimit=None, cipher=None, log_format=None,
                 make_remote_dir=True, relative=None):

        self.__filename = None
        self.__size = None
        self.__bps = None
        self.__ssh_error = False
        self.__sink_size = None

        cmd = self.__build_command(bwlimit, cipher)
        super(CopyUsingSCP, self).__init__(cmd=cmd, rmt_user=rmt_user,
                                           rmt_host=rmt_host,
                                           rmt_dir=rmt_dir,
                                           rmt_subdir=rmt_subdir,
                                           make_remote_dir=make_remote_dir)
    @classmethod
    def __build_command(cls, bwlimit, cipher):
        bwstr = "" if bwlimit is None or bwlimit <= 0 \
                else " -l %d" % (bwlimit * 8)
        cstr = "" if cipher is None else " -c %s" % cipher

        return "nice scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -v -B" + bwstr + cstr

    def build_target(self, rmt_user, rmt_host, rmt_dir, rmt_subdir):
        target = '%s@%s:%s' % (rmt_user, rmt_host, rmt_dir)
        if target[-1] != "/.":
            # make sure `scp` knows the target should be a directory
            target += "/."
        return target

    def parse_line(self, line):
        if line.endswith("\\n"):
            line = line[:-2]

        if line.find("Sending command: scp ") >= 0:
            flds = line.split()
            self.__filename = flds[-1]
            if self.__filename.endswith("/."):
                self.__filename = self.__filename[:-2]
            return

        if line.startswith("scp: "):
            logging.error("SCP error: %s", line[5:].rstrip())
            return

        if line.find("Sink: ") >= 0:
            flds = line.split()
            if len(flds) < 4:
                logging.error("Malformed SCP line: %s", line.rstrip())
                return

            try:
                tmp_size = int(flds[2])
            except:
                logging.error("Bad size \"%s\" in SCP line: %s", flds[2],
                              line.rstrip())
                return

            if self.__sink_size is None:
                self.__sink_size = tmp_size
            else:
                self.__sink_size += tmp_size
            return

        if self.__sink_size is not None:
            if line.find("rtype exit-status reply ") > 0:
                if self.__size is None:
                    self.__size = self.__sink_size
                else:
                    self.__size += self.__sink_size

                self.__sink_size = None
            return

        if line.find("Bytes per second: ") >= 0:
            flds = line.split()
            if not flds[4].endswith(","):
                logging.error("Bad BPS line: %s", line.rstrip())
            else:
                self.__bps = float(flds[4][:-1])
            return

        if line.find("Executing: ") >= 0:
            flds = line.split()
            if flds[1] == "cp":
                # handle local copies (mostly for unit tests?)
                self.__filename = flds[-1]
                if self.__filename.endswith("/."):
                    self.__filename = self.__filename[:-2]

                fsize = os.path.getsize(flds[-2])
                if self.__size is None:
                    self.__size = fsize
                else:
                    self.__size += fsize

            return

        if line.startswith("ssh_exchange_identification:"):
            self.__ssh_error = True
            return

        if self.__ssh_error:
            if line.startswith("rsync: connection unexpectedly") or \
               line.startswith("rsync error: unexplained error"):
                return

        if line.startswith("OpenSSH_") or \
           line.startswith("debug1: ") or \
           line.startswith("Authenticated to ") or \
           line.startswith("Sending file modes: ") or \
           line.startswith("Credentials cache file") or \
           line.startswith("No Kerberos") or \
           (line.startswith("Warning: Permanently added") and line.rstrip().endswith("to the list of known hosts.") ) or \
           line.startswith("Transferred: "):
            return

        if len(line.strip()) > 0:
            self.log_unknown_line("scp", line)

    def summarize(self):
        if self.__filename is None or self.__size is None:
            if not self.__ssh_error and self.__filename is None:
                logging.error("Failed to find filename/size")
            return None

        return (self.__filename, self.__size, None, self.__bps, None)
