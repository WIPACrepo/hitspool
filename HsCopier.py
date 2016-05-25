#!/usr/bin/env python


import logging
import os
import re
import subprocess

from HsException import HsException


class Copier(object):
    def __init__(self, cmdbase, source_list, rmt_user=None, rmt_host=None,
                 rmt_dir=None, rmt_subdir=None, make_remote_dir=False,
                 parse_outstream=True):
        self.__is_valid = False
        self.__filename = None
        self.__size = None
        self.__received = None
        self.__bps = None
        self.__speedup = None

        if source_list is None or len(source_list) == 0:
            raise HsException("No source specified")
        if rmt_host is None or rmt_host == "":
            raise HsException("No remote host specified")
        if rmt_dir is None or rmt_dir == "":
            raise HsException("No remote directory specified")

        if make_remote_dir:
            self.make_remote_directory(rmt_user, rmt_host, rmt_dir)

        source_str = " ".join(source_list)
        target = self.target(rmt_user, rmt_host, rmt_dir, rmt_subdir)
        full_cmd = "%s %s \"%s\"" % (cmdbase, source_str, target)

        logging.info("command: %s", full_cmd)
        #import sys; print >>sys.stderr, "CMD: " + full_cmd
        proc = subprocess.Popen(full_cmd, shell=True, bufsize=256,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        out_lines = proc.stdout.readlines()
        err_lines = proc.stderr.readlines()

        proc.stdout.flush()
        proc.stderr.flush()

        rtncode = proc.wait()

        if parse_outstream:
            # rsync sends verbose output to stdout
            out_stream = out_lines
            err_stream = err_lines
        else:
            # scp sends verbose output to stderr
            out_stream = err_lines
            err_stream = out_lines

        if rtncode != 0 or len(err_stream) > 0:
            raise HsException("failed to copy %s to \"%s\" (rtn=%d):\n%s" %
                              (source_list, target, rtncode, err_lines))

        result = self.parse_output(out_stream, target=target)
        if result is not None:
            self.__is_valid = True
            (self.__filename, self.__size, self.__received, self.__bps,
             self.__speedup) = result

    @classmethod
    def make_remote_directory(cls, rmt_user, rmt_host, rmt_dir):
        if rmt_user is None:
            rstr = rmt_host
        else:
            rstr = "%s@%s" % (rmt_user, rmt_host)
        cmd = ["ssh", rstr, "mkdir", "-p", "\"%s\"" % rmt_dir]
        #print "CMD: " + str(cmd)
        rtncode = subprocess.call(cmd)
        if rtncode != 0:
            raise HsException("Cannot create %s:%s" % (rstr, rmt_dir))

    def parse_output(self, lines, target=None):
        raise NotImplementedError()

    @property
    def size(self):
        return self.__size

    def target(self, rmt_user, rmt_host, rmt_dir, rmt_subdir,
               use_daemon=False):
        raise NotImplementedError()

class CopyUsingRSync(Copier):
    # regular expression used to get the number of bytes sent by rsync
    SENT_PAT = re.compile(r"sent (\d[\d,]*) bytes\s+received (\d[\d,]*) bytes"
                          r"\s+(\d[\d,]*(\.\d[\d,]*)?) bytes\/sec")
    # regular expression used to get the number of bytes sent by rsync
    TOTAL_PAT = re.compile(r"total size is (\d[\d,]*)\s+"
                           r"speedup is (\d[\d,]*(\.\d[\d,]*)?)")
    FIELDS = ('matches', 'hash_hits', 'false_alarms', 'data')

    def __init__(self, source_list, rmt_user, rmt_host, rmt_dir,
                 rmt_subdir, use_daemon, bwlimit=None, log_format="%i%n%L",
                 relative=True):
        self.__use_daemon = use_daemon

        # assemble arguments
        bwstr = "" if bwlimit is None else " --bwlimit=%d" % bwlimit
        logstr = "" if log_format is None \
                 else " --log-format=\"%s\"" % log_format
        relstr = "" if relative else " --no-relative"

        # build command
        cmd = "nice rsync -avv %s%s%s" % (bwstr, logstr, relstr)

        super(CopyUsingRSync, self).__init__(cmd, source_list, rmt_user,
                                             rmt_host, rmt_dir, rmt_subdir)

    def target(self, rmt_user, rmt_host, rmt_dir, rmt_subdir):
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

    @classmethod
    def parse_output(cls, lines, target=None):
        srcfile = None
        filename = None
        size = None
        rcvd = None
        bps = None
        speedup = None
        ssh_error = False

        for line in lines:
            if line.endswith("\\n"):
                line = line[:-2]

            if line.startswith("opening tcp connection to 2ndbuild") or \
               line.startswith("sending incremental file list") or line == "":
                continue

            if line.startswith("sending daemon args: "):
                args = line[21:].split()
                srcfile = args[-2]
                filename = args[-1]

                if srcfile != ".":
                    logging.error("Unexpected source file \"%s\""
                                  " for target \"%s\"", srcfile, filename)

                continue

            if line.startswith("opening connection using:"):
                args = line[26:].split()
                srcfile = args[-2]
                filename = args[-1]

                if srcfile != ".":
                    logging.error("Unexpected source file \"%s\""
                                  " for target \"%s\"", srcfile, filename)

                continue

            if line.startswith("created directory "):
                if filename is None:
                    if target is not None:
                        filename = target
                    else:
                        logging.error("Saw \"created directory\" before"
                                      " \"sending\"")
                        continue

                args = line.split()
                if filename.startswith("hitspool/"):
                    rmtdir = filename[8:]
                elif not filename.startswith("/"):
                    rmtdir = "/" + filename
                else:
                    rmtdir = filename
                if rmtdir.endswith("/"):
                    rmtdir = rmtdir[:-1]
                if args[-1] != rmtdir:
                    logging.error("Unexpected remote directory \"%s\""
                                  " (should be \"%s\")", args[-1], rmtdir)
                continue

            if line.startswith("total: "):
                for pair in line[7:].split():
                    (name, vstr) = pair.split('=')
                    if name not in cls.FIELDS:
                        logging.error("Unknown 'total' field \"%s\"", name)
                        continue

                    try:
                        value = int(vstr)
                    except:
                        logging.error("Bad value \"%s\" for 'total'"
                                      " field \"%s\"", vstr, name)
                        continue

                    if name != 'data':
                        if value != 0:
                            logging.error("Unexpected value %d for 'total'"
                                          " field \"%s\"", value, name)
                        continue

                    if value < 0:
                        logging.error("Illegal size \"%s\" for 'total'"
                                      " field \"%s\"", vstr, name)
                        continue

                    size = value
                    continue

            if line.startswith("sent "):
                m = cls.SENT_PAT.match(line)
                if m is None:
                    logging.error("??? " + line.rstrip())
                    continue

                try:
                    ssize = int(m.group(1).replace(",", ""))
                    rcvd = int(m.group(2).replace(",", ""))
                    bps = float(m.group(3).replace(",", ""))
                except:
                    logging.error("Bad value(s) in " + line.rstrip())
                    continue

                #if ssize != size:
                #    logging.error("Total size was %d, sent size is %d", size,
                #                  ssize)

                continue

            if line.startswith("total size is "):
                m = cls.TOTAL_PAT.match(line)
                if m is None:
                    logging.error("??? " + line.rstrip())
                    continue

                try:
                    tsize = int(m.group(1).replace(",", ""))
                    speedup = float(m.group(2).replace(",", ""))
                except:
                    logging.error("Bad value(s) in " + line.rstrip())
                    continue

                if tsize != size and size != 0:
                    logging.error("Total size was %d, final size is %d", size,
                                  tsize)

                continue

            if line.startswith("ssh_exchange_identification:"):
                ssh_error = True
                continue

            if ssh_error:
                if line.startswith("rsync: connection unexpectedly") or \
                   line.startswith("rsync error: unexplained error"):
                    continue

        if filename is None or size is None:
            if not ssh_error:
                logging.error("Bad lines %s", lines)
            return None

        return (filename, size, rcvd, bps, speedup)


class CopyUsingSCP(Copier):
    def __init__(self, source_list, rmt_user, rmt_host, rmt_dir, rmt_subdir,
                 bwlimit=None, cipher=None, log_format="%i%n%L",
                 make_remote_dir=True, relative=True):
        bwstr = "" if bwlimit is None else " -l %d" % (bwlimit * 8)
        cstr = "" if cipher is None else " -c %s" % cipher

        cmd = "scp -v -B" + bwstr + cstr

        super(CopyUsingSCP, self).__init__(cmd, source_list, rmt_user=rmt_user,
                                           rmt_host=rmt_host, rmt_dir=rmt_dir,
                                           rmt_subdir=rmt_subdir,
                                           make_remote_dir=make_remote_dir,
                                           parse_outstream=False)

    def target(self, rmt_user, rmt_host, rmt_dir, rmt_subdir):
        target = '%s@%s:%s' % (rmt_user, rmt_host, rmt_dir)
        if target[-1] != "/.":
            # make sure `scp` knows the target should be a directory
            target += "/."
        return target

    @classmethod
    def parse_output(cls, lines, target=None):
        filename = None
        size = None
        bps = None
        ssh_error = False

        sink_size = None
        for line in lines:
            if line.endswith("\\n"):
                line = line[:-2]

            #print "::%s:: %s" % (sink_size, line.rstrip())
            if line.find("Sending command: scp ") >= 0:
                flds = line.split()
                filename = flds[-1]
                if filename.endswith("/."):
                    filename = filename[:-2]
                continue

            if line.startswith("scp: "):
                logging.error("SCP error: " + line[5:].rstrip())
                break

            if sink_size is not None:
                if line.find("rtype exit-status reply ") > 0:
                    if size is None:
                        size = sink_size
                    else:
                        size += sink_size

                    sink_size = None
                continue

            if line.find("Sink: ") >= 0:
                flds = line.split()
                sink_size = int(flds[2])
                sink_name = flds[3]

                continue

            if line.find("Bytes per second: ") >= 0:
                flds = line.split()
                if not flds[4].endswith(","):
                    logging.error("Bad BPS line: " + line.rstrip())
                else:
                    bps = float(flds[4][:-1])
                continue

            # handle local copies (mostly for unit tests?)
            if line.find("Executing: cp ") >= 0:
                flds = line.split()
                filename = flds[-1]
                if filename.endswith("/."):
                    filename = filename[:-2]

                fsize = os.path.getsize(flds[-2])
                if size is None:
                    size = fsize
                else:
                    size += fsize

                continue

            if line.startswith("ssh_exchange_identification:"):
                ssh_error = True
                continue

            if ssh_error:
                if line.startswith("rsync: connection unexpectedly") or \
                   line.startswith("rsync error: unexplained error"):
                    continue

        if filename is None or size is None:
            if not ssh_error and filename is None:
                logging.error("Bad lines %s" % lines)
            return None

        return (filename, size, None, bps, None)
