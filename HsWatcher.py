#!/usr/bin/env python

"""
HsWatcher.py
David Heereman

Watching the HitSpool interface services.
Restarts the watched process in case.
"""
import getpass
import logging
import os
import re
import signal
import subprocess
import sys
import time
import zmq

import HsConstants
import HsUtil

from HsBase import HsBase
from HsException import HsException


def add_arguments(parser):
    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hswatcher.log")

    parser.add_argument("-H", "--host", dest="host",
                        help="Forced host name, used for debugging")
    parser.add_argument("-k", "--kill", dest="kill",
                        action="store_true", default=False,
                        help="Kill the watched program")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)


class Daemon(object):
    def __init__(self, basename, executable):
        self.check_executable(basename, executable)

        self.__basename = basename
        self.__executable = executable

    def __str__(self):
        return "%s(%s)" % (self.__basename, self.__executable)

    @property
    def basename(self):
        return self.__basename

    def check_executable(self, basename, executable):
        if not os.path.exists(executable):
            raise SystemError("Cannot find %s; giving up", basename)

    def daemonize(self, stdin=None, stdout=None, stderr=None):
        """
        From http://www.jejik.com/articles/2007/02/\
            a_simple_unix_linux_daemon_in_python/

        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # return so parent can finish its work
                return None
        except OSError, e:
            raise SystemExit("fork #1 failed: %d (%s)\n" %
                             (e.errno, e.strerror))

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                raise SystemExit(0)
        except OSError, e:
            raise SystemExit("fork #2 failed: %d (%s)\n" %
                             (e.errno, e.strerror))

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(stdin if stdin is not None else "/dev/null", 'r')
        so = file(stdout if stdout is not None else "/dev/null", 'a+')
        se = file(stderr if stderr is not None else "/dev/null", 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # start the new process
        os.execv(sys.executable, (sys.executable, self.__executable))

    def list_processes(self):
        """
        List all process IDs.
        Return a tuple with the integer PID, the program name, and
        a list of all arguments (or None if there are no arguments)
        """
        proc = subprocess.Popen(["ps", "x", "-o", "pid,command"],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        pid = None
        try:
            for line in proc.stdout:
                if self.__basename in line:
                    flds = line.split()

                    # convert process ID to its integer value
                    try:
                        pid = int(flds[0])
                    except ValueError:
                        logging.error("Bad integer PID \"%s\" in \"%s\"",
                                      flds[0], line.rstrip())
                        continue

                    if len(flds) == 2:
                        args = None
                    else:
                        args = flds[2:]

                    yield (pid, flds[1], args)

            proc.stdout.close()
        finally:
            proc.wait()

    def run(self):
        subprocess.call((sys.executable, self.__executable, ))


class Watchee(Daemon):
    def __init__(self, basename):
        try:
            path = self.__find_executable_path()
        except:
            path = HsConstants.SANDBOX_INSTALLED
            logging.exception("Cannot find %s path; using %s", basename, path)

        executable = os.path.join(path, basename + ".py")

        super(Watchee, self).__init__(basename, executable)

    def __find_executable_path(self):
        return os.path.dirname(os.path.realpath(__file__))

    def get_pids(self):
        """
        Find the process ID for this executable
        """
        pids = []
        for pid, name, args in self.list_processes():
            # screen out things like 'vi SomeProgram.py'
            if (self.basename not in name and
                ("ython" not in name or
                 (args is not None and self.basename not in args[0]))):
                continue

            pids.append(pid)

        return pids

    def kill_all(self):
        """
        Stop all instances of this program
        """
        num_killed = 0
        for sig in (signal.SIGTERM, signal.SIGKILL):
            pids = self.get_pids()
            if len(pids) == 0:
                break

            # go on a killing spree
            for pid in pids:
                try:
                    os.kill(pid, sig)
                    num_killed += 1
                except OSError, err:
                    errstr = str(err)
                    if errstr.find("No such process") == 0:
                        logging.error("Cannot kill %s at PID %d: %s",
                                      self.basename, pid, errstr)
                        return None

            # give processes a chance to die
            time.sleep(1.0)

        return num_killed


class HsWatcher(HsBase):
    STATUS_PREFIX = "Status: "
    STATUS_STOPPED = "STOPPED"
    STATUS_STARTED = "STARTED"
    STATUS_RUNNING = "RUNNING"
    STATUS_ERROR = "!ERROR!"

    def __init__(self, host=None):
        super(HsWatcher, self).__init__(host=host)

        if self.is_cluster_sps or self.is_cluster_spts:
            expcont = "expcont"
        else:
            expcont = "localhost"

        self.__context = zmq.Context()
        self.__i3socket = self.create_i3socket(expcont)

    def __get_halted_time(self, logfile, num_to_check=4):
        lastline = None
        count = 0
        for line in self.tail(logfile, lines=num_to_check,
                              search_string=self.STATUS_PREFIX):
            if self.STATUS_RUNNING in line or self.STATUS_STARTED in line:
                return None
            lastline = line
            count += 1

        if count < num_to_check:
            return None

        if lastline is not None:
            time_pattern = r'^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}).*'
            m = re.search(time_pattern, lastline)
            if m is not None:
                return m.group(1)

        return "???"

    def __send_error(self, program, message):
        """
        Send error alert.
        """
        header = "ERROR HsInterface Alert: %s@%s" % (program, self.shorthost)
        description = "HsInterface service error notice"

        json = HsUtil.assemble_email_dict(HsConstants.ALERT_EMAIL_DEV, header,
                                          description, message)

        self.__i3socket.send_json(json)

    def __send_if_halted(self, program, logfile):
        '''
        Check for how long HsWatcher is reporting STOPPED state.
        In case service is in STOPPED state for too long: send alert to i3live
        '''
        halted_time = self.__get_halted_time(logfile)
        if halted_time is None:
            return False

        header = "HALTED HsInterface Alert: %s@%s" % (program, self.shorthost)
        description = "HsInterface service halted"
        message = "%s@%s in STOPPED state more than 1h.\n" \
                  "Last seen running before %s" % \
                  (program, self.shorthost, halted_time)

        json = HsUtil.assemble_email_dict(HsConstants.ALERT_EMAIL_DEV,
                                          header, description, message)

        self.__i3socket.send_json(json)

        return True

    def __send_recovery(self, program, logfile):
        """
        Send message when program is started
        """
        # find entries from last 2 HsWatcher status report
        logmsgs = self.tail(logfile, lines=2,
                            search_string=self.STATUS_PREFIX)

        header = "RECOVERY HsInterface Alert: %s@%s" % \
                 (program, self.shorthost)
        description = "HsInterface service recovery notice"
        mlines = ["%s@%s recovered by HsWatcher:" %
                  (program, self.shorthost), ]
        mlines += logmsgs

        json = HsUtil.assemble_email_dict(HsConstants.ALERT_EMAIL_DEV,
                                          header, description,
                                          "\n".join(mlines))

        self.__i3socket.send_json(json)

    def __send_stopped(self, program):
        """
        Send STOPPED alert
        """
        header = "STOPPED HsInterface Alert: %s@%s" % \
                 (program, self.shorthost)
        description = "HsInterface service stopped"
        message = "%s@%s is STOPPED" % (program, self.shorthost)

        json = HsUtil.assemble_email_dict(HsConstants.ALERT_EMAIL_DEV,
                                          header, description, message)

        self.__i3socket.send_json(json)

    def check(self, logpath, sleep_secs=5.0):
        watchee = self.get_watchee()

        pids = watchee.get_pids()

        status = None
        errmsg = None

        if len(pids) == 1:
            status = self.STATUS_RUNNING
        else:
            if len(pids) > 1:
                if watchee.kill_all() is None:
                    raise SystemExit(1)
                logging.error("Found multiple copies of %s;"
                              " killing everything!", watchee.basename)

            watchee.daemonize()
            time.sleep(sleep_secs)
            pids = watchee.get_pids()
            if len(pids) == 0:
                status = self.STATUS_STOPPED
            elif len(pids) == 1:
                status = self.STATUS_STARTED
            else:
                status = self.STATUS_ERROR
                errmsg = "Found multiple copies of %s after starting" % \
                    watchee.basename
                logging.error(errmsg)

        logging.info("%s%s", self.STATUS_PREFIX, status)

        if status == self.STATUS_ERROR:
            self.__send_error(watchee.basename, errmsg)
        elif status == self.STATUS_STARTED:
            self.__send_recovery(watchee.basename, logpath)
        elif status != self.STATUS_RUNNING:
            if not self.__send_if_halted(watchee.basename, logpath):
                self.__send_stopped(watchee.basename)

    def close_all(self):
        self.__i3socket.close()
        self.__context.term()

    def create_i3socket(self, host):
        # Socket for I3Live on expcont
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, HsConstants.I3LIVE_PORT))
        logging.info("connect PUSH socket to i3live on %s port %d", host,
                     HsConstants.I3LIVE_PORT)
        return sock

    def create_watchee(self, basename):
        return Watchee(basename)

    def get_watchee(self):
        """
        Depending on which machines this HsWatcher runs,
        determine the processes it is responsible to watch.
        Watcher at 2ndbuild -->  HsSender
        Watcher at expcont -->  HsPublisher
        Watcher at hub -->  HsWorker
        """
        if "2ndbuild" in self.fullhost:
            return self.create_watchee("HsSender")
        elif "expcont" in self.fullhost:
            return self.create_watchee("HsPublisher")
        elif "hub" in self.fullhost or "scube" in self.fullhost:
            return self.create_watchee("HsWorker")
        elif "david" in self.fullhost:
            return self.create_watchee("HsWorker")

        raise HsException("Unrecognized host \"%s\"" % self.fullhost)

    @property
    def i3socket(self):
        return self.__i3socket

    def load_logfile(self, logpath):
        return open(logpath).readlines()

    def tail(self, path, lines=20, search_string="\n"):
        """
        Get the last lines of a file as efficiently as possible.
        Code adapted from a StackOverflow post
        """
        blocks = []

        f = open(path, "rb")
        try:
            total_lines_wanted = lines

            BLOCK_SIZE = 1024
            f.seek(0, 2)
            block_end_byte = f.tell()
            lines_to_go = total_lines_wanted
            block_number = -1

            # blocks of size BLOCK_SIZE, in reverse order starting
            # from the end of the file
            while lines_to_go > 0 and block_end_byte > 0:
                if block_end_byte - BLOCK_SIZE > 0:
                    # read the last block we haven't yet read
                    f.seek(block_number*BLOCK_SIZE, 2)
                    blocks.append(f.read(BLOCK_SIZE))
                else:
                    # file too small, start from begining
                    f.seek(0, 0)
                    # only read what was not read
                    blocks.append(f.read(block_end_byte))
                lines_found = blocks[-1].count(search_string)
                lines_to_go -= lines_found
                block_end_byte -= BLOCK_SIZE
                block_number -= 1
        finally:
            f.close()

        all_read_text = ''.join(reversed(blocks))
        return [line for line in all_read_text.splitlines()
                if search_string in line][-total_lines_wanted:]


if __name__ == "__main__":
    import argparse

    def main():
        p = argparse.ArgumentParser()

        add_arguments(p)

        args = p.parse_args()

        watcher = HsWatcher(host=args.host)

        logpath = watcher.init_logging(args.logfile, basename="hswatcher",
                                       basehost="testhub", both=False)

        if watcher.is_cluster_sps or watcher.is_cluster_spts:
            user = getpass.getuser()
            if user != "pdaq":
                raise SystemExit("Sorry user %s, you are not pdaq."
                                 " Please try again as pdaq." % user)

        try:
            if args.kill:
                if watcher.get_watchee().kill_all() is None:
                    raise SystemExit(1)
            else:
                watcher.check(logpath)
        finally:
            watcher.close_all()

    main()
