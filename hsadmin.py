#!/usr/bin/env python

from __future__ import print_function

import argparse
import getpass
import os
import select
import socket
import subprocess
import sys
import tempfile

from multiprocessing.pool import ThreadPool

from fabric2 import ThreadingGroup
from fabric2.exceptions import GroupException


# valid administration action keywords
ACTIONS = ("deploy", "install_crontab", "restart", "status", "stop",
           "remove_crontab")


class BaseSystem(object):
    HS_INSTALL_PATH = "/mnt/data/pdaqlocal/HsInterface"
    HS_SOURCE_PATH = "/home/pdaq/HsInterface/current"

    DEFAULT_BWLIMIT = 1000
    DEFAULT_LOG_FORMAT = "%i%n%L"

    RSYNC_EXCLUDE_LIST = (".svn", ".hg", ".hgignore", "*.log", "*.pyc", "*.swp")

    CRONTAB_TEXT = """
###HitSpool Jobs -- Please contact daq-dev@icecube.wisc.edu for questions
HitSpoolPath=/mnt/data/pdaqlocal/HsInterface/current
15 * * * * 	source \$HOME/env/bin/activate && python \$HitSpoolPath/HsWatcher.py
@reboot 	source \$HOME/env/bin/activate && python \$HitSpoolPath/HsWatcher.py
"""

    def __init__(self, targets=None):
        self.__group = ThreadingGroup(*targets)

    def __create_exclude_file(self):
        """
        Create a file containing patterns of files/directories
        which `rsync` should ignore
        """
        tmp_fd, tmpname = tempfile.mkstemp(text=True)

        text = "\n".join(self.RSYNC_EXCLUDE_LIST)
        os.write(tmp_fd, text.encode())

        os.close(tmp_fd)

        return tmpname

    def __get_hs_name(self, hostname):
        if hostname == "2ndbuild":
            return "HsSender"

        if hostname == "expcont":
            return "HsPublisher"

        if "hub" in hostname or hostname == "scube":
            return "HsWorker"

        raise Exception("Unknown HitSpool host \"%s\"" % hostname)

    def __report_failures(self, results, action):
        if not results.failed:
            return 0

        fail_list = None
        for conn, rslt in results.failed.items():
            if hasattr(rslt, "stdout"):
                print("%s ERROR from %s: %s" %
                      (action, conn.host, rslt.stdout.rstrip()),
                      file=sys.stderr)
            else:
                # handle partial results wrapped in a GroupException
                if fail_list is None:
                    fail_list = []
                fail_list.append(conn.host)
        if fail_list is not None:
            print("%s FAILED on %s" % (action, ", ".join(fail_list)),
                  file=sys.stderr)

        return len(results.failed)

    def __rsync_to_remote_host(self, cmdfmt, hostname=None):
        """
        Copy the current HitSpool release to a single remote host
        """

        # hack around the missing 'ThreadPool.startmap()'
        if hostname is None and \
          (isinstance(cmdfmt, tuple) or isinstance(cmdfmt, list)):
            hostname = cmdfmt[1]
            cmdfmt = cmdfmt[0]

        return self.__run_command(hostname, cmdfmt % hostname)

    def __run_command(self, hostname, cmd):
        """
        Run a local command, print any output,
        and return the process's return code (non-zero indicates an error)
        """

        proc = subprocess.Popen(cmd, bufsize=256, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
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
                    if fin == proc.stderr.fileno():
                        line = proc.stderr.readline().decode("utf-8")
                        if line != "":
                            print("%s ERROR: %s" % (line, ), file=sys.stderr)
                        continue

                    if fin != proc.stdout.fileno():
                        # ignore unknown file descriptors
                        continue

                    line = proc.stderr.readline().decode("utf-8")
                    if line != "":
                        print("[%s] %s" % (hostname, line))

                if proc.poll() is not None:
                    break

        finally:
            proc.stdout.close()
            proc.stderr.close()

        return proc.wait()

    def modify_crontab(self, delete=False, debug=False, quiet=False):
        delete_cmd = "egrep -v 'HitSpool|HSiface|HsWatcher|^\$'"
        if delete:
            verb = "delete"
            past_verb = "deleted"
            cmd = "crontab -l | %s | crontab -" % (delete_cmd, )
        else:
            verb = "install"
            past_verb = "installed"
            cmd = "(crontab -l | %s; echo \"%s\") | crontab -" % \
              (delete_cmd, self.CRONTAB_TEXT.strip(), )

        try:
            results = self.__group.run(cmd, hide=quiet)
        except GroupException as gex:
            for key, val in gex.result.items():
                if isinstance(val, Exception):
                    print("Failed to %s crontab on %s\n%s" %
                          (verb, key.host, val.result.tail("stderr")))
            results = gex.result
        num_failed = self.__report_failures(results,
                                            "%sCrontab" % verb.capitalize())

        if not quiet:
            print("%s crontab on %s hosts" %
                  (past_verb.capitalize(), len(self.__group) - num_failed))

    def make_directories(self, debug=False, quiet=False):
        """
        Create HitSpool directories on all remote systems
        """
        for subdir in "current", "logs":
            path = os.path.join(self.HS_INSTALL_PATH, subdir)
            results = self.__group.run("mkdir -p %s" % path, hide=True)
            num_failed = self.__report_failures(results, "MakeDirs")

            if not quiet:
                print("Created '%s' on %s hosts" %
                      (path, len(self.__group) - num_failed))

    def report_status(self):
        results = self.__group.run("ps axwww", hide=True)

        num_failed = self.__report_failures(results, "ProcessList")

        active = []
        inactive = []
        for conn, rslt in sorted(results.items(), key=lambda x: x[0].host):
            hostname = conn.host
            outlines = rslt.stdout

            target = self.__get_hs_name(hostname)
            if target in outlines:
                active.append(hostname)
            else:
                inactive.append(hostname)

        if len(self.__group) == len(active):
            print("All hosts are active")
        elif len(self.__group) == len(inactive):
            print("All hosts are INACTIVE")
        else:
            if len(active) > 0:
                print("Active: %s" % ", ".join(active))
            if len(inactive) > 0:
                print("Inactive: %s" % ", ".join(inactive))

    def rsync_install(self, bwlimit=None, no_relative=False, debug=False,
                      quiet=False):
        if bwlimit is None:
            bwlimit = self.DEFAULT_BWLIMIT

        exclude_file = self.__create_exclude_file()
        try:
            rmtpath = os.path.join(self.HS_INSTALL_PATH, "current")
            cmdfmt = "nice rsync -a --bwlimit=%s%s --exclude-from=%s" \
              " %s/." \
              " \"%%s:%s\"" % \
              (bwlimit, " --no-relative" if no_relative else "",
               exclude_file, self.HS_SOURCE_PATH, rmtpath)

            # build argument lists for all hosts
            params = [(cmdfmt, entry.host) for entry in self.__group]

            # run rsync on all remote hosts
            pool = ThreadPool()
            results = pool.map(self.__rsync_to_remote_host, params)

            synced = None
            for idx, rtncode in enumerate(results):
                hostname = params[idx][1]
                if rtncode != 0:
                    print("WARNING: %s exited with non-zero return code %s" %
                          (params[idx][1], rtncode), file=sys.stderr)
                else:
                    if synced is None:
                        synced = []
                    synced.append(hostname)

            if not quiet and synced is not None:
                print("Deployed to %d hosts" % len(synced))
        finally:
            os.unlink(exclude_file)

    def run_watcher(self, stop_process=False, debug=False, quiet=False):
        watcher = os.path.join(self.HS_INSTALL_PATH, "current", "HsWatcher.py")
        stop_arg = " -k" if stop_process else ""
        try:
            results = self.__group.run("python %s%s" % (watcher, stop_arg),
                                       hide=True)
        except GroupException as gex:
            for key, val in gex.result.items():
                if isinstance(val, Exception):
                    print("Failed to %s %s\n%s" %
                          ("stop" if stop_process else "start", key.host,
                           val.result.tail("stderr")))
            results = gex.result

        num_failed = self.__report_failures(results, "Stop" if stop_process
                                            else "Start")

        if not quiet and num_failed < len(self.__group):
            verb = "Stopped" if stop_process else "Started"
            print("%s HitSpool daemon on %s hosts" %
                  (verb, len(self.__group) - num_failed))

        return num_failed == 0


class SouthPoleSystem(BaseSystem):
    def __init__(self, targets=None):
        if targets is None or len(targets) == 0:
            targets = ["expcont", "2ndbuild"] + \
                ["ichub%02d" % i for i in range(1, 87)] + \
                ["ithub%02d" % i for i in range(1, 12)]

        super(SouthPoleSystem, self).__init__(targets=targets)


class TestSystem(BaseSystem):
    def __init__(self, targets=None):
        if targets is None or len(targets) == 0:
            targets = ["expcont", "2ndbuild", "ichub21", "ichub29", "ithub01", "scube"]

        super(TestSystem, self).__init__(targets=targets)


def add_arguments(parser, valid_actions):
    "Add command-line arguments"

    parser.add_argument("-q", "--quiet", dest="quiet",
                        action="store_true", default=False,
                        help="Do not print details")
    parser.add_argument("-x", "--debug", dest="debug",
                        action="store_true", default=False,
                        help="Print debugging messages")
    parser.add_argument("-t", "--target", dest="target", action='append',
                        help="Perform action only on specified target machine(s)")
    parser.add_argument("--no-stop", dest="no_stop",
                        action="store_true", default=False,
                        help="Do not stop first (e.g. for fresh deploy)")
    parser.add_argument("action", choices=valid_actions,
                        help="Action to perform on host(s)")


def main():
    parser = argparse.ArgumentParser()
    add_arguments(parser, ACTIONS)
    args = parser.parse_args()

    hostname = socket.gethostname()
    if ".icecube." not in hostname:
        raise SystemExit("Please run this on either SPS or SPTS")
    elif "access" not in hostname:
        raise SystemExit("Please run this from 'access'")

    username = getpass.getuser()
    if username != "pdaq":
        raise SystemExit("Please run this from the 'pdaq' account")

    if "wisc.edu" in hostname:
        system = TestSystem(targets=args.target)
    elif "usap.gov" in hostname:
        system = SouthPoleSystem(targets=args.target)
    else:
        raise SystemExit("Please run this on either SPS or SPTS")

    if args.action == "status":
        system.report_status()
        return

    if args.action == "install_crontab":
        system.modify_crontab(debug=args.debug, quiet=args.quiet)
        return

    if args.action == "remove_crontab":
        system.modify_crontab(delete=True, debug=args.debug, quiet=args.quiet)
        return

    if not args.no_stop:
        if args.action == "deploy" or args.action == "restart" or \
          args.action == "stop":
            if not system.run_watcher(stop_process=True, debug=args.debug,
                                      quiet=args.quiet):
                raise SystemExit("ABORTING without %s" % args.action)

    if args.action == "deploy":
        system.make_directories(debug=args.debug, quiet=args.quiet)
        system.rsync_install(debug=args.debug, quiet=args.quiet)

    if args.action == "deploy" or args.action == "restart":
        system.run_watcher(debug=args.debug, quiet=args.quiet)


if __name__ == "__main__":
    main()
