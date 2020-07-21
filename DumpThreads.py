#!/usr/bin/env python
#
# Add this hunk of code to a program (both 'fd' and 'logger' default to None):
#
#     if sys.version_info > (2, 3):
#         from DumpThreads import DumpThreadsOnSignal
#         DumpThreadsOnSignal(fd=<output file>, logger=<output logger>)
#
# then type ^\ (control-backslash) to dump all threads while running

from __future__ import print_function

import signal
import sys
import threading
import traceback


class DumpThreadsOnSignal(object):
    def __init__(self, fd=None, logger=None, signum=signal.SIGQUIT):
        if fd is None and logger is None:
            self.__fd = sys.stderr
        else:
            self.__fd = fd
        self.__logger = logger

        signal.signal(signum, self.__handle_signal)

    @staticmethod
    def __find_thread(tid):
        for thrd in threading.enumerate():
            if thrd.ident == tid:
                return thrd

        return None

    def __handle_signal(self, signum, frame):
        self.dump_threads(self.__fd, self.__logger)

    @classmethod
    def dump_threads(cls, fout=None, logger=None):
        first = True
        for tid, stack in sys._current_frames().items():
            thrd = cls.__find_thread(tid)
            if thrd is None:
                tstr = "Thread #%d" % tid
            else:
                # changed to get the string representation
                # of the thread as it has state, name, and
                # such embedded in it
                tstr = "Thread %s" % thrd

            if first:
                first = False
            elif fout is not None:
                print("", file=fout)

            for filename, lineno, name, line in traceback.extract_stack(stack):
                tstr += "\n  File \"%s\", line %d, in %s" % \
                    (filename, lineno, name)
                if line is not None:
                    tstr += "\n    %s" % line.strip()

            if fout is not None:
                print("%s" % tstr, file=fout)
            if logger is not None:
                logger.error(tstr)

        if fout is not None:
            print("---------------------------------------------", file=fout)
