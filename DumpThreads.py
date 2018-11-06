#!/usr/bin/env python
#
# Add this hunk of code to a program (both 'fd' and 'logger' default to None):
#
#     if sys.version_info > (2, 3):
#         from DumpThreads import DumpThreadsOnSignal
#         DumpThreadsOnSignal(fd=<output file>, logger=<output logger>)
#
# then type ^\ (control-backslash) to dump all threads while running


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

        signal.signal(signum, self.__handleSignal)

    @staticmethod
    def __findThread(tId):
        for t in threading.enumerate():
            if t.ident == tId:
                return t

        return None

    def __handleSignal(self, signum, frame):
        self.dumpThreads(self.__fd, self.__logger)

    @classmethod
    def dumpThreads(cls, fd=None, logger=None):
        first = True
        for tId, stack in sys._current_frames().items():
            thrd = cls.__findThread(tId)
            if thrd is None:
                tStr = "Thread #%d" % tId
            else:
                # changed to get the string representation
                # of the thread as it has state, name, and
                # such embedded in it
                tStr = "Thread %s" % thrd

            if first:
                first = False
            elif fd is not None:
                print >>fd

            for filename, lineno, name, line in traceback.extract_stack(stack):
                tStr += "\n  File \"%s\", line %d, in %s" % \
                    (filename, lineno, name)
                if line is not None:
                    tStr += "\n    %s" % line.strip()

            if fd is not None:
                print >>fd, tStr
            if logger is not None:
                logger.error(tStr)

        if fd is not None:
            print >>fd, "---------------------------------------------"
