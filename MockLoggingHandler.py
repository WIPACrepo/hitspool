#!/usr/bin/env python


import logging
import sys


class MockLoggingHandler(logging.Handler):
    """
    Mock logging handler to save log messages for later inspection.
    """

    def __init__(self, *args, **kwargs):
        self.__expected = []
        self.__verbose = False

        if "out_of_order"in kwargs and kwargs["out_of_order"]:
            out_of_order = True
            del kwargs["out_of_order"]
        else:
            out_of_order = False
        self.__out_of_order = out_of_order

        try:
            super(MockLoggingHandler, self).__init__(*args, **kwargs)
        except TypeError:
            logging.Handler.__init__(self)

    def __check_record(self, record):
        if record.args is None or len(record.args) == 0:
            recmsg = str(record.msg)
        else:
            recmsg = str(record.msg) % record.args

        if self.__verbose:
            print >>sys.stderr, "LOG>> \"%s\"<%s> (Exp#%d)" % \
                (recmsg, type(recmsg), len(self.__expected))

        if len(self.__expected) > 0:
            if not self.__out_of_order:
                xmsg = self.__expected.pop(0)
                if self.__verbose:
                    print >>sys.stderr, "CMP#pop>> %s<%s>" % \
                        (xmsg, type(xmsg))
                errmsg = self.__validate(recmsg, xmsg)
                if errmsg is not None:
                    raise Exception(errmsg)
                return

            for i in range(len(self.__expected)):
                if self.__verbose:
                    print >>sys.stderr, "CMP#%d>> %s<%s>" % \
                        (i, self.__expected[i], type(self.__expected[i]))
                errmsg = self.__validate(recmsg, self.__expected[i])
                if errmsg is None:
                    del self.__expected[i]
                    return
                # if self.__verbose:
                #     print >>sys.stderr, "ERR#%d>> %s" % (i, errmsg)

        raise Exception("Unexpected log message: %s[%s]%s" %
                        (record.name, record.levelname, recmsg))

    def __stringify(self, msglist):
        fixed = []
        for msg in msglist:
            try:
                fixed.append("REGEX(%s)" % msg.pattern)
            except:
                fixed.append(msg)
        return fixed

    def __validate(self, recmsg, xmsg):
        if isinstance(xmsg, str) or isinstance(xmsg, unicode):
            if recmsg == xmsg:
                return None

            return "Got log message \"%s\", expected \"%s\"" % \
                (recmsg, xmsg)
        else:
            try:
                if xmsg.match(recmsg) is not None:
                    return None

                return "Log message \"%s\" does not match \"%s\"" % \
                    (recmsg, xmsg.pattern)
            except:
                pass

        return "Log message \"%s\"<%s> != \"%s\"<%s>" % \
            (recmsg, type(recmsg), xmsg, type(xmsg))

    # pylint: disable=invalid-name
    # match other test methods
    def addExpected(self, msg):
        if self.__verbose:
            if hasattr(msg, 'flags') and hasattr(msg, 'pattern'):
                logmsg = "%s (pattern)" % msg.pattern
            else:
                logmsg = str(msg)
            print >>sys.stderr, "ADDLOG#%d>> %s" % \
                (len(self.__expected), logmsg)

        self.__expected.append(msg)

    def emit(self, record):
        "Save a log record"
        self.acquire()
        try:
            self.__check_record(record)
        finally:
            self.release()

    def reset(self):
        "Clear all cached log records"
        self.acquire()
        try:
            self.__expected[:] = []
        finally:
            self.release()

    # pylint: disable=invalid-name
    # match other test methods
    def setVerbose(self, value=True):
        self.__verbose = (value is True)

    def validate(self):
        if len(self.__expected) > 0:
            raise Exception("Didn't receive %d log messages: %s" %
                            (len(self.__expected),
                             self.__stringify(self.__expected)))
        return True
