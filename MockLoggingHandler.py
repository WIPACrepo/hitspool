#!/usr/bin/env python


import logging


class MockLoggingHandler(logging.Handler):
    """
    Mock logging handler to save log messages for later inspection.
    """

    def __init__(self, *args, **kwargs):
        self.__expected = []
        self.__verbose = False

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
            print "LOG>> %s" % recmsg

        if len(self.__expected) == 0:
            raise Exception("Unexpected log message: %s[%s]%s" %
                            (record.name, record.levelname, recmsg))

        xmsg = self.__expected.pop(0)
        if isinstance(xmsg, str) or isinstance(xmsg, unicode):
            if recmsg != xmsg:
                raise Exception("Got log message \"%s\", expected \"%s\"" %
                                (recmsg, xmsg))
        else:
            try:
                if xmsg.match(recmsg) is None:
                    raise Exception("Log message \"%s\" does not match \"%s\"" %
                                    (recmsg, xmsg.pattern))
            except:
                raise Exception("Log message \"%s\"<%s> != \"%s\"<%s>" %
                                (recmsg, type(recmsg), xmsg,
                                 type(xmsg)))

    # pylint: disable=invalid-name
    # match other test methods
    def addExpected(self, msg):
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
        self.__verbose = value

    def validate(self):
        if len(self.__expected) > 0:
            raise Exception("Didn't receive %d log messages: %s" %
                            (len(self.__expected), self.__expected))
