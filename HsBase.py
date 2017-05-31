#!/usr/bin/env python

import datetime
import getpass
import logging
import logging.handlers
import numbers
import os
import re
import shutil
import socket
import sys
import tarfile

try:
    from lxml import etree
    USE_LXML = True
except ImportError:
    USE_LXML = False

from HsException import HsException


class DAQTime(object):
    # format string used to parse dates
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
    # dictionary of year->datetime("Jan 1 %d" % year)
    JAN1 = {}

    def __init__(self, arg, is_ns=False):
        """
        Parse string as either a timestamp (in nanoseconds) or a date string.
        Return a tuple containing the nanosecond timestamp and a date string,
        deriving one value from the other.
        """
        if arg is None:
            raise HsException("No date/time specified")
        elif isinstance(arg, DAQTime):
            raise HsException("Cannot construct DAQTime from another DAQTime")

        self.__ticks = None
        self.__utc = None

        multiplier = 10 if is_ns else 1
        if isinstance(arg, numbers.Number):
            # argument is a number
            self.__ticks = arg * multiplier
        elif self.__is_string_type(arg) and arg.isdigit():
            # argument is a numeric string
            try:
                self.__ticks = int(arg) * multiplier
            except:
                # argument is unknown
                self.__ticks = None

        self.__utc = None
        if self.__ticks is None:
            # if we haven't parsed the argument, see if it's a date
            dstr = str(arg)
            try:
                self.__utc = datetime.datetime.strptime(dstr, self.TIME_FORMAT)
            except ValueError, err:
                try:
                    # convert high-precision times into parseable times
                    short = re.sub(r"(\.\d{6})\d+", r"\1", dstr)
                    self.__utc = datetime.datetime.strptime(short,
                                                            self.TIME_FORMAT)
                except ValueError:
                    raise HsException("Problem with the time-stamp format"
                                      " \"%s\": %s" % (arg, err))

        if self.__utc is not None:
            jan1 = self.__jan1_by_year(self.__utc)
        else:
            # XXX this breaks when requesting old times after the new year
            jan1 = self.__jan1_by_year(self.__utc)

        if self.__ticks is None and self.__utc is not None:
            self.__ticks = self.__get_daq_ticks(jan1, self.__utc,
                                                is_ns=False)
        elif self.__utc is None and self.__ticks is not None:
            self.__utc = jan1 + datetime.timedelta(seconds=self.__ticks /
                                                   1E10)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.__ticks == other.__ticks

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))

    def __lt__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.__ticks < other.__ticks

    def __ne__(self, other):
        val = self.__eq__(other)
        if not isinstance(val, bool):
            return NotImplemented
        return not val

    def __repr__(self):
        return "DAQTime(%d)" % self.__ticks

    def __str__(self):
        if self.__utc is None:
            ustr = ""
        else:
            ustr = "(%s)" % str(self.__utc)
        return str(self.__ticks) + ustr

    @classmethod
    def __get_daq_ticks(cls, start_time, end_time, is_ns=False):
        """
        Get the difference between two datetimes.
        If `is_ns` is True, returned value is in nanoseconds.
        Otherwise the value is in DAQ ticks (0.1ns)
        """
        if is_ns:
            multiplier = 1E3
        else:
            multiplier = 1E4

        # XXX this should use leapseconds
        delta = end_time - start_time

        return int(((delta.days * 24 * 3600 + delta.seconds) * 1E6 +
                    delta.microseconds) * multiplier)

    @classmethod
    def __is_string_type(self, arg):
        return isinstance(arg, str) or isinstance(arg, unicode)

    @classmethod
    def __jan1_by_year(cls, daq_time=None):
        """
        Return the datetime value for January 1 of the specified year.
        If year is None, return January 1 for the current year.
        """
        if daq_time is not None:
            year = daq_time.year
        else:
            year = datetime.datetime.utcnow().year

        if year not in cls.JAN1:
            cls.JAN1[year] = datetime.datetime(year, 1, 1)

        return cls.JAN1[year]

    @property
    def ticks(self):
        return self.__ticks

    @property
    def utc(self):
        return self.__utc


class HsBase(object):
    SPS = 1
    SPTS = 2
    LOCALHOST = 3
    TEST = 4

    # default log directory
    DEFAULT_LOG_PATH = "/mnt/data/pdaqlocal/HsInterface/logs/"

    # default rsync user
    DEFAULT_RSYNC_USER = "pdaq"
    # default rsync host
    DEFAULT_RSYNC_HOST = "2ndbuild"
    # default destination directory
    DEFAULT_COPY_PATH = "/mnt/data/pdaqlocal/HsDataCopy"

    # maximum number of seconds of data which can be requested
    MAX_REQUEST_SECONDS = 3610

    # if True, write metadata file; otherwise touch semaphore file
    WRITE_META_XML = True
    # suffix for semaphore file
    SEM_SUFFIX = ".sem"
    # suffix for tar file
    TAR_SUFFIX = ".dat.tar.bz2"
    # suffix for metadata file
    META_SUFFIX = ".meta.xml"

    def __init__(self, host=None, is_test=False):
        # allow caller to override the host name
        if host is not None:
            self.__src_mchn = host
        else:
            self.__src_mchn = socket.gethostname()

        # short host is the machine name without the domain
        hdnm = self.__src_mchn.split('.', 1)
        if len(hdnm) == 2:
            self.__src_mchn_short, domainname = hdnm
        else:
            self.__src_mchn_short, domainname = (self.__src_mchn, "")

        # determine cluster based on host/domain name
        if is_test:
            self.__cluster = self.TEST
        elif domainname.endswith("usap.gov"):
            self.__cluster = self.SPS
        elif "spts" in domainname:
            self.__cluster = self.SPTS
        else:
            self.__cluster = self.LOCALHOST

        if self.is_cluster_sps or self.is_cluster_spts:
            self.__rsync_host = self.DEFAULT_RSYNC_HOST
            self.__rsync_user = self.DEFAULT_RSYNC_USER
        else:
            self.__rsync_host = "localhost"
            self.__rsync_user = getpass.getuser()

    @classmethod
    def __convert_tuples_to_xml(cls, root, xmltuples):
        for key, val in xmltuples:
            node = etree.SubElement(root, key)
            if isinstance(val, tuple):
                cls.__convert_tuples_to_xml(node, val)
            else:
                node.text = str(val)

    def __delta_to_seconds(self, delta):
        return float(delta.seconds) + (float(delta.microseconds) / 1000000.0)

    def __fmt_time(self, name, secs):
        if secs is None:
            return ""
        return " %s=%.2fs" % (name, secs)

    @property
    def cluster(self):
        if self.__cluster == self.LOCALHOST:
            return "LOCALHOST"
        if self.__cluster == self.SPTS:
            return "SPTS"
        if self.__cluster == self.SPS:
            return "SPS"
        if self.__cluster == self.TEST:
            return "TEST"
        return "UNKNOWN(#%d)" % self.__cluster

    @property
    def fullhost(self):
        return self.__src_mchn

    def init_logging(self, logfile=None, basename=None, basehost=None,
                     level=logging.INFO, both=False):
        if logfile is not None:
            logdir = os.path.dirname(logfile)
        elif basename is None or basehost is None:
            logdir = None
            if basename is not None or basehost is not None:
                print >>sys.stderr, \
                    "Not logging to file (basehost=%s, basename=%s)" % \
                    (basehost, basename)
        else:
            if self.is_cluster_sps or self.is_cluster_spts:
                logdir = self.DEFAULT_LOG_PATH
            else:
                logdir = "/home/david/TESTCLUSTER/%s/logs" % basehost
            logfile = os.path.join(logdir,
                                   "%s_%s.log" % (basename, self.shorthost))

        if logdir is not None:
            if not os.path.exists(logdir):
                os.makedirs(logdir)
            elif not os.path.isdir(logdir):
                raise HsException("Base log directory \"%s\" isn't"
                                  " a directory!" % logdir)

        logger = logging.getLogger()
        if len(logger.handlers) > 0:
            # any logging done before this point creates a StreamHandler
            # which causes basicConfig() to be ignored in Python 2.6
            logger.handlers = []

        if logfile is not None:
            # set up logging to file
            ten_mb = 1024 * 1024 * 10
            rot = logging.handlers.RotatingFileHandler(logfile,
                                                       maxBytes=ten_mb,
                                                       backupCount=5)
            longfmt = "%(asctime)s %(levelname)s %(message)s"
            rot.setFormatter(logging.Formatter(fmt=longfmt,
                                               datefmt="%Y-%m-%d %H:%M:%S"))
            logger.addHandler(rot)

        if logfile is None or both:
            # set up logging to console
            out = logging.StreamHandler()
            out.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(out)

        # set log level
        logger.setLevel(level)

        return logfile

    @property
    def is_cluster_local(self):
        return self.__cluster == self.LOCALHOST

    @property
    def is_cluster_sps(self):
        return self.__cluster == self.SPS

    @property
    def is_cluster_spts(self):
        return self.__cluster == self.SPTS

    def move_file(self, src, dst):
        if not os.path.exists(dst):
            raise HsException("Directory \"%s\" does not exist,"
                              " cannot move \"%s\"" % (dst, src))

        try:
            shutil.move(src, dst)
        except StandardError, sex:
            raise HsException("Cannot move \"%s\" to \"%s\": %s" %
                              (src, dst, sex))

    def queue_for_spade(self, sourcedir, sourcefile, spadedir, basename,
                        start_time=None, stop_time=None):
        tarname = basename + self.TAR_SUFFIX
        semname = None

        create_path = os.path.join(sourcedir, tarname)
        if os.path.exists(create_path):
            logging.error("WARNING: Overwriting temporary tar file \"%s\"",
                          create_path)

        final_path = os.path.join(spadedir, tarname)
        if os.path.exists(final_path):
            logging.error("WARNING: Overwriting final tar file \"%s\"",
                          final_path)

        try:
            self.write_tarfile(sourcedir, sourcefile, tarname)
            if os.path.normpath(sourcedir) != os.path.normpath(spadedir):
                final_path = os.path.join(spadedir, tarname)
                if os.path.exists(final_path):
                    logging.error("WARNING: Overwriting final tar file \"%s\"",
                                  final_path)

                self.move_file(os.path.join(sourcedir, tarname), spadedir)
            if not self.WRITE_META_XML:
                semname = self.write_sem(spadedir, basename)
            elif USE_LXML:
                semname = self.write_meta_xml(spadedir, basename,
                                              start_time, stop_time)
            else:
                semname = None
            self.remove_tree(os.path.join(sourcedir, sourcefile))
        except HsException, hsex:
            logging.error(str(hsex))
            semname = None

        if semname is None:
            return None

        return (tarname, semname)

    def remove_tree(self, path):
        try:
            shutil.rmtree(path)
        except:
            logging.exception("Cannot remove %s", path)

    @property
    def rsync_host(self):
        return self.__rsync_host

    @property
    def rsync_user(self):
        return self.__rsync_user

    @property
    def shorthost(self):
        return self.__src_mchn_short

    def touch_file(self, name, times=None):
        try:
            with open(name, "a"):
                os.utime(name, times)
        except StandardError, err:
            raise HsException("Failed to 'touch' \"%s\": %s" % (name, err))

    def write_meta_xml(self, spadedir, basename, start_time, stop_time):
        # use the tarfile creation time as the DIF_Creation_Date value
        tarpath = os.path.join(spadedir, basename + self.TAR_SUFFIX)
        try:
            tarstamp = os.path.getmtime(tarpath)
        except OSError:
            raise HsException("Cannot write metadata file:"
                              " %s does not exist" % tarpath)
        tartime = datetime.datetime.fromtimestamp(tarstamp)

        if stop_time is not None:
            stop_utc = stop_time.utc
        else:
            # set stop time to the time the tarfile was written
            stop_utc = tartime
        if start_time is not None:
            start_utc = start_time.utc
        else:
            # set start time to midnight
            start_utc = datetime.datetime(stop_utc.year, stop_utc.month,
                                          stop_utc.day)

        root = etree.Element("DIF_Plus")
        root.set("{http://www.w3.org/2001/XMLSchema-instance}"
                 "noNamespaceSchemaLocation", "IceCubeDIFPlus.xsd")

        # Metadata specification is at:
        # https://docushare.icecube.wisc.edu/dsweb/Get/Document-20546/metadata_specification.pdf
        xmldict = (
            ("DIF", (
                ("Entry_ID", basename),
                ("Entry_Title", "Hitspool_data"),
                ("Parameters",
                 "SPACE SCIENCE > Astrophysics > Neutrinos"),
                ("ISO_Topic_Category", "geoscientificinformation"),
                ("Data_Center", (
                    ("Data_Center_Name",
                     "UWI-MAD/A3RI > Antarctic Astronomy and Astrophysics"
                     " Research Institute, University of Wisconsin, Madison"),
                    ("Personnel", (
                        ("Role", "Data Center Contact"),
                        ("Email", "datacenter@icecube.wisc.edu"),
                    )),
                )),
                ("Summary", "Hitspool data"),
                ("Metadata_Name", "[CEOS IDN DIF]"),
                ("Metadata_Version", "9.4"),
                ("Personnel", (
                    ("Role", "Technical Contact"),
                    ("First_Name", "Dave"),
                    ("Last_Name", "Glowacki"),
                    ("Email", "dglo@icecube.wisc.edu"),
                )),
                ("Sensor_Name", "ICECUBE > IceCube"),
                ("Source_Name",
                 "EXPERIMENTAL > Data with an instrumentation based"
                 " source"),
                ("DIF_Creation_Date", tartime.strftime("%Y-%m-%d")),
            )),
            ("Plus", (
                ("Start_DateTime", start_utc.strftime("%Y-%m-%dT%H:%M:%S")),
                ("End_DateTime", stop_utc.strftime("%Y-%m-%dT%H:%M:%S")),
                ("Category", "internal-system"),
                ("Subcategory", "hit-spooling"),
            )),
        )

        self.__convert_tuples_to_xml(root, xmldict)

        metaname = basename + self.META_SUFFIX
        with open(os.path.join(spadedir, metaname), "w") as out:
            out.write(etree.tostring(root))
        return metaname

    def write_sem(self, spadedir, basename):
        semname = basename + self.SEM_SUFFIX
        self.touch_file(os.path.join(spadedir, semname))
        return semname

    def write_tarfile(self, sourcedir, sourcefiles, tarname):
        if not os.path.exists(sourcedir):
            raise HsException("Source directory \"%s\" does not exist" %
                              sourcedir)

        curdir = os.getcwd()

        # build tarfile inside sourcedir
        os.chdir(sourcedir)
        try:
            self.__write_tarfile_internal(sourcedir, sourcefiles, tarname)
        finally:
            # move back to original directory
            os.chdir(curdir)

    @staticmethod
    def __write_tarfile_internal(sourcedir, sourcefiles, tarname):
        extra = ""
        if tarname.endswith(".gz") or tarname.endswith(".tgz"):
            extra = ":gz"
        elif tarname.endswith(".bz2"):
            extra = ":bz2"

        try:
            tar = tarfile.open(tarname, "w" + extra)
        except StandardError, err:
            raise HsException("Cannot create \"%s\" in \"%s\": %s" %
                              (tarname, sourcedir, err))

        if isinstance(sourcefiles, list):
            sourcelist = sourcefiles
        else:
            sourcelist = [sourcefiles, ]

        try:
            for fnm in sourcelist:
                try:
                    tar.add(fnm)
                except StandardError, err:
                    logging.error("Failed to add \"%s\" to \"%s\": %s",
                                  fnm, os.path.join(sourcedir, tarname), err)
        finally:
            tar.close()


if __name__ == "__main__":
    pass
