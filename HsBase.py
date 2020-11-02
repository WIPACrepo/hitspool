#!/usr/bin/env python
"""
Base HitSpool class
"""

from __future__ import print_function

import datetime
import getpass
import logging
import logging.handlers
import os
import shutil
import socket
import sys
import tarfile

try:
    from lxml import etree
    USE_LXML = True
except ImportError:
    USE_LXML = False

import DAQTime

from HsException import HsException


class HsBase(object):
    "Base HitSpool class"

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
        fullname, shortname, domainname = self.get_host_domain(host=host)
        self.__src_mchn = fullname
        self.__src_mchn_short = shortname

        self.__cluster = self.get_cluster(domainname, is_test=is_test)

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

    @classmethod
    def __delta_to_seconds(cls, delta):
        return float(delta.seconds) + (float(delta.microseconds) / 1000000.0)

    @classmethod
    def __fmt_time(cls, name, secs):
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

    @classmethod
    def get_cluster(cls, domainname, is_test=False):
        "determine cluster based on host/domain name"
        if is_test:
            return cls.TEST
        if domainname.endswith("usap.gov"):
            return cls.SPS
        if "spts" in domainname:
            return cls.SPTS
        return cls.LOCALHOST

    @classmethod
    def get_host_domain(cls, host=None):
        """
        Return fully qualified host name, short host name, and domain name
        """
        # allow caller to override the host name
        if host is not None:
            fullname = host
        else:
            fullname = socket.gethostname()

        # short host is the machine name without the domain
        hdnm = fullname.split('.', 1)
        if len(hdnm) == 2:
            shortname, domainname = hdnm
        else:
            shortname = fullname
            domainname = ""

        return fullname, shortname, domainname

    @classmethod
    def init_logging(cls, logfile=None, basename=None, basehost=None,
                     level=logging.INFO, both=False):
        if logfile is None:
            if basename is not None and basehost is not None:
                logfile = os.path.join(HsBase.DEFAULT_LOG_PATH,
                                       "%s_%s.log" % (basename, basehost))
            elif basename is not None or basehost is not None:
                print("Not logging to file (basehost=%s, basename=%s)" %
                      (basehost, basename), file=sys.stderr)

        if logfile is not None:
            logdir = os.path.dirname(logfile)
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

    @classmethod
    def move_file(cls, src, dst):
        if not os.path.exists(dst):
            raise HsException("Directory \"%s\" does not exist,"
                              " cannot move \"%s\"" % (dst, src))

        try:
            shutil.move(src, dst)
        except Exception as sex:
            raise HsException("Cannot move \"%s\" to \"%s\": %s" %
                              (src, dst, sex))

    def queue_for_spade(self, sourcedir, sourcefile, spadedir, basename,
                        start_ticks=None, stop_ticks=None):
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
                                              start_ticks, stop_ticks)
            else:
                semname = None
            self.remove_tree(os.path.join(sourcedir, sourcefile))
        except HsException as hsex:
            logging.error(str(hsex))
            semname = None

        if semname is None:
            return None

        return (tarname, semname)

    @classmethod
    def remove_tree(cls, path):
        try:
            shutil.rmtree(path)
            if os.path.isdir(path):
                logging.error("%s exists after rmtree()!!!", path)
        except:
            logging.exception("Cannot remove %s", path)

    @property
    def rsync_host(self):
        return self.__rsync_host

    @property
    def rsync_user(self):
        return self.__rsync_user

    @classmethod
    def set_default_copy_path(cls, path):
        cls.DEFAULT_COPY_PATH = path

    @property
    def shorthost(self):
        return self.__src_mchn_short

    @classmethod
    def touch_file(cls, name, times=None):
        try:
            with open(name, "a"):
                os.utime(name, times)
        except Exception as err:
            raise HsException("Failed to 'touch' \"%s\": %s" % (name, err))

    def write_meta_xml(self, spadedir, basename, start_ticks, stop_ticks):
        # use the tarfile creation time as the DIF_Creation_Date value
        tarpath = os.path.join(spadedir, basename + self.TAR_SUFFIX)
        try:
            tarstamp = os.path.getmtime(tarpath)
        except OSError:
            raise HsException("Cannot write metadata file:"
                              " %s does not exist" % tarpath)
        tartime = datetime.datetime.fromtimestamp(tarstamp)

        if stop_ticks is not None:
            stop_utc = DAQTime.ticks_to_utc(stop_ticks)
        else:
            # set stop time to the time the tarfile was written
            stop_utc = tartime
        if start_ticks is not None:
            start_utc = DAQTime.ticks_to_utc(start_ticks)
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
            line = etree.tostring(root)
            try:
                # Python3 needs to convert XML bytes to a string
                line = line.decode("utf-8")
            except:
                pass

            out.write(line)
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
        except Exception as err:
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
                except Exception as err:
                    logging.error("Failed to add \"%s\" to \"%s\": %s",
                                  fnm, os.path.join(sourcedir, tarname), err)
        finally:
            tar.close()


if __name__ == "__main__":
    pass
