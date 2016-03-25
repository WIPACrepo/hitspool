#!/usr/bin/env python

import getpass
import logging
import logging.handlers
import os
import shutil
import socket
import tarfile

from HsException import HsException


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
        if logfile is None and basename is not None and basehost is not None:
            if self.is_cluster_sps or self.is_cluster_spts:
                logdir = self.DEFAULT_LOG_PATH
            else:
                logdir = "/home/david/TESTCLUSTER/%s/logs" % basehost
            logfile = os.path.join(logdir,
                                   "%s_%s.log" % (basename, self.shorthost))

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

    def queue_for_spade(self, sourcedir, sourcefile, spadedir, tarname,
                        semname):
        try:
            self.write_tarfile(sourcedir, sourcefile, tarname)
            if os.path.normpath(sourcedir) != os.path.normpath(spadedir):
                self.move_file(os.path.join(sourcedir, tarname), spadedir)
            self.touch_file(os.path.join(spadedir, semname))
            self.remove_tree(os.path.join(sourcedir, sourcefile))
        except HsException, hsex:
            logging.error(str(hsex))
            return False

        return True

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
