#!/usr/bin/env python

import logging
import os
import shutil
import socket
import sys
import tarfile

from HsException import HsException


class HsBase(object):
    SPS = 1
    SPTS = 2
    LOCALHOST = 3
    TEST = 4

    def __init__(self, host=None, is_test=False):
        fullname = socket.gethostname()

        hdnm = fullname.split('.', 1)
        if len(hdnm) == 2:
            hostname, domainname = hdnm
        else:
            hostname = fullname
            domainname = ""

        # determine cluster based on host/domain name
        if is_test:
            self.__cluster = self.TEST
        elif domainname.endswith("usap.gov"):
            self.__cluster = self.SPS
        elif "spts" in domainname:
            self.__cluster = self.SPTS
        else:
            self.__cluster = self.LOCALHOST

        # allow caller to override the host name
        if host is not None:
            self.__src_mchn = host
        else:
            self.__src_mchn = fullname

        self.__src_mchn_short = hostname

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

    @staticmethod
    def init_logging(logfile=None, level=logging.INFO, both=False):
        if len(logging.getLogger().handlers) > 0:
            # any logging done before this point creates a StreamHandler
            # which causes basicConfig() to be ignored in Python 2.6
            logging.getLogger().handlers = []

        if logfile is None:
            logging.basicConfig(format='%(message)s',
                                level=level,
                                stream=sys.stdout)
        else:
            logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                                level=level,
                                datefmt='%Y-%m-%d %H:%M:%S',
                                filename=logfile)

            if both:
                # copy log messages to console
                out = logging.StreamHandler()
                out.setFormatter(logging.Formatter("%(message)s"))
                logging.getLogger().addHandler(out)

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

    def queue_for_spade(self, sourcedir, sourcefiles, spadedir, tarname,
                        semname):
        try:
            self.write_tarfile(sourcedir, sourcefiles, tarname)
            self.move_file(os.path.join(sourcedir, tarname), spadedir)
            self.touch_file(os.path.join(spadedir, semname))
        except HsException, hsex:
            logging.error(str(hsex))
            return False

        return True

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

        for fnm in sourcelist:
            try:
                tar.add(fnm)
            except StandardError, err:
                logging.error("Failed to add \"%s\" to \"%s\": %s",
                              fnm, os.path.join(sourcedir, tarname), err)

        tar.close()


if __name__ == "__main__":
    pass
