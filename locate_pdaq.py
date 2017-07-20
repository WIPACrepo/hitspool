#!/usr/bin/env python

import os


METADIR = None
CONFIGDIR = None


class DirectoryNotFoundException(Exception):
    pass


class DirectoryAlreadySetException(Exception):
    pass


def find_pdaq_config():
    "find pDAQ's run configuration directory"
    global CONFIGDIR
    if CONFIGDIR is not None:
        return CONFIGDIR

    if "PDAQ_CONFIG" in os.environ:
        path = os.environ["PDAQ_CONFIG"]
        if os.path.exists(path):
            CONFIGDIR = path
            return CONFIGDIR

    path = os.path.join(os.environ["HOME"], "config")
    if os.path.exists(path):
        CONFIGDIR = path
        return CONFIGDIR

    path = os.path.join(find_pdaq_trunk(), "config")
    if os.path.exists(path):
        CONFIGDIR = path
        return CONFIGDIR

    raise DirectoryNotFoundException("Cannot find DAQ configuration directory"
                                     " (PDAQ_CONFIG)")


def find_pdaq_trunk():
    "Find the pDAQ root directory"
    global METADIR
    if METADIR is not None:
        return METADIR

    if "PDAQ_HOME" in os.environ:
        dir = os.environ["PDAQ_HOME"]
        if os.path.exists(dir):
            METADIR = dir
            return METADIR

    homePDAQ = os.path.join(os.environ["HOME"], "pDAQ_current")
    curDir = os.getcwd()
    [parentDir, baseName] = os.path.split(curDir)
    for dir in [curDir, parentDir, homePDAQ]:
        # source tree has 'dash', 'src', and 'StringHub' (and maybe 'target')
        # deployed tree has 'dash', 'src', and 'target'
        if os.path.isdir(os.path.join(dir, 'dash')) and \
            os.path.isdir(os.path.join(dir, 'src')) and \
            (os.path.isdir(os.path.join(dir, 'target')) or
             os.path.isdir(os.path.join(dir, 'StringHub'))):
            METADIR = dir
            return METADIR

    raise DirectoryNotFoundException("Cannot find pDAQ trunk (PDAQ_HOME)")


def set_pdaq_config_dir(config_dir):
    "set location of pDAQ's run configuration directory"
    global CONFIGDIR
    if CONFIGDIR is not None and CONFIGDIR != config_dir:
        raise DirectoryAlreadySetException("Cannot set pDAQ config directory"
                                           " to '%s'; already set to '%s'" %
                                           (config_dir, CONFIGDIR))
    CONFIGDIR = config_dir
