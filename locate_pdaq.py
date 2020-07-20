#!/usr/bin/env python

import os


METADIR = None
CONFIGDIR = None


class DirectoryNotFoundException(Exception):
    "Thrown if a directory cannot be found"


class DirectoryAlreadySetException(Exception):
    "Thrown if the caller attempts to override the existing CONFIGDIR"


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

    path = os.path.expanduser("~/config")
    if os.path.exists(path):
        CONFIGDIR = path
        return CONFIGDIR

    path = os.path.expanduser("~pdaq/config")
    if os.path.exists(path):
        CONFIGDIR = path
        return CONFIGDIR

    # this could be an ancient pDAQ distribution with 'config' embedded in
    #  the source directory
    try:
        trunk = find_pdaq_trunk()
    except DirectoryNotFoundException:
        trunk = None

    if trunk is not None:
        path = os.path.join(trunk, "config")
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
        metadir = os.environ["PDAQ_HOME"]
        if os.path.exists(metadir):
            METADIR = metadir
            return METADIR

    pdaq_home = os.path.join(os.environ["HOME"], "pDAQ_current")
    cur_dir = os.getcwd()
    [parent_dir, _] = os.path.split(cur_dir)
    for xdir in [cur_dir, parent_dir, pdaq_home]:
        # source tree has 'dash', 'src', and 'StringHub' (and maybe 'target')
        # deployed tree has 'dash', 'src', and 'target'
        if os.path.isdir(os.path.join(xdir, 'dash')) and \
            os.path.isdir(os.path.join(xdir, 'src')) and \
            (os.path.isdir(os.path.join(xdir, 'target')) or
             os.path.isdir(os.path.join(xdir, 'StringHub'))):
            METADIR = xdir
            return METADIR

    raise DirectoryNotFoundException("Cannot find pDAQ trunk (PDAQ_HOME)")


def set_pdaq_config_dir(config_dir, override=False):
    "set location of pDAQ's run configuration directory"
    global CONFIGDIR
    if CONFIGDIR is not None and CONFIGDIR != config_dir and not override:
        raise DirectoryAlreadySetException("Cannot set pDAQ config directory"
                                           " to '%s'; already set to '%s'" %
                                           (config_dir, CONFIGDIR))
    CONFIGDIR = config_dir
