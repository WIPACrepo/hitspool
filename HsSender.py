#!/usr/bin/env python


import json
import logging
import os
import re
import shutil
import signal
import sys
import zmq

import HsBase

from HsConstants import I3LIVE_PORT, SENDER_PORT


class HsSender(HsBase.HsBase):
    """
    "sndaq"           "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        ------------
    | REQUEST | <----->| REPLY   |         | IcHub n |        | 2ndbuild |
    -----------        | PUB     | ------> | SUB    n|        | PULL     |
                       ----------          |PUSH     | ---->  |          |
                                            ---------          -----------
    This is the NEW HsSender for the HS Interface.
    It's started via fabric on access.
    It receives messages from the HsWorkers and is responsible of putting
    the HitSpool Data in the SPADE queue.
    """

    HS_SPADE_DIR = "/mnt/data/HitSpool"

    def __init__(self):
        super(HsSender, self).__init__()

        self.__context = zmq.Context()
        self.__reporter = self.create_reporter()
        self.__i3socket = self.create_i3socket("localhost")

    def close_all(self):
        self.__reporter.close()
        self.__i3socket.close()
        self.__context.term()

    def create_reporter(self):
        """Socket which receives messages from Worker"""
        sock = self.__context.socket(zmq.PULL)
        sock.bind("tcp://*:%d" % SENDER_PORT)
        return sock

    def create_i3socket(self, host):
        """Socket for I3Live on localhost"""
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, I3LIVE_PORT))
        return sock

    def handler(self, signum, _):
        """Clean exit when program is terminated from outside (via pkill)"""
        logging.warning("Signal Handler called with signal %s", signum)
        logging.warning("Shutting down...\n")

        i3live_dict = {}
        i3live_dict["service"] = "HSiface"
        i3live_dict["varname"] = "HsSender"
        i3live_dict["value"] = "INFO: SHUT DOWN called by external signal."
        self.__i3socket.send_json(i3live_dict)

        i3live_dict = {}
        i3live_dict["service"] = "HSiface"
        i3live_dict["varname"] = "HsSender"
        i3live_dict["value"] = "STOPPED"
        self.__i3socket.send_json(i3live_dict)

        self.close_all()

        raise SystemExit(1)

    def i3socket(self):
        return self.__i3socket

    def mainloop(self, force_spade=False):
        msg = self.__reporter.recv_json()
        logging.info("HsSender received json: %s", msg)

        try:
            info = json.loads(msg)
            logging.info("HsSender loaded json: %s", info)
        except StandardError:
            logging.error("Cannot load JSON message \"%s\"", msg)
            return

        if not isinstance(info, dict) or not info.has_key("msgtype"):
            logging.error("Ignoring bad message \"%s\"<%s>",
                          info, type(info))
            return

        if info["msgtype"] != "rsync_sum":
            logging.error("Ignoring message type \"%s\"", info["msgtype"])
            return

        try:
            copydir = info["copydir"]
            copydir_user = info["copydir_user"]
        except KeyError:
            logging.error("Ignoring incomplete message \"%s\"", info)
            return

        hs_basedir, no_spade \
            = self.hs_data_location_check(copydir, copydir_user,
                                          force_spade=force_spade)
        if force_spade or self.is_cluster_sps():
            if no_spade and not force_spade:
                logging.info("Not scheduling for SPADE pickup")
            else:
                self.spade_pickup_data(copydir, hs_basedir)

    def movefiles(self, copydir, targetdir):
        """move data to user required directory"""
        if not os.path.exists(targetdir):
            try:
                os.makedirs(targetdir)
            except:
                logging.error("Cannot create target \"%s\"", targetdir)
                return False

        if not os.path.isdir(targetdir):
            logging.error("Target \"%s\" is not a directory,"
                          " cannot move \"%s\"", targetdir, copydir)
            return False

        try:
            shutil.move(copydir, targetdir)
            logging.info("Moved %s to %s", copydir, targetdir)
        except shutil.Error, err:
            logging.error("Cannot move \"%s\" to \"%s\": %s", copydir,
                          targetdir, err)
            return False

        return True

    def reporter(self):
        return self.__reporter

    def hs_data_location_check(self, copydir, copydir_user, force_spade=False):
        """
        Move the data in case its default locations differs from
        the user's desired one.
        """

        logging.info("Checking the data location...")
        logging.info("HS data located at: %s", copydir)
        logging.info("user requested it to be in: %s", copydir_user)

        no_spade = True

        hs_basedir, data_dir_name = os.path.split(copydir)

        match = re.match(r'(\S+)_[0-9]{8}_[0-9]{6}', data_dir_name)
        if match is None or not match.group(1) in ["SNALERT", "HESE", "ANON"]:
            logging.error("Naming scheme validation failed.")
            logging.error("Please put the data manually in the desired"
                          " location: %s", copydir_user)
        else:
            logging.info("HS data name: %s", data_dir_name)

            # XXX use os.path.samefile()
            if force_spade or \
               os.path.normpath(copydir_user) == os.path.normpath(hs_basedir):
                no_spade = False
                logging.info("HS data \"%s\" is located in \"%s\"",
                             data_dir_name, hs_basedir)
            else:
                targetdir = os.path.join(copydir_user, data_dir_name)
                if self.movefiles(copydir, targetdir):
                    hs_basedir = copydir_user

        return hs_basedir, no_spade


    #------ Preparation for SPADE ----#
    def spade_pickup_data(self, copydir, hs_basedir):
        '''
        tar & bzip folder
        create semaphore file for folder
        move .sem & .dat.tar.bz2 file in SPADE directory
        '''

        logging.info("Preparation for SPADE Pickup of HS data started...")

        hs_basedir, data_dir = os.path.split(copydir)
        match = re.match(r'(\S+)_[0-9]{8}_[0-9]{6}_\S+', data_dir)
        if match is None or not match.group(1) in ["SNALERT", "HESE", "ANON"]:
            logging.error("Naming scheme validation failed.")
            logging.error("Please put the data manually in the SPADE"
                          " directory. Use HsSpader.py, for example.")
            return (None, None)

        logging.info("HS data name: %s", data_dir)

        logging.info("copy_basedir is: %s", hs_basedir)

        hs_basename = "HS_"  + data_dir
        hs_bzipname = hs_basename + ".dat.tar.bz2"
        hs_spade_semfile = hs_basename + ".sem"

        if not self.queue_for_spade(hs_basedir, data_dir, self.HS_SPADE_DIR,
                                    hs_bzipname, hs_spade_semfile):
            logging.error("Please put the data manually in the SPADE"
                          " directory. Use HsSpader.py, for example.")
            return (None, None)

        logging.info("Preparation for SPADE Pickup of %s DONE", copydir)
        self.__i3socket.send_json({"service": "HSiface",
                                   "varname": "HsSender@" + self.shorthost(),
                                   "value": "SPADE-ing of %s done" % copydir})

        return (hs_bzipname, hs_spade_semfile)

    def unused_spade_pickup_log(self, info):
        if info["msgtype"] != "log_done":
            return

        logging.info("logfile %s from %s was transmitted to %s",
                     info["logfile_hsworker"], info["hubname"],
                     info["logfile_hsworker"])

        org_logfilename = info["logfile_hsworker"]
        hs_log_basedir = info["logfiledir"]
        hs_log_basename = "HS_%s_%s" % \
                          (info["alertid"], info["logfile_hsworker"])
        hs_log_spadename = hs_log_basename + ".dat.tar.bz2"
        hs_log_spade_sem = hs_log_basename + ".sem"

        if not self.queue_for_spade(hs_log_basedir, org_logfilename,
                                    self.HS_SPADE_DIR, hs_log_spadename,
                                    hs_log_spade_sem):
            logging.error("Please put \"%s\" manually in the SPADE directory.",
                          os.path.join(hs_log_basedir, org_logfilename))



if __name__ == "__main__":
    import getopt


    def process_args():
        force_spade = False
        logfile = None
        spadedir = None

        usage = False
        try:
            opts, _ = getopt.getopt(sys.argv[1:], 'Fhl:S:', ['help', 'logfile'])
        except getopt.GetoptError, err:
            print >>sys.stderr, str(err)
            opts = []
            usage = True

        for opt, arg in opts:
            if opt == '-F':
                force_spade = True
            elif opt == '-l' or opt == '--logfile':
                logfile = str(arg)
            elif opt == '-S':
                spadedir = str(arg)
            elif opt == '-h' or opt == '--help':
                usage = True

        if usage:
            print >>sys.stderr, "usage :: HsSender.py [-l logfile]"
            raise SystemExit(1)

        return (logfile, force_spade, spadedir)

    def main():
        (logfile, force_spade, spadedir) = process_args()

        newmsg = HsSender()

        #handler is called when SIGTERM is called (via pkill)
        signal.signal(signal.SIGTERM, newmsg.handler)

        # override some defaults (generally only used for debugging)
        if spadedir is not None:
            newmsg.HS_SPADE_DIR = spadedir

        if logfile is None:
            if newmsg.is_cluster_local():
                logdir = "/home/david/TESTCLUSTER/2ndbuild/logs"
            else:
                logdir = "/mnt/data/pdaqlocal/HsInterface/logs"
            logfile = os.path.join(logdir, "hssender_%s.log" %
                                   newmsg.shorthost())

        newmsg.init_logging(logfile)

        logging.info("HsSender starts on %s", newmsg.shorthost())

        while True:
            logging.info("HsSender waits for new reports from HsWorkers...")
            try:
                newmsg.mainloop(force_spade=force_spade)
            except KeyboardInterrupt:
                logging.warning("Interruption received, shutting down...")
                raise SystemExit(0)


    main()
