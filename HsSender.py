#!/usr/bin/env python


import datetime
import logging
import os
import re
import shutil
import signal
import sqlite3
import threading
import time
import zmq

import HsConstants
import HsMessage
import HsUtil

from HsBase import DAQTime, HsBase
from HsException import HsException
from HsPrefix import HsPrefix
from RequestMonitor import RequestMonitor


def add_arguments(parser):
    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hssender.log")

    parser.add_argument("-D", "--state-db", dest="state_db",
                        help="Path to HitSpool state database"
                        " (used for testing)")
    parser.add_argument("-F", "--force-spade", dest="force_spade",
                        action="store_true", default=False,
                        help="Always queue files for JADE")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-S", "--spadedir", dest="spadedir",
                        help="Directory where files are queued to SPADE")
    parser.add_argument("-T", "--is-test", dest="is_test",
                        action="store_true", default=False,
                        help="Ignore SPS/SPTS status for tests")


class HsSender(HsBase):
    """
    "sndaq"           "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        ------------
    | REQUEST | <----->| REPLY   |         | IcHub n |        | 2ndbuild |
    -----------        | PUB     | ------> | SUB    n|        | PULL     |
                       ----------          |PUSH     | ---->  |          |
                                            ---------          -----------
    This is the HsSender for the HS Interface.
    It's started via fabric on access.
    It receives messages from the HsWorkers and is responsible for putting
    the HitSpool Data in the SPADE queue.
    """

    HS_SPADE_DIR = "/mnt/data/HitSpool"

    def __init__(self, host=None, is_test=False):
        super(HsSender, self).__init__(host=host, is_test=is_test)

        if self.is_cluster_sps or self.is_cluster_spts:
            expcont = "expcont"
        else:
            expcont = "localhost"

        self.__context = zmq.Context()
        self.__reporter = self.create_reporter()
        self.__workers = self.create_workers_socket()
        self.__i3socket = self.create_i3socket(expcont)
        self.__alert_socket = self.create_alert_socket()

        self.__poller = self.create_poller()
        self.__poller.register(self.__reporter, zmq.POLLIN)
        self.__poller.register(self.__alert_socket, zmq.POLLIN)

        self.__monitor = RequestMonitor(self)
        self.__monitor.start()

    def __validate_destination_dir(self, dirname, prefix):
        if prefix is None:
            match = re.match(r'(\S+)_[0-9]{8}_[0-9]{6}', dirname)
            if match is None:
                logging.error("Bad destination directory name \"%s\""
                              " (no time tag?)", dirname)
                return False

            prefix = match.group(1)

        if not HsPrefix.is_valid(prefix):
            logging.info("Unexpected prefix for destination directory"
                         " name \"%s\"", dirname)

        return True

    def close_all(self):
        self.__reporter.close()
        self.__workers.close()
        self.__i3socket.close()
        self.__alert_socket.close()
        self.__context.term()

        self.__monitor.stop()
        self.__monitor.join()

    def create_alert_socket(self):
        # Socket to receive alert message
        sock = self.__context.socket(zmq.REP)
        sock.identity = "Alert".encode("ascii")
        sock.bind("tcp://*:%d" % HsConstants.ALERT_PORT)
        return sock

    def create_i3socket(self, host):
        """Socket for I3Live on expcont"""
        sock = self.__context.socket(zmq.PUSH)
        sock.identity = "I3Socket".encode("ascii")
        sock.connect("tcp://%s:%d" % (host, HsConstants.I3LIVE_PORT))
        return sock

    def create_poller(self):
        return zmq.Poller()

    def create_reporter(self):
        """Socket which receives messages from Worker"""
        sock = self.__context.socket(zmq.PULL)
        sock.identity = "Reporter".encode("ascii")
        sock.bind("tcp://*:%d" % HsConstants.SENDER_PORT)
        return sock

    def create_workers_socket(self):
        # Socket to talk to Workers
        sock = self.__context.socket(zmq.PUB)
        sock.identity = "Workers".encode("ascii")
        sock.bind("tcp://*:%d" % HsConstants.PUBLISHER_PORT)
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

    @property
    def has_monitor(self):
        return self.__monitor is not None and self.__monitor.is_alive()

    @property
    def i3socket(self):
        return self.__i3socket

    def process_one_message(self, force_spade=False):
        found = False
        for sock, event in self.__poller.poll():
            if event != zmq.POLLIN:
                logging.error("Unknown event \"%s\"<%s> for %s<%s>", event,
                              type(event).__name__, sock, type(sock).__name__)
                continue

            if sock != self.__reporter and sock != self.__alert_socket:
                logging.error("Ignoring unknown incoming socket %s<%s>",
                              sock, type(sock).__name__)
                continue

            error = True
            try:
                msg = HsMessage.receive(sock)
                if msg is None:
                    continue
                error = False
            except HsException:
                raise
            except:
                logging.exception("Cannot receive message from %s",
                                  sock.identity)

            if not error:
                try:
                    self.__monitor.add_message(msg, force_spade)
                    found = True
                except:
                    logging.exception("Received bad message %s", str(msg))
                    error = True

            if sock == self.__alert_socket:
                if error:
                    rtnmsg = "ERROR"
                else:
                    rtnmsg = "DONE"

                # reply to requester:
                #  added \0 to fit C/C++ zmq message termination
                try:
                    answer = sock.send(rtnmsg + "\0")
                except zmq.ZMQError:
                    logging.exception("Cannot return \"%s\" to %s", rtnmsg,
                                      sock.identity)
                    continue

                if answer is not None:
                    logging.error("Failed sending %s to requester: %s", rtnmsg,
                                  answer)

        return found

    @property
    def monitor_started(self):
        return self.__monitor.is_started

    def move_to_destination_dir(self, copydir, copydir_user, prefix=None,
                                force_spade=False):
        """
        Move the data in case its default locations differs from
        the user's desired one.
        """
        if not os.path.isdir(copydir):
            logging.error("Source directory \"%s\" does not exist", copydir)
            return None, True

        logging.info("Checking the data location...")
        logging.info("HS data located at: %s", copydir)
        logging.info("user requested it to be in: %s", copydir_user)

        hs_basedir, data_dir_name = os.path.split(copydir)

        no_spade = False
        if not force_spade:
            if not self.__validate_destination_dir(data_dir_name, prefix):
                logging.error("Please put the data manually in the desired"
                              " location: %s", copydir_user)
                no_spade = True

        # if data looks valid, copy it to the requested directory
        if not no_spade:
            logging.info("HS data name: %s", data_dir_name)

            # strip rsync hostname if present
            try:
                _, target_base = HsUtil.split_rsync_host_and_path(copydir_user)
            except:
                logging.error("Illegal destination directory \"%s\"<%s>",
                              copydir_user, type(copydir_user))
                return None, True

            # build full path to target directory
            if target_base.endswith(data_dir_name):
                targetdir = os.path.dirname(target_base)
            else:
                targetdir = target_base

            if os.path.normpath(target_base) == os.path.normpath(hs_basedir):
                # data is already in the requested directory
                logging.info("HS data \"%s\" is located in \"%s\"",
                             data_dir_name, hs_basedir)
            else:
                # move files to requested directory
                if self.movefiles(copydir, targetdir):
                    hs_basedir = targetdir
                else:
                    no_spade = True

        return hs_basedir, no_spade

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

    @property
    def reporter(self):
        return self.__reporter

    def spade_pickup_data(self, hs_basedir, data_dir, prefix=None,
                          start_time=None, stop_time=None):
        '''
        tar & bzip folder
        move tar file to SPADE directory
        create semaphore/metadata file
        '''

        logging.info("Preparation for SPADE Pickup of HS data started...")

        if not self.__validate_destination_dir(data_dir, prefix):
            logging.error("Please put the data manually in the SPADE"
                          " directory. Use HsSpader.py, for example.")
            return None

        logging.info("HS data name: %s", data_dir)

        logging.info("copy_basedir is: %s", hs_basedir)

        if prefix == HsPrefix.SNALERT or prefix == HsPrefix.HESE:
            hs_basename = "HS_" + data_dir
            spade_dir = self.HS_SPADE_DIR
        else:
            hs_basename = data_dir
            spade_dir = hs_basedir

        result = self.queue_for_spade(hs_basedir, data_dir, spade_dir,
                                      hs_basename, start_time=start_time,
                                      stop_time=stop_time)
        if result is None:
            logging.error("Please put the data manually in the SPADE"
                          " directory. Use HsSpader.py, for example.")
        else:
            logging.info("SPADE file is %s", os.path.join(spade_dir,
                                                          result[0]))

        return result

    def wait_for_idle(self):
        for _ in range(10):
            if self.__monitor.is_idle:
                break
            time.sleep(0.1)

    @property
    def workers(self):
        return self.__workers


if __name__ == "__main__":
    import argparse

    def main():
        p = argparse.ArgumentParser()

        add_arguments(p)

        args = p.parse_args()

        HsBase.init_logging(args.logfile, basename="hssender",
                            basehost="2ndbuild")

        if args.state_db is not None:
            if RequestMonitor.STATE_DB_PATH is not None:
                raise SystemExit("HitSpool state database path has"
                                 " already been set")
            RequestMonitor.STATE_DB_PATH = args.state_db

        sender = HsSender(is_test=args.is_test)

        logging.info("HsSender starts on %s", sender.shorthost)

        # override some defaults (generally only used for debugging)
        if args.spadedir is not None:
            sender.HS_SPADE_DIR = args.spadedir

        # handler is called when SIGTERM is called (via pkill)
        signal.signal(signal.SIGTERM, sender.handler)

        while True:
            logging.info("HsSender waits for new reports from HsWorkers...")
            try:
                sender.process_one_message(force_spade=args.force_spade)
            except SystemExit:
                raise
            except KeyboardInterrupt:
                logging.warning("Interruption received, shutting down...")
                raise SystemExit(0)
            except zmq.ZMQError:
                logging.exception("ZMQ error received, shutting down...")
                raise SystemExit(1)
            except:
                logging.exception("Caught exception, continuing")

    main()
