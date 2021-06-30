#!/usr/bin/env python

import argparse
import datetime
import logging
import os
import re
import shutil
import signal
import threading
import time
import zmq

import HsConstants
import HsMessage
import HsUtil

from HsBase import HsBase
from HsException import HsException
from HsPrefix import HsPrefix
from RequestMonitor import RequestMonitor


def add_arguments(parser):
    "Add all command line arguments to the argument parser"

    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hssender.log")

    parser.add_argument("-C", "--copydir", dest="copydir",
                        help="Directory where files are staged")
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


class PingClient(object):
    """
    Track state of all clients
    """
    def __init__(self, host, last_time):
        self.__host = host
        self.__last_time = last_time
        self.__dead = False

    def __str__(self):
        return "%s[%s]" % \
            (self.__host, "DEAD" if self.__dead else self.__last_time)

    @property
    def is_dead(self):
        return self.__dead

    def set_dead(self):
        self.__dead = True

    def set_time(self, new_time):
        self.__last_time = new_time
        self.__dead = False

    @property
    def time(self):
        return self.__last_time


class PingManager(object):
    """
    Track pings to clients and the clients' responses
    """

    PING_MAX_FAILURES = 3
    PING_SLEEP_SECONDS = 600
    PING_TIMEOUT_MISSING = PING_SLEEP_SECONDS * 2
    PING_TIMEOUT_DEAD = PING_TIMEOUT_MISSING * PING_MAX_FAILURES

    def __init__(self, sock, i3sock):
        self.__sock = sock
        self.__i3socket = i3sock

        self.__lock = threading.RLock()
        self.__running = False

        self.__ping_client = {}

    def __check_ping_clients(self):
        missing = {}
        now = datetime.datetime.now()
        for host, pingobj in self.__ping_client.items():
            pdiff = now - pingobj.time
            if pdiff.days > 0 or pdiff.seconds > self.PING_TIMEOUT_DEAD:
                if not self.__ping_client[host].is_dead:
                    self.__report_dead_host(host, pdiff)
                    self.__ping_client[host].set_dead()
            elif pdiff.seconds > self.PING_TIMEOUT_MISSING:
                missing[host] = pdiff

        if len(missing) > 0:
            self.__report_missing_pings(missing)

    def __report_dead_host(self, host, ping_delta):
        header = "HsSender Alert: %s Worker has died" % (host, )
        description = "HsSender alert"
        mlines = ["Worker on %s has not returned a PING in %s" %
                  (host, ping_delta), ]

        json = HsUtil.assemble_email_dict(HsConstants.ALERT_EMAIL_DEV,
                                          header, "\n".join(mlines),
                                          description=description)

        self.__i3socket.send_json(json)

    @classmethod
    def __report_missing_pings(cls, missing):
        num = len(missing)
        msg = "Missing %d ping%s:" % (num, "" if num == 1 else "s")
        for host, misstime in missing.items():
            msg += " %s(%s)" % (host, misstime)
        logging.error(msg)

    def __report_resurrected_host(self, host):
        header = "HsSender Notice: %s Worker has returned" % (host, )
        description = "HsSender notice"
        mlines = ["Worker on %s is once again returning PINGs" % (host, ), ]

        json = HsUtil.assemble_email_dict(HsConstants.ALERT_EMAIL_DEV,
                                          header, "\n".join(mlines),
                                          description=description)

        self.__i3socket.send_json(json)

    def __run_thread(self):
        self.__running = True
        while self.__running:
            try:
                time.sleep(self.PING_SLEEP_SECONDS)

                self.__check_ping_clients()

                with self.__lock:
                    self.__sock.send_json({"ping": "SENDER"})
            except:
                logging.exception("PingManager problem!")

    def add_reply(self, host):
        now = datetime.datetime.now()
        if host not in self.__ping_client:
            self.__ping_client[host] = PingClient(host, now)
        else:
            pdiff = now - self.__ping_client[host].time
            if pdiff.days > 0 or pdiff.seconds > self.PING_TIMEOUT_DEAD:
                self.__report_resurrected_host(host)
            self.__ping_client[host].set_time(now)

    @property
    def lock(self):
        return self.__lock

    def start_thread(self):
        thrd = threading.Thread(name="PingMgr", target=self.__run_thread)
        thrd.setDaemon(True)
        thrd.start()
        return thrd

    def stop_thread(self):
        self.__running = False


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

    def __init__(self, copydir=None, host=None, is_test=False):
        super(HsSender, self).__init__(host=host, is_test=is_test)

        if self.is_cluster_sps or self.is_cluster_spts:
            expcont = "expcont"
        else:
            expcont = "localhost"

        # make sure the default staging directory exists
        if copydir is None:
            copydir = self.DEFAULT_COPY_PATH
        if not os.path.exists(copydir):
            os.makedirs(copydir)

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

        self.__ping_manager = PingManager(self.__workers, self.__i3socket)
        self.__ping_manager.start_thread()

    @classmethod
    def __validate_destination_dir(cls, dirname, prefix):
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
        self.__ping_manager.stop_thread()

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

    @classmethod
    def create_poller(cls):
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

        self.close_all()

        raise SystemExit(1)

    @property
    def has_monitor(self):
        return self.__monitor is not None and self.__monitor.is_alive()

    @property
    def i3socket(self):
        return self.__i3socket

    def mainloop(self, force_spade=False):
        rtnval = True
        for sock, event in self.__poller.poll():
            if sock not in (self.__reporter, self.__alert_socket):
                logging.error("Ignoring unknown incoming socket %s<%s>",
                              sock.identity, type(sock).__name__)
                continue

            if event != zmq.POLLIN:
                logging.error("Unknown event \"%s\"<%s> for %s<%s>",
                              event, type(event).__name__,
                              sock.identity, type(sock).__name__)
                continue

            # lock the ping manager so it doesn't try to use our sockets
            #  while we're doing "real" work
            with self.__ping_manager.lock:
                rtnval &= self.process_one_message(sock,
                                                   force_spade=force_spade)

        return rtnval

    def process_one_message(self, sock, force_spade=False):
        mdict = sock.recv_json()
        if mdict is None:
            return False

        if not isinstance(mdict, dict):
            raise HsException("Received %s(%s), not dictionary" %
                              (mdict, type(mdict).__name__))

        if sock == self.__reporter and "pingback" in mdict:
            self.__ping_manager.add_reply(mdict["pingback"])
            return True

        error = True
        try:
            msg = HsMessage.from_dict(mdict, allow_old_format=True)
            if msg is None:
                return False
            error = False
        except HsException:
            raise
        except:
            logging.exception("Cannot receive message from %s",
                              sock.identity)

        found = False
        if not error:
            try:
                self.__monitor.add_message(msg, force_spade)
                found = True
            except:
                logging.exception("Received bad message %s", str(msg))
                error = True

        if error:
            rtnmsg = "ERROR"
        else:
            rtnmsg = "DONE"

        if sock != self.__alert_socket:
            logging.error("Cannot send %s alert to unknown socket <%s>%s"
                          " (expected <%s>%s)" %
                          (rtnmsg, type(sock), sock, type(self.__alert_socket),
                           self.__alert_socket))
        else:
            # reply to requester:
            #  added \0 to fit C/C++ zmq message termination
            try:
                answer = sock.send_string(rtnmsg + "\0")
                if answer is not None:
                    logging.error("Failed sending %s to requester: %s",
                                  rtnmsg, answer)
            except zmq.ZMQError:
                logging.exception("Cannot return \"%s\" to %s", rtnmsg,
                                  sock.identity)

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

    @classmethod
    def movefiles(cls, copydir, targetdir):
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
        except shutil.Error as err:
            logging.error("Cannot move \"%s\" to \"%s\": %s", copydir,
                          targetdir, err)
            return False

        return True

    @property
    def reporter(self):
        return self.__reporter

    def spade_pickup_data(self, hs_basedir, data_dir, prefix=None,
                          start_ticks=None, stop_ticks=None):
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

        if prefix in (HsPrefix.SNALERT, HsPrefix.HESE):
            hs_basename = "HS_" + data_dir
            spade_dir = self.HS_SPADE_DIR
        else:
            hs_basename = data_dir
            spade_dir = hs_basedir

        result = self.queue_for_spade(hs_basedir, data_dir, spade_dir,
                                      hs_basename, start_ticks=start_ticks,
                                      stop_ticks=stop_ticks)
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


def main():
    "Main method"

    parser = argparse.ArgumentParser()

    add_arguments(parser)

    args = parser.parse_args()

    HsBase.init_logging(args.logfile, basename="hssender",
                        basehost="2ndbuild")

    if args.state_db is not None:
        if RequestMonitor.STATE_DB_PATH is not None:
            raise SystemExit("HitSpool state database path has"
                             " already been set")
        RequestMonitor.STATE_DB_PATH = args.state_db

    sender = HsSender(copydir=args.copydir, is_test=args.is_test)

    logging.info("HsSender starts on %s", sender.shorthost)

    # override some defaults (generally only used for debugging)
    if args.spadedir is not None:
        sender.HS_SPADE_DIR = args.spadedir

    # handler is called when SIGTERM is called (via pkill)
    signal.signal(signal.SIGTERM, sender.handler)

    while True:
        logging.debug("HsSender waits for new messages")
        try:
            sender.mainloop(force_spade=args.force_spade)
        except SystemExit:
            raise
        except KeyboardInterrupt:
            logging.warning("Interruption received, shutting down...")
            raise SystemExit(0)
        except zmq.ZMQError as zex:
            if str(zex).find("Socket operation on non-socket") < 0:
                logging.exception("ZMQ error received, shutting down...")
            raise SystemExit(1)
        except:
            logging.exception("Caught exception, continuing")


if __name__ == "__main__":
    try:
        main()
    except:
        logging.exception("HsSender failed")
