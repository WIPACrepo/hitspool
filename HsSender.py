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
import traceback
import zmq

import HsMessage
import HsUtil

from HsBase import HsBase
from HsConstants import I3LIVE_PORT, SENDER_PORT
from HsException import HsException
from HsPrefix import HsPrefix


def add_arguments(parser):
    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hssender.log")

    parser.add_argument("-F", "--force-spade", dest="force_spade",
                        action="store_true", default=False,
                        help="Always queue files for JADE")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-S", "--spadedir", dest="spadedir",
                        help="Directory where files are queued to SPADE")


class RequestMonitor(threading.Thread):
    # stand-in for host name in HsPublisher initial message
    INITIAL_REQUEST = "@initial@"
    # key used to indicate an entry contains the request details
    DETAIL_KEY = "@detail@"
    # number of seconds a request can be "idle" before it's closed and
    # declared incomplete
    EXPIRE_SECONDS = 300.0

    def __init__(self, sender):
        self.__sender = sender

        # SQLite connection
        self.__sqlconn = None

        # request cache
        self.__requests = None
        self.__reqlock = threading.Condition()

        # thread-related attributes
        self.__stopping = False
        self.__msgqueue = []
        self.__msglock = threading.Condition()

        super(RequestMonitor, self).__init__(target=self.__run)

    def __close_database(self):
        if self.__sqlconn is not None:
            try:
                self.__sqlconn.close()
            except:
                logging.exception("Problem closing database")
            self.__sqlconn = None

    def __delete_request(self, request_id):
        with self.__reqlock:
            # fail if this is an unknown request
            if request_id not in self.__requests:
                return False

            # delete from the cache
            del self.__requests[request_id]

        # delete from the DB
        if self.__sqlconn is None:
            self.__sqlconn = self.__open_database()
        with self.__sqlconn:
            cursor = self.__sqlconn.cursor()

            # remove request from DB
            cursor.execute("delete from requests where id=?", (request_id, ))
            cursor.execute("delete from request_details where id=?",
                           (request_id, ))

    def __expire_requests(self):
        with self.__reqlock:
            if self.__requests is None or len(self.__requests) == 0:
                return

            now = datetime.datetime.now()
            expire_time = datetime.timedelta(seconds=self.EXPIRE_SECONDS)

            deleted = []
            for req_id in self.__requests:
                # find last update for this request
                latest = None
                for host, val in self.__requests[req_id].iteritems():
                    if host == self.DETAIL_KEY:
                        continue

                    update_time = val[1]
                    if latest is None:
                        latest = update_time
                    elif latest < update_time:
                        latest = update_time

                if latest is None:
                    ldiff = expire_time
                else:
                    ldiff = now - latest

                # if the last update was a long time ago
                if ldiff > expire_time:
                    (status, success, failed) \
                        = self.__get_request_status(req_id)
                    details = self.__requests[req_id][self.DETAIL_KEY]
                    status = HsUtil.STATUS_FAIL

                    # remember to delete this request later
                    deleted.append(req_id)

                    # send final message to Live
                    self.__finish_request(req_id, details[0], details[1],
                                          details[2], details[3], details[4],
                                          status, success, failed)

            # delete any expired requests
            for req_id in deleted:
                self.__delete_request(req_id)

    def __finish_request(self, req_id, username, prefix, start_time, stop_time,
                         dest_dir, status, success=None, failed=None):
        "send final message to LIVE"
        HsUtil.send_live_status(self.__sender.i3socket, req_id, username,
                                prefix, start_time, stop_time, dest_dir,
                                status, success=success, failed=failed)

    def __get_request_status(self, req_id):
        """
        Return True if all hosts for this request have completed
        """
        status = None
        success = []
        failed = []
        with self.__reqlock:
            for host, val in self.__requests[req_id].iteritems():
                if host == self.DETAIL_KEY:
                    continue

                state = val[0]
                if host == self.INITIAL_REQUEST and \
                   state == HsMessage.DBSTATE_INITIAL:
                    # ignore initial message
                    continue

                if state == HsMessage.DBSTATE_START:
                    # found in-progress request
                    return (None, None, None)

                if state == HsMessage.DBSTATE_DONE:
                    success.append(host)
                    new_status = HsUtil.STATUS_SUCCESS
                elif state == HsMessage.DBSTATE_ERROR:
                    failed.append(host)
                    new_status = HsUtil.STATUS_FAIL
                else:
                    logging.error("Unrecognized DB state %s", state)
                    continue

                if status is None:
                    status = new_status
                elif status != new_status:
                    status = HsUtil.STATUS_PARTIAL

        return (status, self.__hubs_to_string(success),
                self.__hubs_to_string(failed))

    def __handle_msg(self, msg, force_spade):
        logging.info("Req#%s %s %s", msg.request_id, msg.host, msg.msgtype)
        if msg.msgtype == HsMessage.MESSAGE_INITIAL:
            return self.__handle_req_initial(msg)
        elif msg.msgtype == HsMessage.MESSAGE_STARTED:
            return self.__handle_req_started(msg)
        elif (msg.msgtype == HsMessage.MESSAGE_DONE or
              msg.msgtype == HsMessage.MESSAGE_FAILED):
            return self.__handle_req_update(msg, force_spade=force_spade)

        logging.error("Ignoring unknown message type \"%s\" in \"%s\"",
                      msg.msgtype, msg)

    def __handle_req_initial(self, msg):
        with self.__reqlock:
            if msg.request_id in self.__requests:
                logging.error("Request %s was initialized multiple times",
                              msg.request_id)
                return

            self.__requests[msg.request_id] = {}

            # add new request to DB
            self.__update_db(msg.request_id, self.INITIAL_REQUEST,
                             HsMessage.DBSTATE_INITIAL)
            self.__insert_detail(msg.request_id, msg.username, msg.prefix,
                                 msg.start_time, msg.stop_time,
                                 msg.destination_dir)

        # send initial message to LIVE
        HsUtil.send_live_status(self.__sender.i3socket, msg.request_id,
                                msg.username, msg.prefix,
                                msg.start_time, msg.stop_time,
                                msg.destination_dir,
                                HsUtil.STATUS_QUEUED)

    def __handle_req_started(self, msg):
        with self.__reqlock:
            if msg.request_id not in self.__requests:
                logging.error("Request %s was not initialized (received"
                              " START from %s)", msg.request_id, msg.host)
                self.__requests[msg.request_id] = {}

            if len(self.__requests[msg.request_id]) == 1 and \
               self.DETAIL_KEY in self.__requests[msg.request_id]:
                # tell Live that the first host has started processing
                HsUtil.send_live_status(self.__sender.i3socket, msg.request_id,
                                        msg.username, msg.prefix,
                                        msg.start_time, msg.stop_time,
                                        msg.destination_dir,
                                        HsUtil.STATUS_IN_PROGRESS)

            if msg.host in self.__requests[msg.request_id]:
                state, _ = self.__requests[msg.request_id][msg.host]
                logging.error("Saw START message for request %s host %s"
                              " but current state is %s", msg.request_id,
                              msg.host, HsMessage.state_name(state))
            else:
                self.__requests[msg.request_id][msg.host] \
                    = (HsMessage.DBSTATE_START, datetime.datetime.now())
                if self.DETAIL_KEY not in self.__requests[msg.request_id]:
                    self.__insert_detail(msg.request_id, msg.username,
                                         msg.prefix, msg.start_time,
                                         msg.stop_time, msg.destination_dir)
                self.__update_db(msg.request_id, msg.host,
                                 HsMessage.DBSTATE_START)

    def __handle_req_update(self, msg, force_spade=False):
        """
        Used to update an individual host's state as either DONE or ERROR
        """
        if msg.msgtype == HsMessage.MESSAGE_DONE:
            moved = self.__handle_success(msg, force_spade=force_spade)
        elif msg.msgtype == HsMessage.MESSAGE_FAILED:
            moved = False
        else:
            raise HsException("Cannot update from request %s message %s" %
                              (msg.request_id, msg.msgtype))

        with self.__reqlock:
            if msg.request_id not in self.__requests:
                logging.error("Request %s was not initialized (received"
                              " %s from %s)", msg.request_id, msg.msgtype,
                              msg.host)
                self.__requests[msg.request_id] = {}

            if msg.host not in self.__requests[msg.request_id]:
                logging.error("Saw %s message for request %s host %s"
                              " without a START message", msg.msgtype,
                              msg.request_id, msg.host)
                self.__insert_detail(msg.request_id, msg.username, msg.prefix,
                                     msg.start_time, msg.stop_time,
                                     msg.destination_dir)

            if (msg.msgtype == HsMessage.MESSAGE_FAILED or
                 (moved is not None and not moved)):
                # if files were not queued for SPADE, record an error
                dbstate = HsMessage.DBSTATE_ERROR
            else:
                dbstate = HsMessage.DBSTATE_DONE

            self.__requests[msg.request_id][msg.host] \
                = (dbstate, datetime.datetime.now())

            # have all hosts reported success or failure?
            (status, success, failed) \
                = self.__get_request_status(msg.request_id)
            if status is None:
                self.__update_db(msg.request_id, msg.host, dbstate)
            else:
                # send final message to Live
                self.__finish_request(msg.request_id, msg.username, msg.prefix,
                                      msg.start_time, msg.stop_time,
                                      msg.destination_dir, status,
                                      success=success, failed=failed)

                # remove request from database
                self.__delete_request(msg.request_id)

    def __handle_success(self, msg, force_spade=False):
        copydir = msg.copy_dir
        copydir_user = msg.destination_dir

        hs_basedir, no_spade \
            = self.__sender.move_to_destination_dir(copydir, copydir_user,
                                                    prefix=msg.prefix,
                                                    force_spade=force_spade)
        if hs_basedir is None:
            # failed to move the files
            return False

        if force_spade or self.__sender.is_cluster_sps or \
           self.__sender.is_cluster_spts:
            if no_spade and not force_spade:
                logging.info("Not scheduling for SPADE pickup")
            else:
                _, data_dir = os.path.split(copydir)
                result = self.__sender.spade_pickup_data(hs_basedir, data_dir,
                                                         prefix=msg.prefix)
                if result is None:
                    return False
        return True

    def __hubs_to_string(self, hublist):
        """
        Convert list of hub hostnames to a compact string of ranges
        like "1-7 9-45 48-86"
        """
        if len(hublist) == 0:
            return None

        # convert hub names to numeric values
        numbers = []
        for hub in hublist:
            hostname = hub.split('.', 1)[0]
            if hostname.startswith("ichub"):
                offset = 0
            elif hostname.startswith("ithub"):
                offset = 200
            else:
                logging.error("Bad hostname \"%s\" for hub", hub)
                continue

            try:
                numbers.append(int(hostname[5:]) + offset)
            except ValueError:
                logging.error("Bad number for hub \"%s\"", hub)

        # sort hub numbers
        numbers.sort()

        # build comma-separated groups of ranges
        numStr = None
        prevNum = -1
        inRange = False
        for n in numbers:
            if numStr is None:
                numStr = str(n)
            else:
                if prevNum + 1 == n:
                    if not inRange:
                        inRange = True
                else:
                    if inRange:
                        numStr += "-" + str(prevNum)
                        inRange = False
                    numStr += " " + str(n)
            prevNum = n
        if numStr is None:
            # this should never happen?
            numStr = ""
        elif inRange:
            # append end of final range
            numStr += "-" + str(prevNum)

    def __insert_detail(self, request_id, username, prefix, start_time,
                        stop_time, dest_dir):
        if self.__sqlconn is None:
            self.__sqlconn = self.__open_database()
        with self.__sqlconn:
            cursor = self.__sqlconn.cursor()

            # add details to DB
            cursor.execute("insert or replace into request_details"
                           "(id, username, prefix, start_time, stop_time,"
                           " destination)"
                           " values (?, ?, ?, ?, ?, ?)",
                           (request_id, username, prefix, start_time,
                            stop_time, dest_dir))

            # add details to cache
            with self.__reqlock:
                self.__requests[request_id][self.DETAIL_KEY] \
                    = (username, prefix, start_time, stop_time, dest_dir)

    def __load_state_db(self):
        if self.__sqlconn is None:
            self.__sqlconn = self.__open_database()
        with self.__sqlconn:  # automatically commits the active cursor
            cursor = self.__sqlconn.cursor()

            # make sure the tables exist
            cursor.execute("create table if not exists requests("
                           " id text not null,"
                           " host text not null,"
                           " phase integer not null,"
                           " update_time timestamp not null,"
                           " primary key (id, host))")
            cursor.execute("create table if not exists request_details("
                           " id text not null,"
                           " username text,"
                           " prefix text,"
                           " start_time timestamp not null,"
                           " stop_time timestamp not null,"
                           " destination text not null,"
                           " primary key (id))")

        # load requests from DB
        requests = {}
        for row in cursor.execute("select id, host, phase, update_time"
                                  " from requests"
                                  " order by id, host, phase"):
            req_id = row[0]
            host = row[1]
            phase = int(row[2])
            update_time = row[3]

            if phase == self.__sender.PHASE_INIT:
                if req_id in requests:
                    logging.error("Found multiple initial entries"
                                  " for request %s", req_id)
                else:
                    requests[req_id] = {}
                continue

            if req_id not in requests:
                logging.error("Request %s does not have an initial DB entry",
                              req_id)
                requests[req_id] = {}

            requests[req_id][host] = (phase, update_time)

        # load request details from DB
        for row in cursor.execute("select id, username, prefix, start_time,"
                                  " stop_time, destination"
                                  " from request_details"):
            req_id = row[0]
            username = row[1]
            prefix = row[2]
            start_time = row[3]
            stop_time = row[4]
            dest_dir = row[5]

            requests[req_id][self.DETAIL_KEY] = (username, prefix, start_time,
                                                 stop_time, dest_dir)

        return requests

    def __mainloop(self):
        # check for messages
        with self.__msglock:
            # if there are no messages, wait
            if not self.__stopping and len(self.__msgqueue) == 0:
                with self.__reqlock:
                    if not self.__stopping:
                        # wake up after a while
                        self.__msglock.wait(self.EXPIRE_SECONDS)

            # process any queued messages
            while len(self.__msgqueue) > 0:
                msg, force_spade = self.__msgqueue.pop(0)
                self.__handle_msg(msg, force_spade)

        # expire overdue requests
        self.__expire_requests()

    def __open_database(self):
        dbpath = HsSender.get_db_path()
        return sqlite3.connect(dbpath)

    def __run(self):
        self.__stopping = False

        # load cached requests from database
        self.__requests = self.__load_state_db()

        # loop until stopped
        while not self.__stopping:
            try:
                self.__mainloop()
            except:
                traceback.print_exc()
                logging.exception("RequestMonitor exception")

        self.__close_database()
        self.__stopping = False

    def __update_db(self, request_id, host, state):
        if self.__sqlconn is None:
            self.__sqlconn = self.__open_database()
        with self.__sqlconn:
            cursor = self.__sqlconn.cursor()

            # add new request to DB
            cursor.execute("insert or replace into requests"
                           "(id, host, phase, update_time)"
                           " values (?, ?, ?, ?)",
                           (request_id, host, state, datetime.datetime.now()))

    def add_message(self, msg, force_spade):
        with self.__msglock:
            self.__msgqueue.append((msg, force_spade))
            self.__msglock.notify()

    def is_idle(self):
        "Only used by unit tests"
        with self.__msglock:
            return (self.__msgqueue is None or
                    len(self.__msgqueue) == 0) and \
                    (self.__requests is None or len(self.__requests) == 0)

    def stop(self):
        with self.__msglock:
            self.__stopping = True
            self.__msglock.notify()


class HsSender(HsBase):
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
    STATE_DB_PATH = None

    PHASE_INIT = 0
    PHASE_START = 1
    PHASE_ERROR = 2
    PHASE_DONE = 99

    def __init__(self, host=None, is_test=False):
        super(HsSender, self).__init__(host=host, is_test=is_test)

        if self.is_cluster_sps or self.is_cluster_spts:
            expcont = "expcont"
        else:
            expcont = "localhost"

        self.__context = zmq.Context()
        self.__reporter = self.create_reporter()
        self.__i3socket = self.create_i3socket(expcont)
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
        self.__i3socket.close()
        self.__context.term()

        self.__monitor.stop()
        self.__monitor.join()

    def create_reporter(self):
        """Socket which receives messages from Worker"""
        sock = self.__context.socket(zmq.PULL)
        sock.bind("tcp://*:%d" % SENDER_PORT)
        return sock

    def create_i3socket(self, host):
        """Socket for I3Live on expcont"""
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, I3LIVE_PORT))
        return sock

    @classmethod
    def get_db_path(cls):
        if cls.STATE_DB_PATH is None:
            cls.STATE_DB_PATH = os.path.join(os.environ["HOME"],
                                             ".hitspool_state.db")
        return cls.STATE_DB_PATH

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
    def i3socket(self):
        return self.__i3socket

    def mainloop(self, force_spade=False):
        msg = HsMessage.receive(self.__reporter)
        if msg is None:
            return

        self.__monitor.add_message(msg, force_spade)

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
            _, target_base = HsUtil.split_rsync_host_and_path(copydir_user)

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

    def spade_pickup_data(self, hs_basedir, data_dir, prefix=None):
        '''
        tar & bzip folder
        create semaphore file for folder
        move .sem & .dat.tar.bz2 file in SPADE directory
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

        hs_bzipname = hs_basename + ".dat.tar.bz2"
        hs_spade_semfile = hs_basename + ".sem"

        if not self.queue_for_spade(hs_basedir, data_dir, spade_dir,
                                    hs_bzipname, hs_spade_semfile):
            logging.error("Please put the data manually in the SPADE"
                          " directory. Use HsSpader.py, for example.")
            return None

        logging.info("SPADE file is %s", os.path.join(spade_dir, hs_bzipname))

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

    def wait_for_idle(self):
        for _ in range(10):
            if self.__monitor.is_idle():
                break
            time.sleep(0.1)


if __name__ == "__main__":
    import argparse

    def main():
        p = argparse.ArgumentParser()

        add_arguments(p)

        args = p.parse_args()

        sender = HsSender()

        # override some defaults (generally only used for debugging)
        if args.spadedir is not None:
            sender.HS_SPADE_DIR = args.spadedir

        # handler is called when SIGTERM is called (via pkill)
        signal.signal(signal.SIGTERM, sender.handler)

        sender.init_logging(args.logfile, basename="hssender",
                            basehost="2ndbuild")

        logging.info("HsSender starts on %s", sender.shorthost)

        while True:
            logging.info("HsSender waits for new reports from HsWorkers...")
            try:
                sender.mainloop(force_spade=args.force_spade)
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
