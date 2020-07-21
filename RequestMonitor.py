#!/usr/bin/env python


import datetime
import logging
import numbers
import os
import sqlite3
import threading

import DAQTime
import HsConstants
import HsMessage
import HsUtil

from HsException import HsException
from HsPrefix import HsPrefix


class RequestMonitor(threading.Thread):
    "Monitor requests from all hubs"

    # path to the SQLite database which acts as a disk cache
    STATE_DB_PATH = None

    # stand-in for host name in HsPublisher initial message
    INITIAL_REQUEST = "@initial@"
    # key used to indicate an entry contains the request details
    DETAIL_KEY = "@detail@"
    # number of seconds a request can be "idle" before it's closed and
    # declared incomplete
    EXPIRE_SECONDS = 3600.0

    # database request phases
    DBPHASE_INITIAL = 0
    DBPHASE_QUEUED = 20
    DBPHASE_START = 40
    DBPHASE_WORKING = 60
    DBPHASE_DONE = 80
    DBPHASE_IGNORED = 90
    DBPHASE_ERROR = 99

    def __init__(self, sender):
        self.__sender = sender

        # SQLite connection
        self.__sqlconn = None

        # request cache
        self.__requests = None
        self.__reqlock = threading.Condition()
        self.__active = None

        # thread-related attributes
        self.__stopping = False
        self.__running = False
        self.__msgqueue = []
        self.__msglock = threading.Condition()

        super(RequestMonitor, self).__init__(target=self.__run)

    def __build_json_email(self, start_ticks, stop_ticks, prefix, extract):
        alertmsg = "Start: %s\nStop: %s\n(no possible leapseconds applied)" % \
                   (DAQTime.ticks_to_utc(start_ticks),
                    DAQTime.ticks_to_utc(stop_ticks))
        if extract:
            alertmsg += "\nExtracting matching hits"

        notify_hdr = "DATA REQUEST HsInterface Alert: %s" % \
                     str(self.__sender.cluster)

        # build list of recipients
        address_list = HsConstants.ALERT_EMAIL_DEV[:]
        if prefix == HsPrefix.SNALERT:
            address_list += HsConstants.ALERT_EMAIL_SN

        # if we found one or more recipients, send an alert email
        if len(address_list) == 0:
            return None

        return HsUtil.assemble_email_dict(address_list, notify_hdr, alertmsg,
                                          prio=1)

    def __check_if_active(self, msg):
        "If this message is not for the active request, log an error"
        if msg.request_id != self.__active:
            if self.__active is None:
                astr = " (no active request)"
            else:
                astr = " (Req#%s is active)" % self.__active
            logging.error("Received unexpected %s message from %s for"
                          " Req#%s%s", msg.msgtype, msg.host, msg.request_id,
                          astr)

    def __close_database(self):
        if self.__sqlconn is not None:
            try:
                self.__sqlconn.close()
            except:
                logging.exception("Problem closing database")
            self.__sqlconn = None

    def __delete_request(self, request_id):
        with self.__reqlock:
            if self.__requests is not None:
                # fail if this is an unknown request
                if request_id not in self.__requests:
                    return False

                # delete from the cache
                del self.__requests[request_id]

                # if this was the active request, it's not active anymore!
                if self.__active == request_id:
                    self.__active = None

        # delete from the DB
        if self.__sqlconn is None:
            self.__sqlconn = self.__open_database()
        with self.__sqlconn:
            cursor = self.__sqlconn.cursor()

            # remove request from DB
            cursor.execute("delete from requests where id=?", (request_id, ))
            cursor.execute("delete from request_details where id=?",
                           (request_id, ))

        return True

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
                host = None
                for hst, val in self.__requests[req_id].items():
                    if hst == self.DETAIL_KEY:
                        continue

                    update_time = val[1]
                    if latest is None:
                        latest = update_time
                        host = hst
                    elif latest < update_time:
                        latest = update_time
                        host = hst

                if latest is None:
                    ldiff = expire_time
                else:
                    try:
                        ldiff = now - latest
                    except TypeError:
                        logging.exception("Skipping req#%s: now %s<%s>"
                                          " latest %s<%s> (host %s)", req_id,
                                          now, type(now), latest, type(latest),
                                          host)
                        continue

                # if the last update was a long time ago
                if ldiff > expire_time:
                    # remember to delete this request later
                    deleted.append(req_id)

                    # gather request details
                    (success, failed) \
                        = self.__get_request_status(req_id)
                    details = self.__requests[req_id][self.DETAIL_KEY]

                    # send final message to Live
                    self.__finish_request(req_id, details[0], details[1],
                                          details[2], details[3], details[4],
                                          HsUtil.STATUS_FAIL, success=success,
                                          failed=failed)

            # delete any expired requests
            for req_id in deleted:
                self.__delete_request(req_id)

    def __finish_request(self, req_id, username, prefix, start_ticks,
                         stop_ticks, dest_dir, status, success=None,
                         failed=None):
        "send final message to LIVE"
        logging.info("Req#%s %s%s%s", req_id, status,
                     "" if success is None else " success=%s" % success,
                     "" if failed is None else " failed=%s" % failed)
        HsUtil.send_live_status(self.__sender.i3socket, req_id, username,
                                prefix, start_ticks, stop_ticks, dest_dir,
                                status, success=success, failed=failed)

    def __get_request_status(self, req_id):
        """
        Return lists of all hosts which have finished the specified request
        """
        success = []
        failed = []
        with self.__reqlock:
            for host, val in self.__requests[req_id].items():
                if host == self.DETAIL_KEY:
                    continue

                phase, _ = val
                if host == self.INITIAL_REQUEST and \
                   phase == self.DBPHASE_INITIAL:
                    # ignore initial message
                    continue

                if phase in (self.DBPHASE_START, self.DBPHASE_WORKING):
                    # found in-progress request
                    return (None, None)

                if phase == self.DBPHASE_DONE:
                    success.append(host)
                elif phase == self.DBPHASE_ERROR:
                    failed.append(host)
                elif phase != self.DBPHASE_IGNORED:
                    logging.error("Unrecognized DB phase %s", phase)
                    continue

        return (HsUtil.hubs_to_string(success), HsUtil.hubs_to_string(failed))

    def __handle_msg(self, msg, force_spade):
        if msg.msgtype == HsMessage.INITIAL:
            self.__handle_req_initial(msg)
        elif msg.msgtype == HsMessage.STARTED:
            self.__handle_req_started(msg)
        elif msg.msgtype == HsMessage.WORKING:
            self.__handle_req_working(msg)
        elif msg.msgtype == HsMessage.DELETE:
            self.__handle_req_delete(msg)
        elif (msg.msgtype == HsMessage.IGNORED or
              msg.msgtype == HsMessage.DONE or
              msg.msgtype == HsMessage.FAILED):
            self.__handle_req_completed(msg, force_spade=force_spade)
        else:
            logging.error("Not handling message type \"%s\" in \"%s\"",
                          msg.msgtype, msg)

    def __handle_req_initial(self, msg):
        with self.__reqlock:
            if msg.request_id in self.__requests:
                logging.error("Request %s was initialized multiple times",
                              msg.request_id)
                return

            self.__requests[msg.request_id] = {}

            if hasattr(msg, "start_ticks") and hasattr(msg, "stop_ticks"):
                start_ticks = msg.start_ticks
                stop_ticks = msg.stop_ticks
            elif hasattr(msg, "start_time") and hasattr(msg, "stop_time"):
                start_ticks = DAQTime.string_to_ticks(msg.start_time)
                stop_ticks = DAQTime.string_to_ticks(msg.stop_time)
            else:
                logging.error("Missing/bad start/stop times in request %s",
                              str(msg))
                return

            # make a copy of the 'extract' field, may need to change it below
            extract = msg.extract

            duration = stop_ticks - start_ticks
            if duration <= 0:
                logging.error("Start time is after stop time in request %s",
                              str(msg))
                return
            if extract and duration > HsConstants.INTERVAL * 2:
                # don't extract large requests to a new file,
                #  it can overload the hubs and lose data
                extract = False

            # add new request to DB
            self.__update_db(msg.request_id, self.INITIAL_REQUEST,
                             self.DBPHASE_QUEUED)
            self.__insert_detail(msg.request_id, msg.username, msg.prefix,
                                 start_ticks, stop_ticks,
                                 msg.destination_dir, msg.hubs, extract,
                                 self.DBPHASE_QUEUED)

        # tell LIVE that we've received the request
        HsUtil.send_live_status(self.__sender.i3socket, msg.request_id,
                                msg.username, msg.prefix, start_ticks,
                                stop_ticks, msg.destination_dir,
                                HsUtil.STATUS_QUEUED)

    def __handle_req_delete(self, msg):
        with self.__reqlock:
            if msg.request_id not in self.__requests:
                logging.error("Request %s was not found (for DELETE)",
                              msg.request_id)
                return

            if len(self.__requests[msg.request_id]) > 1:
                logging.error("Request %s is already active", msg.request_id)
                return

            # remove request from database
            self.__delete_request(msg.request_id)
            logging.info("Deleted request %s", msg.request_id)

    def __handle_req_started(self, msg):
        with self.__reqlock:
            self.__check_if_active(msg)
            if msg.request_id not in self.__requests:
                logging.error("Request %s was not initialized (received"
                              " START from %s)", msg.request_id, msg.host)
                self.__requests[msg.request_id] = {}

            if len(self.__requests[msg.request_id]) == 1 and \
               self.DETAIL_KEY in self.__requests[msg.request_id]:
                # tell Live that the first host has started processing
                HsUtil.send_live_status(self.__sender.i3socket, msg.request_id,
                                        msg.username, msg.prefix,
                                        msg.start_ticks, msg.stop_ticks,
                                        msg.destination_dir,
                                        HsUtil.STATUS_IN_PROGRESS)

            if msg.host in self.__requests[msg.request_id]:
                phase, _ = self.__requests[msg.request_id][msg.host]
                logging.error("Saw START message for request %s host %s"
                              " but current phase is %s", msg.request_id,
                              msg.host, self.__phase_name(phase))
            else:
                self.__requests[msg.request_id][msg.host] \
                    = (self.DBPHASE_START, datetime.datetime.now())
                if self.DETAIL_KEY not in self.__requests[msg.request_id]:
                    self.__insert_detail(msg.request_id, msg.username,
                                         msg.prefix, msg.start_ticks,
                                         msg.stop_ticks, msg.destination_dir,
                                         msg.hubs, msg.extract,
                                         self.DBPHASE_START)
                self.__update_db(msg.request_id, msg.host,
                                 self.DBPHASE_START)

    def __handle_req_working(self, msg):
        with self.__reqlock:
            self.__check_if_active(msg)
            if msg.request_id not in self.__requests:
                logging.error("Request %s was not initialized (received"
                              " WORKING from %s)", msg.request_id, msg.host)
                self.__requests[msg.request_id] = {}

            if msg.host not in self.__requests[msg.request_id]:
                logging.error("Saw WORKING message for request %s host %s"
                              " but host was unknown", msg.request_id,
                              msg.host)
            else:
                self.__requests[msg.request_id][msg.host] \
                    = (self.DBPHASE_WORKING, datetime.datetime.now())
                if self.DETAIL_KEY not in self.__requests[msg.request_id]:
                    self.__insert_detail(msg.request_id, msg.username,
                                         msg.prefix, msg.start_ticks,
                                         msg.stop_ticks, msg.destination_dir,
                                         msg.hubs, msg.extract,
                                         self.DBPHASE_WORKING)
                self.__update_db(msg.request_id, msg.host,
                                 self.DBPHASE_WORKING)

    def __handle_req_completed(self, msg, force_spade=False):
        """
        Used to update an individual host's phase as either DONE or ERROR
        """
        if msg.msgtype == HsMessage.DONE:
            moved = self.__handle_success(msg, force_spade=force_spade)
        else:
            moved = False

        with self.__reqlock:
            self.__check_if_active(msg)
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
                                     msg.start_ticks, msg.stop_ticks,
                                     msg.destination_dir, msg.hubs,
                                     msg.extract, self.DBPHASE_WORKING)

            if msg.msgtype == HsMessage.IGNORED:
                dbphase = self.DBPHASE_IGNORED
            elif msg.msgtype == HsMessage.FAILED or not moved:
                # if files were not queued for SPADE, record an error
                dbphase = self.DBPHASE_ERROR
            else:
                dbphase = self.DBPHASE_DONE

            self.__requests[msg.request_id][msg.host] \
                = (dbphase, datetime.datetime.now())

            # have all hosts reported success or failure?
            (success, failed) \
                = self.__get_request_status(msg.request_id)
            if (success is None or len(success) == 0) and \
               (failed is None or len(failed) == 0):
                # no completed hubs, update the current hub's phase
                self.__update_db(msg.request_id, msg.host, dbphase)
            else:
                if success is None or len(success) == 0:
                    # no successful hubs, request must have failed
                    status = HsUtil.STATUS_FAIL
                elif failed is None or len(failed) == 0:
                    # no failed hubs, request must have succeeded
                    status = HsUtil.STATUS_SUCCESS
                else:
                    # found a mix of successful and failed hubs
                    status = HsUtil.STATUS_PARTIAL

                # send final message to Live
                self.__finish_request(msg.request_id, msg.username, msg.prefix,
                                      msg.start_ticks, msg.stop_ticks,
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
                snd = self.__sender
                result = snd.spade_pickup_data(hs_basedir, data_dir,
                                               prefix=msg.prefix,
                                               start_ticks=msg.start_ticks,
                                               stop_ticks=msg.stop_ticks)
                if result is None:
                    return False
        return True

    def __insert_detail(self, request_id, username, prefix, start_ticks,
                        stop_ticks, dest_dir, hubs, extract, phase):
        if start_ticks is None:
            raise HsException("Start time cannot be None")
        elif not isinstance(start_ticks, numbers.Number):
            raise HsException("Start time must be number, not %s(%s)" %
                              (type(start_ticks), str(start_ticks)))
        if stop_ticks is None:
            raise HsException("Stop time cannot be None")
        elif not isinstance(stop_ticks, numbers.Number):
            raise HsException("Stop time must be number, not %s(%s)" %
                              (type(stop_ticks), str(stop_ticks)))

        if self.__sqlconn is None:
            self.__sqlconn = self.__open_database()
        with self.__sqlconn:
            cursor = self.__sqlconn.cursor()

            # add details to DB
            cursor.execute("insert or replace into request_details"
                           "(id, username, prefix, start_time, stop_time,"
                           " destination, hubs, extract, phase)"
                           " values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (request_id, username, prefix, start_ticks,
                            stop_ticks, dest_dir, "" if hubs is None else hubs,
                            1 if extract else 0, phase))

            # add details to cache
            with self.__reqlock:
                self.__requests[request_id][self.DETAIL_KEY] \
                    = (username, prefix, start_ticks, stop_ticks, dest_dir,
                       hubs, extract, phase)

    @classmethod
    def __list_dbtable_columns(cls, conn, table_name):
        cursor = conn.execute("select * from %s" % table_name)
        return [desc[0] for desc in cursor.description]

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
                           " hubs text not null,"
                           " extract integer not null,"
                           " phase integer not null,"
                           " primary key (id))")

        # load requests from DB
        requests = {}
        for row in cursor.execute("select id, host, phase, update_time"
                                  " from requests"
                                  " order by id, host, phase"):
            req_id = row[0]
            host = row[1]
            phase = int(row[2])
            update_time = datetime.datetime.strptime(row[3],
                                                     DAQTime.TIME_FORMAT)

            if phase in (self.DBPHASE_INITIAL, self.DBPHASE_QUEUED):
                if req_id in requests:
                    logging.error("Found multiple initial entries"
                                  " for request %s", req_id)
                else:
                    requests[req_id] = {}
                continue

            if req_id not in requests:
                logging.error("Request %s does not have an initial DB entry"
                              " (phase is %s)", req_id,
                              self.__phase_name(phase))
                requests[req_id] = {}

            requests[req_id][host] = (phase, update_time)

        # load request details from DB
        for row in cursor.execute("select id, username, prefix, start_time,"
                                  " stop_time, destination, hubs, extract,"
                                  " phase"
                                  " from request_details"):
            req_id = row[0]
            username = row[1]
            prefix = row[2]
            start_ticks = row[3]
            stop_ticks = row[4]
            dest_dir = row[5]
            hubs = row[6]
            extract = row[7] != 0
            phase = row[8]

            try:
                requests[req_id][self.DETAIL_KEY] \
                    = (username, prefix, start_ticks, stop_ticks, dest_dir,
                       hubs, extract, phase)
            except KeyError:
                logging.error("Req#%s is in request_details table"
                              " but not requests dictionary; ignored", req_id)
                continue

            if phase == self.DBPHASE_QUEUED:
                if self.__active is None:
                    self.__active = req_id
                elif len(requests[req_id]) > 1:
                    if len(requests[self.__active]) == 1:
                        self.__active = req_id
                    else:
                        logging.error("Found multiple active requests"
                                      " (%s and %s)", self.__active, req_id)

        return requests

    @classmethod
    def __migrate_db_if_needed(cls, conn):
        "Update database tables/columns if needed"
        try:
            detail_cols = cls.__list_dbtable_columns(conn, "request_details")
        except:
            return

        if "hubs" not in detail_cols:
            # the Hit_Girl release added a 'hubs' column
            #  to 'request_details'
            conn.execute("alter table request_details"
                         " add column \"hubs\" \"text\"")
            logging.info("Added 'hubs' column to 'request_details' table")

        if "extract" not in detail_cols:
            # the Hit_Girl release added an 'extract' column
            #  to 'request_details'
            conn.execute("alter table request_details"
                         " add column \"extract\" \"integer\"")
            logging.info("Added 'extract' column to 'request_details' table")

        if "phase" not in detail_cols:
            # the Hit_Girl release added a 'phase' column
            #  to 'request_details'
            conn.execute("alter table request_details"
                         " add column \"phase\" \"integer\"")
            logging.info("Added 'phase' column to 'request_details' table")

    def __open_database(self):
        dbpath = self.get_db_path()
        conn = sqlite3.connect(dbpath)
        self.__migrate_db_if_needed(conn)
        return conn

    @classmethod
    def __phase_name(cls, phase):
        if phase == cls.DBPHASE_INITIAL:
            return "INITIAL"
        if phase == cls.DBPHASE_QUEUED:
            return "QUEUED"
        if phase == cls.DBPHASE_START:
            return "START"
        if phase == cls.DBPHASE_WORKING:
            return "WORKING"
        if phase == cls.DBPHASE_DONE:
            return "DONE"
        if phase == cls.DBPHASE_ERROR:
            return "ERROR"
        return "??#%s??" % (phase, )

    def __update_db(self, request_id, host, phase):
        if self.__sqlconn is None:
            self.__sqlconn = self.__open_database()
        with self.__sqlconn:
            cursor = self.__sqlconn.cursor()

            # add new request to DB
            cursor.execute("insert or replace into requests"
                           "(id, host, phase, update_time)"
                           " values (?, ?, ?, ?)",
                           (request_id, host, phase, datetime.datetime.now()))

    def __run(self):
        # load cached requests from database
        self.__requests = self.__load_state_db()

        # ready to start running!
        self.__running = True

        # loop until stopped
        while not self.__stopping:
            try:
                # if there's a queued request, start it now
                with self.__reqlock:
                    if self.__active is None:
                        self.__start_next_request()

                # check for messages
                with self.__msglock:
                    # if there are no messages, wait
                    if not self.__stopping and len(self.__msgqueue) == 0:
                        # wake up after a while
                        self.__msglock.wait(self.EXPIRE_SECONDS)

                    # get next queued message
                    if len(self.__msgqueue) == 0:
                        msg, force_spade = None, False
                    else:
                        msg, force_spade = self.__msgqueue.pop(0)

                if msg is not None:
                    self.__handle_msg(msg, force_spade)

                # expire overdue requests
                self.__expire_requests()
            except:
                logging.exception("RequestMonitor exception")

        self.__close_database()
        self.__running = False
        self.__stopping = False

    def __start_next_request(self):
        """
        Start next queued request (if any).
        NOTE: This should always be called from inside a self.__reqlock block:
        ```
        with self.__reqlock:
            self.__start_next_request()
        ```
        """
        if self.__active is not None:
            return

        next_id = None
        next_start = None
        next_details = None
        for req_id in self.__requests:
            # a queued request will only have a DETAIL entry
            if len(self.__requests[req_id]) != 1:
                continue

            # oops, skip broken requests without a DETAIL entry
            if self.DETAIL_KEY not in self.__requests[req_id]:
                logging.error("Missing details for Req#%s (found key %s)",
                              req_id, list(self.__requests[req_id].keys())[0])
                continue

            # verify that the detail data is as expected
            details = self.__requests[req_id][self.DETAIL_KEY]
            if details is None or len(details) != 8:
                logging.error("Expected 8 detail fields for Req#%s, found %s",
                              req_id, "None" if details is None
                              else len(details))
                continue

            # unpack the DETAIL fields
            (_, _, start_time, _, _, _, _, phase) = details

            # oops, entry has a confusing 'phase'
            if phase != self.DBPHASE_QUEUED:
                logging.error("Found Req#%s in phase %s, not %s",
                              req_id, self.__phase_name(phase),
                              self.__phase_name(self.DBPHASE_QUEUED))
                continue

            # save this request if it's the earliest found
            if next_id is None or next_start is None or \
               next_details is None or start_time < next_start:
                next_id = req_id
                next_start = start_time
                next_details = details

        if next_id is None:
            # No queued requests
            return

        # unpack details
        (username, prefix, start_ticks, stop_ticks, dest_dir, hubs, extract,
         phase) = next_details

        # send request to workers
        HsMessage.send(self.__sender.workers, HsMessage.INITIAL,
                       next_id, username, start_ticks, stop_ticks,
                       dest_dir, prefix=prefix, hubs=hubs, extract=extract,
                       host=self.__sender.shorthost)

        # this is now the active request
        self.__active = next_id

        # send Live alert JSON for email notification:
        alertjson = self.__build_json_email(start_ticks, stop_ticks, prefix,
                                            extract)
        self.__sender.i3socket.send_json(alertjson)

    def add_message(self, msg, force_spade):
        if not isinstance(msg, tuple) or not hasattr(msg, "_fields"):
            raise TypeError("Unexpected type <%s> for %s" %
                            (type(msg).__name__, msg))

        # validate message type
        if msg.msgtype not in (HsMessage.INITIAL, HsMessage.STARTED,
                               HsMessage.WORKING, HsMessage.IGNORED,
                               HsMessage.DONE, HsMessage.FAILED,
                               HsMessage.DELETE):
            raise ValueError("Unknown message type \"%s\" in \"%s\"" %
                             (msg.msgtype, msg))

        # validate list of hubs
        if msg.hubs is not None and len(msg.hubs) > 0:
            for host in msg.hubs.split(","):
                try:
                    HsUtil.hub_name_to_id(host)
                except ValueError:
                    raise ValueError("Bad hub \"%s\" in list \"%s\"" %
                                     (host, msg.hubs))

        with self.__msglock:
            self.__msgqueue.append((msg, force_spade))
            self.__msglock.notify()

    @classmethod
    def get_db_path(cls):
        "Return the path to the hitspool state database"
        if cls.STATE_DB_PATH is None:
            cls.STATE_DB_PATH = os.path.join(os.environ["HOME"],
                                             ".hitspool_state.db")
        return cls.STATE_DB_PATH

    @property
    def has_message(self):
        "Only used by unit tests"
        with self.__msglock:
            return self.__msgqueue is not None and len(self.__msgqueue) > 0

    @property
    def is_idle(self):
        "Only used by unit tests (via HsSender.wait_for_idle())"
        with self.__msglock:
            mlen = 0 if self.__msgqueue is None else len(self.__msgqueue)
            rlen = 0 if self.__requests is None else len(self.__requests)
            return mlen == 0 and rlen == 0

    @property
    def is_started(self):
        return self.__running

    def request_state(self, req_id):
        in_progress = []
        done = []
        error = []
        with self.__reqlock:
            if self.__requests is None or req_id not in self.__requests:
                return None

            for key, phase_time in list(self.__requests[req_id].items()):
                if key == self.DETAIL_KEY:
                    continue

                try:
                    host_id = HsUtil.hub_name_to_id(key)
                except ValueError:
                    logging.error("request_state() ignoring bad hub name %s",
                                  key)
                    continue

                phase, _ = phase_time
                if phase in (self.DBPHASE_START, self.DBPHASE_WORKING):
                    in_progress.append(host_id)
                elif phase == self.DBPHASE_DONE:
                    done.append(host_id)
                elif phase == self.DBPHASE_ERROR:
                    error.append(host_id)
                elif phase != self.DBPHASE_IGNORED:
                    logging.error("Not reporting %s phase %s", key, phase)
                    continue
        return (in_progress, error, done)

    def stop(self):
        with self.__msglock:
            self.__stopping = True
            self.__msglock.notify()
