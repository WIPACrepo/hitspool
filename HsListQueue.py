#!/usr/bin/env python


import datetime
import logging
import numbers
import os
import sqlite3

import DAQTime
import HsConstants
import HsUtil


class ListQueue(object):
    # path to the SQLite database which acts as a disk cache
    STATE_DB_PATH = None

    # key used to indicate an entry contains the request details
    DETAIL_KEY = "@detail@"

    # database request phases
    DBPHASE_INITIAL = 0
    DBPHASE_QUEUED = 20
    DBPHASE_START = 40
    DBPHASE_WORKING = 60
    DBPHASE_DONE = 80
    DBPHASE_IGNORED = 90
    DBPHASE_ERROR = 99

    def __init__(self):
        self.__sqlconn = None
        self.__active = None

    def __close_database(self):
        if self.__sqlconn is not None:
            try:
                self.__sqlconn.close()
            except:
                logging.exception("Problem closing database")
            self.__sqlconn = None

    @classmethod
    def __duration_string(cls, start_val, stop_val):
        ticks_per_sec = HsConstants.TICKS_PER_SECOND
        secs_per_day = 60 * 60 * 24

        if isinstance(start_val, numbers.Number) and \
           isinstance(stop_val, numbers.Number):
            duration = stop_val - start_val
            days = int(duration / (secs_per_day * ticks_per_sec))
            seconds = int(duration / ticks_per_sec) - (days * secs_per_day)
            microseconds = float(duration % ticks_per_sec) / 10000.0
        elif isinstance(start_val, datetime.datetime) and \
             isinstance(stop_val, datetime.datetime):
            delta = stop_val - start_val
            days = delta.days
            seconds = delta.seconds
            microseconds = delta.microseconds
        elif (isinstance(start_val, str) or isinstance(start_val, unicode)) \
             and (isinstance(stop_val, str) or isinstance(stop_val, unicode)):
            start_time = datetime.datetime.strptime(start_val,
                                                    DAQTime.TIME_FORMAT)
            stop_time = datetime.datetime.strptime(stop_val,
                                                   DAQTime.TIME_FORMAT)
            delta = stop_time - start_time
            days = delta.days
            seconds = delta.seconds
            microseconds = delta.microseconds
        else:
            print "Bad start/stop value (start %s<%s>, stop %s<%s>)" % \
                (start_val, type(start_val).__name__, stop_val,
                 type(stop_val).__name__)
            return "???"

        if days > 0:
            if seconds == 0:
                # no fractional part
                return "%dd" % (days, )

            total = float(days) + float(seconds) / 86400.0
            return "%.2fd" % (total, )

        if seconds > 0:
            if seconds >= 3600:
                # one or more hours
                if seconds % 3600 == 0:
                    return "%dh" % (seconds / 3600, )
                return "%.2fh" % (float(seconds) / 3600.0, )
            if seconds >= 60:
                # one or more minutes
                if seconds % 60 == 0:
                    return "%dm" % (seconds / 60, )
                return "%.2fm" % (float(seconds) / 60.0, )

        # one or more seconds
        if microseconds == 0:
            return "%ds" % (seconds, )

        total = float(seconds) + float(microseconds) / 1000000.
        return "%.2fs" % (total, )

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

            if phase == self.DBPHASE_INITIAL or phase == self.DBPHASE_QUEUED:
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

    def __open_database(self):
        dbpath = self.get_db_path()
        conn = sqlite3.connect(dbpath)
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

    @classmethod
    def get_db_path(cls):
        "Return the path to the hitspool state database"
        if cls.STATE_DB_PATH is None:
            cls.STATE_DB_PATH = os.path.join(os.environ["HOME"],
                                             ".hitspool_state.db")
        return cls.STATE_DB_PATH

    def list_requests(self):
        "List all requests"
        requests = self.__load_state_db()

        for rid, rdict in sorted(requests.items(), key=lambda x: x[0]):
            print "Req#%s" % (rid, )

            phase_hosts = {}
            for host, values in sorted(rdict.items(), key=lambda x: x[0]):
                if host != self.DETAIL_KEY:
                    (phase, update_time) = values
                    if phase not in phase_hosts:
                        phase_hosts[phase] = []
                    phase_hosts[phase].append((host, update_time))
                    continue

                (username, prefix, start_ticks, stop_ticks, _, _, _,
                 phase) = values
                dstr = self.__duration_string(start_ticks, stop_ticks)
                if isinstance(start_ticks, datetime.datetime):
                    tstr = str(start_ticks)
                else:
                    ticks = DAQTime.string_to_ticks(start_ticks)
                    tstr = str(DAQTime.ticks_to_utc(ticks))

                print "\t%s: %s (user %s) %s @ %s" % \
                    (self.__phase_name(phase), prefix, username, dstr, tstr)

            for phase, data in sorted(phase_hosts.items(), key=lambda x: x[0]):
                hosts = []
                for host, update_time in data:
                    hosts.append(host)

                print "\t\t%s: %s" % (self.__phase_name(phase),
                                      HsUtil.hubs_to_string(hosts), )


if __name__ == "__main__":
    ListQueue().list_requests()
