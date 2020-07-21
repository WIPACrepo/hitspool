#!/usr/bin/env python

from __future__ import print_function

import argparse
import datetime
import logging
import numbers
import os
import sqlite3
import sys

import DAQTime
import HsConstants
import HsUtil


if sys.version_info.major > 2:
    # pylint: disable=invalid-name
    # unicode isn't present in Python3
    unicode = str


class ListQueue(object):
    # path to the SQLite database which tracks hitspool requests
    STATE_DB_PATH = None

    # directory containing hitspool data files
    HITSPOOL_DIR = "/mnt/data/pdaqlocal/hitspool"
    # path to the SQLite database which tracks hitspool file details
    HITSPOOL_DB_PATH = None

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
        self.__active = None

    def __close_database(self):
        pass

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
        elif isinstance(start_val, (str, unicode)) \
             and isinstance(stop_val, (str, unicode)):
            start_time = datetime.datetime.strptime(start_val,
                                                    DAQTime.TIME_FORMAT)
            stop_time = datetime.datetime.strptime(stop_val,
                                                   DAQTime.TIME_FORMAT)
            delta = stop_time - start_time
            days = delta.days
            seconds = delta.seconds
            microseconds = delta.microseconds
        else:
            print("Bad start/stop value (start %s<%s>, stop %s<%s>)" %
                  (start_val, type(start_val).__name__, stop_val,
                   type(stop_val).__name__))
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
                    return "%dh" % int(seconds / 3600)
                return "%.2fh" % (float(seconds) / 3600.0, )
            if seconds >= 60:
                # one or more minutes
                if seconds % 60 == 0:
                    return "%dm" % int(seconds / 60)
                return "%.2fm" % (float(seconds) / 60.0, )

        # one or more seconds
        if microseconds == 0:
            return "%ds" % (seconds, )

        total = float(seconds) + float(microseconds) / 1000000.
        return "%.2fs" % (total, )

    def __load_state_db(self, conn):
        cursor = conn.cursor()

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

    def __open_hitspool_database(self):
        dbpath = self.get_hitspool_db_path()
        if not os.path.exists(dbpath):
            logging.error("Cannot find DB file \"%s\"", dbpath)
            return None

        conn = sqlite3.connect(dbpath)
        return conn

    def __open_state_database(self):
        dbpath = self.get_state_db_path()
        if not os.path.exists(dbpath):
            logging.error("Cannot find DB file \"%s\"", dbpath)
            return None

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
    def get_hitspool_db_path(cls):
        "Return the path to the hitspool state database"
        if cls.HITSPOOL_DB_PATH is None:
            cls.HITSPOOL_DB_PATH = os.path.join(cls.HITSPOOL_DIR,
                                                "hitspool.db")
        return cls.HITSPOOL_DB_PATH

    @classmethod
    def get_state_db_path(cls):
        "Return the path to the hitspool state database"
        if cls.STATE_DB_PATH is None:
            cls.STATE_DB_PATH = os.path.join(os.environ["HOME"],
                                             ".hitspool_state.db")
        return cls.STATE_DB_PATH

    def list_hitspool(self):
        one_second = 10000000000

        listed = False

        conn = self.__open_hitspool_database()
        if conn is None:
            return

        try:
            cursor = conn.cursor()

            range_count = 0
            range_start = None

            prev_stop = None
            for row in cursor.execute("select start_tick, stop_tick"
                                      " from hitspool"
                                      " order by start_tick"):
                start_ticks = row[0]
                stop_ticks = row[1]

                if prev_stop is None:
                    range_start = start_ticks
                else:
                    gap = start_ticks - prev_stop
                    if gap > one_second:
                        self.__print_data_range(range_start, prev_stop,
                                                range_count,
                                                print_header=not listed)
                        listed = True
                        range_start = start_ticks
                        range_count = 0

                range_count += 1
                prev_stop = stop_ticks

            self.__print_data_range(range_start, prev_stop, range_count,
                                    print_header=not listed)
            listed = True
        finally:
            conn.commit()
            conn.close()

        if not listed:
            print("No hitspool files!")

    @classmethod
    def __print_data_range(cls, start_tick, stop_tick, file_count,
                           print_header=False):
        if print_header:
            print("Hitspool files\n==============")

        tstr = str(DAQTime.ticks_to_utc(start_tick))
        print("%d file%s :: [%d-%d] %s" %
              (file_count, "" if file_count == 1 else "s", start_tick,
               stop_tick, tstr))

    def list_requests(self):
        "List all requests"
        conn = self.__open_state_database()
        if conn is None:
            return

        try:
            requests = self.__load_state_db(conn)
        finally:
            conn.commit()
            conn.close()

        count = 0
        for rid, rdict in sorted(requests.items(), key=lambda x: x[0]):
            print("Req#%s" % (rid, ))
            count += 1

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

                print("\t%s: %s (user %s) %s @ %s" %
                      (self.__phase_name(phase), prefix, username, dstr, tstr))

            for phase, data in sorted(phase_hosts.items(),
                                      key=lambda x: x[0]):
                hosts = []
                for host, update_time in data:
                    hosts.append(host)

                print("\t\t%s: %s" % (self.__phase_name(phase),
                                      HsUtil.hubs_to_string(hosts), ))

        if count == 0:
            print("No active requests")


def add_arguments(parser):
    "Add all command line arguments to the argument parser"

    parser.add_argument("-D", "--state-db", dest="state_db",
                        help="Path to HitSpool state database"
                        " (used for testing)")
    parser.add_argument("-H", "--hitspool-db", dest="hitspool_db",
                        help="Path to HitSpool file database"
                        " (used for testing)")
    parser.add_argument("-l", "--list-spool", dest="list_spool",
                        action="store_true", default=False,
                        help="List the time ranges of files in the"
                        " hitspool cache")


def main():
    "Main program"

    parser = argparse.ArgumentParser()
    add_arguments(parser)
    args = parser.parse_args()

    if args.hitspool_db is not None:
        if ListQueue.HITSPOOL_DB_PATH is not None:
            raise SystemExit("HitSpool database path has already been set")
        ListQueue.HITSPOOL_DB_PATH = args.hitspool_db

    if args.state_db is not None:
        if ListQueue.STATE_DB_PATH is not None:
            raise SystemExit("HitSpool state database path has"
                             " already been set")
        ListQueue.STATE_DB_PATH = args.state_db

    lsq = ListQueue()
    if args.list_spool:
        lsq.list_hitspool()
        print("")
    lsq.list_requests()


if __name__ == "__main__":
    main()
