#!/usr/bin/env python


import os
import sqlite3

from icecube.daq.payload import read_payloads

def build_hs(hubdir):
    hsdir = os.path.join(hubdir, "hitspool")
    if not os.path.exists(hsdir):
        os.mkdir(hsdir)

    conn = sqlite3.connect(os.path.join(hsdir, "hitspool.db"))
    cursor = conn.cursor()

    # create table
    cursor.execute("create table if not exists hitspool("
              "filename text primary key not null," +
              "timestamp integer)")
    conn.commit()

    # link all files to hitspool dir and add DB entries
    nextnum = 1
    try:
        for dnm in ('lastRun', 'currentRun'):
            nextnum = link_dir(cursor, os.path.join(hubdir, dnm), hsdir,
                               nextnum)
    finally:
        conn.commit()


def link_dir(cursor, rundir, hsdir, nextnum):
    for entry in os.listdir(rundir):
        hitfile = os.path.join(rundir, entry)
        if not entry.endswith(".dat"):
            if entry != "info.txt":
                print "Ignoring %s" % hitfile
            continue

        # get first hit time
        timestamp = get_first_time(hitfile)

        # create file name inside unified directory
        newname = "HitSpool-%d.dat" % nextnum
        nextnum += 1

        # link old file to unified directory
        newfile = os.path.join(hsdir, newname)
        os.link(hitfile, newfile)

        # add DB entry for new file
        sql = "insert or replace into hitspool(timestamp, filename)" \
              " values (?, ?)"
        cursor.execute(sql, (timestamp, newname))

    return nextnum


def get_first_time(hitfile):
    with open(hitfile) as fin:
        for payload in read_payloads(fin):
            return payload.utime


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("Usage: %s directory")

    topdir = sys.argv[1]
    if not os.path.exists(topdir):
        raise SystemExit("Directory %d does not exist" % topdir)

    build_hs(topdir)
