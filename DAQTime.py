#!/usr/bin/env python

from __future__ import print_function

import datetime
import numbers
import re
import sys

from HsException import HsException
from leapseconds import LeapSeconds


# dictionary which maps year to the datetime object for January 1 of that year
JAN1 = {}

# format string used to parse dates
TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def jan1_by_year(daq_time=None):
    """
    Return the datetime value for January 1 of the specified year.
    If year is None, return January 1 for the current year.
    """
    if daq_time is not None:
        year = daq_time.year
    else:
        year = datetime.datetime.utcnow().year

    if year not in JAN1:
        JAN1[year] = datetime.datetime(year, 1, 1)

    return JAN1[year]


def ticks_to_utc(ticks):
    "Convert an integral DAQ tick value to a time object"
    if ticks is None:
        raise HsException("No tick value specified")
    if not isinstance(ticks, numbers.Number):
        raise HsException("Tick value %s should be number, not %s" %
                          (ticks, type(ticks).__name__))
    return jan1_by_year() + datetime.timedelta(seconds=ticks / 1E10)


def string_to_ticks(timestr, is_ns=False):
    "Convert a time string to an integral DAQ tick value"
    if timestr is None:
        raise HsException("Found null value for start/stop time in %s" %
                          (timestr, ))

    multiplier = 10 if is_ns else 1
    if isinstance(timestr, numbers.Number):
        return int(timestr * multiplier)

    if isinstance(timestr, str):
        if timestr.isdigit():
            try:
                return int(timestr) * multiplier
            except:
                raise HsException("Cannot convert \"%s\" to ticks" %
                                  (timestr, ))

        try:
            utc = datetime.datetime.strptime(timestr, TIME_FORMAT)
        except ValueError:
            # Python date parser can only handle milliseconds
            if timestr.find(".") > 0:
                short = re.sub(r"(\.\d{6})\d+", r"\1", timestr)
                try:
                    utc = datetime.datetime.strptime(short, TIME_FORMAT)
                except:
                    raise HsException("Cannot convert \"%s\" to datetime" %
                                      (timestr, ))
            elif TIME_FORMAT.endswith(".%f"):
                shortfmt = TIME_FORMAT[:-3]
                try:
                    utc = datetime.datetime.strptime(timestr, shortfmt)
                except:
                    raise HsException("Cannot convert \"%s\" to datetime" %
                                      (timestr, ))

        return utc_to_ticks(utc)

    raise HsException("Cannot convert %s(%s) to ticks" %
                      (type(timestr).__name__, timestr))


def utc_to_ticks(utc):
    """
    Get the number of 0.1ns ticks (since Jan 1) for the 'utc' datetime
    """
    delta = utc - jan1_by_year(utc)

    # get the number of leap seconds (0 or 1) since the start of the year
    leap = LeapSeconds.instance()
    jan1_leapsecs = leap.get_leap_offset(0, year=utc.year)
    utc_leapsecs = leap.get_leap_offset(delta.days, year=utc.year)
    extrasecs = utc_leapsecs - jan1_leapsecs

    return int(((delta.days * 24 * 3600 + delta.seconds + extrasecs) *
                1000000 + delta.microseconds) * 10000)


def main():
    "Main program"

    is_ns = False
    for arg in sys.argv[1:]:
        if arg == "-n":
            is_ns = True
            continue

        ticks = string_to_ticks(arg, is_ns=is_ns)
        utc = ticks_to_utc(ticks)
        tick2 = utc_to_ticks(utc)

        print("Arg \"%s\"\n\t-> ticks %s\n\t->utc \"%s\"\n\t-> ticks %s" %
              (arg, ticks, utc, tick2))


if __name__ == "__main__":
    main()
