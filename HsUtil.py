#!/usr/bin/env python


import datetime
import numbers
import re

from datetime import datetime, timedelta

from HsException import HsException


# dictionary of year->datetime("Jan 1 %d" % year)
JAN1 = {}


def get_daq_ticks(start_time, end_time, is_sn_ns=False):
    # SN times are in nanoseconds, DAQ times are in 0.1ns
    # XXX this should use leapseconds
    if is_sn_ns:
        multiplier = 1E3
    else:
        multiplier = 1E4

    delta = end_time - start_time

    return int(((delta.days * 24 * 3600 + delta.seconds) * 1E6 +
                delta.microseconds) * multiplier)


def jan1_by_year(self, year_index=None):
    """
    Return the datetime value for January 1 of the specified year.
    If year is None, return January 1 for the current year.
    """
    if not year_index in JAN1:
        if year_index is not None:
            year = year_index
        else:
            year = datetime.utcnow().year

        JAN1[year_index] = datetime(year, 1, 1)

    return JAN1[year_index]


def fix_date_or_timestamp(ticks, time, is_sn_ns=False):
    """
    convert to UTC or DAQ whatever direction is needed
    """
    if time is not None:
        jan1 = jan1_by_year(time.year)
    else:
        jan1 = jan1_by_year(None)

    if (ticks is None or ticks == 0) and time is not None:
        ticks = get_daq_ticks(jan1, time, is_sn_ns=is_sn_ns)

    if is_sn_ns:
        multiplier = 1E9
    else:
        multiplier = 1E10

    if time is None and (ticks is not None and ticks > 0):
        time = jan1 + timedelta(seconds=ticks / multiplier)

    return (ticks, time)


def parse_sntime(arg):
    """
    Parse string as either a timestamp (in nanoseconds) or a date string.
    Return a tuple containing the nanosecond timestamp and a date string,
    deriving one value from the other.
    """
    if arg is None:
        raise HsException("No date/time specified")

    if isinstance(arg, numbers.Number):
        nsec = arg
        utc = None
    elif (isinstance(arg, str) or isinstance(arg, unicode)) and arg.isdigit():
        nsec = int(arg)
        utc = None
    else:
        nsec = 0
        dstr = str(arg)
        try:
            utc = datetime.strptime(dstr, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError, err:
            try:
                short = re.sub(r"\.[0-9]{9}", '', dstr)
                utc = datetime.strptime(short, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise HsException("Problem with the time-stamp format"
                                  " \"%s\": %s" % (arg, err))

    return fix_date_or_timestamp(nsec, utc, is_sn_ns=True)
