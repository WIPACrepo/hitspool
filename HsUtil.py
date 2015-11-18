#!/usr/bin/env python


import numbers
import re

from datetime import datetime, timedelta

from HsException import HsException


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


def fix_dates_or_timestamps(start_tick, stop_tick, start_time, stop_time,
                            is_sn_ns=False):
    """
    convert to UTC or DAQ whatever direction is needed
    """
    if start_time is not None:
        daqyear = start_time.year
    elif stop_time is not None:
        daqyear = stop_time.year
    else:
        daqyear = datetime.utcnow().year

    jan1 = datetime(daqyear, 1, 1)

    if (start_tick is None or start_tick == 0) and start_time is not None:
        start_tick = get_daq_ticks(jan1, start_time, is_sn_ns=is_sn_ns)
    if (stop_tick is None or stop_tick == 0) and stop_time is not None:
        stop_tick = get_daq_ticks(jan1, stop_time, is_sn_ns=is_sn_ns)

    if is_sn_ns:
        multiplier = 1E9
    else:
        multiplier = 1E10

    if start_time is None and (start_tick is not None and start_tick > 0):
        start_time = jan1 + timedelta(seconds=start_tick / multiplier)
    if stop_time is None and (stop_tick is not None and stop_tick > 0):
        stop_time = jan1 + timedelta(seconds=stop_tick / multiplier)

    return (start_tick, stop_tick, start_time, stop_time)


def parse_date(arg):
    """
    Parse string as either a timestamp or a date string
    """
    if arg is None:
        raise HsException("Cannot parse None")

    if isinstance(arg, numbers.Number):
        timestamp = int(arg)
        utc = None
    elif (isinstance(arg, str) or isinstance(arg, unicode)) and arg.isdigit():
        timestamp = int(arg)
        utc = None
    else:
        timestamp = 0
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

    return timestamp, utc
