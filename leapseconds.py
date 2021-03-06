"""
A python set of leapsecond utilities
Note that the doctests assume that the nist ntp tai offset file available from:

ftp://tycho.usno.navy.mil/pub/ntp/leap-seconds.nnnnnnnn

is present and is named 'leap-seconds.latest'
"""

from __future__ import print_function

import calendar
import doctest
import os
import re
import time

from i3helper import Comparable
from locate_pdaq import find_pdaq_config


class LeapsecondException(Exception):
    "General LeapSecond exception"


class MJD(Comparable):
    "Modified Julain Date"

    def __init__(self, year=None, month=None, day=None, hour=None,
                 minute=None, second=None, rawvalue=None):
        """Convert the date to a modified julian date.

        >>> MJD(2004, 1, 1).value
        53005.0
        >>> MJD(2005, 1, 1).value
        53371.0
        >>> MJD(2005, 1, 30).value
        53400.0
        >>> MJD(1985, 2, 17.25).value
        46113.25
        >>> MJD(rawvalue=56109.0).value
        56109.0
        """

        if rawvalue is not None:
            is_bad = year is not None or month is not None or day is not None
            is_bad = is_bad or hour is not None or minute is not None or \
              second is not None
            if is_bad:
                raise LeapsecondException("Cannot specify 'rawvalue' with"
                                          " any time-based parameters")
            value = rawvalue
        else:
            if month in (1, 2):
                year = year - 1
                month = month + 12

            if hour is None or minute is None or second is None:
                frac = 0.0
            else:
                frac = ((((second / 60.0) + minute) / 60.0) + hour) / 24.0

            # assume that we will never be calculating
            # mjd's before oct 15 1582

            aval = int(year / 100)
            bval = 2 - aval + int(aval / 4)
            cval = int(365.25 * year)
            dval = int(30.600 * (month + 1.0))

            value = bval + cval + dval + day + frac - 679006.0

        self.__value = value

    @property
    def compare_key(self):
        "Return the keys to be used by the Comparable methods"
        return self.__value

    def __sub__(self, other):
        return self.__value - other.value

    def __str__(self):
        return str(self.__value)

    def add(self, days):
        self.__value += days

    @classmethod
    def current_year(cls):
        return time.gmtime().tm_year

    @staticmethod
    def from_ntp(ntp_timestamp):
        """convert an ntp timestamp to a modified julian date
        Note that this equation comes directly from the documentation
        of the leapsecond file"""
        return MJD(rawvalue=(ntp_timestamp / 86400. + 15020))

    @staticmethod
    def from_timestruct(tstruct):
        """Calculate the modified julian date for a time"""
        return MJD(tstruct.tm_year, tstruct.tm_mon,
                   tstruct.tm_mday, tstruct.tm_hour,
                   tstruct.tm_min, tstruct.tm_sec)

    @staticmethod
    def now():
        """Calculate the modified julian date for the current system time"""
        return MJD.from_timestruct(time.gmtime())

    @property
    def ntp(self):
        """convert an ntp timestamp to a modified julian date
        Note that this equation comes directly from the documentation
        of the leapsecond file"""
        return (self.__value - 15020) * 86400.

    @property
    def timestruct(self):
        """Convert a modified julian date to a python time tuple.

        >>> jul1_2012 = MJD(2012, 7, 1)
        >>> jul1_2012.timestruct
        time.struct_time(tm_year=2012, tm_mon=7, tm_mday=1, tm_hour=0, tm_min=0, tm_sec=0, tm_wday=6, tm_yday=183, tm_isdst=0)
        """

        jdate = self.__value + 2400000.5

        jdate = jdate + 0.5
        jint = int(jdate)
        odd_val = jdate % 1

        if jint > 2299160:
            aval = int((jint - 1867216.25) / 36524.25)
            bval = jint + 1 + aval - int(aval / 4)
        else:
            bval = jint

        cval = bval + 1524.
        dval = int((cval - 122.1) / 365.25)
        fval = int(365.25 * dval)
        gval = int((cval - fval) / 30.6001)

        day = cval - fval + odd_val - int(30.6001 * gval)
        if gval < 13.5:
            month = gval - 1
        else:
            month = gval - 13

        if month > 2.5:
            year = dval - 4716
        else:
            year = dval - 4715

        # note that day will be a fractional day
        # and python handles that
        time_str = (year, month, day, 0, 0, 0, 0, 0, -1)
        # looks silly, but have to deal with fractional day
        gm_epoch = calendar.timegm(time_str)
        time_str = time.gmtime(gm_epoch)

        return time_str

    @property
    def value(self):
        return self.__value


class LeapSeconds(object):
    "Leapsecond-related methods"

    # default file name
    DEFAULT_FILENAME = "leapseconds-latest"
    # First year covered by NIST file
    NIST_EPOCH_YEAR = 1972

    # private copy of the configuration directory path
    __CONFIG_DIR = None
    # cached singleton instance
    __INSTANCE = None

    def __init__(self, filename, year=None):
        if year is None:
            year = MJD.current_year()
        elif year < self.NIST_EPOCH_YEAR:
            raise ValueError("NIST does not provide leap second info"
                             " prior to %d" % self.NIST_EPOCH_YEAR)

        # remember file name for reload check
        self.__filename = filename

        # set the default year for lookups
        self.__default_year = year

        # declare remaining attributes
        self.__expiry = None
        self.__base_offset_year = None
        self.__leap_offsets = None

        NISTParser(self).parse(filename, self.__default_year)

        self.__compute_and_set_seconds_in_year()

        # remember the modification time for reload_check()
        self.__mtime = os.stat(self.__filename).st_mtime

    def __compute_and_set_seconds_in_year(self):
        "Precompute the total seconds for every year"
        mjd1 = MJD(self.__base_offset_year, 1, 1)
        for idx in range(0, len(self.__leap_offsets)):
            mjd2 = MJD(self.__base_offset_year + 1, 1, 1)

            total_seconds = int(mjd2.value - mjd1.value) * 3600 * 24
            self.__leap_offsets[idx].total_seconds = total_seconds
            mjd1 = mjd2

    def dump_offsets(self):
        for yidx in range(len(self.__leap_offsets)):
            self.__leap_offsets[yidx].dump(self.__base_offset_year + yidx)

    @property
    def expiry(self):
        "Return the Modified Julian Date when the current NIST file expires"
        return self.__expiry

    @property
    def filename(self):
        "Return the path for the leapseconds file"
        return self.__filename

    @classmethod
    def get_latest_path(cls):
        "Return the absolute path to the default file"
        return os.path.realpath(cls.instance().filename)

    def get_leap_offset(self, day_of_year, year=None):
        if year is None:
            year = self.__default_year

        yr_idx = year - self.__base_offset_year
        if yr_idx >= len(self.__leap_offsets):
            yr_idx = len(self.__leap_offsets) - 1
            day_of_year = 366

        return self.__leap_offsets[yr_idx].get_leap_seconds(day_of_year)

    @classmethod
    def instance(cls, config_dir=None):
        if config_dir is None:
            if cls.__CONFIG_DIR is None or \
               not os.path.isdir(cls.__CONFIG_DIR):
                cls.__CONFIG_DIR = find_pdaq_config()
            config_dir = cls.__CONFIG_DIR

        if cls.__INSTANCE is None or \
           not cls.__INSTANCE.is_config_dir(config_dir):
            cls.__INSTANCE = LeapSeconds(os.path.join(config_dir, "nist",
                                                      cls.DEFAULT_FILENAME))

        return cls.__INSTANCE

    def is_config_dir(self, config_dir):
        return self.__filename is not None and config_dir is not None and \
            self.__filename.startswith(config_dir)

    def reload_check(self):
        if not os.path.exists(self.__filename):
            return self.__mtime is not None

        new_mtime = os.stat(self.__filename).st_mtime
        if new_mtime == self.__mtime:
            return False

        try:
            NISTParser(self).parse(self.__filename, self.__default_year)
        except Exception as ex:
            raise LeapsecondException("Cannot reload leapsecond file %s: %s" %
                                      (self.__filename, ex))

        self.__mtime = new_mtime
        return True

    def set_data(self, expiry, base_offset_year, leap_offsets):
        self.__expiry = expiry
        self.__base_offset_year = base_offset_year
        self.__leap_offsets = leap_offsets


class LeapOffsets(object):
    "Leapsecond offset data extracted froma NIST file"
    def __init__(self, initial_offset, days):
        self.__initial_offset = initial_offset
        self.__days = days
        self.__total_seconds = None

    def dump(self, year):
        print("[%d]:" % year, end=' ')
        for didx in range(len(self.__days)):
            print(str(self.__days[didx]), end=' ')
        print()

    def get_leap_seconds(self, day_of_year):
        num_leap_secs = 0

        for idx in range(0, len(self.__days)):
            if day_of_year <= self.__days[idx]:
                break

            num_leap_secs += 1

        return num_leap_secs

    @property
    def initial_offset(self):
        return self.__initial_offset

    @property
    def total_seconds(self):
        return self.__total_seconds

    @total_seconds.setter
    def total_seconds(self, value):
        self.__total_seconds = value


class NISTParser(object):
    "NIST file parser"

    MAX_PRECALCULATE_SPAN = 100

    NIST_DATA_PAT = re.compile(r"^(\d+)\s+(\d+)")

    def __init__(self, ls_object):
        self.__ls_object = ls_object

    def __init_object(self, default_year, expiry, tai_map):
        expire_year = expiry.timestruct.tm_year

        if default_year > expire_year:
            final_year = default_year
        else:
            final_year = expire_year

        if final_year - LeapSeconds.NIST_EPOCH_YEAR < \
           self.MAX_PRECALCULATE_SPAN:
            first_year = LeapSeconds.NIST_EPOCH_YEAR
        else:
            first_year = final_year - self.MAX_PRECALCULATE_SPAN

        base_offset_year = first_year

        leap_offsets = []

        leap_seconds = sorted(tai_map.keys())

        jan1 = MJD(first_year, 1, 1).value

        index = 0
        for year in range(first_year, final_year + 1):
            next_jan1 = MJD(year + 1, 1, 1).value

            # find current offset
            while index < len(leap_seconds) - 2 and \
              jan1 > leap_seconds[index].value:
                index += 1
            if index >= len(leap_seconds):
                index = len(leap_seconds) - 1

            first_mjd = leap_seconds[index]

            # if the first leap second is on Jan 1, skip it
            first_leap_day = int(first_mjd.value - jan1)
            if first_leap_day == 0:
                index += 1

            year_offsets = []

            while index < len(leap_seconds) and \
              next_jan1 >= leap_seconds[index].value:
                day = int(leap_seconds[index].value - jan1)
                if day > 0:
                    year_offsets.append(day)
                index += 1

            leap_offsets.append(LeapOffsets(tai_map.get(first_mjd),
                                            year_offsets))

            jan1 = next_jan1
            if index > 0 and \
               leap_seconds[index - 1].value <= next_jan1:
                index -= 1

        self.__ls_object.set_data(expiry, base_offset_year, leap_offsets)

    def __parse_lines(self, rdr, tai_map):
        expiry = None

        for line in rdr:
            line = line.rstrip()

            if line == "":
                continue

            if line[0] == "#":
                # found a comment line
                if len(line) > 4 and line[1] == "@":
                    # but it's really the expiration date
                    val = int(line[3:].strip())
                    expiry = MJD.from_ntp(val)

                continue

            match = self.NIST_DATA_PAT.match(line)
            if match is not None:
                tval = MJD.from_ntp(int(match.group(1)))
                offset = int(match.group(2))

                tai_map[tval] = offset
                continue
        if expiry is None:
            raise LeapsecondException("No expiration line found")
        if len(tai_map) == 0:  # pylint: disable=len-as-condition
            raise LeapsecondException("No leapsecond data found")

        return expiry

    def parse(self, filename, default_year):
        # mapping from MJD to TAI offset
        tai_map = {}

        with open(filename, "r") as rdr:
            expiry = self.__parse_lines(rdr, tai_map)

        self.__init_object(default_year, expiry, tai_map)


def main():
    "Main program"

    ls_inst = LeapSeconds.instance()

    ls_inst.dump_offsets()


def test():
    doctest.testmod()


if __name__ == "__main__":
    # main()
    test()
