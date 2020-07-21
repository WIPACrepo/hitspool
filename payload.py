#!/usr/bin/env python
"""
IceCube payloads and associated classes

If you find/fix any bugs or add improvements, please also broadcast them
on daq-dev@icecube.wisc.edu
"""

from __future__ import print_function

import argparse
import bz2
import gzip
import numbers
import os
import struct

try:
    from cStringIO import StringIO
except:  # ModuleNotFoundError only works under 2.7/3.0
    from io import StringIO

from i3helper import Comparable


class PayloadException(Exception):
    "Payload exception"


# pylint: disable=too-few-public-methods
class StopMessage(object):
    "Payload used to indicate that an input stream has stopped sending data"
    @property
    def bytes(self):
        "Return the binary representation of this payload"
        return struct.pack(">I", 4)


class Payload(Comparable):
    "Base payload class"
    TYPE_ID = None
    ENVELOPE_LENGTH = 16

    def __init__(self, utime, data, keep_data=True):
        "Payload time and non-envelope data bytes"
        self.__utime = utime
        if keep_data and data is not None:
            self.__data = data
            self.__valid_data = True
        else:
            self.__data = None
            self.__valid_data = False

    @property
    def bytes(self):
        "Return the binary representation of this payload"
        if not self.__valid_data:
            raise PayloadException("Data was discarded; cannot return bytes")
        return self.envelope + self.__data

    @property
    def compare_key(self):
        "Return the keys to be used by the Comparable methods"
        return self.__utime

    @property
    def data_bytes(self):
        "Return the data bytes (should not include the 16 byte envelope)"
        if not self.__valid_data:
            raise PayloadException("Data was discarded; cannot return bytes")
        return self.__data

    @property
    def data_length(self):
        "Return the number of data bytes in this payload"
        if not self.__valid_data:
            raise PayloadException("Data was discarded; cannot return length")
        return len(self.__data)

    @property
    def envelope(self):
        "Return the envelope bytes"
        return struct.pack(">2IQ", self.data_length + self.ENVELOPE_LENGTH,
                           self.payload_type_id(), self.__utime)

    @classmethod
    def extract_clock_bytes(cls, rawval):
        "Extract the DOM clock bytes from a number or list"
        if isinstance(rawval, numbers.Number):
            tmpbytes = []
            for _ in range(6):
                tmpbytes.insert(0, rawval & 0xff)
                rawval >>= 8
            return tmpbytes

        if isinstance(rawval, (list, tuple)):
            if len(rawval) != 6:
                raise PayloadException("Expected 6 clock bytes, not " +
                                       len(rawval))
            return rawval

        raise PayloadException("Cannot convert %s to clock bytes" %
                               (type(rawval).__name__, ))

    @property
    def has_data(self):
        "Did this payload retain the original data bytes?"
        return self.__valid_data

    @classmethod
    def payload_type_id(cls):
        "Integer value representing this payload's type"
        if cls.TYPE_ID is None:
            return NotImplementedError()
        return cls.TYPE_ID

    @staticmethod
    def source_name(src_id):
        "Translate the source ID into a human-readable string"
        comp_type = int(src_id / 1000)
        comp_num = src_id % 1000

        if comp_type == 3:
            comp_name = "icetopHandler-%d" % comp_num
        elif comp_type == 12:
            comp_name = "stringHub-%d" % comp_num
        elif comp_type == 13:
            comp_name = "simHub-%d" % comp_num
        else:
            if comp_num != 0:
                raise PayloadException("Unexpected component#%d for"
                                       " source#%d" %
                                       (comp_num, comp_type * 1000))

            if comp_type == 4:
                comp_name = "inIceTrigger"
            elif comp_type == 5:
                comp_name = "iceTopTrigger"
            elif comp_type == 6:
                comp_name = "globalTrigger"
            elif comp_type == 7:
                comp_name = "eventBuilder"
            elif comp_type == 8:
                comp_name = "tcalBuilder"
            elif comp_type == 9:
                comp_name = "moniBuilder"
            elif comp_type == 10:
                comp_name = "amandaTrigger"
            elif comp_type == 11:
                comp_name = "snBuilder"
            elif comp_type == 14:
                comp_name = "secondaryBuilders"
            elif comp_type == 15:
                comp_name = "trackEngine"
            else:
                comp_name = "??component#%d??" % (comp_type, )

        return comp_name

    @property
    def utime(self):
        "UTC time from payload header"
        return self.__utime


class UnknownPayload(Payload):
    "A payload which has not been implemented in this library"

    def __init__(self, type_id, utime, data, keep_data=True):
        "Create an unknown payload"
        self.__type_id = type_id

        super(UnknownPayload, self).__init__(utime, data, keep_data=keep_data)

    def payload_type_id(self):
        "Integer value representing this payload's type"
        return self.__type_id

    def __str__(self):
        "Payload description"
        if self.has_data:
            lenstr = ", %d bytes" % (len(self.data_bytes) +
                                     self.ENVELOPE_LENGTH)
        else:
            lenstr = ""
        return "UnknownPayload#%d[@%d%s]" % \
            (self.__type_id, self.utime, lenstr)


class SimpleHit(Payload):
    "The simplified hit format sent to the local trigger handlers"

    TYPE_ID = 1
    MIN_LENGTH = 38

    # pylint: disable=too-many-arguments
    def __init__(self, utime, data_or_trig_type, cfg_id=None, src_id=None,
                 mbid=None, keep_data=True):
        """Create a simple hit"""

        if data_or_trig_type is not None and \
           (cfg_id is None or src_id is None or mbid is None):
            # assume 'trig_type' is actually binary data
            flds = struct.unpack(">3iqh", data_or_trig_type)
            self.__trig_type = flds[0]
            self.__cfg_id = flds[1]
            self.__src_id = flds[2]
            self.__mbid = flds[3]
            tmp_type = flds[4]
        else:
            self.__trig_type = data_or_trig_type
            self.__cfg_id = cfg_id
            self.__src_id = src_id
            self.__mbid = mbid
            tmp_type = data_or_trig_type
            keep_data = False

        if tmp_type != self.__trig_type:
            raise PayloadException("SimpleHit@%d: type %0x != mode %0x" %
                                   (utime, self.__trig_type, tmp_type))

        super(SimpleHit, self).__init__(utime, data_or_trig_type,
                                        keep_data=keep_data)

    def __str__(self):
        "Payload description"
        return "SimpleHit@%d[%s %s cfg %d type %0x]" % \
            (self.utime, self.source_name(self.__src_id), self.mbid_str,
             self.__cfg_id, self.__trig_type)

    @property
    def bytes(self):
        if self.has_data:
            return super(SimpleHit, self).bytes

        return struct.pack(">IIQIIIQH", self.MIN_LENGTH, self.TYPE_ID,
                           self.utime, self.__trig_type, self.__cfg_id,
                           self.__src_id, self.__mbid, self.__trig_type)

    @property
    def config_id(self):
        "Trigger configuration ID"
        return self.__cfg_id

    @property
    def mbid(self):
        "Mainboard ID as an integer"
        return self.__mbid

    @property
    def mbid_str(self):
        "Mainboard ID as a string"
        return "%012x" % self.__mbid

    @property
    def trigger_type(self):
        "Trigger flags"
        return self.__trig_type


# pylint: disable=invalid-name
# This is an internal class
class delta_codec(object):
    """
    Delta compression decoder (stolen from icecube.daq.slchit in pDAQ's PyDOM)
    """
    def __init__(self, buf):
        "Load the buffer and prepare to decode"
        self.tape = StringIO(buf)
        self.valid_bits = 0
        self.register = 0
        self.bpw = None
        self.bth = None

    def decode(self, length):
        "Decode the specified number of bytes"
        self.bpw = 3
        self.bth = 2
        last = 0
        out = []
        for _ in range(length):
            while True:
                wrd = self.get_bits()
                # print("%d: Got %d" % (i, wrd))
                if wrd != (1 << (self.bpw - 1)):
                    break
                self.shift_up()
            if abs(wrd) < self.bth:
                self.shift_down()
            last += wrd
            # print("out %s" % last)
            out.append(last)
        return out

    def get_bits(self):
        "Decode the next byte"
        while self.valid_bits < self.bpw:
            next_byte, = struct.unpack('B', self.tape.read(1))
            # print("Read %s" % (next_byte, ))
            self.register |= (next_byte << self.valid_bits)
            self.valid_bits += 8
        # print("Bit register: %s" % bitstring(self.register, self.valid_bits))
        val = self.register & ((1 << self.bpw) - 1)
        if val > (1 << (self.bpw - 1)):
            val -= (1 << self.bpw)
        self.register >>= self.bpw
        self.valid_bits -= self.bpw
        return val

    def shift_up(self):
        "Shift up"
        if self.bpw == 1:
            self.bpw = 2
            self.bth = 1
        elif self.bpw == 2:
            self.bpw = 3
            self.bth = 2
        elif self.bpw == 3:
            self.bpw = 6
            self.bth = 4
        elif self.bpw == 6:
            self.bpw = 11
            self.bth = 32
        else:
            raise ValueError("Bad BPW value %d" % self.bpw)
        # print("Shifted up to %s %s" % (self.bpw, self.bth)

    def shift_down(self):
        "Shift down"
        if self.bpw == 2:
            self.bpw = 1
            self.bth = 0
        elif self.bpw == 3:
            self.bpw = 2
            self.bth = 1
        elif self.bpw == 6:
            self.bpw = 3
            self.bth = 2
        elif self.bpw == 11:
            self.bpw = 6
            self.bth = 4
        else:
            raise ValueError("Bad BPW value %d" % self.bpw)
        # print("Shifted down to %s %s" % (self.bpw, self.bth))


class HitPayload(Payload):
    "Superclass for all hit payloads"

    def __init__(self, utime, data, keep_data=True):
        super(HitPayload, self).__init__(utime, data, keep_data=keep_data)

    @property
    def simple_hit(self):
        "Return the simplified version of this hit"
        return struct.pack(">2iq3iqh", 38, self.payload_type_id(), self.utime,
                           self.trigger_type, self.config_id, self.source_id,
                           self.mbid, self.trigger_mode)

    @property
    def config_id(self):
        "Return the configuration ID"
        raise NotImplementedError()

    @property
    def mbid(self):
        "Return the mainboard ID"
        raise NotImplementedError()

    @property
    def source_id(self):
        "Return the source ID"
        raise NotImplementedError()

    @property
    def trigger_mode(self):
        "Return the trigger mode"
        raise NotImplementedError()

    @property
    def trigger_type(self):
        "Return the trigger type"
        raise NotImplementedError()


# pylint: disable=too-many-instance-attributes,too-many-public-methods
# hits hold a lot of information
class DeltaCompressedHit(HitPayload):
    "Delta-compressed (omicron) hits"

    TYPE_ID = 3
    MIN_LENGTH = 54 - Payload.ENVELOPE_LENGTH

    def __init__(self, mbid, data, keep_data=True, little_endian=False):
        """
        Extract delta-compressed hit data from the buffer
        """
        if len(data) < self.MIN_LENGTH:
            raise PayloadException("Expected at least %d data bytes, got %d" %
                                   (self.MIN_LENGTH, len(data)))

        if little_endian:
            order = "<"
        else:
            order = ">"

        hdr = struct.unpack("%s8xQ3HQ" % order, data[:30])

        if hdr[1] != 1:
            raise PayloadException(("Bad order-check %d for DeltaSenderHit "
                                    "(should be 1)") % hdr[1])

        super(DeltaCompressedHit, self).__init__(hdr[0], data,
                                                 keep_data=keep_data)

        self.__mbid = mbid
        self.__version = hdr[2]
        self.__pedestal = hdr[3]
        self.__domclk = hdr[4]
        self.__word0, self.__word2 = struct.unpack("%s2I" % order, data[30:38])

        self.__decoded = False
        self.__fadc = None
        self.__atwd = None

    def __str__(self):
        "Payload description"
        return "DeltaCompressedHit@%d[v%d %s]" % \
            (self.utime, self.version, self.mbid_str)

    def __decode_waveforms(self):
        "Uncompress waveforms"
        if self.__decoded:
            return

        codec = delta_codec(self.data_bytes[38:])

        if self.has_fadc:
            self.__fadc = codec.decode(256)

        if self.has_atwd:
            self.__atwd = []
            for _ in range(self.atwd_channels + 1):
                self.__atwd.append(codec.decode(128))

        self.__decoded = True

    @property
    def a_or_b(self):
        "ATWD A or B?"
        return "A" if self.atwd_chip == 0 else "B"

    def atwd(self, channel):
        """
        ATWD values (note that these are in time-reversed order)
        """
        if not self.has_atwd:
            raise PayloadException("No available ATWD channels")

        if not self.__decoded:
            self.__decode_waveforms()

        return self.__atwd[channel]

    @property
    def atwd_channels(self):
        "Number of ATWD channels"
        return (self.__word0 & 0x3000) >> 12

    @property
    def atwd_chip(self):
        "Return 0 if ATWD A is selected, 1 if B is selected"
        return (self.__word0 >> 11) & 1

    @property
    def chargestamp(self):
        "Charge stamp"
        if (self.__pedestal & 2) != 0:
            return ((self.__word2 >> 17) & 3, self.__word2 & 0x1ffff, 0, 0)

        lsh = 1 if self.__word2 & 0x80000000 else 0

        return ((self.__word2 >> 27) & 0xf,
                ((self.__word2 >> 18) & 0x1ff) << lsh,
                ((self.__word2 >> 9) & 0x1ff) << lsh,
                ((self.__word2 & 0x1ff) << lsh))

    @property
    def config_id(self):
        "Always return 0, since no configuration ID is available"
        return 0

    @property
    def domclk(self):
        "Unadjusted DOM clock"
        return self.__domclk

    @property
    def envelope(self):
        return struct.pack(">2IQ", self.data_length + self.ENVELOPE_LENGTH,
                           self.payload_type_id(), self.__mbid)

    @property
    def fadc(self):
        "fADC values"
        if not self.has_fadc:
            raise PayloadException("No available fADC")

        if not self.__decoded:
            self.__decode_waveforms()

        return self.__fadc[:]

    @property
    def has_atwd(self):
        "Does the hit have ATWD?"
        return self.__word0 & 0x4000 != 0

    @property
    def has_fadc(self):
        "Does the hit have fADC?"
        return self.__word0 & 0x8000 != 0

    @property
    def hit_size(self):
        "Hit size"
        return self.__word0 & 0x7ff

    @property
    def lc(self):
        "LC bits"
        return (self.__word0 >> 16) & 3

    @property
    def mb(self):
        "Min bias flag"
        return (self.__word0 >> 30) & 1

    @property
    def mbid(self):
        "Mainboard ID as an integer"
        return self.__mbid

    @property
    def mbid_str(self):
        "Mainboard ID as a string"
        return "%012x" % self.__mbid

    @property
    def source_id(self):
        "Always return 0, since no source ID is available"
        return 0

    @property
    def trigger_mode(self):
        bits = (self.__word0 >> 18) & 0x1017
        if (bits & 0x1000) == 0x1000:
            return 4
        if (bits & 0x0010) == 0x0010:
            return 3
        if (bits & 0x0003) != 0:
            return 2
        if (bits & 0x0004) == 0x0004:
            return 1
        return 0

    @property
    def trigger_type(self):
        "Always return 0, since no trigger type is available"
        return 0

    @property
    def trigmask(self):
        "Trigger mask"
        return (self.__word0 >> 18) & 0xfff

    @property
    def version(self):
        "Payload version"
        return self.__version

    def word(self, num):
        "Trigger words"
        if num == 0:
            return self.__word0
        return self.__word2


class EventV5(Payload):
    "Standard event payload"

    TYPE_ID = 21
    MIN_LENGTH = 18

    def __init__(self, utime, data, keep_data=True):
        """
        Extract V5 event data from the buffer
        """
        if len(data) < self.MIN_LENGTH:
            raise PayloadException("Expected at least %d data bytes, got %d" %
                                   (self.MIN_LENGTH, len(data)))

        super(EventV5, self).__init__(utime, data, keep_data=keep_data)

        hdr = struct.unpack(">IHIII", data[:18])

        self.__stop_time = utime + hdr[0]
        self.__year = hdr[1]
        self.__uid = hdr[2]
        self.__run = hdr[3]
        self.__subrun = hdr[4]

        offset = 18
        self.__hit_records, offset = self.__load_hit_records(utime, data,
                                                             offset)
        self.__trig_records, offset = self.__load_trig_records(utime, data,
                                                               offset)

    def __str__(self):
        "Payload description"
        return "EventV5[#%d [%d-%d] yr %d run %d hitRecs*%d" \
            " trigRecs*%d]" % \
            (self.uid, self.start_time, self.stop_time, self.year,
             self.run, len(self.__hit_records), len(self.__trig_records))

    @staticmethod
    def __load_hit_records(base_time, data, offset):
        "Return all the hit records"

        # extract the number of hit records
        num_recs = struct.unpack(">I", data[offset:offset+4])[0]
        offset += 4

        recs = []
        for _ in range(num_recs):
            hdrdata = data[offset:offset+BaseHitRecord.HEADER_LEN]
            rechdr = struct.unpack(">HBBHI", hdrdata)
            if rechdr[1] == EngineeringHitRecord.TYPE_ID:
                rec = EngineeringHitRecord(base_time, rechdr, data,
                                           offset + BaseHitRecord.HEADER_LEN)
            elif rechdr[1] == DeltaHitRecord.TYPE_ID:
                rec = DeltaHitRecord(base_time, rechdr, data,
                                     offset + BaseHitRecord.HEADER_LEN)
            else:
                raise PayloadException("Unknown hit record type #%d" %
                                       rechdr[1])

            recs.append(rec)
            offset += len(rec)

        return recs, offset

    @staticmethod
    def __load_trig_records(base_time, data, offset):
        "Return all the trigger records"

        # extract the number of trigger records
        num_recs = struct.unpack(">I", data[offset:offset+4])[0]
        offset += 4

        recs = []
        for _ in range(num_recs):
            offend = offset + TriggerRecord.HEADER_LEN
            rechdr = struct.unpack(">6i", data[offset:offend])
            rec = TriggerRecord(base_time, rechdr, data,
                                offset + TriggerRecord.HEADER_LEN)
            recs.append(rec)
            offset += len(rec)

        return recs, offset

    def hit(self, idx):
        "Return the requested hit record, or None if the index is not valid"
        if idx < 0 or idx >= len(self.__hit_records):
            return None
        return self.__hit_records[idx]

    @property
    def hit_count(self):
        "Return count of hit records"
        return len(self.__hit_records)

    @property
    def hits(self):
        "Return list of hit records"
        return self.__hit_records[:]

    @property
    def run(self):
        "Run number"
        return self.__run

    @property
    def start_time(self):
        "First time (in ticks) covered by this event"
        return self.utime

    @property
    def stop_time(self):
        "Last time (in ticks) covered by this event"
        return self.__stop_time

    @property
    def subrun(self):
        "Subrun number"
        return self.__subrun

    @property
    def trigger_count(self):
        "Return count of trigger records"
        return len(self.__trig_records)

    @property
    def triggers(self):
        "Return list of trigger records"
        return self.__trig_records[:]

    @property
    def uid(self):
        "Unique event ID"
        return self.__uid

    @property
    def year(self):
        "Year this event was seen"
        return self.__year


class BaseHitRecord(object):
    "Generic hit record class"
    HEADER_LEN = 10

    def __init__(self, base_time, hdr, data, offset):
        self.__flags = hdr[2]
        self.__chan_id = hdr[3]
        self.__utime = base_time + hdr[4]
        if hdr[0] == self.HEADER_LEN:
            self.__data = []
        else:
            self.__data = data[offset+10:offset+hdr[0]]

    def __len__(self):
        return self.HEADER_LEN + len(self.__data)

    def __str__(self):
        return "%d@%d[flags %x]" % (self.__chan_id, self.__utime, self.__flags)

    @property
    def channel_id(self):
        "Return DOM channel ID"
        return self.__chan_id

    @property
    def flags(self):
        "Return flag bits"
        return self.__flags

    @property
    def timestamp(self):
        "Return payload envelope time"
        return self.__utime


class DeltaHitRecord(BaseHitRecord):
    "Delta-compressed hit record inside V5 event payload"
    TYPE_ID = 1


class EngineeringHitRecord(BaseHitRecord):
    "Engineering hit record inside V5 event payload"
    TYPE_ID = 0


# pylint: disable=too-few-public-methods
class Monitor(object):
    "Monitoring record base class"

    TYPE_ID = 5

    def __init__(self):
        """
        Extract time calibration data from the buffer
        """
        raise NotImplementedError("Use Monitor.subtype()")

    @classmethod
    def subtype(cls, utime, data, keep_data=True):
        "Return record subtype"
        if len(data) < 12:
            raise PayloadException("Truncated monitoring record")

        subhdr = struct.unpack(">Qhh6B", data[:18])
        if subhdr[1] != len(data) - 8:
            raise PayloadException("Expected %d-byte record, not %d" %
                                   (subhdr[1], len(data) - 8))

        dom_id = subhdr[0]

        if subhdr[2] & 0xff > 0:
            rectype = subhdr[2] & 0xff
        else:
            rectype = (subhdr[2] >> 8) & 0xff

        domclock = subhdr[3:]

        if rectype == MonitorHardware.SUBTYPE_ID:
            return MonitorHardware(utime, dom_id, domclock, data[18:])
        if rectype == MonitorConfig.SUBTYPE_ID:
            return MonitorConfig(utime, dom_id, domclock, data[18:])
        if rectype == MonitorConfigChange.SUBTYPE_ID:
            return MonitorConfigChange(utime, dom_id, domclock, data[18:])
        if rectype == MonitorASCII.SUBTYPE_ID:
            return MonitorASCII(utime, dom_id, domclock, data[18:])
        if rectype == MonitorGeneric.SUBTYPE_ID:
            return MonitorGeneric(utime, dom_id, domclock, data[18:])

        return UnknownPayload(cls.TYPE_ID, utime, data, keep_data=keep_data)


class MonitorRecord(object):
    "Superclass for all monitoring records"

    def __init__(self, utime, dom_id, domclock):
        self.__utime = utime
        self.__dom_id = dom_id
        self.__clock_bytes = Payload.extract_clock_bytes(domclock)

    @property
    def bytes(self):
        "Return the binary representation of this payload"
        return NotImplementedError()

    @property
    def clockbytes(self):
        "Return the raw DOM clock bytes"
        return self.__clock_bytes[:]

    @property
    def dom_id(self):
        "Return the DOM ID"
        return self.__dom_id

    @property
    def domclock(self):
        "Return the DOM clock value"
        val = 0
        for byte in self.__clock_bytes:
            val = (val << 8) + byte
        return val

    @property
    def has_data(self):
        "Return True if this payload has additional data bytes"
        return False

    @property
    def utime(self):
        "Return payload envelope time"
        return self.__utime


class MonitorASCII(MonitorRecord):
    "ASCII monitoring record"

    SUBTYPE_ID = 0xcb

    def __init__(self, utime, dom_id, domclock, data):
        self.__text = struct.unpack("%ds" % len(data), data)[0]

        super(MonitorASCII, self).__init__(utime, dom_id, domclock)

    def __str__(self):
        return "MonitorASCII@%d[dom %012x clk %d \"%s\"]" % \
            (self.utime, self.dom_id, self.domclock, self.__text)

    @property
    def bytes(self):
        if self.has_data:
            return super(MonitorASCII, self).bytes

        txtlen = len(self.__text)
        paylen = 34 + txtlen

        clkstr = struct.pack(">%sB" % (len(self.clockbytes), ),
                             *self.clockbytes)
        return struct.pack(">IIQQHH%ds%ds" % (len(clkstr), len(self.__text)),
                           paylen, Monitor.TYPE_ID, self.utime, self.dom_id,
                           txtlen + 10, self.SUBTYPE_ID, clkstr, self.__text)

    @property
    def subtype(self):
        "Return record subtype"
        return self.SUBTYPE_ID

    @property
    def text(self):
        "Return the monitoring text"
        return self.__text


class MonitorConfig(MonitorRecord):
    "Configuration monitoring record"

    SUBTYPE_ID = 0xc9

    def __init__(self, utime, dom_id, domclock, data):
        self.__data = data

        super(MonitorConfig, self).__init__(utime, dom_id, domclock)

    def __str__(self):
        return "MonitorConfig@%d[dom %012x clk %d data*%d]" % \
            (self.utime, self.dom_id, self.domclock, len(self.__data))

    @property
    def subtype(self):
        "Return record subtype"
        return self.SUBTYPE_ID


class MonitorConfigChange(MonitorRecord):
    "Configuration change monitoring record"

    SUBTYPE_ID = 0xca

    def __init__(self, utime, dom_id, domclock, data):
        self.__data = data

        super(MonitorConfigChange, self).__init__(utime, dom_id, domclock)

    def __str__(self):
        return "MonitorConfigChange@%d[dom %012x clk %d data*%d]" % \
            (self.utime, self.dom_id, self.domclock, len(self.__data))

    @property
    def subtype(self):
        "Return record subtype"
        return self.SUBTYPE_ID


class MonitorGeneric(MonitorRecord):
    "Generic monitoring record"

    SUBTYPE_ID = 0xcc

    def __init__(self, utime, dom_id, domclock, data):
        self.__data = data

        super(MonitorGeneric, self).__init__(utime, dom_id, domclock)

    def __str__(self):
        return "MonitorGeneric@%d[dom %012x clk %d data*%d]" % \
            (self.utime, self.dom_id, self.domclock, len(self.__data))

    @property
    def data(self):
        "Return data bytes"
        return self.__data[:]

    @property
    def subtype(self):
        "Return record subtype"
        return self.SUBTYPE_ID


class MonitorHardware(MonitorRecord):
    "Hardware monitoring record"

    SUBTYPE_ID = 0xc8

    def __init__(self, utime, dom_id, domclock, data):
        self.__data = data

        super(MonitorHardware, self).__init__(utime, dom_id, domclock)

    def __str__(self):
        return "MonitorHardware@%d[dom %012x clk %d data*%d]" % \
            (self.utime, self.dom_id, self.domclock, len(self.__data))

    @property
    def subtype(self):
        "Return record subtype"
        return self.SUBTYPE_ID


class Supernova(Payload):
    "Supernova scaler payload"
    TYPE_ID = 16
    MAGIC_NUMBER = 300

    # pylint: disable=too-many-arguments
    def __init__(self, utime, data_or_dom_id, dom_clock=None,
                 scaler_bytes=None, keep_data=False):
        """Create a supernova payload"""

        if data_or_dom_id is not None and \
           (dom_clock is None or scaler_bytes is None):
            scaler_len = len(data_or_dom_id) - 18
            flds = struct.unpack(">QHH6B%dB" % (scaler_len, ), data_or_dom_id)
            if flds[2] != self.MAGIC_NUMBER:
                raise PayloadException("Supernova magic number is %d, not %d" %
                                       (flds[2], self.MAGIC_NUMBER))
            self.__dom_id = flds[0]
            self.__clock_bytes = flds[3:9]
            self.__scaler_bytes = flds[9:]
        else:
            self.__dom_id = data_or_dom_id
            self.__clock_bytes = self.extract_clock_bytes(dom_clock)
            self.__scaler_bytes = scaler_bytes

        super(Supernova, self).__init__(utime, data_or_dom_id,
                                        keep_data=keep_data)

    def __str__(self):
        return "Supernova@%d[dom %012x clk %012x scalerData*%d" % \
            (self.utime, self.__dom_id, self.domclock,
             len(self.__scaler_bytes))

    @property
    def bytes(self):
        if self.has_data:
            return super(Supernova, self).bytes

        reclen = len(self.__scaler_bytes)
        paylen = 34 + reclen

        clkstr = struct.pack(">%sB" % (len(self.__clock_bytes), ),
                             *self.__clock_bytes)
        sclstr = struct.pack(">%sB" % (len(self.__scaler_bytes), ),
                             *self.__scaler_bytes)

        return struct.pack(">IIQQHH%ds%ds" % (len(self.__clock_bytes), reclen),
                           paylen, self.TYPE_ID, self.utime, self.__dom_id,
                           reclen + 10, self.MAGIC_NUMBER, clkstr, sclstr)

    @property
    def domclock(self):
        "Return the unadjusted DOM clock time"
        val = 0
        for byte in self.__clock_bytes:
            val = (val << 8) + byte
        return val


class TimeCalibration(Payload):
    "Time calibration"

    TYPE_ID = 4
    LENGTH = 322

    def __init__(self, utime, data, keep_data=True):
        """
        Extract time calibration data from the buffer
        """
        if len(data) != self.LENGTH:
            raise PayloadException("Expected %d data bytes, got %d" %
                                   (self.LENGTH, len(data)))

        super(TimeCalibration, self).__init__(utime, data, keep_data=keep_data)

        dombytes = struct.unpack(">Q", data[:8])
        self.__dom_id = dombytes[0]

        hdr = struct.unpack("<HHQQ128xQQ128xB12sc", data[8:self.LENGTH - 8])
        self.__pktlen = hdr[0]
        self.__format = hdr[1]
        self.__dor_tx = hdr[2]
        self.__dor_rx = hdr[3]
        self.__dor_waveform = data[28:156]
        self.__dom_rx = hdr[4]
        self.__dom_tx = hdr[5]
        self.__dom_waveform = data[172:300]
        self.__start_of_gps = hdr[6]
        self.__julianstr = hdr[7]
        self.__quality = hdr[8]

        st = struct.unpack(">Q", data[self.LENGTH - 8:])
        self.__synctime = st[0]

    def __str__(self):
        "Payload description"
        return "TimeCalibration[dom %012x dor:tx#%d rx#%d,dom:rx#%d tx#%d," \
            " \"%s\" Q'%s' S%d]" % \
            (self.__dom_id, self.__dor_tx, self.__dor_rx, self.__dom_rx,
             self.__dom_tx, self.__julianstr, self.__quality, self.__synctime)

    @property
    def dom_id(self):
        "Return DOM mainboard ID"
        return self.__dom_id

    @property
    def dom_rx(self):
        "Return DOM receive time"
        return self.__dom_rx

    @property
    def dom_tx(self):
        "Return DOM transmit time"
        return self.__dom_tx

    @property
    def dor_rx(self):
        "Return DOR transmit time"
        return self.__dor_rx

    @property
    def dor_tx(self):
        "Return DOR transmit time"
        return self.__dor_tx


class TriggerRecord(object):
    "Encoded trigger request inside V5 event payload"
    HEADER_LEN = 24

    def __init__(self, base_time, hdr, data, offset):
        self.__type = hdr[0]
        self.__config_id = hdr[1]
        self.__source_id = hdr[2]
        self.__start_time = base_time + hdr[3]
        self.__end_time = base_time + hdr[4]
        self.__hit_index = []

        num = hdr[5]
        for _ in range(num):
            idx = struct.unpack(">I", data[offset:offset+4])
            self.__hit_index.append(idx[0])
            offset += 4

    def __len__(self):
        return self.HEADER_LEN + (len(self.__hit_index) * 4)

    def __str__(self):
        "Payload description"
        return "TriggerRecord[%s typ %d cfg %d [%d-%d] hits*%d]" % \
            (self.source_name, self.__type, self.__config_id,
             self.__start_time, self.__end_time, self.hit_count)

    @property
    def config_id(self):
        "Return the configuration ID of the trigger which created this payload"
        return self.__config_id

    @property
    def end_time(self):
        "Return the end of the time window (in DAQ ticks)"
        return self.__end_time

    @property
    def hit_count(self):
        "Return the total number of hits bundled with this trigger"
        return len(self.__hit_index)

    @property
    def hit_indexes(self):
        "Iterate through the list of hit indexes"
        for idx in self.__hit_index:
            yield idx

    @property
    def source_id(self):
        "Return the ID of the component which created this payload"
        return self.__source_id

    @property
    def source_name(self):
        "Return the component name which created this payload"
        return Payload.source_name(self.__source_id)

    @property
    def start_time(self):
        "Return the start of the time window (in DAQ ticks)"
        return self.__start_time

    @property
    def trigger_type(self):
        "Return the type of the trigger which created this payload"
        return self.__type


class PayloadReader(object):
    "Read DAQ payloads from a file"
    def __init__(self, filename, keep_data=True):
        """
        Open a payload file
        """
        if not os.path.exists(filename):
            raise PayloadException("Cannot read \"%s\"" % filename)

        if filename.endswith(".gz"):
            fin = gzip.open(filename, "rb")
        elif filename.endswith(".bz2"):
            fin = bz2.BZ2File(filename)
        else:
            fin = open(filename, "rb")

        self.__filename = filename
        self.__fin = fin
        self.__keep_data = keep_data
        self.__num_read = 0

    def __enter__(self):
        """
        Return this object as a context manager to used as
        `with PayloadReader(filename) as payrdr:`
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close the open filehandle when the context manager exits
        """
        self.close()

    def __iter__(self):
        """
        Generator which returns payloads in `for payload in payrdr:` loops
        """
        while True:
            if self.__fin is None:
                # generator has been explicitly closed
                return

            # decode the next payload
            try:
                pay = next(self)
            except StopIteration:
                return
            if pay is None:
                # must have hit the end of the file
                return

            # return the next payload
            yield pay

    def __next__(self):
        "Read the next payload"
        pay = self.decode_payload(self.__fin, keep_data=self.__keep_data)
        self.__num_read += 1
        return pay

    def close(self):
        """
        Explicitly close the filehandle
        """
        if self.__fin is not None:
            try:
                self.__fin.close()
            finally:
                self.__fin = None

    @property
    def nrec(self):
        "Number of payloads read to this point"
        return self.__num_read

    @property
    def filename(self):
        "Name of file being read"
        return self.__filename

    @classmethod
    def decode_payload(cls, stream, keep_data=True):
        """
        Decode and return the next payload
        """
        envelope = stream.read(Payload.ENVELOPE_LENGTH)
        if len(envelope) == 0:  # pylint: disable=len-as-condition
            return None

        length, type_id, utime = struct.unpack(">iiq", envelope)
        if length <= Payload.ENVELOPE_LENGTH:
            rawdata = None
        else:
            rawdata = stream.read(length - Payload.ENVELOPE_LENGTH)

        if type_id == SimpleHit.TYPE_ID:
            pay = SimpleHit(utime, rawdata, keep_data=keep_data)
        elif type_id == DeltaCompressedHit.TYPE_ID:
            # 'utime' is actually mainboard ID
            pay = DeltaCompressedHit(utime, rawdata, keep_data=keep_data)
        elif type_id == EventV5.TYPE_ID:
            pay = EventV5(utime, rawdata, keep_data=keep_data)
        elif type_id == TimeCalibration.TYPE_ID:
            pay = TimeCalibration(utime, rawdata, keep_data=keep_data)
        elif type_id == Monitor.TYPE_ID:
            pay = Monitor.subtype(utime, rawdata, keep_data=keep_data)
        elif type_id == Supernova.TYPE_ID:
            pay = Supernova(utime, rawdata, keep_data=keep_data)
        else:
            pay = UnknownPayload(type_id, utime, rawdata, keep_data=keep_data)

        return pay

    next = __next__  # XXX backward compatibility for Python 2


def read_file(filename, max_payloads, write_simple_hits=False):
    "Read a binary payload file and print a description of each payload"
    if write_simple_hits and filename.startswith("HitSpool-"):
        out = open("SimpleHit-" + filename[9:], "w")
    else:
        out = None

    try:
        with PayloadReader(filename) as rdr:
            for pay in rdr:
                if max_payloads is not None and rdr.nrec > max_payloads:
                    break

                print(str(pay))
                if out is not None:
                    out.write(pay.simple_hit)
    finally:
        if out is not None:
            out.close()


def main():
    "Main program"

    parser = argparse.ArgumentParser()

    parser.add_argument("-S", "--simple-hits", dest="write_simple_hits",
                        action="store_true", default=False,
                        help="Rewrite hits to trigger-friendly SimpleHits")
    parser.add_argument("-n", "--max_payloads", type=int,
                        dest="max_payloads", default=None,
                        help="Maximum number of payloads to dump")
    parser.add_argument(dest="fileList", nargs="+")

    args = parser.parse_args()

    for fnm in args.fileList:
        if os.path.isfile(fnm):
            read_file(fnm, args.max_payloads, args.write_simple_hits)
            continue

        for entry in os.listdir(fnm):
            path = os.path.join(fnm, entry)
            if os.path.isfile(path):
                read_file(path, args.max_payloads, args.write_simple_hits)

if __name__ == "__main__":
    main()
