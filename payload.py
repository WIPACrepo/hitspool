#!/usr/bin/env python
#
# IceCube payloads and associated classes
#
# NOTE: This is borrowed from pDAQ.  After pDAQ_Minocqua has been released,
# the HS code should import these classes from there


import bz2
import cStringIO
import gzip
import os
import struct


class PayloadException(Exception):  # pragma: no cover
    "Payload exception"
    pass


class Payload(object):  # pragma: no cover
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

    def __cmp__(self, other):
        "Compare envelope times"
        if self.__utime < other.utime:
            return -1
        if self.__utime > other.utime:
            return 1
        return 0

    @property
    def bytes(self):
        "Return the binary representation of this payload"
        if not self.__valid_data:
            raise PayloadException("Data was discarded; cannot return bytes")
        return self.envelope + self.__data

    @property
    def data_bytes(self):
        "Data bytes (should not include the 16 byte envelope)"
        if not self.__valid_data:
            raise PayloadException("Data was discarded; cannot return bytes")
        return self.__data

    @property
    def data_length(self):
        if not self.__valid_data:
            raise PayloadException("Data was discarded; cannot return length")
        return len(self.__data)

    @property
    def envelope(self):
        return struct.pack(">2IQ", self.data_length + self.ENVELOPE_LENGTH,
                           self.payload_type_id(), self.__utime)

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
            return "icetopHandler-%d" % comp_num
        elif comp_type == 12:
            return "stringHub-%d" % comp_num
        elif comp_type == 13:
            return "simHub-%d" % comp_num

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

        if comp_num != 0:
            raise PayloadException("Unexpected component#%d for %s" %
                                   (comp_num, comp_name))

        return comp_name

    @property
    def utime(self):
        "UTC time from payload header"
        return self.__utime


class UnknownPayload(Payload):  # pragma: no cover
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

    def __init__(self, utime, data, keep_data=True):
        "Create a simple hit"

        flds = struct.unpack(">3iqh", data)
        self.__trig_type = flds[0]
        self.__cfg_id = flds[1]
        self.__src_id = flds[2]
        self.__mbid = flds[3]
        if flds[4] != self.__trig_type:
            raise PayloadException("SimpleHit@%d: type %0x != mode %0x" %
                                   (self.__trig_type, flds[4]))

        super(SimpleHit, self).__init__(utime, data, keep_data=keep_data)

    def __str__(self):
        "Payload description"
        return "SimpleHit@%d[%s %s cfg %d type %0x]" % \
            (self.utime, self.source_name(self.__src_id), self.mbid_str,
             self.__cfg_id, self.__trig_type)

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
class delta_codec(object):  # pragma: no cover
    """
    Delta compression decoder (stolen from icecube.daq.slchit in pDAQ's PyDOM)
    """
    def __init__(self, buf):
        "Load the buffer and prepare to decode"
        self.tape = cStringIO.StringIO(buf)
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
                # print "%d: Got %d" % (i, wrd)
                if wrd != (1 << (self.bpw - 1)):
                    break
                self.shift_up()
            if abs(wrd) < self.bth:
                self.shift_down()
            last += wrd
            # print "out", last
            out.append(last)
        return out

    def get_bits(self):
        "Decode the next byte"
        while self.valid_bits < self.bpw:
            next_byte, = struct.unpack('B', self.tape.read(1))
            # print "Read", next_byte
            self.register |= (next_byte << self.valid_bits)
            self.valid_bits += 8
        # print "Bit register:", bitstring(self.register, self.valid_bits)
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
        # print "Shifted up to", self.bpw, self.bth

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
        # print "Shifted down to", self.bpw, self.bth


class HitPayload(Payload):
    def __init__(self, utime, data, keep_data=True):
        super(HitPayload, self).__init__(utime, data, keep_data=keep_data)

    @property
    def simple_hit(self):
        return struct.pack(">2iq3iqh", 38, self.payload_type_id(), self.utime,
                           self.trigger_type, self.config_id, self.source_id,
                           self.mbid, self.trigger_mode)

    @property
    def config_id(self):
        raise NotImplementedError()

    @property
    def trigger_mode(self):
        raise NotImplementedError()

    @property
    def trigger_type(self):
        raise NotImplementedError()

    @property
    def trigger_type(self):
        raise NotImplementedError()


# pylint: disable=too-many-instance-attributes
# hits hold a lot of information
class DeltaCompressedHit(Payload):  # pragma: no cover
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


class EventV5(Payload):  # pragma: no cover
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
        if idx < 0 or idx >= len(self.__hit_records):
            return None
        return self.__hit_records[idx]

    @property
    def hit_count(self):
        return len(self.__hit_records)

    @property
    def hits(self):
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
        return len(self.__trig_records)

    def triggers(self):
        return self.__trig_records[:]

    @property
    def uid(self):
        "Unique event ID"
        return self.__uid

    @property
    def year(self):
        "Year this event was seen"
        return self.__year


class BaseHitRecord(object):  # pragma: no cover
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

    @property
    def channel_id(self):
        return self.__chan_id

    @property
    def flags(self):
        return self.__flags

    @property
    def timestamp(self):
        return self.__utime


class DeltaHitRecord(BaseHitRecord):  # pragma: no cover
    "Delta-compressed hit record inside V5 event payload"
    TYPE_ID = 1

    def __init__(self, base_time, hdr, data, offset):
        super(DeltaHitRecord, self).__init__(base_time, hdr, data, offset)


class EngineeringHitRecord(BaseHitRecord):  # pragma: no cover
    "Engineering hit record inside V5 event payload"
    TYPE_ID = 0

    def __init__(self, base_time, hdr, data, offset):
        super(EngineeringHitRecord, self).__init__(base_time, hdr, data,
                                                   offset)


class TimeCalibration(Payload):  # pragma: no cover
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

        hdr = struct.unpack("<QHHQQ128xQQ128xB12sc", data[:314])
        self.__dom_id = hdr[0]
        self.__pktlen = hdr[1]
        self.__format = hdr[2]
        self.__dor_tx = hdr[3]
        self.__dor_rx = hdr[4]
        self.__dor_waveform = data[28:156]
        self.__dom_rx = hdr[5]
        self.__dom_tx = hdr[6]
        self.__dom_waveform = data[172:300]
        self.__start_of_gps = hdr[7]
        self.__julianstr = hdr[8]
        self.__quality = hdr[9]

        st = struct.unpack(">Q", data[314:])
        self.__synctime = st[0]

    def __str__(self):
        "Payload description"
        return "TimeCalibration[dor:tx#%d rx#%d,dom:rx#%d tx#%d," \
            " \"%s\" Q'%s' S%d]" % \
            (self.__dor_tx, self.__dor_rx, self.__dom_rx, self.__dom_tx,
             self.__julianstr, self.__quality, self.__synctime)

    @property
    def dom_id(self):
        return self.__dom_id

    @property
    def dom_rx(self):
        return self.__dom_rx

    @property
    def dom_tx(self):
        return self.__dom_tx

    @property
    def dor_rx(self):
        return self.__dor_rx

    @property
    def dor_tx(self):
        return self.__dor_tx


class TriggerRecord(object):  # pragma: no cover
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
        self.__num_read = 0L

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
            pay = self.next()
            if pay is None:
                # must have hit the end of the file
                return

            # return the next payload
            yield pay

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
        if len(envelope) == 0:
            return None

        length, type_id, utime = struct.unpack(">iiq", envelope)
        if length <= Payload.ENVELOPE_LENGTH:
            rawdata = None
        else:
            rawdata = stream.read(length - Payload.ENVELOPE_LENGTH)

        if type_id == SimpleHit.TYPE_ID:
            return SimpleHit(utime, rawdata, keep_data=keep_data)
        if type_id == DeltaCompressedHit.TYPE_ID:
            # 'utime' is actually mainboard ID
            return DeltaCompressedHit(utime, rawdata, keep_data=keep_data)
        if type_id == EventV5.TYPE_ID:
            return EventV5(utime, rawdata, keep_data=keep_data)
        if type_id == TimeCalibration.TYPE_ID:
            return TimeCalibration(utime, rawdata, keep_data=keep_data)

        return UnknownPayload(type_id, utime, rawdata, keep_data=keep_data)

    def next(self):
        "Read the next payload"
        pay = self.decode_payload(self.__fin, keep_data=self.__keep_data)
        self.__num_read += 1
        return pay


if __name__ == "__main__":
    def main():
        "Dump all payloads"
        import argparse

        parser = argparse.ArgumentParser()

        parser.add_argument("-n", "--max_payloads", type=int,
                            dest="max_payloads", default=None,
                            help="Maximum number of payloads to dump")
        parser.add_argument(dest="fileList", nargs="+")

        args = parser.parse_args()

        for fnm in args.fileList:
            if fnm.startswith("HitSpool-"):
                outnm = "SimpleHit-" + fnm[9:]
            else:
                import sys
                print >>sys.stderr, "Unknown file name " + fnm
                continue

            with open(outnm, "w") as out:
                with PayloadReader(fnm) as rdr:
                    for pay in rdr:
                        if args.max_payloads is not None and \
                           rdr.nrec > args.max_payloads:
                            break

                    out.write(pay.simple_hit)

    main()
