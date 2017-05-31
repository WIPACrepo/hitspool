#!/usr/bin/env python


"""
#Hit Spool Worker to be run on hubs
#author: dheereman i3.hsinterface@gmail.com
#check out the icecube wiki page for instructions:
https://wiki.icecube.wisc.edu/index.php/HitSpool_Interface_Operation_Manual
"""
import functools
import logging
import os
import signal
import zmq

from zmq import ZMQError

import HsMessage
import HsUtil

from HsBase import DAQTime, HsBase
from HsException import HsException
from HsRSyncFiles import HsRSyncFiles


def add_arguments(parser):
    dflt_copydir = "%s@%s:%s" % (HsBase.DEFAULT_RSYNC_USER,
                                 HsBase.DEFAULT_RSYNC_HOST,
                                 HsBase.DEFAULT_COPY_PATH)

    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hsworker.log")

    parser.add_argument("-C", "--copydir", dest="copydir",
                        default=os.path.join(dflt_copydir, "test"),
                        help="Final directory on 2ndbuild as user pdaq")
    parser.add_argument("-H", "--hostname", dest="hostname",
                        default=None,
                        help="Name of this host")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-R", "--hubroot", dest="hubroot",
                        default=os.path.join(dflt_copydir, "test"),
                        help="Final directory on 2ndbuild as user pdaq")
    parser.add_argument("-T", "--is-test", dest="is_test",
                        action="store_true", default=False,
                        help="Ignore SPS/SPTS status for tests")


class Worker(HsRSyncFiles):
    """
    "sndaq/HsGrabber"  "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        --------------
    | REQ     | <----->| REP     |         | IcHub n |        | 2ndbuild    |
    -----------        | PUB     | ------> | SUB   n |        | PUSH(13live)|
                       ----------          | PUSH    | ---->  | PULL        |
                                            ---------         --------------
    HsWorker.py of the HitSpool Interface.
    This class
    1. analyzes the alert message
    2. looks for the requested files / directory
    3. copies them over to the requested directory specified in the message.
    4. writes a short report about was has been done.
    """

    def __init__(self, progname, host=None, is_test=False):
        super(Worker, self).__init__(host=host, is_test=is_test)

        self.__service = "HSiface"
        self.__varname = "%s@%s" % (progname, self.shorthost)

    def alert_parser(self, req, logfile, update_status=None, delay_rsync=True):
        """
        Parse the Alert message for starttime, stoptime, sn-alert-time-stamp
        and directory where-to the data has to be copied.
        """

        if isinstance(req.start_time, DAQTime):
            start_time = req.start_time
        else:
            raise TypeError("Start time %s<%s> is not a DAQTime" %
                            (req.start_time, type(req.start_time)))

        if isinstance(req.stop_time, DAQTime):
            stop_time = req.stop_time
        else:
            raise TypeError("Stop time %s<%s> is not a DAQTime" %
                            (req.stop_time, type(req.stop_time)))

        # should we extract only the matching hits to a new file?
        extract_hits = req.extract

        # parse destination string
        try:
            hs_ssh_access, hs_copydir \
                = HsUtil.split_rsync_host_and_path(req.destination_dir)
        except Exception, err:
            self.send_alert("ERROR: destination parsing failed for"
                            " \"%s\". Abort request." % req.destination_dir)
            logging.error("Destination parsing failed for \"%s\":\n"
                          "Abort request.", req.destination_dir)
            return None

        if hs_ssh_access != "":
            logging.info("Ignoring rsync user/host \"%s\"", hs_ssh_access)

        logging.info("SN START [ns] = %d", start_time.ticks)
        logging.info("ALERTSTART: %s", start_time.utc)

        logging.info("SN STOP [ns] = %s", stop_time.ticks)
        logging.info("ALERTSTOP = %s", stop_time.utc)

        # check for data range
        tick_secs = (stop_time.ticks - start_time.ticks) / 1E10
        if tick_secs > self.MAX_REQUEST_SECONDS:
            errmsg = "Request for %.2fs exceeds limit of allowed data time" \
                     " range of %.2fs. Abort request..." % \
                     (tick_secs, self.MAX_REQUEST_SECONDS)
            self.send_alert("ERROR: " + errmsg)
            logging.error(errmsg)
            return None

        rsyncdir = self.request_parser(req, start_time, stop_time,
                                       hs_copydir, extract_hits=extract_hits,
                                       update_status=update_status,
                                       delay_rsync=delay_rsync,
                                       make_remote_dir=False)
        if rsyncdir is None:
            logging.error("Request failed")
            return None

        return rsyncdir

    # --- Clean exit when program is terminated from outside (via pkill) ---#
    def handler(self, signum, _):
        logging.warning("Signal Handler called with signal %s", signum)
        logging.warning("Shutting down...\n")

        if self.i3socket is not None:
            i3live_dict = {}
            i3live_dict["service"] = "HSiface"
            i3live_dict["varname"] = "HsWorker@%s" % self.shorthost
            i3live_dict["value"] = "INFO: SHUT DOWN called by external signal."
            self.i3socket.send_json(i3live_dict)

            i3live_dict = {}
            i3live_dict["service"] = "HSiface"
            i3live_dict["varname"] = "HsWorker@%s" % self.shorthost
            i3live_dict["value"] = "STOPPED"
            self.i3socket.send_json(i3live_dict)

        self.close_all()

        raise SystemExit(0)

    def mainloop(self, logfile):
        if self.subscriber is None:
            raise Exception("Subscriber has not been initialized")

        logging.info("ready for new alert...")

        try:
            req = self.receive_request(self.subscriber)
        except KeyboardInterrupt:
            raise
        except ZMQError:
            raise
        except:
            logging.exception("Cannot read request")
            return False

        logging.info("HsWorker received request:\n"
                     "%s\nfrom Publisher", req)

        # extract actual directory from rsync path
        try:
            _, destdir = HsUtil.split_rsync_host_and_path(req.destination_dir)
        except:
            logging.exception("Illegal destination directory \"%s\"<%s>",
                              req.destination_dir, type(req.destination_dir))
            return False

        update_status = functools.partial(HsMessage.send_for_request,
                                          self.sender, req, self.shorthost)

        update_status(req.copy_dir, req.destination_dir,
                      HsMessage.MESSAGE_STARTED, use_ticks=True)

        try:
            rsyncdir = self.alert_parser(req, logfile,
                                         update_status=update_status)
        except:
            logging.exception("Cannot process request \"%s\"", req)
            rsyncdir = None

        if rsyncdir is not None:
            msgtype = HsMessage.MESSAGE_DONE
        else:
            msgtype = HsMessage.MESSAGE_FAILED

        update_status(rsyncdir, destdir, msgtype, use_ticks=True)

    def receive_request(self, sock):
        req_dict = sock.recv_json()
        if not isinstance(req_dict, dict):
            raise HsException("JSON message should be a dict: \"%s\"<%s>" %
                              (req_dict, type(req_dict)))

        # ensure 'start_time' and 'stop_time' are present and are DAQTimes
        for tkey in ("start_time", "stop_time"):
            if tkey in req_dict and not isinstance(req_dict[tkey], DAQTime):
                try:
                    req_dict[tkey] = DAQTime(req_dict[tkey], is_ns=False)
                except:
                    raise TypeError("Cannot make %s DAQTime from %s<%s>" %
                                    (tkey, req_dict[tkey],
                                     type(req_dict[tkey])))

        # ensure 'extract' field is present and is a boolean value
        req_dict["extract"] = "extract" in req_dict and \
            req_dict["extract"] is True

        alert_flds = ("request_id", "username", "start_time", "stop_time",
                      "destination_dir", "prefix", "extract")

        return HsUtil.dict_to_object(req_dict, alert_flds, 'WorkerRequest')


if __name__ == '__main__':
    import argparse
    import sys

    def main():
        p = argparse.ArgumentParser()

        add_arguments(p)

        args = p.parse_args()

        usage = False
        if not usage:
            if args.copydir is not None and not os.path.exists(args.copydir):
                print >>sys.stderr, \
                    "Copy directory \"%s\" does not exist" % args.copydir
                usage = True
            elif args.hubroot is not None and not os.path.exists(args.hubroot):
                print >>sys.stderr, \
                    "Hub directory \"%s\" does not exist" % args.hubroot
                usage = True

        worker = Worker("HsWorker", host=args.hostname, is_test=args.is_test)

        # override some defaults (generally only used for debugging)
        if args.copydir is not None:
            worker.TEST_COPY_DIR = args.copydir
        if args.hubroot is not None:
            worker.TEST_HUB_DIR = args.hubroot

        # handler is called when SIGTERM is called (via pkill)
        signal.signal(signal.SIGTERM, worker.handler)

        logfile = worker.init_logging(args.logfile, basename="hsworker",
                                      basehost="testhub")

        logging.info("this Worker runs on: %s", worker.shorthost)

        while True:
            try:
                worker.mainloop(logfile)
            except SystemExit:
                raise
            except KeyboardInterrupt:
                logging.warning("Interruption received, shutting down...")
                raise SystemExit(0)
            except zmq.ZMQError:
                logging.exception("ZMQ error received, shutting down...")
                raise SystemExit(1)
            except:
                logging.exception("Caught exception, continuing")

    main()
