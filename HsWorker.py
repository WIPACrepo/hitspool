#!/usr/bin/env python
"""
#Hit Spool Worker to be run on hubs
#author: dheereman i3.hsinterface@gmail.com
#check out the icecube wiki page for instructions:
https://wiki.icecube.wisc.edu/index.php/HitSpool_Interface_Operation_Manual
"""

import datetime
import functools
import logging
import numbers
import os
import signal
import threading
import time
import zmq

import DAQTime
import HsMessage
import HsUtil

from HsBase import HsBase
from HsException import HsException
from HsRSyncFiles import HsRSyncFiles
from HsSender import PingManager


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


class PingWatcher(object):
    PING_SLEEP_SECONDS = PingManager.PING_SLEEP_SECONDS
    PING_TIMEOUT_DEAD = PingManager.PING_TIMEOUT_DEAD

    def __init__(self, host, sock):
        self.__host = host
        self.__sock = sock

        self.__last_ping = datetime.datetime.now()
        self.__running = False

    def __thread_loop(self):
        self.__running = True
        while self.__running:
            try:
                time.sleep(self.PING_SLEEP_SECONDS)

                pdiff = datetime.datetime.now() - self.__last_ping
                if pdiff.days > 0 or pdiff.seconds >= self.PING_TIMEOUT_DEAD:
                    logging.error("No ping from sender in %s -- dying", pdiff)

                    # SIGUSR1 is sent to the main process to kill this program
                    os.kill(os.getpid(), signal.SIGUSR1)
            except:
                logging.exception("PingWatcher problem!")

    def start_thread(self):
        thrd = threading.Thread(name="PingWatcher[%s]" % self.__host,
                                target=self.__thread_loop)
        thrd.setDaemon(True)
        thrd.start()
        return thrd

    def stop_thread(self):
        self.__running = False

    def update(self):
        self.__last_ping = datetime.datetime.now()


class Worker(HsRSyncFiles):
    """
     Requester           HsSender            HsWorker
    -----------       ---------------       -----------
    | sni3daq |       | 2ndbuild    |       | icHub n |
    | REQ     |<----->| PUSH(13live)|<----->| SUB   n |
    -----------       | PULL        |       | PUSH    |
                      ---------------       -----------

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

        self.__ping_watcher = PingWatcher(self.fullhost, self.subscriber)
        self.__ping_watcher.start_thread()

    def __in_hub_list(self, hublist):
        if hublist is None:
            return True

        for hub in hublist.split(","):
            if hub == self.shorthost:
                return True

        return False

    def alert_parser(self, req, logfile, update_status=None,
                     delay_rsync=True):
        """
        Parse the Alert message for starttime, stoptime, sn-alert-time-stamp
        and directory where-to the data has to be copied.
        """

        start_ticks = req.start_ticks
        stop_ticks = req.stop_ticks

        # should we extract only the matching hits to a new file?
        extract_hits = req.extract

        # parse destination string
        try:
            hs_ssh_access, hs_copydir \
                = HsUtil.split_rsync_host_and_path(req.destination_dir)
        except Exception:
            self.send_alert("ERROR: destination parsing failed for"
                            " \"%s\". Abort request." % req.destination_dir)
            logging.error("Destination parsing failed for \"%s\":\n"
                          "Abort request.", req.destination_dir)
            return None

        if hs_ssh_access != "":
            logging.info("Ignoring rsync user/host \"%s\"", hs_ssh_access)

        logging.info("START = %d (%s)", start_ticks,
                     DAQTime.ticks_to_utc(start_ticks))
        logging.info("STOP  = %d (%s)", stop_ticks,
                     DAQTime.ticks_to_utc(stop_ticks))

        # check for data range
        tick_secs = (stop_ticks - start_ticks) / 1E10
        if tick_secs > self.MAX_REQUEST_SECONDS:
            errmsg = "Request for %.2fs exceeds limit of allowed data time" \
                     " range of %.2fs. Abort request..." % \
                     (tick_secs, self.MAX_REQUEST_SECONDS)
            self.send_alert("ERROR: " + errmsg)
            logging.error(errmsg)
            return None

        rsyncdir = self.request_parser(req, start_ticks, stop_ticks,
                                       hs_copydir, extract_hits=extract_hits,
                                       update_status=update_status,
                                       delay_rsync=delay_rsync,
                                       make_remote_dir=False)
        if rsyncdir is None:
            logging.error("Request failed")
            return None

        return rsyncdir

    def close_all(self):
        self.__ping_watcher.stop_thread()

        super(Worker, self).close_all()

    def handler(self, signum, _):
        """
        Handle Unix signals
        """
        logging.warning("Signal Handler called with signal %s", signum)
        logging.warning("Shutting down...\n")

        if self.i3socket is not None:
            if signum != signal.SIGUSR1:
                i3live_dict = {}
                i3live_dict["service"] = "HSiface"
                i3live_dict["varname"] = "HsWorker@%s" % self.shorthost
                i3live_dict["value"] = "INFO: SHUT DOWN due to signal %s" % \
                                       (signum, )
                self.i3socket.send_json(i3live_dict)

            i3live_dict = {}
            i3live_dict["service"] = "HSiface"
            i3live_dict["varname"] = "HsWorker@%s" % self.shorthost
            i3live_dict["value"] = "STOPPED"
            self.i3socket.send_json(i3live_dict)

        self.close_all()

        raise SystemExit(0)

    def mainloop(self, logfile, fail_sleep=1.5):
        if self.subscriber is None:
            raise Exception("Subscriber has not been initialized")

        logging.debug("ready for new alert...")

        try:
            req = self.receive_request(self.subscriber)
        except KeyboardInterrupt:
            raise
        except zmq.ZMQError:
            raise
        except:
            logging.exception("Cannot read request")
            return

        if req is None:
            return

        logging.info("HsWorker received request:\n"
                     "%s\nfrom Publisher", str(req))

        update_status = functools.partial(HsMessage.send_worker_status,
                                          self.sender, req, self.shorthost)

        update_status(req.copy_dir, req.destination_dir,
                      HsMessage.STARTED)

        if not self.__in_hub_list(req.hubs):
            update_status(req.copy_dir, req.destination_dir,
                          HsMessage.IGNORED)
            return

        # extract actual directory from rsync path
        try:
            _, destdir = HsUtil.split_rsync_host_and_path(req.destination_dir)
        except:
            logging.exception("Illegal destination directory \"%s\"<%s>",
                              req.destination_dir, type(req.destination_dir))

            # give other hubs time to start before sending failure message
            time.sleep(fail_sleep)

            update_status(req.copy_dir, req.destination_dir,
                          HsMessage.FAILED)
            return

        try:
            rsyncdir = self.alert_parser(req, logfile,
                                         update_status=update_status)
        except:
            logging.exception("Cannot process request \"%s\"", req)
            rsyncdir = None

        if rsyncdir is not None:
            msgtype = HsMessage.DONE
        else:
            msgtype = HsMessage.FAILED

        update_status(rsyncdir, destdir, msgtype)

    def receive_request(self, sock):
        req_dict = sock.recv_json()
        if req_dict is None:
            return None

        if not isinstance(req_dict, dict):
            raise HsException("JSON message should be a dict: \"%s\"<%s>" %
                              (req_dict, type(req_dict)))

        if "ping" in req_dict:
            self.__ping_watcher.update()
            self.sender.send_json({"pingback": self.fullhost})
            return

        # ensure 'start_time' and 'stop_time' are present and numbers
        for tkey in ("start_ticks", "stop_ticks"):
            if tkey not in req_dict:
                raise HsException("Request does not contain '%s'" % (tkey, ))
            elif not isinstance(req_dict[tkey], numbers.Number):
                raise HsException("Request '%s' should be a number, not %s" %
                                  (tkey, type(req_dict[tkey]).__name__))

        # ensure 'extract' field is present and is a boolean value
        req_dict["extract"] = "extract" in req_dict and \
            req_dict["extract"] is True

        alert_flds = ("request_id", "username", "start_ticks", "stop_ticks",
                      "destination_dir", "prefix", "extract")

        return HsUtil.dict_to_object(req_dict, alert_flds, 'WorkerRequest')


if __name__ == '__main__':
    import argparse
    import sys

    def main():
        parser = argparse.ArgumentParser()

        add_arguments(parser)

        args = parser.parse_args()

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
            worker.TEST_COPY_PATH = args.copydir
        if args.hubroot is not None:
            worker.TEST_HUB_DIR = args.hubroot

        # shut down cleanly when a signal is received (via pkill)
        signal.signal(signal.SIGTERM, worker.handler)
        signal.signal(signal.SIGUSR1, worker.handler)

        logfile = worker.init_logging(args.logfile, basename="hsworker",
                                      basehost=worker.shorthost)

        logging.info("this Worker runs on: %s", worker.shorthost)

        while True:
            try:
                worker.mainloop(logfile)
            except SystemExit:
                raise
            except KeyboardInterrupt:
                logging.warning("Interruption received, shutting down...")
                raise SystemExit(0)
            except zmq.ZMQError, zex:
                if str(zex).find("Socket operation on non-socket") < 0:
                    logging.exception("ZMQ error received, shutting down...")
                raise SystemExit(1)
            except:
                logging.exception("Caught exception, continuing")

    main()
