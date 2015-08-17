#!/usr/bin/env python


import ast
import logging
import os
import signal
import zmq

from datetime import datetime, timedelta

import HsBase
import HsConstants


class Receiver(HsBase.HsBase):
    """
    "sndaq"           "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        ------------
    | REQUEST | <----->| REPLY   |         | IcHub n |        | 2ndbuild |
    -----------        | PUB     | ------> | SUB    n|        | PULL     |
                       ----------          |PUSH     | ---->  |          |
                                            ---------          -----------
    Handle incoming request message from sndaq or any other process.
    Monitors ALERT Socket for the request coming from sndaq.
    Sends log messages to I3Live.
    """

    def __init__(self, is_test=False):
        super(Receiver, self).__init__(is_test=is_test)

        if self.is_cluster_local():
            expcont = "localhost"
        else:
            expcont = "expcont"

        self.__context = zmq.Context()
        self.__socket = self.create_alert_socket()
        self.__publisher = self.create_publisher()
        self.__i3socket = self.create_i3socket(expcont)

    def __build_json(self, alert, sn_start_utc, sn_stop_utc):
        alertmsg = "%s\nstart in UTC : %s\nstop  in UTC : %s\n" \
                   "(no possible leapseconds applied)" % \
                   (alert, sn_start_utc, sn_stop_utc)

        notifies = []
        for email in (HsConstants.ALERT_EMAIL_DEV, HsConstants.ALERT_EMAIL_SN):
            ndict = {
                "receiver": email,
                "notifies_txt"   : alertmsg,
                "notifies_header": "DATA REQUEST HsInterface Alert: %s" % \
                                   self.cluster(),
                }
            notifies.append(ndict)

        value = {"condition"    : "DATA REQUEST HsInterface Alert: %s" % \
                                  self.cluster(),
                 "desc"         : "HsInterface Data Request",
                 "notifies"     : notifies,
                 "short_subject": "true",
                 "quiet"        : "true",
                }

        return {"service":   "HSiface",
                "varname":   "alert",
                "prio"   :   1,
                "t"      :   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "value"  :   value,
               }

    def alert_socket(self):
        return self.__socket

    def close_all(self):
        self.__socket.close()
        self.__publisher.close()
        self.__i3socket.close()
        self.__context.term()

    def create_alert_socket(self):
        # Socket to receive alert message
        sock = self.__context.socket(zmq.REP)
        sock.bind("tcp://*:%d" % ALERT_PORT)
        logging.info("bind REP socket for receiving alert messages to port %d",
                     ALERT_PORT)
        return sock

    def create_i3socket(self, host):
        # Socket for I3Live on expcont
        sock = self.__context.socket(zmq.PUSH)
        sock.connect("tcp://%s:%d" % (host, I3LIVE_PORT))
        logging.info("connect PUSH socket to i3live on %s port %d", host,
                     I3LIVE_PORT)
        return sock

    def create_publisher(self):
        # Socket to talk to Workers
        sock = self.__context.socket(zmq.PUB)
        sock.bind("tcp://*:%d" % PUBLISHER_PORT)
        logging.info("bind PUB socket to port %d", PUBLISHER_PORT)
        return sock

    def handler(self, signum, frame):
        """Clean exit when program is terminated from outside (via pkill)"""
        logging.warning("Signal Handler called with signal %s", signum)
        logging.warning("Shutting down...\n")

        i3live_dict = {}
        i3live_dict["service"] = "HSiface"
        i3live_dict["varname"] = "HsPublisher"
        i3live_dict["value"] = "INFO: SHUT DOWN called by external signal."
        self.__i3socket.send_json(i3live_dict)

        i3live_dict = {}
        i3live_dict["service"] = "HSiface"
        i3live_dict["varname"] = "HsPublisher"
        i3live_dict["value"] = "STOPPED"
        self.__i3socket.send_json(i3live_dict)

        self.close_all()

        raise SystemExit(0)

    def i3socket(self):
        return self.__i3socket

    def publisher(self):
        return self.__publisher

    def reply_request(self):
        # We want to have a stable connection FOREVER to the client
        while True:
            # Wait for next request from client
            try:
                # receive alert message:
                alert = self.__socket.recv()
                logging.info("received request:\n%s", alert)


                # alert is NOT a real JSON or dict here
                # because it comes from C code it is only a string
                try:
                    alertdict = ast.literal_eval(str(alert))
                    alert_start = int(alertdict['start'])
                    alert_stop = int(alertdict['stop'])
                except (ValueError, SyntaxError):
                    sn_start_utc = "TBD"
                    sn_stop_utc = "TBD"
                else:
                    jan1 = datetime(datetime.utcnow().year, 1, 1)

                    # sndaq time units are nanoseconds
                    sn_start_utc = jan1 + timedelta(seconds=alert_start*1.0E-9)
                    sn_stop_utc = jan1 + timedelta(seconds=alert_stop*1.0E-9)

                #send JSON for moni Live page:
                self.__i3socket.send_json({"service": "HSiface",
                                           "varname": "HsPublisher",
                                           "value": "Received data request" \
                                                    " for [%s , %s] " % \
                                                    (sn_start_utc, sn_stop_utc),
                                          })

                #publish the request for the HsWorkers:
                #forwarder.publish(alert)
                self.__publisher.send("["+alert+"]")
                logging.info("Publisher published: %s", alert)

                self.__i3socket.send_json({"service": "HSiface",
                                           "varname": "HsPublisher",
                                           "value": "Published request to"
                                                    " HsWorkers"})
                # send Live alert JSON for email notification:

                alertjson = self.__build_json(alert, sn_start_utc,
                                              sn_stop_utc)

                self.__i3socket.send_json(alertjson)

                #reply to requester:
                # added \0 to fit C/C++ zmq message termination
                answer = self.__socket.send("DONE\0")
                if answer is None:
                    logging.info("send confirmation back to requester: DONE")
                else:
                    logging.error("failed sending confirmation to requester")

            except KeyboardInterrupt:
                # catch termintation signals: can be Ctrl+C (if started loacally)
                # or another termination message from fabfile
                logging.warning("KeyboardInterruption received, shutting down...")
                break
            except zmq.ZMQError, zex:
                logging.warning("Quitting: %s", zex)
                break

if __name__ == '__main__':
    import getopt
    import sys

    from HsConstants import ALERT_PORT, I3LIVE_PORT, PUBLISHER_PORT


    def process_args():
        logfile = None

        usage = False
        try:
            opts, _ = getopt.getopt(sys.argv[1:], 'hl:', ['help', 'logfile'])
        except getopt.GetoptError, err:
            print >>sys.stderr, str(err)
            opts = []
            usage = True

        for opt, arg in opts:
            if opt == '-l':
                logfile = str(arg)
            elif opt == '-h' or opt == '--help':
                usage = True

        if usage:
            print >>sys.stderr, "usage :: HsPublisher.py [-l logfile]"
            raise SystemExit(1)

        return logfile

    def main():
        logfile = process_args()

        receiver = Receiver()

        #handler is called when SIGTERM is called (via pkill)
        signal.signal(signal.SIGTERM, receiver.handler)

        if logfile is None:
            if receiver.is_cluster_local():
                logdir = "/home/david/TESTCLUSTER/expcont/logs"
            else:
                logdir = "/mnt/data/pdaqlocal/HsInterface/logs"
            logfile = os.path.join(logdir, "hspublisher_%s.log" %
                                   receiver.shorthost())

        receiver.init_logging(logfile)

        logging.info("HsPublisher started on %s", receiver.shorthost())

        receiver.reply_request()

    main()
