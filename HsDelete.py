#!/usr/bin/env python
"""
Hit Spool Request Deletion
"""

from __future__ import print_function

import getpass
import logging
import os
import sys
import traceback  # used by LogToConsole
import zmq

import HsMessage

from HsBase import HsBase
from HsConstants import ALERT_PORT
from i3helper import read_input


# requests longer than this will provoke a warning message
#  (requests longer than HsBase.MAX_REQUEST_SECONDS will fail)
WARN_SECONDS = 95


def add_arguments(parser):
    "Add all command line arguments to the argument parser"

    example_log_path = os.path.join(HsBase.DEFAULT_LOG_PATH, "hsdelete.log")

    parser.add_argument("-i", "--request-id", dest="request_id",
                        required=True,
                        help="Unique ID used to track this request")
    parser.add_argument("-l", "--logfile", dest="logfile",
                        help="Log file (e.g. %s)" % example_log_path)
    parser.add_argument("-T", "--is-test", dest="is_test",
                        action="store_true", default=False,
                        help="Ignore SPS/SPTS status for tests")
    parser.add_argument("-u", "--username", dest="username",
                        help="Name of user making the requests")


class LogToConsole(object):  # pragma: no cover
    "Simulate a logger"

    @staticmethod
    def info(msg, *args):
        "Print INFO message to stdout"
        print(msg % args)

    @staticmethod
    def error(msg, *args):
        "Print ERROR message to stderr"
        print(msg % args, file=sys.stderr)

    @staticmethod
    def exception(msg, *args):
        "Print ERROR message and exception stacktrace to stderr"
        print(msg % args, file=sys.stderr)
        traceback.print_exc()

    @staticmethod
    def warn(msg, *args):
        "Print WARN message to stderr"
        print(msg % args, file=sys.stderr)


class HsDelete(HsBase):
    '''
    Delete a request which has not yet been started.
    '''

    def __init__(self, is_test=False):
        "Create a request deletion object"
        super(HsDelete, self).__init__(is_test=is_test)

        if self.is_cluster_sps or self.is_cluster_spts:
            sec_bldr = "2ndbuild"
        else:
            sec_bldr = "localhost"

        self.__context = zmq.Context()
        self.__sender = self.create_sender(sec_bldr)
        self.__poller = self.create_poller((self.__sender, ))

    def close_all(self):
        "Close all sockets"
        if self.__sender is not None:
            self.__sender.close()
        self.__context.term()

    def create_sender(self, host):  # pragma: no cover
        "Socket used to send alert message to HsSender"
        sock = self.__context.socket(zmq.REQ)
        sock.identity = "Sender".encode("ascii")
        sock.connect("tcp://%s:%d" % (host, ALERT_PORT))
        return sock

    @classmethod
    def create_poller(cls, sockets):  # pragma: no cover
        "Create ZMQ poller to watch ZMQ sockets"
        poller = zmq.Poller()
        for sock in sockets:
            poller.register(sock, zmq.POLLIN)
        return poller

    @property
    def poller(self):
        "Return ZMQ socket poller"
        return self.__poller

    @property
    def sender(self):
        "Return ZMQ sender socket"
        return self.__sender

    def send_alert(self, request_id=None, username=None,
                   print_to_console=False):
        '''
        Send request to Sender and wait for response
        '''

        if print_to_console:
            print_log = LogToConsole
        else:
            print_log = logging

        if print_to_console:
            answer = read_input("Do you want to proceed? [y/n] : ")
            if not answer.lower().startswith("y"):
                return False

        logging.info("Deleting request %s", request_id)

        try:
            if not HsMessage.send(self.__sender, HsMessage.DELETE, request_id,
                                  username, 0, 0, "/dev/null",
                                  host=self.shorthost):
                print_log.error("Delete message was not sent!")
            else:
                print_log.info("HsDelete sent deletion request")

        except:
            print_log.exception("Failed to send deletion request")

        return True

    def wait_for_response(self, timeout=10, print_to_console=False):
        "Wait for an answer from server"
        count = 0
        while True:
            count += 1
            if count > timeout:
                break

            for sock, event in self.__poller.poll(timeout * 100):
                if event != zmq.POLLIN:
                    logging.error("Unknown event \"%s\"<%s> for %s<%s>",
                                  event, type(event).__name__,
                                  sock, type(sock).__name__)
                    continue

                if sock != self.__sender:
                    if sock is not None:
                        logging.error("Ignoring unknown incoming socket"
                                      " %s<%s>", sock.identity,
                                      type(sock).__name__)
                    continue

                try:
                    msg = sock.recv()
                    if msg is None:
                        continue
                except:
                    logging.exception("Cannot receive message from %s",
                                      sock.identity)
                    continue

                if msg.startswith("DONE"):
                    logging.info("Request sent.")
                    return True
                elif msg.startswith("ERROR"):
                    logging.error("Request ERROR")
                    return False

                logging.info("Unknown response: %s", msg)

            if print_to_console:
                print(".", end="")
                sys.stdout.flush()

        logging.error("No response within %s seconds.\nAbort request.",
                      timeout)

        return False


if __name__ == "__main__":
    import argparse

    def main():
        ''''Process arguments'''
        epilog = "HsDelete deletes a request from the HitSpool system"
        parser = argparse.ArgumentParser(epilog=epilog, add_help=False)
        parser.add_argument("-?", "--help", action="help",
                            help="show this help message and exit")

        add_arguments(parser)

        args = parser.parse_args()

        hsd = HsDelete(is_test=args.is_test)

        hsd.init_logging(args.logfile, level=logging.INFO)

        logging.info("HsDelete running on: %s", hsd.fullhost)

        if args.username is not None:
            username = args.username
        else:
            username = getpass.getuser()

        if not hsd.send_alert(request_id=args.request_id, username=username,
                              print_to_console=True):
            raise SystemExit(1)

        hsd.wait_for_response()

    main()
