#!/usr/bin/env python

import getpass
import unittest

import HsMessage

from HsPrefix import HsPrefix


class MockSocket(object):
    def __init__(self):
        self.__msg = None

    def recv_json(self):
        return self.__msg

    def send_json(self, msg):
        if not isinstance(msg, dict):
            raise Exception("Message \"%s\" is %s, not dict" %
                            (msg, type(msg)))

        self.__msg = msg
        return True


class HsMessageTest(unittest.TestCase):
    def __check_request(self, req, msgtype, req_id, username, start_ticks,
                        stop_ticks, dest_dir, prefix, copy_dir, extract, hubs,
                        host, version):

        self.assertEquals(req.start_ticks, start_ticks,
                          "Expected start ticks %s<%s>, not %s<%s>" %
                          (start_ticks, type(start_ticks), req.start_ticks,
                           type(req.start_ticks)))
        self.assertEquals(req.stop_ticks, stop_ticks,
                          "Expected stop ticks %s<%s>, not %s<%s>" %
                          (stop_ticks, type(stop_ticks), req.stop_ticks,
                           type(req.stop_ticks)))
        self.assertEquals(req.destination_dir, dest_dir,
                          "Expected destination directory %s, not %s" %
                          (dest_dir, req.destination_dir))
        self.assertEquals(req.username, username,
                          "Expected username %s, not %s" %
                          (username, req.username))
        self.assertEquals(req.host, host,
                          "Expected host %s, not %s" % (host, req.host))
        self.assertEquals(req.copy_dir, copy_dir,
                          "Expected copy directory %s, not %s" %
                          (copy_dir, req.copy_dir))
        self.assertEquals(req.extract, extract,
                          "Expected 'extract' %s, not %s" %
                          (extract, req.extract))
        self.assertEquals(req.prefix, prefix,
                          "Expected prefix %s, not %s" %
                          (prefix, req.prefix))
        self.assertEquals(req.version, version,
                          "Expected version %s, not %s" %
                          (version, req.version))
        self.assertEquals(req.request_id, req_id,
                          "Expected request ID %s, not %s" %
                          (req_id, req.request_id))
        self.assertEquals(req.hubs, hubs,
                          "Expected hubs %s, not %s" %
                          (hubs, req.hubs))

    def test_send_recv(self):
        sock = MockSocket()

        start_ticks = 1234567890L
        stop_ticks = start_ticks + long(1E8)
        dest_dir = "/foo/dest"

        username = getpass.getuser()

        HsMessage.send_initial(sock, None, start_ticks, stop_ticks, dest_dir)

        req = HsMessage.receive(sock)

        self.assertTrue(req.request_id is not None,
                        "Request ID should not be None")
        self.__check_request(req, HsMessage.INITIAL, req.request_id,
                             username, start_ticks, stop_ticks,
                             dest_dir, HsPrefix.ANON, None, False, None, None,
                             HsMessage.CURRENT_VERSION)

        new_host = "xyz"
        new_copydir = "/copy/dir"
        new_destdir = "/dest/dir"
        new_msgtype = HsMessage.DONE

        HsMessage.send_worker_status(sock, req, new_host, new_copydir,
                                     new_destdir, new_msgtype)

        nreq = HsMessage.receive(sock)

        self.__check_request(nreq, new_msgtype, req.request_id,
                             req.username, req.start_ticks, req.stop_ticks,
                             new_destdir, req.prefix, new_copydir,
                             req.extract, req.hubs, new_host, req.version)


if __name__ == '__main__':
    unittest.main()
