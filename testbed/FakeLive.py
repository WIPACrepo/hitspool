#!/usr/bin/env python

import zmq
import time

context = zmq.Context()
socket = context.socket(zmq.PULL)
socket.bind("tcp://127.0.0.1:6668")

while True:
  message = socket.recv_json()
  print "recv:", str(message)

time.sleep(10)   # naive wait for tasks to drain
