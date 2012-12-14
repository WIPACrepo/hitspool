#!/usr/bin/python

"""
"sico_tester"        "HsPublisher"      "HsWorker"        "HsI3liveSink"
-----------        -----------
| sni3daq |        | expcont |         -----------        ------------
| REQUEST | <----->| REPLY   |         | IcHub n |        | expcont  |
-----------        | PUSH    | ------> | PULL   n|        | PULL     |
                   ----------          |PUSH     | ---->  |          |
                                        ---------          -----------

 Task sink
 Binds PULL socket to tcp://localhost:55560
 Collects results from workers via that socket and sends it to I3Live                                                                 
"""

# author: dheereman guided by JohnJacobsen


import sys
import zmq #@UnresolvedImport

context = zmq.Context()

# Socket to receive messages on from Worker
receiver = context.socket(zmq.PULL)
receiver.bind("tcp://*:55560")
print "bind Sink to port 55560 on spts-expcont"

# Socket for I3Live on expcont
socket = context.socket(zmq.PUSH) # former ZMQ_DOWNSTREAM is depreciated alias 
socket.connect("tcp://10.2.2.12:6668") 
print "connected to i3live socket on port 6668"

while True:
    try:
        message = receiver.recv() 
        print "Sink received from Worker:\n",message
        messagelist = eval(message)
        
        #i3live_json = 
        socket.send_json({"service": "HSiface", "varname": messagelist[0], "value": messagelist[1]})        
        
        print "succesfully sent to I3live the follwing json: " , 

    except KeyboardInterrupt:
        print " Interruption received, proceeding..."
#        socket.close()
#        context.term()  
        sys.exit()