#!/usr/bin/python
"""
"sndaq"           "HsPublisher"      "HsWorker"    "HsI3liveSink"
-----------        -----------
| sni3daq |        | expcont |         -----------        ------------
| REQUEST | <----->| REPLY   |         | IcHub n |        | expcont  |
-----------        | PUB     | ------> | SUB    n|        | PULL     |
                   ----------          |PUSH     | ---->  |          |
                                        ---------          -----------
This is the Publisher for the HS Interface. 
It contains a REPLY Socket  for the request coming from sico_testers REQUEST.
Inspired by 0mq guide. 

"""

import sys
import getopt
import zmq #@UnresolvedImport
import time
import json

context = zmq.Context()


def usage():
    print >>sys.stderr, """
    usage :: HS_Publisher.py [options]
            -n         | number of Workers running
    """
    sys.exit(1)
        
##take arguments from command line and check for correct input
opts, args = getopt.getopt(sys.argv[1:], 'hn:', ['help','workers=',])
for o, a in opts:
    if o == '-n':
        workers = int(a)
        print "Number of connected workers: ", workers 
        #print sys.argv[1]
    elif o == '-h' or o == '--help':
        usage()       
if len(sys.argv) < 2 :
    print usage()
    
# build 0MQ sockets    
context = zmq.Context()
# Socket to receive alert message
socket = context.socket(zmq.REP)
socket.bind("tcp://10.2.2.12:55557")   #connection = tcp, host = bond0 on spts-expcont ip , port 
print "bind REP socket for receiving alert messages to port 55557"

# Socket to talk to Workers
publisher = context.socket(zmq.PUB)
publisher.setsockopt(zmq.HWM, 50) #keep up to 50 alert messages in memory,  each alert has 205 bytes
publisher.bind("tcp://*:55561")
print "bind PUB socket to port 55561"

# Socket to receive sync signals form Workers
syncservice = context.socket(zmq.PULL)
syncservice.bind("tcp://*:55562")
print "bind sync REP socket"

     
class MyForwarder(object):
    """
    The Forwarder creates PUB sockets and sends the alert 
    message to the Worker hubs that are connected after geeting 
    a sync msg via REQ-REP pattern.
    """
    def sync(self):
        """
        REQ-REP synchronization pattern toensure that the workers are all up and running before the Publisher sends out data
        """
        
        #Get synchronization signal from Workers
        subscribers = 0
        print "number of workers: " , workers
        while subscribers < workers:
            # wait for synchronization request
            msg = syncservice.recv()
            print "Publisher received synchronzation request from Worker:\n %s" % msg
            print "OK"
            subscribers +=1
            print "+1 subscriber"
        
        
    def publish(self, data):
        """
        Method for publishing the alert message to the subscribed Workers
        """
            
        # broadcast the alert-message:
        try:
            print "message to forward:\n", data
            data_json = json.dumps(data,separators=(',', ': '))
            publisher.send_json(data_json) 
            print "Publisher published:\n %s \nfor  %i workers " % (data, workers)

        except KeyboardInterrupt:
            print " Interruption received, proceeding..."
#            socket.close()
#            context.term()  
            sys.exit()    
                
class Receiver(object):
    '''
    Class to handle initial request message from sndaq or any other process. 
    '''
    def reply_request(self):             
        
        print "make sure the Workers are up and running: "
        forwarder = MyForwarder()
        forwarder.sync()
        # We want to have a stable connection Forever to the client -> while True loop
        while True:
            #Wait for next request from client and make the alert global accessible:
            global alert
            try:
                #wait for alert message:
                alert = socket.recv()
                print "Received request:\n", alert

                print "Publish the alert..."
                forwarder.publish(alert)
                socket.send ("DONE\0")
                print "Forwarder sended back to sndaq's sico_tester: DONE"

            except KeyboardInterrupt:
                print "  Interrupt received, proceeding..."
                sys.exit()

request = Receiver()
request.reply_request()

