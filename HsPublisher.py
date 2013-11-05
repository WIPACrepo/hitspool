#!/usr/bin/python


import sys
import zmq
import subprocess
import logging
import re
import signal
import time
from datetime import datetime



# --- Clean exit when program is terminated from outside (via pkill) ---#
def handler(signum, frame):
    logging.warning("Signal Handler called with signal " + str( signum))
    logging.warning( "Shutting down...\n")
    i3live_dict = {}
    i3live_dict["service"] = "HSiface"
    i3live_dict["varname"] = "HsPublisher"
    i3live_dict["value"] = "INFO: SHUT DOWN called by external signal." 
    i3socket.send_json(i3live_dict)
    i3live_dict2 = {}
    i3live_dict2["service"] = "HSiface"
    i3live_dict2["varname"] = "HsPublisher"
    i3live_dict2["value"] = "STOPPED" 
    i3socket.send_json(i3live_dict2)
    socket.close()
    publisher.close()
    i3socket.close()
    context.term()
    sys.exit()

signal.signal(signal.SIGTERM, handler)    #handler is called when SIGTERM is called (via pkill)

#class MyPublisher(object):
#    """
#    Creates PUB sockets and sends the alert 
#    message to the Worker hubs that are connected.
#    """
#        
#    def publish(self, data):
#        """
#        Publishing the alert message to the subscribed Workers
#        """
#            
#        # broadcast the alert-message:
#        try:
#            publisher.send("["+data+"]")
#            logging.info("Publisher published: " + str(data))
#
#        except KeyboardInterrupt:
#            logging.warning("KeyboardInterruption received, shut down...")  
#            sys.exit()    
            
                
class Receiver(object):
    '''
    Handle incoming request message from sndaq or any other process. 
    '''
    def reply_request(self):             
#        forwarder = MyPublisher()
        # We want to have a stable connection FOREVER to the client -> while True loop
        while True:
            #Wait for next request from client and make the alert global accessible:
            global alert
            try:
                #receive for alert message:
                alert = socket.recv()
                logging.info("received request:\n"+ str(alert))
                
                #send JSON for moni Live page:
                i3socket.send_json({"service": "HSiface", 
                                    "varname": "HsPublisher", 
                                    "value": "Received request msg"})
 
                #publish the request for the HsWorkers:
                #forwarder.publish(alert)
                publisher.send("["+alert+"]")
                logging.info("Publisher published: " + str(alert))
                
                i3socket.send_json({"service": "HSiface", 
                                    "varname": "HsPublisher", 
                                    "value": "published alert"})
                # send Live alert JSON for email notification:
#                i3socket.send_json({"service": "HSiface", 
#                                    "varname": "alert", 
#                                    "short_subject": "true",
#                                    "quiet": "true",
#                                    "value": {"condition": "DATA REQUEST HsInterface Alert: received and published to HsWorkers", 
#                                              "prio": 1,
#                                              "notify": "i3.hsinterface@gmail.com",
#                                              "vars": alert,}})
                
                alertmsg = "DATA REQUEST HsInterface Alert: HsPublisher received and published to HsWorkers at " + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n the following message:\n" + str(alert)
#                
                alertjson = {"service" :   "HSiface",
                                  "varname" :   "alert",
#                                  "quiet"   :   "true",
                                  "prio"    :   1,
                                  "t"    :   str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                                  "value"   :   {"condition"    : "DATA REQUEST HsInterface Alert: ",
                                                 "desc"         : "HsInterface Data Reuqest",
                                                 "notifies"     : [{"receiver"      : "i3.hsinterface@gmail.com",
                                                                    "notifies_txt"  : alertmsg,
                                                                    "notifies_header" : "DATA REQUEST HsInterface Alert: "},
                                                                   {"receiver"      : "icecube-sn-dev@lists.uni-mainz.de",
                                                                    "notifies_txt"  : alertmsg,
                                                                    "notifies_header" : "DATA REQUEST HsInterface Alert: "}],
                                                 "short_subject": "true"}}

                i3socket.send_json(alertjson)

                #reply to requester:
                answer = socket.send("DONE\0") # added \0 to fit C/C++ zmq message termination
#                print answer
                if answer is None:
                    logging.info("send confirmation back to requester: DONE")
                else:
                    logging.error("failed sending confirmation to requester")

            except KeyboardInterrupt:
                # catch termintation signals: can be Ctrl+C (if started loacally)
                # or another termination message from fabfile
                logging.warning( "KeyboardInterruption received, shutting down...")
                sys.exit()

if __name__=='__main__':
    
    """
    "sndaq"           "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        ------------
    | REQUEST | <----->| REPLY   |         | IcHub n |        | 2ndbuild |
    -----------        | PUB     | ------> | SUB    n|        | PULL     |
                       ----------          |PUSH     | ---->  |          |
                                            ---------          -----------
    It receives a request from sndaq and sends log messages to I3Live.
    It contains a REPLY Socket  for the request coming from sndaq REQUEST.
    """    
    
    p = subprocess.Popen(["hostname"], stdout = subprocess.PIPE)
    out, err = p.communicate()
    src_mchn = out.rstrip()
    
    if "sps" in src_mchn:
        src_mchn_short = re.sub(".icecube.southpole.usap.gov", "", src_mchn)
        cluster = "SPS"
    elif "spts" in src_mchn:
        src_mchn_short = re.sub(".icecube.wisc.edu", "", src_mchn)
        cluster = "SPTS"
    else:
        src_mchn_short = src_mchn
        cluster = "localhost"
        
    if cluster == "localhost":
        logfile = "/home/david/TESTCLUSTER/expcont/logs/hspublisher_" + src_mchn_short + ".log"    
    else:
        logfile = "/mnt/data/pdaqlocal/HsInterface/logs/hspublisher_" + src_mchn_short + ".log"
    
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
                        level=logging.INFO, stream=sys.stdout, 
                        datefmt= '%Y-%m-%d %H:%M:%S', 
                        filename=logfile)    
        
    logging.info("HsPublisher started on " + str(src_mchn_short))    
    # build 0MQ sockets    
    context = zmq.Context()
    # Socket to receive alert message
    socket = context.socket(zmq.REP)
    # Socket to talk to Workers
    publisher = context.socket(zmq.PUB)
    # Socket to receive sync signals from Workers
#    syncservice = context.socket(zmq.PULL)
    # Socket for I3Live on expcont
    i3socket = context.socket(zmq.PUSH) # former ZMQ_DOWNSTREAM is depreciated in recent releases, use PUSH instead

    if cluster == "localhost":
        socket.bind("tcp://*:55557")   
        logging.info("bind REP socket for receiving alert messages to port 55557")
        publisher.setsockopt(zmq.HWM, 50) #keep up to 50 alert messages in memory,  each alert has 205 bytes
        publisher.bind("tcp://*:55561")
        logging.info("bind PUB socket to port 55561")
#        syncservice.bind("tcp://*:55562")
#        logging.info("bind PULL socket to port 55562")
        i3socket.connect("tcp://localhost:6668") 
        logging.info("connected to i3live socket on port 6668")    
    
    else:
        socket.bind("tcp://*:55557")   
        logging.info("bind REP socket for receiving alert messages to port 55557")
        publisher.setsockopt(zmq.HWM, 50) #keep up to 50 alert messages in memory,  each alert has 205 bytes
        publisher.bind("tcp://*:55561")
        logging.info("bind PUB socket to port 55561")
#        syncservice.bind("tcp://*:55562")
#        logging.info("bind PULL socket to port 55562")
        i3socket.connect("tcp://expcont:6668") 
        logging.info("connect PUSH socket to i3live on port 6668") 

    request = Receiver()
    request.reply_request()

