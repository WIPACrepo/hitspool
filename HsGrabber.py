#!/usr/bin/python
#
#
#Hit Spool Grabber to be run on access
#author: dheereman
#
'''
For grabbing hs data from hubs independent (without sndaq providing alert).
HsGrabber sends json to HsPublisher to grab hs data from hubs.
Uses the Hs Interface infrastructure.

Needs HsPublisher on expcont and HsWorker on hub running!!
'''
from datetime import datetime, timedelta
import sys
import getopt
import zmq

def usage():
    print >>sys.stderr, """
    usage :: HsGrabber.py [options]
            -b         | begin of data "YYYY-MM-DD UTC HH:mm:ss"
            -e         | end of data "YYYY-MM-DD UTC HH:mm:ss"
            -c         | copydir e.g. "pdaq@2ndbuild:/mnt/data/pdaqlocal/HsDataCopy/"
    """
    sys.exit(1)
        
##take arguments from command line and check for correct input
opts, args = getopt.getopt(sys.argv[1:], 'hb:e:c:', ['help','alert='])
for o, a in opts:
    if o == '-b':
        alert_begin = datetime.strptime(str(a), "%Y-%m-%d UTC %H:%M:%S")
    if o == '-e':
        alert_end = datetime.strptime(str(a), "%Y-%m-%d UTC %H:%M:%S")
    if o == '-c':
        copydir = str(a)
    elif o == '-h' or o == '--help':
        usage()       
if len(sys.argv) < 5 :
    print usage()
# build 0MQ sockets    
context = zmq.Context()
# Socket to receive alert message
grabber = context.socket(zmq.REQ)
grabber.connect("tcp://10.1.2.20:55557")   #connection = tcp, host = bond0 on sps-expcont ip , port 

#class MyGrabber(object):
def send_alert():
    print "connect REQ socket for  sendingalert messages to port 55557 on expcont"
    daqyearstart = datetime.strptime(str(alert_begin.year)+"-01-01", "%Y-%m-%d")
    alert_begin_delta = alert_begin - daqyearstart
    alert_end_delta = alert_end - daqyearstart
    alert_start = int(((alert_begin_delta.microseconds + (alert_begin_delta.seconds + alert_begin_delta.days * 24 * 3600) * 10**6) / 10**6)*10**10)
    alert_stop = int(((alert_end_delta.microseconds + (alert_end_delta.seconds + alert_end_delta.days * 24 * 3600) * 10**6) / 10**6)*10**10)        
    alert_msg = "{\"start\": " + str(alert_start) + " , \"stop\": " + str(alert_stop) + " , \"copy\": \"" + copydir + "\"}"
    print "alert_dict to string looks like: " , alert_msg
    grabber.send(alert_msg)
    print "HsGrabber sent his Request"
    answer = grabber.recv()
    print "HsGrabber got the answer: " , answer , "from the HsPublisher"
    print "HsWorkers should now be processing your request..."


if __name__=="__main__":
    send_alert()
    
    
#x = MyGrabber()
#x.send_alert()


