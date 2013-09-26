#!/usr/bin/python
#
#
#Hit Spool Grabber to be run on access
#author: dheereman
#

from datetime import datetime, timedelta
import sys
import getopt
import zmq

# build 0MQ sockets    
context = zmq.Context()
# Socket to send alert message
grabber = context.socket(zmq.REQ)
grabber.connect("tcp://expcont:55557")   #connection = tcp, host = localhost ip , port 
print "connected to HsPublisher on expcont at port 55557"
# Socket for I3Live on expcont
i3socket = context.socket(zmq.PUSH) # former ZMQ_DOWNSTREAM is depreciated in recent releases, use PUSH instead
i3socket.connect("tcp://expcont:6668") 
print "connected to i3live socket on expcont at port 6668"    

#class MyGrabber(object):
def send_alert():
    print "connect PUSH socket for sending alert messages to port 55557 on expcont"
    daqyearstart = datetime.strptime(str(alert_begin.year)+"-01-01", "%Y-%m-%d")
    alert_begin_delta = alert_begin - daqyearstart
    print "alert_begin_delta: " , alert_begin_delta
    alert_end_delta = alert_end - daqyearstart
    print "alert_end_delta: " , alert_end_delta
    print alert_begin_delta.microseconds
    print alert_end_delta.microseconds
    print alert_begin_delta.seconds
    

    alert_start = (alert_begin_delta.microseconds + (alert_begin_delta.seconds + alert_begin_delta.days * 24 * 3600) * 10**6) * 10**3
    alert_stop = (alert_end_delta.microseconds + (alert_end_delta.seconds + alert_end_delta.days * 24 * 3600) * 10**6) * 10**3        
    alert_msg = "{\"start\": " + str(alert_start) + " , \"stop\": " + str(alert_stop) + " , \"copy\": \"" + copydir + "\"}"
    print "ALERT BEGIN UTC time: ", alert_begin
    print "ALERT BEGIN SNDAQ time: ", alert_start    
    print "ALERT END UTC time: ", alert_end
    print "ALERT END SNDAQ time: ", alert_stop
    print "alert_dict to string looks like: " , alert_msg
    grabber.send(alert_msg)
    print "HsGrabber sent his Request"
    try:
        pubanswer = grabber.recv()
        print "succesfully to HsPublisher: ", pubanswer
    except Exception, err:
        print "without success to HsPublisher: " , err
    i3socket.send_json({"service": "HSiface", 
                    "varname": "HsGrabber", 
                    "value": "pushed request"})
    sys.exit()
    
    
    


if __name__=="__main__":
    '''
    For grabbing hs data from hubs independent (without sndaq providing alert).
    HsGrabber sends json to HsPublisher to grab hs data from hubs.
    Uses the Hs Interface infrastructure.
    
    Needs HsPublisher on expcont and HsWorker on hub running!!
    '''
    def usage():
        print >>sys.stderr, """
        usage :: HsGrabber.py [options]
            -b         | begin of data "YYYY-MM-DD HH:mm:ss.[subsec]"    subsec = 6digits
            -e         | end of data "YYYY-MM-DD HH:mm:ss.[subsec]"      subsec = 6digits
            -c         | copydir e.g. "pdaq@2ndbuild:/mnt/data/pdaqlocal/HsDataCopy/"
            
            HsGrabber reads UTC timestamps and converts them to sndaq time units [nanoseconds from beginning ofthe year]. This is 
            in analogy to the message format that sndaq, as a first user of the hitspool interface, is sending.
        """
        sys.exit(1)
        
    ##take arguments from command line and check for correct input
    opts, args = getopt.getopt(sys.argv[1:], 'hb:e:c:', ['help','alert='])
    for o, a in opts:
        if o == '-b':
            try:
                alert_begin = datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S.%f")
            except ValueError, e:
                print "Problem with the time-stamp format: ", e
                try:
                    print "Try matching format without microsecond precision:"
                    alert_begin = datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S")
                    print "matched"
                except ValueError,e:
                    print  "Problem with the time-stamp format: ", e
            else:
                pass
        if o == '-e':
            alert_end = datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S.%f")
        if o == '-c':
            copydir = str(a)
        elif o == '-h' or o == '--help':
            usage()       
    if len(sys.argv) < 5 :
        print usage()
    
    datarange = alert_end - alert_begin
    print "requesting " , datarange , "minutes of HS data"
    
    maxrange = timedelta(0,100) # 100 seconds
    
    print "maxrange " , maxrange, " minutes"
    
    if datarange >  maxrange:
        answer = raw_input("Warning: You are requesting more HS data than usual. Sure you want to proceed? [y/n] : ")
        if answer in ["Yes", "yes", "y", "Y"]:
            send_alert()
        else:
            print "HS data request stopped. Try a smaller time window."
            pass
        
        