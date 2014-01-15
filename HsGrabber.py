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
import subprocess
import re

  

#class MyGrabber(object):
def send_alert(timeout, alert_start_sn, alert_stop_sn, alert_begin_utc, alert_end_utc):
    should_continue = False

    if (alert_start_sn == 0) and (alert_stop_sn == 0) :
        daqyearstart = datetime.strptime(str(alert_begin_utc.year)+"-01-01", "%Y-%m-%d")
        alert_begin_utc_delta = alert_begin_utc - daqyearstart
        alert_end_utc_delta = alert_end_utc - daqyearstart
        alert_start_sn = (alert_begin_utc_delta.microseconds + (alert_begin_utc_delta.seconds + alert_begin_utc_delta.days * 24 * 3600) * 10**6) * 10**3
        alert_stop_sn = (alert_end_utc_delta.microseconds + (alert_end_utc_delta.seconds + alert_end_utc_delta.days * 24 * 3600) * 10**6) * 10**3        
    
    elif (alert_begin_utc == 0) and (alert_begin_utc == 0):        
        daqyear = int(datetime.utcnow().year)
        alert_begin_utc = datetime.strptime(str(datetime(daqyear, 1, 1) + timedelta(seconds = alert_start_sn*1.0E-9)), "%Y-%m-%d %H:%M:%S.%f")
        alert_end_utc = datetime.strptime(str(datetime(daqyear, 1, 1) + timedelta(seconds = alert_stop_sn*1.0E-9)), "%Y-%m-%d %H:%M:%S.%f")
        

    print "ALERT BEGIN UTC time: ", alert_begin_utc
    print "ALERT END UTC time: ", alert_end_utc
    print "ALERT BEGIN SNDAQ time: ", alert_start_sn    
    print "ALERT END SNDAQ time: ", alert_stop_sn
    
    # -- checking data range ---#
    datarange = alert_end_utc - alert_begin_utc
    print "requesting " , datarange , "minutes of HS data"
    
    stdrange = timedelta(0,95)      # 95 seconds
    maxrange = timedelta(0, 610)    # 510 seconds
    minrange = timedelta(0,0)
    print "(maxrange: " , maxrange, " minutes)"


    if datarange > stdrange:
        if datarange > maxrange:
            print "Requested time range is too huge: %s.\nHsWorker processes request only up to 610 sec.\nTry a smaller time window." % datarange
        else:
            answer = raw_input("Warning: You are requesting more HS data than usual (90 sec). Sure you want to proceed? [y/n] : ")
            if answer in ["Yes", "yes", "y", "Y"]:
                    alert_msg = "{\"start\": " + str(alert_start_sn) + " , \"stop\": " + str(alert_stop_sn) + " , \"copy\": \"" + copydir + "\"}"
                    print "alert_dict to string looks like: " , alert_msg
                    grabber.send(alert_msg)
                    print "HsGrabber sent his Request"
                    should_continue = True
            else:
                print "HS data request stopped. Try a smaller time window."
                
    elif datarange < minrange:
        print "Requesting negative time range: %s. Try another time window." % datarange

    else: # datarange in range:
        alert_msg = "{\"start\": " + str(alert_start_sn) + " , \"stop\": " + str(alert_stop_sn) + " , \"copy\": \"" + copydir + "\"}"
        print "alert_dict to string looks like: " , alert_msg
        grabber.send(alert_msg)
        print "HsGrabber sent his Request"
        should_continue = True

    
    #--- waiting for answer from Publisher ---#
    count = 0
    while should_continue:
        count += 1
        print "."
        if count > timeout:
            print "didn't receive answer within %s seconds. shutting down..." % timeout
            break
        
        socks = dict(poller.poll(timeout * 100)) # poller takes msec argument
        if grabber in socks and socks[grabber] == zmq.POLLIN:
            message = grabber.recv()
            print "received control command: %s" % message
            if message == "DONE\0":
                i3socket.send_json({"service": "HSiface", 
                    "varname": "HsGrabber", 
                    "value": "pushed request"})
                print "Received DONE. Not waiting for more messages. Shutting down..." 
                should_continue = False
                sys.exit(1)
                
if __name__=="__main__":
    '''
    For grabbing hs data from hubs independent (without sndaq providing alert).
    HsGrabber sends json to HsPublisher to grab hs data from hubs.
    Uses the Hs Interface infrastructure.
    '''
    def usage():
        print >>sys.stderr, """
        usage :: HsGrabber.py [options]
            -b         | begin of data: "YYYY-mm-dd HH:MM:SS.[ns]" OR SNDAQ timestamp [ns from beginning of the year]
            -e         | end of data "YYYY-mm-dd HH:MM:SS.[ns]"   OR SNDAQ timestamp [ns from beginning of the year]   
            -c         | copydir e.g. "pdaq@2ndbuild:/mnt/data/pdaqlocal/HsDataCopy/"
            
            HsGrabber reads UTC timestamps or SNDAQ timestamps.
            It sends SNDAQ timestamps to HsInterface (HsPublisher).
            """
        sys.exit(1)

    p = subprocess.Popen(["hostname"], stdout = subprocess.PIPE)
    out, err = p.communicate()
    src_mchn = out.rstrip()
    
    if ".usap.gov" in src_mchn:
        src_mchn_short = re.sub(".icecube.usap.gov", "", src_mchn)
        cluster = "SPS"
    elif ".wisc.edu" in src_mchn:
        src_mchn_short = re.sub(".icecube.wisc.edu", "", src_mchn)
        cluster = "SPTS"
    else:
        src_mchn_short = src_mchn    
        cluster = "localhost"
        
    print "This HsGrabber runs on: " , src_mchn
        
    ##take arguments from command line and check for correct input
    opts, args = getopt.getopt(sys.argv[1:], 'hb:e:c:', ['help','alert='])
    for o, a in opts:
        if o == '-b':            
            if not a.isdigit():
                alert_start_sn = 0 #SNDAQ timestamp not yet defined
                try:        
                    alert_begin_utc = datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S.%f")
                except ValueError, e:
                    #print "Problem with the time-stamp format: ", e
                    try:
                        print "Try matching format without subsecond precision..."
                        alert_begin_short = re.sub(".[0-9]{9}", '', str(a))
                        alert_begin_utc = datetime.strptime(alert_begin_short, "%Y-%m-%d %H:%M:%S")
                        print "matched"
                    except ValueError,e:
                        print  "Problem with the time-stamp format: ", e
            else:
                alert_start_sn = int(a)
                alert_begin_utc = 0
        if o == '-e':
            if not a.isdigit():
                alert_stop_sn = 0 #SNDAQ timestamp not yet defined do that later on
                try:        
                    alert_end_utc = datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S.%f")
                except ValueError, e:
                    #print "Problem with the time-stamp format: ", e
                    try:
                        print "Try matching format without subsecond precision..."
                        alert_end_short = re.sub(".[0-9]{9}", '', str(a))
                        alert_end_utc = datetime.strptime(alert_end_short, "%Y-%m-%d %H:%M:%S")
                        print "matched"
                    except ValueError,e:
                        print  "Problem with the time-stamp format: ", e
#                alert_end_utc = datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S.%f")
            else:
                alert_stop_sn = int(a)
                alert_end_utc = 0
        if o == '-c':
            copydir = str(a)
        elif o == '-h' or o == '--help':
            usage()       
    if len(sys.argv) < 5 :
        print usage()
        
        
    # build 0MQ sockets    
    context = zmq.Context()
    grabber = context.socket(zmq.REQ)
    poller = zmq.Poller() # needed for handling timeout if Publisher doesnt answer
    poller.register(grabber, zmq.POLLIN)
    i3socket = context.socket(zmq.PUSH) # former ZMQ_DOWNSTREAM is depreciated in recent releases, use PUSH instead

    timeout = 10 # number of trys to wait for answer from Publisher
    
    if cluster == "localhost":
        # Socket to send alert message to HsPublisher
        grabber.connect("tcp://localhost:55557")   #connection = tcp, host = localhost ip , port 
        print "connected to HsPublicher on localhost at port 55557"
        # Socket for I3Live on expcont
        i3socket.connect("tcp://localhost:6668") 
        print "connected to i3live socket on localhost at port 6668"      

    else:
        # Socket to send alert message to HsPublisher
        grabber.connect("tcp://expcont:55557")   #connection = tcp, host = localhost ip , port 
        print "connected to HsPublicher on expcont at port 55557"
        # Socket for I3Live on expcont
        i3socket.connect("tcp://expcont:6668") 
        print "connected to i3live socket on expcont at port 6668" 
    
    
    send_alert(timeout, alert_start_sn, alert_stop_sn, alert_begin_utc, alert_end_utc)

        
        