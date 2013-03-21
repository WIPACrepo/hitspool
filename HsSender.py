#!/usr/bin/python

"""
"sndaq"           "HsPublisher"      "HsWorker"           "HsSender"
-----------        -----------
| sni3daq |        | expcont |         -----------        ------------
| REQUEST | <----->| REPLY   |         | IcHub n |        | expcont  |
-----------        | PUB     | ------> | SUB    n|        | PULL     |
                   ----------          |PUSH     | ---->  |          |
                                        ---------          -----------
This is the HsSender for the HS Interface. 
It receives messages from the HsWorkers and is responsible of putting
the HitSpool Data in the SPADE queue. Furthermore, it handles logs and moni values for I3Live

"""

import sys
import zmq 
import json
from datetime import datetime, timedelta
import re
import subprocess
from subprocess import CalledProcessError
context = zmq.Context()

# Socket to receive messages on from Worker
reporter = context.socket(zmq.PULL)
reporter.bind("tcp://*:55560")
print "bind Sink to port 55560 on spts-2ndbuild"

# Socket for I3Live on expcont
i3socket = context.socket(zmq.DOWNSTREAM) # former ZMQ_DOWNSTREAM is depreciated 
i3socket.connect("tcp://expcont:6668") 
print "connected to i3live socket on port 6668"

#LEVELS = {'debug': logging.DEBUG,
#          'info': logging.INFO,
#          'warning':logging.WARNING,
#          'error': logging.ERROR,
#          'critical': logging.CRITICAL}

#def sub_logger():
#    sublogger = context.socket(zmq.SUB)
#    sublogger.bind("tcp://*:55500")
#    print "bin Logger SUB socket to port 55500"
#    sublogger.setsockopt(zmq.SUBSCRIBE, "")
#    logging.basicConfig(level=LEVELS,
#                        format='%(asctime)s %(levelname)s %(message)s',
#                        filename='/mnt/data/pdaqlocal/HsDataCopy/hsinterface.log',
#                        filemode='a')

class HsSender(object):
    def receive_from_worker(self):
        print "HsSender waits for reports from HsWorkers..."
        msg = reporter.recv_json()
        print "HsSender received json msg from HsWorker: " , msg
        return msg
        
    def live_log(self, msg):
        
        infodict = json.loads(msg)
        hubname = infodict['hub']
        dataload = infodict['dataload']
        start = infodict['start']
        stop = infodict['stop']
        copydir = infodict['copydir']
        
        start_utc = datetime(2013, 1, 1) + timedelta(seconds = start*1.0E-9)    # from sndaq time stanp: units in nanoseconds
        stop_utc  = datetime(2013, 1, 1) + timedelta(seconds = stop*1.0E-9)     # from sndaq time stanp: units in nanoseconds
        src_mchn = re.search('i[c,t]hub[0-9]{2}', copydir)
        
        

        
        # fill a new dictionary with info from infodict:
        value_dict1 = {}
        value_dict1["dataload"] = infodict['dataload']
        value_dict1["start"] = str(start_utc)
        value_dict1["stop"] = str(stop_utc)
        value_dict1["copydir"] = infodict['copydir']
        print "message: \n", hubname , '\n' ,dataload , '\n', start , '\n', stop , '\n', copydir    
        i3live_dict1 = {}
        i3live_dict1["service"] = "HSiface"
        i3live_dict1["varname"] = src_mchn.group(0)
        i3live_dict1["value"] = value_dict1

        i3socket.send_json(i3live_dict1)
        
        print "message to I3Live: ", i3live_dict1
        
        i3live_dict2 ={}
        i3live_dict2["service"] = "HSiface"
        i3live_dict2["varname"] = src_mchn.group(0)       
        i3live_dict2["value"] = "data processed"       
        i3socket.send_json(i3live_dict2)
        print "message to I3Live: ", i3live_dict2      

        
#        start_stop_delta = str(stop_utc - start_utc)
#        
#        i3live_json2 = {"service": "HSiface", "varname": "SnAlertInterval", "value": start_stop_delta}
#        
        


    def spade_pickup(self, info):
        infodict = json.loads(info)
        print "Preparation for SPADE Pickup started..."
        copydir = infodict['copydir']
        copy_basedir = re.search('[/\w+]*/(?=SNALERT_[0-9]{8}_[0-9]{6})', copydir)
        if copy_basedir:
            data_dir = re.search('(?<=' + copy_basedir.group(0) + ').*', copydir)
            print "Uniquely named folder for hs data is called: " , data_dir.group(0)
        else:
            print "Naming scheme validation failed."
            print "Please put the data manually in the SPADE directory"
            pass
        datastart = re.search('[0-9]{8}_[0-9]{6}', copydir)
        src_mchn = re.search('i[c,t]hub[0-9]{2}', copydir)
        print "copy_basedir from json is: " , copy_basedir.group(0)
        if copy_basedir and datastart and src_mchn and data_dir:  
            hs_basename = "HS_SNALERT_"  + datastart.group(0) + "_"+src_mchn.group(0)  
            hs_tarname = hs_basename + ".dat.tar" 
            # WATCH OUT!This is a relative directory name. Absoulte path provided by "cwd=copy_basedir.group(0)" in subprocess call 
            print " the copydir: %s goes into tarname in this way: %s " % (copydir, hs_tarname)
            print "start tarring inside the hitspool copy dir..."
            subprocess.check_call(['nice', 'tar', '-cvf', hs_tarname , data_dir.group(0)], cwd=copy_basedir.group(0))
            print "tarring done"
#            subprocess.check_call(['rm', '-rv', data_dir.group(0)], cwd=copy_basedir.group(0))
#            print "removed untarred data"
            hs_spade_name = "/mnt/data/HitSpool/" + hs_tarname
            hs_spade_semfile = "/mnt/data/HitSpool/" + hs_basename + ".sem"
            print "Finished tarball for %s" % src_mchn.group(0)
#            try:
#                print "move tarfolder to SPADE dir and name correctly:\n%s " % hs_spade_name
#                mv_result = subprocess.check_call(['mv', hs_tarname, hs_spade_name], cwd=copy_basedir.group(0))
#                if mv_result is not None:
#                    print "moving the data didn't succeed"
#                else:
#                    print "create .sem file"
#                    subprocess.check_call("touch " + hs_spade_semfile, shell=True)
#                    print "delete the hitspool data %s" %copydir
#                    subprocess.check_call("rm -rf " + copydir, shell=True)
#                    print "Done"
#            except IOError, subprocess.CalledProcessError:
#                print "Error: Loading data in SPADE directory failed"
#                print "Please put the data manually in the SPADE directory"
        else:
            print "Naming scheme validation failed."
            print "Please put the data manually in the SPADE directory"
            pass



class Reporter(object):
    
    
    def report(self):
        x = HsSender ()

        while True:
            try:         
                infodict = x.receive_from_worker()
                print "HsWorker report received and DONE."
                x.spade_pickup(infodict)
                print "Preparation for SPADE Pickup DONE"
                x.live_log(infodict)
                print "HsSender sended to I3Live"
                
            except KeyboardInterrupt:
                print "Interruption received, proceeding..."
                i3socket.close()
                context.term()  
                sys.exit()

newdata = Reporter()
newdata.report()            
