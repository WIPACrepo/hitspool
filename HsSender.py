#!/usr/bin/python


import sys
import zmq 
import json
from datetime import datetime, timedelta
import re
import subprocess
from subprocess import CalledProcessError
import logging
import signal


# --- Clean exit when program is terminated from outside (via pkill) ---#
def handler(signum, frame):
    logging.warning("Signal Handler called with signal " + str( signum))
    logging.warning( "Shutting down...\n")
    i3live_dict = {}
    i3live_dict["service"] = "HSiface"
    i3live_dict["varname"] = "HsSender"
    i3live_dict["value"] = "INFO: SHUT DOWN called by external signal." 
    i3socket.send_json(i3live_dict)
    i3live_dict2 = {}
    i3live_dict2["service"] = "HSiface"
    i3live_dict2["varname"] = "HsSender"
    i3live_dict2["value"] = "STOPPED" 
    i3socket.send_json(i3live_dict2)
    
    reporter.close()
    i3socket.close()
    context.term() 
    sys.exit()

signal.signal(signal.SIGTERM, handler)    #handler is called when SIGTERM is called (via pkill)

class HsSender(object):
    """
    Handles post processing of HS data at pole:
    Packing ("SPADE-ing") the data etc
    """
    def receive_from_worker(self):
        msg = reporter.recv_json()
        logging.info( "report received json: " + str(msg))
        info = json.loads(msg)
        logging.info("HsSender loaded json: " +str(info))
        return info
        
    def hs_data_location_check(self, info):
        """
        Move the data in case its default locations differs from the user's desired one.
        """
        #infodict = json.loads(info)
        if info["msgtype"] == "rsync_sum":
            logging.info( "Checking the data location...")
            copydir         = info['copydir']
            copydir_user    = info["copydir_user"] 
            logging.info("HS data lcoated at: " + str(copydir))
            logging.info("user requested it to be in: " + str(copydir_user))
            copy_basedir = re.search('[/\w+]*/(?=SNALERT_[0-9]{8}_[0-9]{6})', copydir)
            if copy_basedir:
                hs_basedir = copy_basedir.group(0)
                data_dir = re.search('(?<=' + hs_basedir + ').*', copydir)
                data_dir_name = data_dir.group(0)
                logging.info( "HS data name: " + str(data_dir_name))
    
                if copydir_user != hs_basedir:
                    #move data to user required directory:
                    subprocess.check_call(["mkdir", "-p", copydir_user + data_dir_name])
                    logging.info("mkdir " + str(copydir_user + data_dir_name))
                    subprocess.check_call(['mv',"-v",copydir, copydir_user + data_dir_name])
                    logging.info("moved hs files from " +  str(hs_basedir) + str(data_dir_name) + " to " +str(copydir_user) + str(data_dir_name))
                    hs_basedir = copydir_user
                logging.info("HS data " + str(data_dir_name) + " is located in " + str(hs_basedir))
                return hs_basedir, data_dir_name
            else:
                logging.error("Naming scheme validation failed.")
                logging.error("Please put the data manually in the desired location: " + str(copydir_user))
                pass

        else:
            #this json message doesnt contain information to be checked here
            pass

    #------ Preparatin for SPADE ----#
    def spade_pickup_data(self, infodict, hs_basedir, data_dir_name):
        '''
        tar & bzip folder
        create semaphore file for folder
        move .sem & .dat.tar.bz2 file in SPADE directory
        '''
        
        #infodict = json.loads(info)
        logging.info( "Preparation for SPADE Pickup of HS data started...")
        copydir         = infodict['copydir']        
        copy_basedir    = re.search('[/\w+]*/(?=SNALERT_[0-9]{8}_[0-9]{6})', copydir)
        
        if copy_basedir:
            hs_basedir = copy_basedir.group(0)
            data_dir = re.search('(?<=' + hs_basedir + ').*', copydir)
            logging.info( "HS data name: " + str(data_dir.group(0)))
        else:
            logging.error("Naming scheme validation failed.")
            logging.error("Please put the data manually in the SPADE directory")
            pass

        datastart = re.search('[0-9]{8}_[0-9]{6}', copydir)
        src_mchn = re.search('i[c,t]hub[0-9]{2}', copydir)
        logging.info( "copy_basedir is: " + str(hs_basedir))
        if copy_basedir and datastart and src_mchn and data_dir:  
            hs_basename = "HS_SNALERT_"  + datastart.group(0) + "_" + src_mchn.group(0)  
            hs_bzipname = hs_basename + ".dat.tar.bz2"
            hs_spade_dir = "/mnt/data/HitSpool/"
            hs_spade_semfile = hs_basename + ".sem"
            
            # WATCH OUT!This is a relative directory name.
            #Current working directory path provided by "cwd=copy_basedir.group(0)" in subprocess call 
            subprocess.check_call(['nice', 'tar', '-jcvf', hs_bzipname , data_dir.group(0)], cwd=hs_basedir)
            logging.info( "tarring and zipping done: " + str(hs_bzipname) + " for " + str(src_mchn.group(0)))
#
            try:
                mv_result1 = subprocess.check_call(['mv', '-v', hs_bzipname, hs_spade_dir], cwd=hs_basedir)
                logging.info( "move tarfolder to SPADE dir\n%s " + str(hs_spade_dir))
#                mv_result2 = subprocess.check_call(['mv -v', hs_spade_semfile, hs_spade_dir], cwd=copy_subdir)
                if mv_result1 == 0:
                    subprocess.check_call(["touch", hs_spade_semfile], cwd=hs_spade_dir)
                    logging.info("create .sem file")
#                    subprocess.check_call("rm -rf " + copydir, shell=True)
#                    logging.info("delete the untarred hitspool data "  + str(copydir))
                    logging.info("Preparation for SPADE Pickup of " + str(copydir) + "DONE")
                    i3socket.send_json({"service": "HSiface", "varname": "HsSender@" + src_mchn_short, 
                                "value": "SPADE-ing of %s done" % str(copydir)}) 
                else:
                    logging.error("failed moving the tarred data.")
                    logging.error("Please put the data manually in the SPADE directory. Use HsSpader.py, for example.")

            except (IOError,OSError,subprocess.CalledProcessError):
                logging.error( "Loading data in SPADE directory failed")
                logging.error( "Please put the data manually in the SPADE directory. Use HsSpader.py, for example.")
        else:
            logging.error("Naming scheme validation failed.")
            logging.error("Please put the data manually in the SPADE directory. Use HsSpader.py, for example.")
            pass
        
    def spade_pickup_log(self, info):    
        #infodict = json.loads(info)            
        if info["msgtype"] == "log_done":
            logging.info("logfile " + str(info["logfile_hsworker"]) + " from " + str(info["hubname"]) + " was transmitted to " + str(info["logfiledir"]))
            
            org_logfilename     = info["logfile_hsworker"]
            hs_log_basedir      = info["logfiledir"]
            hs_log_basename     = "HS_" + info["alertid"] + "_" + info["logfile_hsworker"]
            hs_log_spadename    =  hs_log_basename + ".dat.tar.bz2"
            hs_log_spadedir     = "/mnt/data/HitSpool/"
            hs_log_spade_sem    = hs_log_basename + ".sem"
            
            tar_log_cmd = subprocess.check_call(['nice', 'tar', '-jvcf', hs_log_spadename ,org_logfilename], cwd=hs_log_basedir)
            logging.info("tarred the log file: " +str(hs_log_spadename))
            mv_log_cmd = subprocess.check_call(["mv", "-v", hs_log_spadename, hs_log_spadedir], cwd= hs_log_basedir)
            logging.info("moved the log file to: " + str(hs_log_spadedir))
            sem_log_cmd = subprocess.check_call(["touch", hs_log_spade_sem], cwd=hs_log_spadedir)
            logging.info("created .sem file in: " + str(hs_log_spadedir))
        else:
            #do nothing
            pass

if __name__ == "__main__":
    
    
    """
    "sndaq"           "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        ------------
    | REQUEST | <----->| REPLY   |         | IcHub n |        | 2ndbuild |
    -----------        | PUB     | ------> | SUB    n|        | PULL     |
                       ----------          |PUSH     | ---->  |          |
                                            ---------          -----------
    This is the NEW HsSender for the HS Interface. 
    It's started via fabric on access.
    It receives messages from the HsWorkers and is responsible of putting
    the HitSpool Data in the SPADE queue. 
    """    
    
    p = subprocess.Popen(["hostname"], stdout = subprocess.PIPE)
    out, err = p.communicate()
    hostname = out.rstrip()
    
    if "sps" in hostname:
        src_mchn_short = re.sub(".icecube.southpole.usap.gov", "", hostname)
        cluster = "SPS"
    elif "spts" in hostname:
        src_mchn_short = re.sub(".icecube.wisc.edu", "", hostname)
        cluster = "SPTS"
    else:
        src_mchn_short = hostname
        cluster = "localhost"
        
    if cluster == "localhost" :    
        logfile = "/home/david/TESTCLUSTER/2ndbuild/logs/hssender_" + src_mchn_short + ".log"  
    else:
        logfile = "/mnt/data/pdaqlocal/HsInterface/logs/hssender_" + src_mchn_short + ".log"

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
                        level=logging.INFO, stream=sys.stdout, 
                        datefmt= '%Y-%m-%d %H:%M:%S', 
                        filename=logfile) 
    
    logging.info("HsSender starts on " + str(src_mchn_short))
    
    context = zmq.Context()
    # Socket to receive messages on from Worker
    reporter = context.socket(zmq.PULL)
    reporter.bind("tcp://*:55560")
    logging.info( "bind Sink to port 55560")
    
    # Socket for I3Live on expcont
    i3socket = context.socket(zmq.PUSH) # former ZMQ_DOWNSTREAM is depreciated 
    i3socket.connect("tcp://localhost:6668")
    logging.info("connected to i3live socket on port 6668")
    
    newmsg = HsSender()
    while True:
        logging.info( "HsSender waits for new reports from HsWorkers...")
        try:         
            info = newmsg.receive_from_worker()
            if info["msgtype"]== "rsync_sum":
                hs_basedir, data_dir_name = newmsg.hs_data_location_check(info)
                if cluster == "SPS":
                    newmsg.spade_pickup_data(info, hs_basedir, data_dir_name)    
            elif info["msgtype"] == "log_done":
                    newmsg.spade_pickup_log(info)
            else:
                pass
                        
        except KeyboardInterrupt:
            logging.warning( "Interruption received, shutting down...")
            sys.exit()    
