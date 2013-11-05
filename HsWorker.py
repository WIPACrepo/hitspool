#!/usr/bin/python


"""
#Hit Spool Worker to be run on hubs
#author: dheereman i3.hsinterface@gmail.com
#check out the icecube wiki page for instructions: https://wiki.icecube.wisc.edu/index.php/HitSpool_Interface
"""
import time
from datetime import datetime, timedelta
import re, sys
import zmq #@UnresolvedImport
import subprocess
import json
import random
import logging
import signal


# --- Clean exit when program is terminated from outside (via pkill) ---#
def handler(signum, frame):
    logging.warning("Signal Handler called with signal " + str( signum))
    logging.warning( "Shutting down...\n")
    i3live_dict = {}
    i3live_dict["service"] = "HSiface"
    i3live_dict["varname"] = "HsWorker@" + src_mchn_short
    i3live_dict["value"] = "INFO: SHUT DOWN called by external signal." 
    i3socket.send_json(i3live_dict)
    i3live_dict2 = {}
    i3live_dict2["service"] = "HSiface"
    i3live_dict2["varname"] = "HsWorker@" + src_mchn_short
    i3live_dict2["value"] = "STOPPED" 
    i3socket.send_json(i3live_dict2)
    i3socket.close()
    subscriber.close()
    sender.close()
    context.term()
    sys.exit()

signal.signal(signal.SIGTERM, handler)    #handler is called when SIGTERM is called (via pkill)

class MyAlert(object):
    """
    This class
    1. analyzes the alert message
    2. looks for the requested files / directory 
    3. copies them over to the requested directory specified in the message.
    4. writes a short report about was has been done.
    """
    
    def alert_parser(self, alert, src_mchn, src_mchn_short, cluster):
        """
        Parse the Alert message for starttime, stoptime,
        sn-alert-trigger-time-stamp and directory where-to the data has to be copied.
        """
        
        logging.info("HsInterface srunning on: " + str(cluster))
        
        if cluster == "localhost":
            hs_sourcedir_current     = '/home/david/TESTCLUSTER/testhub/currentRun/'
            hs_sourcedir_last        = '/home/david/TESTCLUSTER/testhub/lastRun/'

        else:
            hs_sourcedir_current     = '/mnt/data/pdaqlocal/currentRun/'
            hs_sourcedir_last        = '/mnt/data/pdaqlocal/lastRun/'
        
        packer_start = str(datetime.utcnow())

#        # --- Parsing alert message JSON ----- :
        alertParse1 = False
        alertParse2 = False
        alertParse3 = False
        alertDatamax = False
            
        alert_info = json.loads(alert)
        start = int(alert_info[0]['start'])         # timestamp in DAQ units as a string
        stop = int(alert_info[0]['stop'])           # timestamp in DAQ units as a string
        hs_user_machinedir = alert_info[0]['copy']  # should be something like: pdaq@expcont:/mnt/data/pdaqlocal/HsDataCopy/

        i3live_dict1 = {}
        i3live_dict1["service"] = "HSiface"
        i3live_dict1["varname"] = "HsWorker@" + src_mchn_short
        i3live_dict1["value"] = "received request at %s " % packer_start
        i3socket.send_json(i3live_dict1)
        
        try:
            sn_start = int(start)
            logging.info( "SN START [ns] = " + str(sn_start))
            utc_now = datetime.utcnow()
            sn_start_utc = str(datetime(int(utc_now.year),1,1) + timedelta(seconds=sn_start*1.0E-9))  #sndaq time units are nanoseconds
            logging.info( "SN START [UTC]: " + str(sn_start_utc))
            ALERTSTART = datetime.strptime(sn_start_utc,"%Y-%m-%d %H:%M:%S.%f")
            logging.info( "ALERTSTART = " + str(ALERTSTART))            
            TRUETRIGGER = ALERTSTART + timedelta(0,30)          # time window around trigger is [-30,+60] -> TRUETRIGGER = ALERTSTART + 30seconds 
            logging.info( "TRUETRIGGER = sndaq trigger time: " + str(TRUETRIGGER))
            alertParse1 = True
        except Exception, err:
            i3live_dict2 = {}
            i3live_dict2["service"] = "HSiface"
            i3live_dict2["varname"] = "HsWorker@" + src_mchn_short
            i3live_dict2["value"] = "ERROR: start time parsing failed. Abort request." 
            i3socket.send_json(i3live_dict2)  
            logging.error("start time parsing failed:\n" + str(err) + "\nAbort request.")
            return None
        else:
            pass

        try:
            sn_stop = int(stop)
            logging.info( "SN STOP [ns] = " + str(sn_stop))
            sn_stop_utc = str(datetime(int(utc_now.year),1,1) + timedelta(seconds=sn_stop*1.0E-9))  #sndaq time units are nanosecond
            logging.info( "SN STOP [UTC]: " + str(sn_stop_utc))
            ALERTSTOP = datetime.strptime(sn_stop_utc,"%Y-%m-%d %H:%M:%S.%f")
            logging.info( "ALERTSTOP = " + str(ALERTSTOP))
            alertParse2 = True
        except Exception, err:
            i3live_dict3 = {}
            i3live_dict3["service"] = "HSiface"
            i3live_dict3["varname"] = "HsWorker@" + src_mchn_short
            i3live_dict3["value"] = "ERROR: stop time parsing failed. Abort request."
            i3socket.send_json(i3live_dict3)
            logging.error("stop time parsing failed:\n" + str(err) +"\nAbort request.")
            return None  
        else:
            pass          
            
        try :
            logging.info( "HS machinedir = " + str(hs_user_machinedir))

            if (cluster == "SPS") or (cluster == "SPTS"):
            #for the REAL interface
                hs_ssh_access = re.sub(':/\w+/\w+/\w+/\w+/', "", hs_user_machinedir)
                                
            else:
                hs_ssh_access = re.sub(':/[\w+/]*', "", hs_user_machinedir)
            
            logging.info("HS COPY SSH ACCESS: " + str(hs_ssh_access))
            
            #default copy destination:
            if (cluster == "SPS") or (cluster == "SPTS"):
                copydir_dft = "/mnt/data/pdaqlocal/HsDataCopy/"
            
            else:
                copydir_dft = "/home/david/data/HitSpool/copytest/"
            
            # copydir defined in request message:
            hs_copydir = re.sub(hs_ssh_access + ":", '', hs_user_machinedir)  
            
            if copydir_dft != hs_copydir:
                logging.warning("requested HS data copy destination differs from default!")
                logging.warning("data will be sent to default destination: " + str(copydir_dft))
                logging.info("HsSender will redirect it later on to: " +str(hs_copydir) + "on 2ndbuild")
            
            logging.info("HS COPYDIR = " + str(hs_copydir))
            alertParse3 = True
            
        except Exception, err:
            i3live_dict4 = {}
            i3live_dict4["service"] = "HSiface"
            i3live_dict4["varname"] = "HsWorker@" + src_mchn_short
            i3live_dict4["value"] = "ERROR: copy directory parsing failed. Abort request." 
            i3socket.send_json(i3live_dict4)
            logging.error("copy directory parsing failed:\n" + str(err) +"\nAbort request.")
        else: 
            pass
        
        # stop here if parsing failed at any stage:
        if (alertParse1 != True) or (alertParse2 != True) or (alertParse3 != True):
            i3socket.send_json({"service": "HSiface", "varname": "HsWorker@" + src_mchn_short, 
                                "value": "ERROR: Request could not be parsed correctly. Abort request..."}) 
            
            logging.error("Request could not be parsed correctly. Abort request...")
            
        # after correcting parsing, check for data range alertDatamax:
        if alertParse1 and alertParse2 and alertParse3:
            # make limit : 550 sec maximal HS data requestable
            # in hs TFT proposal we said 500 sec data for a 10 significance sn trigger
            datarange = ALERTSTOP - ALERTSTART
            datamax = timedelta(0,300)
            if datarange > datamax: 
                i3socket.send_json({"service": "HSiface", "varname": "HsWorker@" + src_mchn_short, 
                                "value": "ERROR: Request exceeds limit of allowed data time range of %s s. Abort request..." % datamax}) 
                logging.error("Request exceeds limit of allowed data time range of " + str(datamax)+ " s. Abort request...")
            else:
                alertDatamax = True
            
#            print alertParse1, alertParse2, alertParse3, alertDatamax
    
        # continue processing request:            
        if alertParse1 and alertParse2 and alertParse3 and alertDatamax:
            
#            i3socket.send_json({"service": "HSiface", "varname": "HsWorker@" + src_mchn_short, 
#                                "value": "successfully parsed the request."})
            
            alertid = TRUETRIGGER.strftime("%Y%m%d_%H%M%S")             
            sn_start_file = None
            sn_stop_file = None
            
            #------Parsing hitspool info.txt from currentRun to find the requested files-------:        
            # Find the right file(s) that contain the start/stoptime and the actual sn trigger time stamp=sntts
            # useful: datetime.timedelta 
                       
            try:
                filename = hs_sourcedir_current + 'info.txt'
                fin = open (filename)
                logging.info("open " + str(filename))
            except IOError as (errno, strerror):
                i3live_dict5 = {}
                i3live_dict5["service"] = "HSiface"
                i3live_dict5["varname"] = "HsWorker@" + src_mchn_short
                i3live_dict5["value"] = "ERROR: Cannot open %s file. I/O error({0}): {1}".format(errno, strerror)  % filename
                i3socket.send_json(i3live_dict5)
                logging.error("cannot open " + str(filename))
                logging.error(str("I/O error({0}): {1}".format(errno, strerror)))
                return None
            
            else:
                infodict= {}
                for line in open(filename):
                    (key, val) = line.split()
                    infodict[str(key)] = int(val)
                fin.close()
            
            startrun = int(infodict['T0'])              # time-stamp of first HIT at run start -> this HIT is not in buffer anymore if HS_Loop > 0 !            
            CURT = infodict['CURT']                     # current time stamp in DAQ units
            IVAL = infodict['IVAL']                     # len of each file in integer 0.1 nanoseconds
            IVAL_SEC = IVAL*1.0E-10                     # len of each file in integer seconds
            CURF = infodict['CURF']                     # file index of currently active hit spool file
            MAXF = infodict['MAXF']                     # number of files per cycle
    #        startdata = int(CURT - ((MAXF-1)*IVAL_SEC))      # oldest, in hit spool buffer existing time stamp in DAQ units  
            TFILE = (CURT - startrun)%IVAL              # how long already writing to current file, time in DAQ units 
            TFILE_SEC = TFILE*1.0E-10  
            HS_LOOP = int((CURT-startrun)/(MAXF*IVAL))
            if HS_LOOP == 0:
                OLDFILE = 0              # file index of oldest in buffer existing file
                startdata = startrun
            else:
                OLDFILE = (CURF+1)
    #            startdata = int(CURT - (((MAXF-1)*IVAL_SEC) + TFILE))    # oldest, in hit spool buffer existing time stamp in DAQ units
                startdata = int(CURT - (MAXF-1)*IVAL - TFILE)    # oldest, in hit spool buffer existing time stamp in DAQ units         
    
            #converting the INFO dict's first entry into a datetime object:
            startrun_utc = str(datetime(2013,1,1) + timedelta(seconds=startrun*1.0E-10))    #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
            RUNSTART = datetime.strptime(startrun_utc,"%Y-%m-%d %H:%M:%S.%f")
            startdata_utc = str(datetime(2013,1,1) + timedelta(seconds=startdata*1.0E-10))  #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
            BUFFSTART = datetime.strptime(startdata_utc,"%Y-%m-%d %H:%M:%S.%f")
            stopdata_utc = str(datetime(2013,1,1) + timedelta(seconds=CURT*1.0E-10))        #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
            BUFFSTOP = datetime.strptime(stopdata_utc,"%Y-%m-%d %H:%M:%S.%f")
            #outputstring1 = "first HIT ever in this Run on this String in nanoseconds: %d\noldest HIT's time-stamp existing in buffer in nanoseconds: %d\noldest HIT's time-stamp in UTC:%s\nnewest HIT's timestamp in nanoseconds: %d\nnewest HIT's time-stamp in UTC: %s\neach hit spool file contains %d * E-10 seconds of data\nduration per file in integer seconds: %d\nhit spooling writes to %d files per cycle \nHitSpooling writes to newest file: HitSpool-%d since %d DAQ units\nThe Hit Spooler is currently writing iteration loop: %d\nThe oldest file is: HitSpool-%s\n"
            logging.info( "first HIT ever in current Run on this String in nanoseconds: " + str(startrun) + "\n" +
            "oldest HIT's time-stamp existing in buffer in nanoseconds: " + str(startdata) +"\n" + 
            "oldest HIT's time-stamp in UTC: " + str(BUFFSTART) + "\n" +
            "newest HIT's timestamp in nanoseconds: " + str(CURT) + "\n" +
            "newest HIT's time-stamp in UTC: " + str(BUFFSTOP) + "\n" +
            "each hit spool file contains " + str(IVAL) + " * E-10 seconds of data\n" +
            "duration per file in integer seconds: " + str(IVAL_SEC) + "\n" +
            "hit spooling writes to " + str(MAXF) + " files per cycle \n" +
            "HitSpooling writes to newest file: HitSpool-" + str(CURF) + " since " + str(TFILE) + " DAQ units\n" +
            "HitSpooling is currently writing iteration loop: " + str(HS_LOOP) + "\n" +
            "The oldest file is: HitSpool-" + str(OLDFILE))        
           
           
            #------Parsing hitspool info.txt from lastRun to find the requested files-------:        
            # Find the right file(s) that contain the start/stoptime and the actual sn trigger time stamp=sntts
            # useful: datetime.timedelta
            try:
                filename = hs_sourcedir_last+'info.txt'
                #logging.info( filename)
                fin = open (filename)
            except IOError as (errno, strerror):
                i3live_dict6 = {}
                i3live_dict6["service"] = "HSiface"
                i3live_dict6["varname"] = "HsWorker@" + src_mchn_short
                i3live_dict6["value"] = "ERROR: Cannot open %s file. I/O error({0}): {1}".format(errno, strerror)  % filename
                i3socket.send_json(i3live_dict6)   
                logging.info( "cannot open " + str(filename))
                logging.info( str("I/O error({0}): {1}".format(errno, strerror)))
                return None
            else:
                infodict2= {}
                for line in open(filename):
                    (key, val) = line.split()
                    infodict2[str(key)] = int(val)
                fin.close()
            
            last_startrun = int(infodict2['T0'])                # time-stamp of first HIT at run start -> this HIT is not in buffer anymore if HS_Loop > 0 !            
            LAST_CURT = infodict2['CURT']                       # current time stamp in DAQ units
            LAST_IVAL = infodict2['IVAL']                       # len of each file in integer 0.1 nanoseconds
            LAST_IVAL_SEC = IVAL*1.0E-10                        # len of each file in integer seconds
            LAST_CURF = infodict2['CURF']                       # file index of currently active hit spool file
            LAST_MAXF = infodict2['MAXF']                       # number of files per cycle
    #        startdata = int(CURT - ((MAXF-1)*IVAL_SEC))        # oldest, in hit spool buffer existing time stamp in DAQ units  
            LAST_TFILE = (CURT - startrun)%IVAL                 # how long already writing to current file, time in DAQ units 
            LAST_TFILE_SEC = TFILE*1.0E-10  
            LAST_HS_LOOP = int((CURT-startrun)/(MAXF*IVAL))
            if LAST_HS_LOOP == 0:
                LAST_OLDFILE = 0              # file index of oldest in buffer existing file
                last_startdata = last_startrun
            else:
                LAST_OLDFILE = (LAST_CURF+1)
    #            startdata = int(CURT - (((MAXF-1)*IVAL_SEC) + TFILE))    # oldest, in hit spool buffer existing time stamp in DAQ units
                last_startdata = int(LAST_CURT - (LAST_MAXF-1)*LAST_IVAL - LAST_TFILE)    # oldest, in hit spool buffer existing time stamp in DAQ units         
    #        logging.info( "get information from %s..." % filename
    
            #converting the INFO dict's first entry into a datetime object:
            last_startrun_utc = str(datetime(2013,1,1) + timedelta(seconds=last_startrun*1.0E-10))    #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
            LAST_RUNSTART = datetime.strptime(last_startrun_utc,"%Y-%m-%d %H:%M:%S.%f")
            last_startdata_utc = str(datetime(2013,1,1) + timedelta(seconds=last_startdata*1.0E-10))  #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
            LAST_BUFFSTART = datetime.strptime(last_startdata_utc,"%Y-%m-%d %H:%M:%S.%f")
            last_stopdata_utc = str(datetime(2013,1,1) + timedelta(seconds=LAST_CURT*1.0E-10))        #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
            LAST_BUFFSTOP = datetime.strptime(last_stopdata_utc,"%Y-%m-%d %H:%M:%S.%f")
            #outputstring1 = "first HIT ever in this Run on this String in nanoseconds: %d\noldest HIT's time-stamp existing in buffer in nanoseconds: %d\noldest HIT's time-stamp in UTC:%s\nnewest HIT's timestamp in nanoseconds: %d\nnewest HIT's time-stamp in UTC: %s\neach hit spool file contains %d * E-10 seconds of data\nduration per file in integer seconds: %d\nhit spooling writes to %d files per cycle \nHitSpooling writes to newest file: HitSpool-%d since %d DAQ units\nThe Hit Spooler is currently writing iteration loop: %d\nThe oldest file is: HitSpool-%s\n"
            logging.info( "first HIT ever in last Run on this String in nanoseconds: " + str(last_startrun) + "\n" +
            "oldest HIT's time-stamp existing in buffer in nanoseconds: " + str(last_startdata) +"\n" + 
            "oldest HIT's time-stamp in UTC: " + str(LAST_BUFFSTART) + "\n" +
            "newest HIT's timestamp in nanoseconds: " + str(LAST_CURT) + "\n" +
            "newest HIT's time-stamp in UTC: " + str(LAST_BUFFSTOP) + "\n" +
            "each hit spool file contains " + str(LAST_IVAL) + " * E-10 seconds of data\n" +
            "duration per file in integer seconds: " + str(LAST_IVAL_SEC) + "\n" +
            "hit spooling writes to " + str(LAST_MAXF) + " files per cycle \n" +
            "HitSpooling writes to newest file: HitSpool-" + str(LAST_CURF) + " since " + str(LAST_TFILE) + " DAQ units\n" +
            "HitSpooling is currently writing iteration loop: " + str(LAST_HS_LOOP) + "\n" +
            "The oldest file is: HitSpool-" + str(LAST_OLDFILE)) 
            #%( last_startrun, last_startdata, LAST_BUFFSTART, LAST_CURT, LAST_BUFFSTOP, LAST_IVAL, LAST_IVAL_SEC, LAST_MAXF, LAST_CURF, LAST_TFILE, LAST_HS_LOOP, LAST_OLDFILE)
    
            #------ CHECK ALERT DATA LOCATION  -----#
            
            logging.info( "ALERTSTART has format: " + str(ALERTSTART))
            logging.info( "ALERTSTOP has format: " + str(ALERTSTOP))
    
            # Check if required sn_start / sn_stop data still exists in buffer. 
            # If sn request comes in from earlier times -->  Check lastRun directory
            # Decide in this section which Run directory is the correct one: lastRun or currentRun ?
    
            # send convertion of the timestamps:
            i3socket.send_json({"service": "HSiface",
                                "varname": "HsWorker@" + src_mchn_short,
                                "value": {"START": sn_start,
                                          "STOP": sn_stop,
                                          "UTCSTART": str(ALERTSTART),
                                          "UTCSTOP": str(ALERTSTOP)}}) 
            
            if LAST_BUFFSTOP < ALERTSTART < BUFFSTART < ALERTSTOP:
                logging.info( "Data location case 1")          
                hs_sourcedir = hs_sourcedir_current
                logging.info( "HitSpool source data is in directory: " + str(hs_sourcedir))
                
#                i3live_dict7 = {}
#                i3live_dict7["service"] = "HSiface"
#                i3live_dict7["varname"] = "HsWorker@" + src_mchn_short
#                i3live_dict7["value"] = "Found requested data in %s." % hs_sourcedir
#                i3socket.send_json(i3live_dict7)
                
                
                sn_start_file = OLDFILE
                logging.warning( "Sn_start doesn't exits in " + str(hs_sourcedir) + " buffer anymore! Start with oldest possible data: HitSpool-" + str(OLDFILE))
                
                sn_start_file_str = hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat" 
    
#                i3socket.send_json({"service": "HSiface",
#                                    "varname": "HsWorker@" + src_mchn_short,
#                                    "value": "StartTime out of HsBuffer. Assign: sn_startfile = %s" % sn_start_file_str})
                
                timedelta_stop = (ALERTSTOP - BUFFSTART)
                #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
                timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
                logging.info( "time diff from hit spool buffer start to sn_stop in seconds: " + str(timedelta_stop_seconds))
                sn_stop_file = int(((timedelta_stop_seconds/IVAL_SEC) + OLDFILE) % MAXF)        
                #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
                sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
                logging.info( "sn_stops's data is included in file " + str(sn_stop_file_str))    
    
            elif BUFFSTART < ALERTSTART < ALERTSTOP < BUFFSTOP: 
                logging.info( "Data location case 2")          
                hs_sourcedir = hs_sourcedir_current
                logging.info( "HitSpool source data is in directory: " + str(hs_sourcedir))
                
#                i3live_dict8 = {}
#                i3live_dict8["service"] = "HSiface"
#                i3live_dict8["varname"] = "HsWorker@" + src_mchn_short
#                i3live_dict8["value"] = "Found requested data in %s." % hs_sourcedir
#                i3socket.send_json(i3live_dict8)
                
                timedelta_start = (ALERTSTART - BUFFSTART) # should be a datetime.timedelta object 
                #time passed after data_start when sn alert started: sn_start - data_start in seconds:
                timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
                logging.info( "There are " + str(timedelta_start_seconds) + " seconds of data before the Alert started")             
                sn_start_file = int(((timedelta_start_seconds/IVAL_SEC) + OLDFILE) % MAXF)
                sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
                logging.info( "sn_start's data is included in file " + str(sn_start_file_str))    
    
                timedelta_stop = (ALERTSTOP - BUFFSTART)
                #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
                timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
                logging.info( "time diff from hit spool buffer start to sn_stop in seconds: " + str(timedelta_stop_seconds))
                sn_stop_file = int(((timedelta_stop_seconds/IVAL_SEC) + OLDFILE) % MAXF)        
                #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
                sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
                logging.info( "sn_stops's data is included in file " + str(sn_stop_file_str)) 
            
            elif LAST_BUFFSTOP < ALERTSTART < ALERTSTOP < BUFFSTART:
                logging.info( "Data location case 3")   
    
                logging.error("requested data doesn't exist in HitSpool Buffer anymore!Abort request.")
    
                i3socket.send_json({"service": "HSiface",
                        "varname": "HsWorker@" + src_mchn_short,
                        "value": "Requested data doesn't exist anymore in HsBuffer. Abort request."})
                return None
            
            
            # tricky case: ALERTSTART in lastRun and ALERTSTOP in currentRun
            elif LAST_BUFFSTART < ALERTSTART < LAST_BUFFSTOP < ALERTSTOP:
                logging.info( "Data location case 4")          
    
                hs_sourcedir = hs_sourcedir_last
                logging.info( "requested data distributed over both HS Run directories")
                
#                i3socket.send_json({"service": "HSiface",
#                        "varname": "HsWorker@" + src_mchn_short,
#                        "value": "requested data distributed over both HS Run directories"})
                
                
                # -- start file --#
                timedelta_start = (ALERTSTART - LAST_BUFFSTART) # should be a datetime.timedelta object 
                #time passed after data_start when sn alert started: sn_start - data_start in seconds:
                timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
                logging.info( "There are " + str(timedelta_start_seconds) + " seconds of data before the Alert started")             
                sn_start_file = int(((timedelta_start_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)
                sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
                logging.info( "SN_START file: " +  str(sn_start_file_str))         
                
                # -- stop file --#
                logging.warning( "sn_stop's data is not in buffer anymore. Take LAST_CURF as ALERTSTOP")
                sn_stop_file = LAST_CURF
                sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
                logging.info( "SN_STOP file: " + str(sn_stop_file_str))
                    
                # if sn_stop is available from currentRun:
                # define 3 sn_stop files indices to have two data sets: sn_start-sn_stop1 & sn_stop2-sn_top3    
                if BUFFSTART < ALERTSTOP:
                    logging.info( "SN_START & SN_STOP distributed over lastRun and currentRun")
                    logging.info( "add relevant files from currentRun directory...")
                    #in currentRun:
                    sn_stop_file2 = OLDFILE
                    sn_stop_file_str2 = hs_sourcedir_current +"HitSpool-" + str(sn_stop_file2) + ".dat"
                    logging.info( "SN_STOP part2 file %s" + str(sn_stop_file_str2))              
                    timedelta_stop3 = (ALERTSTOP - BUFFSTART)
                    #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
                    timedelta_stop_seconds3 =  (timedelta_stop3.seconds + timedelta_stop3.days * 24 * 3600)      
                    logging.info( "time diff from hit spool buffer start to sn_stop in seconds: " + str(timedelta_stop_seconds3))
                    sn_stop_file3 = int(((timedelta_stop_seconds3/IVAL_SEC) + OLDFILE) % MAXF)        
                    #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
                    sn_stop_file_str3 = hs_sourcedir_current +"HitSpool-" + str(sn_stop_file3) + ".dat"
                    logging.info( "SN_STOP part3 file " + str(sn_stop_file_str3))
                    
                                    
#                    i3socket.send_json({"service": "HSiface",
#                                        "varname": "HsWorker@" + src_mchn_short,
#                                        "value": "sn_start_file pt1 = %s" % sn_start_file_str})
#        
#                    i3socket.send_json({"service": "HSiface",
#                                        "varname": "HsWorker@" + src_mchn_short,
#                                        "value": "sn_stop_file pt1 = %s" % sn_stop_file_str})
#                    
#                    i3socket.send_json({"service": "HSiface",
#                                        "varname": "HsWorker@" + src_mchn_short,
#                                        "value": "sn_start_file pt2 = %s" % sn_stop_file_str2})
#        
#                    i3socket.send_json({"service": "HSiface",
#                                        "varname": "HsWorker@" + src_mchn_short,
#                                        "value": "sn_stop_file pt2 = %s" % sn_stop_file_str3})
#    
            
    #        elif ALERTSTOP > BUFFSTART and ALERTSTOP > ALERTSTART:   # sn_stop > BUFFEND is not possible by definition
    #            timedelta_stop = (ALERTSTOP - BUFFSTART)
    #            #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
    #            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
    #            logging.info( "time diff from hit spool buffer start to sn_stop in seconds: %s" % timedelta_stop_seconds
    #            sn_stop_file = int(((timedelta_stop_seconds/IVAL_SEC) + OLDFILE) % MAXF)        
    #            #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
    #            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
    #            logging.info( "sn_stops's data is included in file %s" % (sn_stop_file_str)
                
                
            elif LAST_BUFFSTART < ALERTSTART < ALERTSTOP < LAST_BUFFSTOP:
                logging.info( "Data location case 5")          
                hs_sourcedir = hs_sourcedir_last
                logging.info( "HS source data is in directory: " + str(hs_sourcedir))  
                
#                i3live_dict9 = {}
#                i3live_dict9["service"] = "HSiface"
#                i3live_dict9["varname"] = "HsWorker@" + src_mchn_short
#                i3live_dict9["value"] = "HS source data is in directory: %s" % hs_sourcedir
#                i3socket.send_json(i3live_dict9)
                
                
                timedelta_start = (ALERTSTART - LAST_BUFFSTART) # should be a datetime.timedelta object 
                #time passed after data_start when sn alert started: sn_start - data_start in seconds:
                timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
#                logging.info( "There are " + str(timedelta_start_seconds) + " seconds of data before the Alert started")            
                sn_start_file = int(((timedelta_start_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)
                sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
                logging.info( "sn_start's data is included in file " + str(sn_start_file_str))     
                
                timedelta_stop = (ALERTSTOP - LAST_BUFFSTART)
                #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
                timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
#                logging.info( "time diff from hit spool buffer start to sn_stop in seconds: " + str(timedelta_stop_seconds))
                sn_stop_file = int(((timedelta_stop_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)        
                #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
                sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
                logging.info( "sn_stop's data is included in file " + str(sn_stop_file_str))   
#                i3socket.send_json({"service": "HSiface",
#                                    "varname": "HsWorker@" + src_mchn_short,
#                                    "value": "sn_start_file = %s" % sn_start_file_str})
#    
#                i3socket.send_json({"service": "HSiface",
#                                    "varname": "HsWorker@" + src_mchn_short,
#                                    "value": "sn_stop_file = %s" % sn_stop_file_str})
#                
            elif ALERTSTART < LAST_BUFFSTART < ALERTSTOP < LAST_BUFFSTOP:
                logging.info( "Data location case 6")
                hs_sourcedir = hs_sourcedir_last
                logging.info( "HS source data is in directory: " + str(hs_sourcedir))
#                i3live_dict10 = {}
#                i3live_dict10["service"] = "HSiface"
#                i3live_dict10["varname"] = "HsWorker@" + src_mchn_short
#                i3live_dict10["value"] = "HS source data is in directory: %s." % hs_sourcedir
#                i3socket.send_json(i3live_dict10)            
                
                sn_start_file = LAST_OLDFILE
                logging.warning("sn_start doesn't exits in" + str( hs_sourcedir) +  "buffer anymore! Start with oldest possible data: HitSpool-"+str(LAST_OLDFILE))
                sn_start_file_str = hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"               
                
#                i3socket.send_json({"service": "HSiface",
#                        "varname": "HsWorker@" + src_mchn_short,
#                        "value": "StartTime out of HsBuffer. Assign: sn_startfile = %s" % sn_start_file_str})
                
                timedelta_stop = (ALERTSTOP - LAST_BUFFSTART)
                #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
                timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
                logging.info( "time diff from hit spool buffer start to sn_stop in seconds: " + str(timedelta_stop_seconds))
                sn_stop_file = int(((timedelta_stop_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)        
                #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
                sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
                logging.info( "sn_stops's data is included in file " + str(sn_stop_file_str))
                
#                i3socket.send_json({"service": "HSiface",
#                        "varname": "HsWorker@" + src_mchn_short,
#                        "value": "sn_stop_file = %s" % sn_stop_file_str})
                
            elif ALERTSTART < ALERTSTOP < LAST_BUFFSTART:
                logging.info( "Data location case 7")
                logging.error( "requested data doesn't exist in HitSpool Buffer anymore! Abort request.")
                i3socket.send_json({"service": "HSiface",
                        "varname": "HsWorker@" + src_mchn_short,
                        "value": "Requested data doesn't exist anymore in HsBuffer. Abort request."})
                return None
                     
            elif ALERTSTOP < ALERTSTART:
                logging.error("sn_start & sn_stop time-stamps inverted. Abort request.")
                i3socket.send_json({"service": "HSiface",
                        "varname": "HsWorker@" + src_mchn_short,
                        "value": "ALERTSTOP < ALERTSTART. Abort request."})            
                return None
            
            elif BUFFSTOP < ALERTSTART:
                #logging.info( "Sn_start & sn_stop time-stamps error. \nAbort request."
                logging.error( "ALERTSTART is in the FUTURE ?!")
                i3socket.send_json({"service": "HSiface",
                        "varname": "HsWorker@" + src_mchn_short,
                        "value": "Requested data is younger than most recent HS data. Abort request."})     
                return None
            
            elif ALERTSTART < LAST_BUFFSTART < LAST_BUFFSTOP < ALERTSTOP < BUFFSTART:
                logging.info( "Data location case 8")
                hs_sourcedir = hs_sourcedir_last                
                logging.warning("ALERTSTART < lastRun < ALERSTOP < currentRun. Assign: all HS data of lastRun instead.")
                sn_start_file = LAST_OLDFILE
                sn_stop_file = LAST_CURF
                sn_start_file_str = hs_sourcedir +"HitSpool-" + str(LAST_OLDFILE) + ".dat"
                sn_stop_file = hs_sourcedir +"HitSpool-" + str(LAST_CURF) + ".dat"
                
            elif LAST_BUFFSTOP < ALERTSTART < BUFFSTART < BUFFSTOP < ALERTSTOP:
                logging.info( "Data location case 9")
                hs_sourcedir = hs_sourcedir_current               
                logging.warning("lastRun < ALERTSTART < currentRun < ALERSTOP.  Assign: all HS data of currentRun instead.")
                sn_start_file = OLDFILE
                sn_stop_file = CURF
                sn_start_file_str = hs_sourcedir +"HitSpool-" + str(OLDFILE) + ".dat"
                sn_stop_file = hs_sourcedir +"HitSpool-" + str(CURF) + ".dat"
                
                 
            # ---- HitSpool Data Access and Copy ----:
            #how many files n_rlv_files do we have to move and copy:
            logging.info( "Start & Stop File: " + str(sn_start_file) + " and " + str(sn_stop_file)) 
            if sn_start_file < sn_stop_file:
                n_rlv_files = ((sn_stop_file - sn_start_file) + 1) % MAXF
                logging.info( "NUMBER of relevant files = " + str(n_rlv_files))
            else:
                n_rlv_files = ((sn_stop_file - sn_start_file)+ MAXF + 1) % MAXF # mod MAXF for the case that sn_start & sn_stop are in the same HS file
                logging.info( "NUMBER of relevant files = " + str(n_rlv_files))
                
            # in case the alerts data is spread over both lastRun and currentRun directory, there will be two extra variable defined:    
            try:
                sn_stop_file2
                sn_stop_file3
                if sn_stop_file2 < sn_stop_file3:
                    n_rlv_files_extra = ((sn_stop_file3 - sn_stop_file2) + 1) % MAXF
                else:
                    n_rlv_files_extra = ((sn_stop_file3 - sn_stop_file2)+ MAXF + 1) % MAXF # mod MAXF for the case that sn_start & sn_stop are in the same HS file
                logging.info( "an additional Number of " + str(n_rlv_files_extra) + " files from currentRun")
                
            except NameError:
                pass
    
            # -- building tmp directory for relevant hs data copy -- # 
            truetrigger = TRUETRIGGER.strftime("%Y%m%d_%H%M%S") 
            truetrigger_dir = "SNALERT_" + truetrigger + "_" + src_mchn + "/"
            hs_copydest = copydir_dft + truetrigger_dir
            logging.info( "unique naming for folder: " + str(hs_copydest))
    
            #move these files aside into subdir /tmp/ to prevent from being overwritten from next hs cycle while copying:
            #make subdirectory "/tmp" . if it exist doesn't already
    
            if cluster == "localhost":         
                tmp_dir = "/home/david/TESTCLUSTER/testhub/tmp/" + truetrigger + "/"
            else:
                tmp_dir = "/mnt/data/pdaqlocal/tmp/SNALERT_" + truetrigger + "/"
            try:
                subprocess.check_call("mkdir -p " + tmp_dir, shell=True)
                logging.info( "created subdir for relevant hs files")
            except subprocess.CalledProcessError:
                logging.info( "Subdir in /mnt/data/padqlocal/tmp/ already exists") 
                pass
#            i3socket.send_json({"service": "HSiface",
#                                "varname": "HsWorker@" + src_mchn_short,
#                                "value": "tmp directory for hs file copy created"}) 
            
            # -- building file list -- # 
            copy_files_list = []   
            for i in range (n_rlv_files):
                sn_start_file_i = (sn_start_file+i)%MAXF
                next_file = re.sub("HitSpool-" + str(sn_start_file), "HitSpool-" + str(sn_start_file_i), sn_start_file_str)
                #logging.info( "relevant file: " + str(next_file))
                #move these files aside to prevent from being overwritten from next hs cycle while copying: 
                #do a hardlink here instead of real copy! "cp -a" ---> "cp -l"                      
                hs_tmp_copy = subprocess.check_call("cp -l " + next_file + " " + tmp_dir, shell=True)
                if hs_tmp_copy == 0:
                    next_tmpfile = tmp_dir + "HitSpool-" + str(sn_start_file_i) + ".dat"
                    copy_files_list.append(next_tmpfile)
                    logging.info("linked the file: " + str(next_file) + " to tmp directory")
                else:
                    logging.error("failed to link file " + str(sn_start_file_i) + " to tmp dir")      
                    i3socket.send_json({"service": "HSiface",
                                        "varname": "HsWorker@" + src_mchn_short,
                                        "value": "ERROR: linking hitspool file  %s to tmp dir failed" %  str(sn_start_file_i) })
                    
            # for data location case where the requested data is spread over both HS source dirs:        
            try:
                for i in range(n_rlv_files_extra):
                    sn_stop_file_i = (sn_stop_file2+i)%MAXF
                    next_file2 = re.sub("HitSpool-" + str(sn_stop_file2), "HitSpool-" + str(sn_stop_file_i), sn_stop_file_str2)
                    logging.info( "next relevant file: " + str(next_file2))
                    #move these files aside to prevent from being overwritten from next hs cycle while copying:
                    #do a hardlink here instead of real copy! "cp -a" ---> "cp -l"   
                    hs_tmp_copy = subprocess.check_call("cp -l " + next_file2 + " " + tmp_dir, shell=True)
                    if hs_tmp_copy == 0:
                        next_tmpfile2 = tmp_dir + "HitSpool-" + str(sn_stop_file_i) + ".dat"
                        #logging.info( "\nnext file to copy is: %s" % next_copy
                        copy_files_list.append(next_tmpfile2)
                        logging.info("linked the file: " + str(next_file) + " to tmp directory")
                        i3socket.send_json({"service": "HSiface",
                                        "varname": "HsWorker@" + src_mchn_short,
                                        "value": "linked %s to tmp dir: " % str(next_file)})
                    else:
                        logging.error("failed to link hitspool file " + str(sn_stop_file_i) + " to tmp dir")         
                    i3socket.send_json({"service": "HSiface",
                                        "varname": "HsWorker@" + src_mchn_short,
                                        "value": "ERROR: linking hitspool file  %s to tmp dir failed" %  str(sn_stop_file_i) })                         
            except NameError:
                pass
             
            logging.info("list of relevant files: " + str(copy_files_list))
                        
            copy_files_str = " ".join(copy_files_list) 
            
            #----- Add random Sleep time window ---------#
            #necessary in order to strech time window of rsync requests
            #Simultaneously rsyncing from 97 hubs caused issues in the past
            wait_time = random.uniform(1,5)
            logging.info( "wait with the rsync request for some seconds: " + str( wait_time))
            time.sleep(wait_time)
            
            # ---- Rsync the relevant files to 2ndbuild ---- #
            
            # ------- the REAL rsync command for SPS and SPTS:-----#
            # hitspool/ points internally to /mnt/data/pdaqlocal/HsDataCopy/ this is set fix on SPTS and SPS by Ralf Auer     
            if (cluster == "SPS") or (cluster == "SPTS") :  
                logging.info("default rsync destination is (rsync deamon): " + str(copydir_dft) + "on 2ndbuild")
                
                rsync_cmd = "nice rsync -avv --bwlimit=300 --log-format=%i%n%L " + copy_files_str + " " + hs_ssh_access + '::hitspool/' + truetrigger_dir        
                
                logging.info( "rsync does:\n " + str(rsync_cmd)) 
            
            #------ the localhost rsync command -----#           
            else:   
                rsync_cmd = "nice rsync -avv --bwlimit=300 --log-format=%i%n%L " + copy_files_str + " " + copydir_dft + truetrigger_dir
                
                logging.info("rsync command: " + str(rsync_cmd))             
                
            hs_rsync = subprocess.Popen(rsync_cmd, shell=True, bufsize=256, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            hs_rsync_out = hs_rsync.stdout.readlines()
            hs_rsync_err = hs_rsync.stderr.readlines()
            
            # --- catch rsync error --- #
            if len(hs_rsync_err) is not 0:
                logging.error("failed rsync process:\n" + str(hs_rsync_err))
                logging.info("KEEP tmp dir with data")
                i3socket.send_json({"service": "HSiface",
                                        "varname": "HsWorker@" + src_mchn_short,
                                        "value": "ERROR in rsync. Keep tmp dir."})
                i3socket.send_json({"service": "HSiface",
                            "varname": "HsWorker@" + src_mchn_short,
                            "value": hs_rsync_err})
            
                # send msg to HsSender
                report_json = json.dumps({"hubname": src_mchn_short, "alertid": alertid, "dataload": int(0), 
                                          "datastart": str(ALERTSTART), "datastop": str(ALERTSTOP), 
                                          "copydir": hs_copydest, "copydir_user": hs_copydir, "msgtype": "rsync_sum"})
                
                sender.send_json(report_json)
                logging.info("sent rsync report json to HsSender: " + str(report_json))
            
            # --- proceed if no error --- #
            else:
                logging.info("rsync out:\n" +str(hs_rsync_out))
                logging.info("successful copy of HS data from " + str(hs_sourcedir) + " at " + str(src_mchn) + " to " + str(hs_copydest) +" at " + str(hs_ssh_access))
                #report to I3Live successful data transfer
#                i3socket.send_json({"service": "HSiface",
#                    "varname": "HsWorker@" + src_mchn_short,
#                    "value": "data transferred to %s at %s " % (hs_copydest, hs_ssh_access)})

                #rsync_dataload = re.sub( r'total size is ', '',re.sub(r' speedup is [0-9]*\.[0-9]*\s', '',hs_rsync_out[-1]))
                rsync_dataload = re.search(r'(?<=total size is )[0-9]*', hs_rsync_out[-1])
                if rsync_dataload is not None:
                    dataload_mb = str(float(int(rsync_dataload.group(0))/1024**2))
                else:
                    dataload_mb = "TBD"
                
                #report about dataload of copied data directly to I3Live 
#                i3socket.send_json({"service": "HSiface",
#                                "varname": "HsWorker@" + src_mchn_short,
#                                "value": "dataload of %s in [MB]:\n%s" % (hs_copydest, dataload_mb)})
#                
                i3socket.send_json({"service": "HSiface",
                                    "varname": "HsWorker@" + src_mchn_short,
                                    "prio"    :   1,
                                    "value": " %s [MB] HS data transferred to %s " % (dataload_mb, hs_ssh_access)})
                logging.info(str("dataload of %s in [MB]:\n%s" % (hs_copydest, dataload_mb )))
                
                
                # send msg to HsSender to make start SPADE pickup
                report_json = json.dumps({"hubname": src_mchn_short, "alertid": alertid, "dataload": dataload_mb, 
                                          "datastart": str(ALERTSTART), "datastop": str(ALERTSTOP), 
                                          "copydir": hs_copydest, "copydir_user": hs_copydir, "msgtype": "rsync_sum"})
                
                sender.send_json(report_json)
                logging.info("sent rsync report json to HsSender: " + str(report_json))
                    
                # remove tmp dir:
                try:
                    remove_tmp_files = "rm -r " + tmp_dir
                    subprocess.check_call(remove_tmp_files, shell=True)
#                    i3socket.send_json({"service": "HSiface",
#                                        "varname": "HsWorker@" + src_mchn_short,
#                                        "value": "Deleted tmp dir"})
                    logging.info("tmp dir deleted.")
                    
                except subprocess.CalledProcessError:
                    logging.error("failed removing tmp files...")
                    
                    i3socket.send_json({"service": "HSiface",
                                        "varname": "HsWorker@" + src_mchn_short,
                                        "value": "ERROR: Deleting tmp dir failed"})
                    pass

            hs_rsync.stdout.flush()
            hs_rsync.stderr.flush()

            # -- also trasmitt the log file to the HitSpool copy directory:
            if (cluster == "SPS") or (cluster == "SPTS"):
                logfiledir = "/mnt/data/pdaqlocal/HsInterface/logs/workerlogs/"  
                hs_rsync_log_cmd = "nice rsync -avv --no-relative " + logfile + " " + hs_ssh_access + ":" + logfiledir
            
            else:
                logfiledir = "/home/david/data/HitSpool/copytest/logs/"
                hs_rsync_log_cmd = "nice rsync -avv --no-relative " + logfile + " " + logfiledir
            
            log_rsync = subprocess.Popen(hs_rsync_log_cmd, shell=True, bufsize=256, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            log_rsync_out = log_rsync.stdout.readlines()
            log_rsync_err = log_rsync.stderr.readlines()
            if len(log_rsync_err) is not 0:
                logging.error("failed to rsync logfile:\n" + str(log_rsync_err))
                
            else:
                logging.info("logfile transmitted to copydir: " + str(log_rsync_out))
                log_json = json.dumps({"hubname": src_mchn_short, "alertid": truetrigger, "logfiledir": logfiledir, "logfile_hsworker": logfile, "msgtype": "log_done"})
                sender.send_json(log_json)
                logging.info("sent json to HsSender: " + str(log_json))
            
            log_rsync.stdout.flush()
            log_rsync.stderr.flush()            
#class MyHubserver(object):
#    """
#    Creating the server socket on the hub and listen for messages from Publisher on access.
#    """
#    def worker(self):        
#
#        
#        # Socket to synchronize the Publisher with the Workers becomes obsolete in the Major Upgrade July 2013
##        syncclient = context.socket(zmq.PUSH)
##        syncclient.connect("tcp://"+spts_expcont_ip+":55562")
##        logging.info( "connected sync PUSH socket to  port 55562"
#
#        # send a synchronization request:
##        try:
##            syncclient.send("Hi! Hub wants to connect!")
##            logging.info( "syncservice sended sync request"
##        except KeyboardInterrupt:
##            sys.exit()
#            
#        # while True loop to keep connection open "forever"
#        while True:             
#            try:
#                logging.info("ready for new alert...")
#                message = subscriber.recv()
#                #logging.info( "received message")
#                self.message = message
#                logging.info("HsWorker got alert message:\n" +  str(message) + "\nfrom Publisher")
#                logging.info("start processing alert...\n")
#                newalert = MyAlert()
#                newalert.alert_parser(message, src_mchn, src_mchn_short)
#                
#                # if alert_parser succesful -> returns summary info from process: dataload, stop, start etc:                
##                sender.send_json({"hub": src_mchn, "dataload": "TBD", "start": "TBD", "stop": "TBD", "copy": "TBD" })
##                report_dict = {"hub": src_mchn, "dataload": "TBD", "start": "TBD", "stop": "TBD", "copydir": "TBD" }
#
#
#
#
#                report_json = json.dumps(report_dict)
#                sender.send_json(report_json)
#                logging.info("sent report json to HsSender: " + str(report_json))
#                    
#            except KeyboardInterrupt:
#                logging.warning("interruption received, shutting down...")
#                sys.exit()

if __name__=='__main__':

    """
    "sndaq/HsGrabber"  "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        --------------
    | REQ     | <----->| REP     |         | IcHub n |        | 2ndbuild    |
    -----------        | PUB     | ------> | SUB   n |        | PUSH(13live)|
                       ----------          | PUSH    | ---->  | PULL        |
                                            ---------         --------------
    HsWorker.py of the HitSpool Interface.
    Gets the relevant HS files according to the requested time-window and sends them to the 
    specified copy directory.
    
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
        logfile = "/home/david/TESTCLUSTER/testhub/logs/hsworker_" + src_mchn_short + ".log" 
    else:
        logfile = "/mnt/data/pdaqlocal/HsInterface/logs/hsworker_" + src_mchn_short + ".log"

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
                        level=logging.INFO, stream=sys.stdout, 
                        datefmt= '%Y-%m-%d %H:%M:%S', 
                        filename=logfile)
    logging.info( "this Worker runs on: " + str(src_mchn_short))
    
    context = zmq.Context()
    #spts_expcont_ip = "10.2.2.12"
    #spts_expcont_ip = "expcont"
    
    # Socket for I3Live on expcont
    i3socket = context.socket(zmq.PUSH) # former ZMQ_DOWNSTREAM is depreciated 
    # Socket to receive message on:
    subscriber = context.socket(zmq.SUB)
    # Socket to send message to :
    sender = context.socket(zmq.PUSH)
    
    if cluster == "localhost":
        i3socket.connect("tcp://localhost:6668") 
        logging.info("connected to i3live: port 6668 on localhost")
        subscriber.setsockopt(zmq.IDENTITY, src_mchn)
        subscriber.setsockopt(zmq.SUBSCRIBE, "")#        subscriber.connect("tcp://"+spts_expcont_ip+":55561")
        subscriber.connect("tcp://localhost:55561")
        logging.info("SUB-Socket to receive message from HsPublisher: port 55561 on localhost")        
        sender.connect("tcp://localhost:55560")
        logging.info( "PUSH-Socket to send message to HsSender: port 55560 on localhost")
    else:
        i3socket.connect("tcp://expcont:6668") 
        logging.info("connected to i3live: port 6668 on expcont")
        subscriber.setsockopt(zmq.IDENTITY, src_mchn)
        subscriber.setsockopt(zmq.SUBSCRIBE, "")#        subscriber.connect("tcp://"+spts_expcont_ip+":55561")
        subscriber.connect("tcp://expcont:55561")
        logging.info("SUB-Socket to receive message from HsPublisher: port 55561 on expcont")        
        sender.connect("tcp://2ndbuild:55560")
        logging.info( "PUSH-Socket to send message to HsSender: port 55560 on 2ndbuild")        

    while True:             
        try:
            logging.info("ready for new alert...")
            message = subscriber.recv()
            #logging.info( "received message")
            #self.message = message
            logging.info("HsWorker received alert message:\n" +  str(message) + "\nfrom Publisher")
            logging.info("start processing alert...")
            newalert = MyAlert()
            newalert.alert_parser(message, src_mchn, src_mchn_short, cluster)
            
            # if alert_parser successful -> already sent info during alert_parsing to HsSender & i3Live 
            # if not successful : sent report now:        
#                sender.send_json({"hub": src_mchn, "dataload": "TBD", "start": "TBD", "stop": "TBD", "copy": "TBD" })
#                report_dict = {"hub": src_mchn, "dataload": "TBD", "start": "TBD", "stop": "TBD", "copydir": "TBD" }
 
        except KeyboardInterrupt:
            logging.warning("interruption received, shutting down...")

            sys.exit()


#    x = MyHubserver()
#    x.worker()
