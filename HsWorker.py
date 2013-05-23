#!/usr/bin/python
#
#
#Hit Spool Worker to be run on hubs
#author: dheereman
#
import time
"""
"sico_tester"        "HsPublisher"      "HsWorker"        "HsSender"
-----------        -----------
| sni3daq |        | expcont |         -----------        ------------
| REQUEST | <----->| REPLY   |         | IcHub n |        | expcont  |
-----------        | PUSH    | ------> | PULL   n|        | PULL     |
                   ----------          | PUSH    | ---->  |          |
                                        ---------          -----------
                                                                 
"""

from datetime import datetime, timedelta
import re, sys
import zmq #@UnresolvedImport
import subprocess
import json
import random

context = zmq.Context()
#spts_expcont_ip = "10.2.2.12"
spts_expcont_ip = "expcont"

p = subprocess.Popen(["hostname"], stdout = subprocess.PIPE)
out, err = p.communicate()
src_mchn = out.rstrip()
print "this Worker is running on: " , src_mchn
logfile = "/mnt/data/pdaqlocal/HsInterface/hs_summary_"+src_mchn+".log"


class MyAlert(object):
    """
    This class
    1. analyzes the alert message
    2. looks for the requested files / directory 
    3. copies them over to the requested directory specified in the message.
    4. writes a short report about was has been done.
    """
    
    def alert_parser(self, alert, src_mchn):
        """
        Parse the Alert message for starttime, stoptime,
        sn-alert-trigger-time-stamp and directory where-to the data has to be copied.
        """
        hs_sourcedir_current     = '/mnt/data/pdaqlocal/currentRun/'
        hs_sourcedir_last        = '/mnt/data/pdaqlocal/lastRun/'
        
        #hs_copydest_list = list() 
        fsummary = open(logfile, "a")
        packer_start = str(datetime.utcnow())
        print >> fsummary, "\n**********************************************\nStarted HitSpoolWorker on\n%s\nat:\n%s" % (src_mchn, packer_start)
        fsummary.close()
        

#        # --- Parsing alert message JSON ----- :
        
        print "the message is of type: " , type(alert), "and looks like:\n", alert
        
        alert_info = json.loads(alert)
        print "alert_info message in json format: \n" , alert_info , "\nand is of type: \n", type(alert_info)
        start = int(alert_info[0]["start"]) # timestamp in DAQ units as a string
        stop = int(alert_info[0]['stop'])   # timestamp in DAQ units as a string
        hs_user_machinedir = alert_info[0]['copy'] # should be something like: pdaq@expcont:/mnt/data/pdaqlocal/HsDataCopy/
        print "contains: " , start, " ", stop, " " , hs_user_machinedir
        



        try:
            sn_start = int(start)
            print "SN START [ns] = ", sn_start
            sn_start_utc = str(datetime(2013,1,1) + timedelta(seconds=sn_start*1.0E-9))  #sndaq time units are nanoseconds
            print "SNDAQ time-stamp in UTC: ", sn_start_utc
            ALERTSTART = datetime.strptime(sn_start_utc,"%Y-%m-%d %H:%M:%S.%f")
            print "ALERTSTART = ", ALERTSTART 
            print "SN START = %d\n\
            in UTC = %s" %(sn_start, ALERTSTART)
            TRUETRIGGER = ALERTSTART + timedelta(0,30)          # time window around trigger is [-30,+60] -> TRUETRIGGER = ALERTSTART + 30seconds 
            print "TRUETRIGGER = time-stamp when SN candidate trigger: %s" % TRUETRIGGER
            
            
        except (TypeError,ValueError):
            print "ERROR in json message parsing: no start timestamp found. \nAbort request..."
            fsummary = open(logfile, "a")
            print >> fsummary, "No timestamps found in alert message. \nAbort request... "
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close()  
            return None
        else:
            pass
        
        try:
            sn_stop = int(stop)
            print "SN STOP [ns] = ", sn_stop
            sn_stop_utc = str(datetime(2013,1,1) + timedelta(seconds=sn_stop*1.0E-9))  #sndaq time units are nanosecond
            print "SNDAQ time-stamp in UTC: ", sn_stop_utc
            ALERTSTOP = datetime.strptime(sn_stop_utc,"%Y-%m-%d %H:%M:%S.%f")
            print "ALERTSTOP =", ALERTSTOP
        except (TypeError,ValueError):
            print "ERROR in json message: no stop timestamp found. \nAbort request..."
            fsummary = open(logfile, "a")
            print >> fsummary, "No timestamps found in alert message. \nAbort request... "
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close()
            return None            
            
        try :
            print "HS machinedir = ", hs_user_machinedir
            hs_copydir = re.sub('\w+\@\w+:', '', hs_user_machinedir)
            print " HS COPYDIR = ", hs_copydir
            hs_ssh_access = re.sub(':/\w+/\w+/\w+/\w+/', "", hs_user_machinedir)
        except (TypeError, ValueError):
            #"ERROR in json message: no copydir found. \nAbort request..."
            fsummary = open(logfile, "a")
            print >> fsummary, "ERROR in json message: no copydir found. \nAbort request..."
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close()
            
        #------Parsing hitspool info.txt from currentRun to find the requested files-------:        
        # Find the right file(s) that contain the start/stoptime and the actual sn trigger time stamp=sntts
        # useful: datetime.timedelta
        try:
            filename = hs_sourcedir_current+'info.txt'
            print filename
            fin = open (filename)
        except IOError as (errno, strerror):
            print "cannot open %s" % filename
            print "I/O error({0}): {1}".format(errno, strerror)
            fsummary = open(logfile, "a")
            print >> fsummary, "I/O error({0}): {1}".format(errno, strerror)
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close()
            return None
        else:
            infodict= {}
            for line in open(filename):
                (key, val) = line.split()
                infodict[str(key)] = int(val)
            fin.close()
        
        startrun = int(infodict['T0'])              # time-stamp of first HIT at run start -> this HIT is not in buffer anymore if HS_Loop > 0 !            
        CURT = infodict['CURT']                     # current file's first hit time stamp in DAQ units
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
        print "get information from %s..." % filename

        #converting the INFO dict's first entry into a datetime object:
        startrun_utc = str(datetime(2013,1,1) + timedelta(seconds=startrun*1.0E-10))    #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        RUNSTART = datetime.strptime(startrun_utc,"%Y-%m-%d %H:%M:%S.%f")
        startdata_utc = str(datetime(2013,1,1) + timedelta(seconds=startdata*1.0E-10))  #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        BUFFSTART = datetime.strptime(startdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        stopdata_utc = str(datetime(2013,1,1) + timedelta(seconds=CURT*1.0E-10))        #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        BUFFSTOP = datetime.strptime(stopdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        #outputstring1 = "first HIT ever in this Run on this String in nanoseconds: %d\noldest HIT's time-stamp existing in buffer in nanoseconds: %d\noldest HIT's time-stamp in UTC:%s\nnewest HIT's timestamp in nanoseconds: %d\nnewest HIT's time-stamp in UTC: %s\neach hit spool file contains %d * E-10 seconds of data\nduration per file in integer seconds: %d\nhit spooling writes to %d files per cycle \nHitSpooling writes to newest file: HitSpool-%d since %d DAQ units\nThe Hit Spooler is currently writing iteration loop: %d\nThe oldest file is: HitSpool-%s\n"
        print "first HIT ever in this Run on this String in nanoseconds: %d\n\
        oldest HIT's time-stamp existing in buffer in nanoseconds: %d\n\
        oldest HIT's time-stamp in UTC:%s\n\
        newest HIT's timestamp in nanoseconds: %d\n\
        newest HIT's time-stamp in UTC: %s\n\
        each hit spool file contains %d * E-10 seconds of data\n\
        duration per file in integer seconds: %d\n\
        hit spooling writes to %d files per cycle \n\
        HitSpooling writes to newest file: HitSpool-%d since %d DAQ units\n\
        The Hit Spooler is currently writing iteration loop: %d\n\
        The oldest file is: HitSpool-%s" %( startrun, startdata, BUFFSTART, CURT, BUFFSTOP, IVAL, IVAL_SEC, MAXF, CURF, TFILE, HS_LOOP, OLDFILE)

        fsummary = open(logfile, "a")
        print >> fsummary, "From currentRun info.txt:\n\
        RUNSTART= %s\n\
        BUFFSTART= %s\n\
        BUFFSTOP=  %s\n\
        IVAL_SEC=  %d\n\
        CURF=  HitSpool-%s\n\
        MAXF= %s\n\
        HS_LOOP= %s\n" %(RUNSTART, BUFFSTART, BUFFSTOP, IVAL_SEC, CURF, MAXF, HS_LOOP)
        fsummary.close()
        
        
        
        #------ Parsing info.txt from lastRun in case old data is needed -------#
        
                #------Parsing hitspool info.txt from currentRun to find the requested files-------:        
        # Find the right file(s) that contain the start/stoptime and the actual sn trigger time stamp=sntts
        # useful: datetime.timedelta
        try:
            filename = hs_sourcedir_last+'info.txt'
            print filename
            fin = open (filename)
        except IOError as (errno, strerror):
            print "cannot open %s" % filename
            print "I/O error({0}): {1}".format(errno, strerror)
            fsummary = open(logfile, "a")
            print >> fsummary, "I/O error({0}): {1}".format(errno, strerror)
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close()
            return None
        else:
            infodict2= {}
            for line in open(filename):
                (key, val) = line.split()
                infodict2[str(key)] = int(val)
            fin.close()
        
        last_startrun = int(infodict2['T0'])                   # time-stamp of first HIT at run start -> this HIT is not in buffer anymore if HS_Loop > 0 !            
        LAST_CURT = infodict2['CURT']                     # current time stamp in DAQ units
        LAST_IVAL = infodict2['IVAL']                     # len of each file in integer 0.1 nanoseconds
        LAST_IVAL_SEC = IVAL*1.0E-10                     # len of each file in integer seconds
        LAST_CURF = infodict2['CURF']                     # file index of currently active hit spool file
        LAST_MAXF = infodict2['MAXF']                     # number of files per cycle
#        startdata = int(CURT - ((MAXF-1)*IVAL_SEC))      # oldest, in hit spool buffer existing time stamp in DAQ units  
        LAST_TFILE = (CURT - startrun)%IVAL              # how long already writing to current file, time in DAQ units 
        LAST_TFILE_SEC = TFILE*1.0E-10  
        LAST_HS_LOOP = int((CURT-startrun)/(MAXF*IVAL))
        if LAST_HS_LOOP == 0:
            LAST_OLDFILE = 0              # file index of oldest in buffer existing file
            last_startdata = last_startrun
        else:
            LAST_OLDFILE = (LAST_CURF+1)
#            startdata = int(CURT - (((MAXF-1)*IVAL_SEC) + TFILE))    # oldest, in hit spool buffer existing time stamp in DAQ units
            last_startdata = int(LAST_CURT - (LAST_MAXF-1)*LAST_IVAL - LAST_TFILE)    # oldest, in hit spool buffer existing time stamp in DAQ units         
        print "get information from %s..." % filename

        #converting the INFO dict's first entry into a datetime object:
        last_startrun_utc = str(datetime(2013,1,1) + timedelta(seconds=last_startrun*1.0E-10))    #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        LAST_RUNSTART = datetime.strptime(last_startrun_utc,"%Y-%m-%d %H:%M:%S.%f")
        last_startdata_utc = str(datetime(2013,1,1) + timedelta(seconds=last_startdata*1.0E-10))  #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        LAST_BUFFSTART = datetime.strptime(last_startdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        last_stopdata_utc = str(datetime(2013,1,1) + timedelta(seconds=LAST_CURT*1.0E-10))        #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        LAST_BUFFSTOP = datetime.strptime(last_stopdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        #outputstring1 = "first HIT ever in this Run on this String in nanoseconds: %d\noldest HIT's time-stamp existing in buffer in nanoseconds: %d\noldest HIT's time-stamp in UTC:%s\nnewest HIT's timestamp in nanoseconds: %d\nnewest HIT's time-stamp in UTC: %s\neach hit spool file contains %d * E-10 seconds of data\nduration per file in integer seconds: %d\nhit spooling writes to %d files per cycle \nHitSpooling writes to newest file: HitSpool-%d since %d DAQ units\nThe Hit Spooler is currently writing iteration loop: %d\nThe oldest file is: HitSpool-%s\n"
        print "first HIT ever in last Run on this String in nanoseconds: %d\n\
        oldest HIT's time-stamp existing in buffer in nanoseconds: %d\n\
        oldest HIT's time-stamp in UTC:%s\n\
        newest HIT's timestamp in nanoseconds: %d\n\
        newest HIT's time-stamp in UTC: %s\n\
        each hit spool file contains %d * E-10 seconds of data\n\
        duration per file in integer seconds: %d\n\
        hit spooling writes to %d files per cycle \n\
        HitSpooling writes to newest file: HitSpool-%d since %d DAQ units\n\
        The Hit Spooler is currently writing iteration loop: %d\n\
        The oldest file is: HitSpool-%s" %( last_startrun, last_startdata, LAST_BUFFSTART, LAST_CURT, LAST_BUFFSTOP, LAST_IVAL, LAST_IVAL_SEC, LAST_MAXF, LAST_CURF, LAST_TFILE, LAST_HS_LOOP, LAST_OLDFILE)

        fsummary = open(logfile, "a")
        print >> fsummary, "From lastRun info.txt:\n\
        LAST RUNSTART= %s\n\
        LAST BUFFSTART= %s\n\
        LAST BUFFSTOP=  %s\n\
        LAST IVAL_SEC=  %d\n\
        LAST CURF=  HitSpool-%s\n\
        LAST MAXF= %s\n\
        LAST HS_LOOP= %s\n" %(LAST_RUNSTART, LAST_BUFFSTART, LAST_BUFFSTOP, LAST_IVAL_SEC, LAST_CURF, LAST_MAXF, LAST_HS_LOOP)
        fsummary.close()
        
        
        
        
        #------ CHECK ALERT DATA LOCATION  -----#
        
        print "ALERTSTART has format: %s" %  ALERTSTART
        print "ALERTSTOP has format: %s" % ALERTSTOP

        # Check if required sn_start / sn_stop data still exists in buffer. 
        # If sn request comes in from earlier times -->  Check lastRun directory
        # Decide in this section which Run directory is the correct one: lastRun or currentRun ?

        
        if LAST_BUFFSTOP < ALERTSTART < BUFFSTART < ALERTSTOP:
            print "Data location case 1"          

            hs_sourcedir = hs_sourcedir_current
            print "HitSpool source data is in directory: " , hs_sourcedir
            
            sn_start_file = OLDFILE
            print "Sn_start doesn't exits in %s buffer anymore! Start with oldest possible data: HitSpool-%s" % (hs_sourcedir, OLDFILE)
            fsummary = open(logfile, "a")
            print >> fsummary, "Sn_start doesn't exits in buffer anymore! Start with oldest possible data: HitSpool-%s" % OLDFILE
            sn_start_file_str = hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat" 
            
            timedelta_stop = (ALERTSTOP - BUFFSTART)
            #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
            print "time diff from hit spool buffer start to sn_stop in seconds: %s" % timedelta_stop_seconds
            sn_stop_file = int(((timedelta_stop_seconds/IVAL_SEC) + OLDFILE) % MAXF)        
            #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            print "sn_stops's data is included in file %s" % (sn_stop_file_str)            

          

        elif BUFFSTART < ALERTSTART < ALERTSTOP < BUFFSTOP: 
            print "Data location case 2"          

            hs_sourcedir = hs_sourcedir_current
            print "HitSpool source data is in directory: " , hs_sourcedir
            
            timedelta_start = (ALERTSTART - BUFFSTART) # should be a datetime.timedelta object 
            #time passed after data_start when sn alert started: sn_start - data_start in seconds:
            timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
            print "There are \n%s\n seconds of data before the Alert started" % timedelta_start_seconds            
            sn_start_file = int(((timedelta_start_seconds/IVAL_SEC) + OLDFILE) % MAXF)
            sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
            print "sn_start's data is included in file %s" % (sn_start_file_str)    

            timedelta_stop = (ALERTSTOP - BUFFSTART)
            #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
            print "time diff from hit spool buffer start to sn_stop in seconds: %s" % timedelta_stop_seconds
            sn_stop_file = int(((timedelta_stop_seconds/IVAL_SEC) + OLDFILE) % MAXF)        
            #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            print "sn_stops's data is included in file %s" % (sn_stop_file_str) 

        
            
        elif LAST_BUFFSTOP < ALERTSTART < ALERTSTOP < BUFFSTART:
            print "Data location case 3"          

            print "requested data doesn't exist in HitSpool Buffer anymore! \nAbort request."
            fsummary = open(logfile, "a")
            print >> fsummary, "start : %s " % (sn_start )
            print >> fsummary, "stop : %s " % (sn_stop )
            print >> fsummary, "UTC start time stamp: %s" % str(ALERTSTART)
            print >> fsummary, "UTC stop time stamp: %s" % str(ALERTSTOP)
            print >> fsummary, "requested data doesn't exist in HitSpool Buffer anymore! \nAbort request... "
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close() 
            return None
        
        
        # tricky case: ALERTSTART in lastRun and LAERTSTOP in currentRun
        elif LAST_BUFFSTART < ALERTSTART < LAST_BUFFSTOP < ALERTSTOP:
            print "Data location case 4"          

            hs_sourcedir = hs_sourcedir_last
            print "HitSpool start data is partially in directory: " , hs_sourcedir
            # -- start file --#
            timedelta_start = (ALERTSTART - LAST_BUFFSTART) # should be a datetime.timedelta object 
            #time passed after data_start when sn alert started: sn_start - data_start in seconds:
            timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
            print "There are \n%s\n seconds of data before the Alert started" % timedelta_start_seconds            
            sn_start_file = int(((timedelta_start_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)
            sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
            print "SN_START file: %s" % (sn_start_file_str)         
            
            # -- stop file --#
            print "sn_stop's data is not in buffer anymore. Take LAST_CURF as ALERTSTOP"
            sn_stop_file = LAST_CURF
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            print "SN_STOP file:  %s" % (sn_stop_file_str)
                
            # if sn_stop is available from currentRun:
            # define 3 sn_stop files indices to have two data sets: sn_start-sn_stop1 & sn_stop2-sn_top3    
            if BUFFSTART < ALERTSTOP:
                print "SN_START & SN_STOP distributed over lastRun and currentRun"
                print "add relevant files from currentRun directory..."
                #in currentRun:
                sn_stop_file2 = OLDFILE
                sn_stop_file_str2 = hs_sourcedir_current +"HitSpool-" + str(sn_stop_file2) + ".dat"
                print "SN_STOP part2 file %s" % (sn_stop_file_str2)                
                timedelta_stop3 = (ALERTSTOP - BUFFSTART)
                #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
                timedelta_stop_seconds3 =  (timedelta_stop3.seconds + timedelta_stop3.days * 24 * 3600)      
                print "time diff from hit spool buffer start to sn_stop in seconds: %s" % timedelta_stop_seconds3
                sn_stop_file3 = int(((timedelta_stop_seconds3/IVAL_SEC) + OLDFILE) % MAXF)        
                #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
                sn_stop_file_str3 = hs_sourcedir_current +"HitSpool-" + str(sn_stop_file3) + ".dat"
                print "SN_STOP part3 file %s" % (sn_stop_file_str3)                
                

        
#        elif ALERTSTOP > BUFFSTART and ALERTSTOP > ALERTSTART:   # sn_stop > BUFFEND is not possible by definition
#            timedelta_stop = (ALERTSTOP - BUFFSTART)
#            #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
#            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
#            print "time diff from hit spool buffer start to sn_stop in seconds: %s" % timedelta_stop_seconds
#            sn_stop_file = int(((timedelta_stop_seconds/IVAL_SEC) + OLDFILE) % MAXF)        
#            #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
#            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
#            print "sn_stops's data is included in file %s" % (sn_stop_file_str)
            
            
        elif LAST_BUFFSTART < ALERTSTART < ALERTSTOP < LAST_BUFFSTOP:
            print "Data location case 5"          
            hs_sourcedir = hs_sourcedir_last
            print "HitSpool source data is in directory: " , hs_sourcedir  
            timedelta_start = (ALERTSTART - LAST_BUFFSTART) # should be a datetime.timedelta object 
            #time passed after data_start when sn alert started: sn_start - data_start in seconds:
            timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
            print "There are \n%s\n seconds of data before the Alert started" % timedelta_start_seconds            
            sn_start_file = int(((timedelta_start_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)
            sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
            print "sn_start's data is included in file %s" % (sn_start_file_str)     
            
            
            
            timedelta_stop = (ALERTSTOP - LAST_BUFFSTART)
            #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
            print "time diff from hit spool buffer start to sn_stop in seconds: %s" % timedelta_stop_seconds
            sn_stop_file = int(((timedelta_stop_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)        
            #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            print "sn_stops's data is included in file %s" % (sn_stop_file_str)        


        elif ALERTSTART < LAST_BUFFSTART < ALERTSTOP < LAST_BUFFSTOP:
            print "Data location case 6"
            hs_sourcedir = hs_sourcedir_last
            print "HitSpool source data is in directory: " , hs_sourcedir            
            sn_start_file = LAST_OLDFILE
            print "Sn_start doesn't exits in %s buffer anymore! Start with oldest possible data: HitSpool-%s" % (hs_sourcedir, LAST_OLDFILE)
            fsummary = open(logfile, "a")
            print >> fsummary, "Sn_start doesn't exits in buffer anymore! Start with oldest possible data: HitSpool-%s" % LAST_OLDFILE
            sn_start_file_str = hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"               
            
            timedelta_stop = (ALERTSTOP - LAST_BUFFSTART)
            #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
            print "time diff from hit spool buffer start to sn_stop in seconds: %s" % timedelta_stop_seconds
            sn_stop_file = int(((timedelta_stop_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)        
            #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            print "sn_stops's data is included in file %s" % (sn_stop_file_str)
            
            
        elif ALERTSTART < ALERTSTOP < LAST_BUFFSTART:
            print "Data location case 7"
            print "requested data doesn't exist in HitSpool Buffer anymore! \nAbort request."
            fsummary = open(logfile, "a")
            print >> fsummary, "start : %s" % (sn_start)
            print >> fsummary, "stop : %s" % (sn_stop)
            print >> fsummary, "UTC start time stamp: %s" % str(ALERTSTART)
            print >> fsummary, "UTC stop time stamp: %s" % str(ALERTSTOP)            
            print >> fsummary, "requested data doesn't exist in HitSpool Buffer anymore!\nAbort request... "
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close() 
            return None
                
            
        elif ALERTSTOP < ALERTSTART:
            #print "Sn_start & sn_stop time-stamps error. \nAbort request."
            fsummary = open(logfile, "a")
            #sys.exit("Sn_start & sn_stop time-stamps error. \nAbort request.")
            print >> fsummary, "start : %s" % (sn_start)
            print >> fsummary, "stop : %s" % (sn_stop)
            print >> fsummary, "UTC start time stamp: %s" % str(ALERTSTART)
            print >> fsummary, "UTC stop time stamp: %s" % str(ALERTSTOP)      
            print >> fsummary, "Sn_start & sn_stop time-stamps error. \nAbort request... "
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close()
            return None
        
        elif BUFFSTOP < ALERTSTART:
            #print "Sn_start & sn_stop time-stamps error. \nAbort request."
            print "ALERTSTART is in the FUTURE ?!"
            fsummary = open(logfile, "a")
            #sys.exit("Sn_start & sn_stop time-stamps error. \nAbort request.")
            print >> fsummary, "start : %s" % (sn_start)
            print >> fsummary, "stop : %s" % (sn_stop)
            print >> fsummary, "UTC start time stamp: %s" % str(ALERTSTART)
            print >> fsummary, "UTC stop time stamp: %s" % str(ALERTSTOP)      
            print >> fsummary, "Sn_start & sn_stop time-stamps error.\nAbort request... "
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close()
            return None
                
        
            
            
        
        fsummary = open(logfile, "a")
        print >> fsummary, "start : %s \nis included in \n%s" % (sn_start ,sn_start_file_str)
        try:
            print >> fsummary, "additional relevant files added from currentRun dir:\n %s until %s" %(sn_stop_file2, sn_stop_file3) 
            print >> fsummary, "stop : %s \nis included in \n%s" % (sn_stop, sn_stop_file_str)
        except NameError:
            print >> fsummary, "stop : %s \nis included in \n%s" % (sn_stop, sn_stop_file_str)
            pass
        print >> fsummary, "UTC start time stamp: %s" % str(ALERTSTART)
        print >> fsummary, "UTC stop time stamp: %s" % str(ALERTSTOP)
        fsummary.close()



        # ---- HitSpoolData Access and Copy ----:
        #how many files n_rlv_files do we have to move and copy:

        if sn_start_file < sn_stop_file:
            n_rlv_files = ((sn_stop_file - sn_start_file) + 1) % MAXF
            print "NUMBER of relevant files = %s " % n_rlv_files
        else:
            n_rlv_files = ((sn_stop_file - sn_start_file)+ MAXF + 1) % MAXF # mod MAXF for the case that sn_start & sn_stop are in the same HS file
            print "NUMBER of relevant files = %s " % n_rlv_files
            
        # in case the alerts data is spread over both lastRun and currentRun directory, there will be two extra variable defined:    
        try:
            sn_stop_file2
            sn_stop_file3
            if sn_stop_file2 < sn_stop_file3:
                n_rlv_files_extra = ((sn_stop_file3 - sn_stop_file2) + 1) % MAXF
            else:
                n_rlv_files_extra = ((sn_stop_file3 - sn_stop_file2)+ MAXF + 1) % MAXF # mod MAXF for the case that sn_start & sn_stop are in the same HS file
            print "an additional Number of %s files from currentRun" % n_rlv_files_extra
            
        except NameError:
            pass


        # -- building tmp directory for relevant hs data copy -- # 
        truetrigger = TRUETRIGGER.strftime("%Y%m%d_%H%M%S") 
        truetrigger_dir = "SNALERT_" + truetrigger+"_"+src_mchn + "/"
        hs_copydest = hs_copydir + truetrigger_dir
        print "unique naming for folder: %s " % hs_copydest

        #move these files aside into subdir /tmp/ to prevent from being overwritten from next hs cycle while copying:
        #make subdirectory "/tmp" . if it exists already
        tmp_dir = "/mnt/data/pdaqlocal/tmp/SNALERT_"+truetrigger + "/"
        try:
            subprocess.check_call("mkdir -p " + tmp_dir, shell=True)
            print "created subdir for relevant hs files"
        except subprocess.CalledProcessError:
            print "Subdir  in /mnt/data/padqlocal/tmp/ already exists" 
            pass
    
    
    
        # -- buiilding file list -- # 
        copy_files_list = []   
        for i in range (n_rlv_files):
            sn_start_file_i = (sn_start_file+i)%MAXF
            next_file = re.sub("HitSpool-" + str(sn_start_file), "HitSpool-" + str(sn_start_file_i), sn_start_file_str)
            print "relevant file: " , next_file
            #move these files aside to prevent from being overwritten from next hs cycle while copying:    
            hs_tmp_copy = subprocess.check_call("cp -a " + next_file + " " + tmp_dir, shell=True)
            if hs_tmp_copy == 0:
                next_tmpfile = tmp_dir + "HitSpool-" + str(sn_start_file_i) + ".dat"
                #print "\nnext file to copy is: %s" % next_copy
                copy_files_list.append(next_tmpfile)
            else:
                print "ERROR: copy hitspool file  %s to tmp dir failed"      %    str(sn_start_file_i)  
                
        try:
            for i in range(n_rlv_files_extra):
                sn_stop_file_i = (sn_stop_file2+i)%MAXF
                next_file2 = re.sub("HitSpool-" + str(sn_stop_file2), "HitSpool-" + str(sn_stop_file_i), sn_stop_file_str2)
                print "relevant file: " , next_file2
                #move these files aside to prevent from being overwritten from next hs cycle while copying:    
                hs_tmp_copy = subprocess.check_call("cp -a " + next_file2 + " " + tmp_dir, shell=True)
                if hs_tmp_copy == 0:
                    next_tmpfile2 = tmp_dir + "HitSpool-" + str(sn_stop_file_i) + ".dat"
                    #print "\nnext file to copy is: %s" % next_copy
                    copy_files_list.append(next_tmpfile2)
                else:
                    print "ERROR: copy hitspool file  %s to tmp dir failed"      %    str(sn_stop_file_i)                         
        except NameError:
            pass
         
        print "list of relevant files %s" % copy_files_list
        copy_files_str = " ".join(copy_files_list) 
        print "joined string of relevant files :\n %s" % copy_files_str
        print "last relevant is:\n%s" % sn_stop_file_str
        
        #----- Add random Sleep time window ---------#
        # necessary in order to strech time window of rsync requests. 
        #Simultaneously rsyncing from 97 hubs caused issues in the past
        
        wait_time = random.uniform(1,10)
        print "wait for %s seconds with the rsync request..." % wait_time
        fsummary = open(logfile, "a")
        print >> fsummary, "wait for %s seconds with the rsync request..." % wait_time
        fsummary.close()
        time.sleep(wait_time)
        
        
        
        
        # ---- Rsync the relevant files to 2ndbuild ---- #
#        rsync_cmd = "nice rsync -avv --bwlimit=30 --log-format=%i%n%L " + copy_files_str + " " + hs_ssh_access + ':' + hs_copydest + " >>" + logfile
#        rsync_cmd = "nice rsync -avv --bwlimit=100000 --log-format=%i%n%L " + copy_files_str + " " + hs_ssh_access + ':' + hs_copydest + " >>" + logfile
        
        # use a special encryption flag for reducing the cpu usage on the hub: 
#        rsync_cmd = "nice rsync -avv -e 'ssh -c arcfour' --bwlimit=300 --log-format=%i%n%L " + copy_files_str + " " + hs_ssh_access + ':' + hs_copydest + " >>" + logfile

        #running rsync daemon --> "::" instead of single ':'
        rsync_cmd = "nice rsync -avv --bwlimit=100 --log-format=%i%n%L " + copy_files_str + " " + hs_ssh_access + '::hitspool/' + truetrigger_dir + " >>" + logfile
        
        print "rsync does:\n %s" % rsync_cmd 
        fsummary = open(logfile, "a")
        print >> fsummary, "\n *** from rsync process: ***\n"
        fsummary.close()

        
        try:
            hs_rsync = subprocess.check_call(rsync_cmd,shell=True)
            print hs_rsync
            #return hs_rsync
            print "copied data from %s at machine %s to %s at machine %s" % (hs_sourcedir, src_mchn, hs_copydest, hs_ssh_access)
            print "data is copied & stored to %s at %s " % (hs_copydest, hs_ssh_access)
            fsummary = open(logfile, "a")
            print >>fsummary, "data is copied to %s at %s " % (hs_copydest, hs_ssh_access)
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close()
                
        except subprocess.CalledProcessError:
            print "\nError in rsync from %s to %s....\n" %(src_mchn, hs_ssh_access)
            fsummary = open(logfile, "a")
            print >> fsummary, "rsyncing failed !!"
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at:\n%s\n**********************************************\n" % packer_stop
            fsummary.close()
        
        try:
            remove_tmp_files = "rm -rv " + tmp_dir
            rm_tmp = subprocess.check_call(remove_tmp_files, shell=True)
            
        except subprocess.CalledProcessError:
            print "Error while removing tmp files..."
            pass
            
            
            
            
            
            
    def info_report(self, logfile):
        """
        summerizes what has been done: hostname, dataload copied and timestamps of the alert
        """
        infolist = [src_mchn]
        dl_list = [] #new empty list to store the data load patterns found in the logfile
        start_list = []
        stop_list = []
        copydest_list = []
        report_dict = {}
        #print "the infolist contains: ", infolist
        dl_pattern = re.compile(r'total\ssize\sis\s[0-9]*', flags=re.MULTILINE)
        sn_start_pattern = re.compile(r'start\s:\s[0-9]*', flags=re.MULTILINE) 
        sn_stop_pattern = re.compile(r'stop\s:\s[0-9]*', flags=re.MULTILINE)
        copydest_pattern = re.compile(r'data\sis\scopied\sto\s.*(?=\sat)')

                
        #---- read-in logfile ----#
        
        
        infile = open(logfile, "r")
        
        # read in file from the end: 
        # the idea is to detect the last request was a success or got aborted to any problem
        # need to check for "Abort request..." statements in the last entry
        # if there is one that means it was not succesfull
        # the send a modified json message to the HsSender
        # oterhwise look for the info from the succesfull transfer
        
#        for line in reversed(infile.readlines()):
#            txtline = line.rstrip()
#            new_dl = dl_pattern.findall(txtline) # new_dl is a list with one element 
#            new_sn_start = sn_start_pattern.findall(txtline)
#            new_sn_stop = sn_stop_pattern.findall(txtline)
#            new_copydest = copydest_pattern.findall(txtline)
#            
#            
#            
#            
#            if txtline == "Abort request...":
#                report_dict['hub'] = src_mchn
#                report_dict['start'] = int(re.sub('start\s\:' , '' ,start_list[-1][0]))
#                report_dict['stop'] = int(re.sub('stop\s\:' , '', stop_list[-1][0]))
#                report_dict['dataload'] = 0
#                report_dict['copydir'] = "failed"
#        
#                print "report_dict now contains:\n" , report_dict





        
        for txtline in infile.readlines():
            txtline = txtline.rstrip()
            new_dl = dl_pattern.findall(txtline) # new_dl is a list with one element 
            new_sn_start = sn_start_pattern.findall(txtline)
            new_sn_stop = sn_stop_pattern.findall(txtline)
            new_copydest = copydest_pattern.findall(txtline)
            
            # by using the reversed read-in the following is obsolete:
            if new_dl is not None and new_dl not in dl_list:
                dl_list.append( new_dl)
            elif new_sn_start is not None and new_sn_start not in start_list:
                start_list.append(new_sn_start)
            elif new_sn_stop is not None and new_sn_stop not in stop_list:
                stop_list.append(new_sn_stop)
            elif new_copydest is not None and new_copydest not in copydest_list:
                copydest_list.append(new_copydest)
                
        report_dict['hub'] = src_mchn
        report_dict['start'] = int(re.sub('start\s\:' , '' ,start_list[-1][0]))
        report_dict['stop'] = int(re.sub('stop\s\:' , '', stop_list[-1][0]))
        report_dict['dataload'] = int(re.sub('total\ssize\sis\s', '',dl_list[-1][0]))
        report_dict['copydir'] = re.sub('data\sis\scopied\sto\s', '',copydest_list[-1][0])

        print "report_dict now contains:\n" , report_dict
        #infostring = str(infolist)
        return report_dict        
        

class MyHubserver(object):
    """
    This is the class for creating the server socket on the hub and listen for messages from expcont.
    """

    def sync_worker(self):
        
        # Socket to receive message on:
        subscriber = context.socket(zmq.SUB)
        subscriber.setsockopt(zmq.IDENTITY, src_mchn)
        subscriber.setsockopt(zmq.SUBSCRIBE, "")
        subscriber.connect("tcp://"+spts_expcont_ip+":55561")
        print "SUB-Socket to receive message on:\nport:55561 from %s" % spts_expcont_ip        
                 
        # Socket to send message to :
        sender = context.socket(zmq.PUSH)
        sender.connect("tcp://2ndbuild:55560")
        print "PUSH-Socket to send message to:\nport 55560 on 2ndbuild"
        
        # Socket to synchronize the Publisher with the Workers
        syncclient = context.socket(zmq.PUSH)
        syncclient.connect("tcp://"+spts_expcont_ip+":55562")
        print "connected sync PUSH socket to  port 55562"

        # send a synchronization request:
        try:
            syncclient.send("Hi! Hub wants to connect!")
            print "syncservice sended sync request"
        except KeyboardInterrupt:
            sys.exit()
        # while True loop to keep connection open "forever"
        while True:             
            try:
                print 'waiting for new alert...'
                message = subscriber.recv()
                #print "received message"
                self.message = message
                print "HubServer got alert message:\n%s\n from Publisher" % (message)
                print "now parsing should start...\n"
                newalert = MyAlert()
                newalert.alert_parser(message, src_mchn)
                #print "parsing and copying finished succesful...\n now summarize the :\n "
                report_dict = newalert.info_report(logfile)
                print " report_dict gives:\n", report_dict
                # wait for alert parser to be finished and that it provides the infolist!
                # send back: 'DONE' message and information about the copy process: hostname timestamps & dataload copied: infolist[]    
                # print "the infolist contains: " , infolist
                report_dict_json = json.dumps(report_dict)
                print "dump dict to json"
                sender.send_json(report_dict_json)
                print "HsWorker sends report JSON to HsSender on 2ndbuild..."
                print "\nHS_Worker ready for next alert\n"
            except KeyboardInterrupt:
                print " Interruption received, shutting down..."
                sys.exit()

x = MyHubserver()
x.sync_worker()
