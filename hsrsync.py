#!/usr/bin/python

import subprocess
from datetime import datetime, timedelta
import re, sys
import getopt
import logging
import time
from fabric.api import *


#----- fabric functionxs ----#
def fabmkdir_copydest(host, cluster, hs_copydir, truetrigger_dir):
    totdir = hs_copydir + '/' + truetrigger_dir
    fabcmd = 'mkdir -p %s' %totdir
    with settings(host_string=host):
        if cluster == 'localhost':        
            local(fabcmd)
        else:
            run(fabcmd)

def fabmkdir_tmpdir(host, cluster, tmp_dir):
    fabcmd = 'mkdir -p %s' tmp_dir
    with settings(host_string=host):
        if cluster == 'localhost':        
            local(fabcmd)
        else:
            run(fabcmd)

    

def request_parser(request_begin_utc, request_end_utc, request_start, request_stop, copydir, src_mchn, src_mchn_short, cluster):
    
    #-----------  TimeStamps ----------- # 
    ALERTSTART = request_begin_utc
    ALERTSTOP = request_end_utc
    utc_now = datetime.utcnow()    
    
    #----------- parse copydir ----------- #
    hs_user_machinedir = copydir
    if (cluster == "SPS") or (cluster == "SPTS"):
        #for the REAL interface
        hs_ssh_access = re.sub(':/\w+/\w+/\w+/\w+/', "", hs_user_machinedir)
    else:
        hs_ssh_access = re.sub(':/[\w+/]*', "", hs_user_machinedir)         
    logging.info("HS COPY SSH ACCESS: " + str(hs_ssh_access))
    hs_copydir = re.sub(hs_ssh_access + ":", '', hs_user_machinedir)
    hs_dest_mchn = re.sub('\w+@', '', hs_ssh_access)
    logging.info("HS COPYDIR = " + str(hs_copydir))
    logging.info("HS DESTINATION HOST: " + str(hs_dest_mchn))

    #----------- parse info.txt files--------------#
    logging.info("HsInterface running on: " + str(cluster))
    
    if cluster == "localhost":
        hs_sourcedir_current     = '/home/david/TESTCLUSTER/testhub/currentRun/'
        hs_sourcedir_last        = '/home/david/TESTCLUSTER/testhub/lastRun/'

    else:
        hs_sourcedir_current     = '/mnt/data/pdaqlocal/currentRun/'
        hs_sourcedir_last        = '/mnt/data/pdaqlocal/lastRun/'


    infoParseLast = False
    infoParseCurrent = False
    # try max 10 times to parse info.txt to dictionary:
    retries_max = 10
    
    # for currentRun:
    retries = 0
    for i in range(1, retries_max):
        retries +=1    
        try:
            filename = hs_sourcedir_current + 'info.txt'
            fin = open(filename, "r")
            logging.info("read " + str(filename))
        except IOError:
            #print "couldn't open file, Retry in 4 seconds."
            time.sleep(4)        
        else:
            infodict = {}
            for line in fin:
                (key, val) = line.split()
                infodict[str(key)] = int(val)
            fin.close()        
            try:
                startrun = int(infodict['T0'])              
                CURT = infodict['CURT']                     
                IVAL = infodict['IVAL']                     
                IVAL_SEC = IVAL*1.0E-10                     
                CURF = infodict['CURF']                     
                MAXF = infodict['MAXF']                     
                infoParseCurrent = True
                break
            except KeyError:
                #print "Mapping info.txt to dictionary failed. Retrying in 4 seconds"
                time.sleep(4)
                
    if not infoParseCurrent:
        logging.error("CurrentRun info.txt reading/parsing failed")
        
    # for lastRun:
    retries = 0
    for i in range(1, retries_max):
        retries +=1    
        try:
            filename = hs_sourcedir_last + 'info.txt'
            fin = open(filename, "r")
            logging.info("read " + str(filename))
        except IOError:
            #print "couldn't open file, Retry in 4 seconds."
            time.sleep(4)        
        else:
            infodict2 = {}
            for line in fin:
                (key, val) = line.split()
                infodict2[str(key)] = int(val)
            fin.close()        
            try:
                last_startrun = int(infodict2['T0'])                # time-stamp of first HIT at run start -> this HIT is not in buffer anymore if HS_Loop > 0 !            
                LAST_CURT = infodict2['CURT']                       # current time stamp in DAQ units
                LAST_IVAL = infodict2['IVAL']                       # len of each file in integer 0.1 nanoseconds
                LAST_IVAL_SEC = IVAL*1.0E-10                        # len of each file in integer seconds
                LAST_CURF = infodict2['CURF']                       # file index of currently active hit spool file
                LAST_MAXF = infodict2['MAXF']                       # number of files per cycle                     
                infoParseLast = True
                break
            except KeyError:
                #print "Mapping info.txt to dictionary failed. Retrying in 4 seconds"
                time.sleep(4)
                
    if not infoParseLast:
        logging.error("LastRun info.txt reading/parsing failed")
    


    # ----- continue processing request only if all boundary conditions are met ----------- #
    if infoParseLast and infoParseCurrent:
        alertid = ALERTSTART.strftime("%Y%m%d_%H%M%S")             
        sn_start_file = None
        sn_stop_file = None
        
        # from currentRun :
        TFILE = (CURT - startrun)%IVAL              # how long already writing to current file, time in DAQ units 
        TFILE_SEC = TFILE*1.0E-10  
        HS_LOOP = int((CURT-startrun)/(MAXF*IVAL))
        if HS_LOOP == 0:
            OLDFILE = 0              # file index of oldest in buffer existing file
            startdata = startrun
        else:
            OLDFILE = (CURF+1)
            startdata = int(CURT - (MAXF-1)*IVAL - TFILE)    # oldest, in hit spool buffer existing time stamp in DAQ units 
    
        #converting the INFO dict's first entry into a datetime object:
        startrun_utc = str(datetime(int(utc_now.year),1,1) + timedelta(seconds=startrun*1.0E-10))    #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        RUNSTART = datetime.strptime(startrun_utc,"%Y-%m-%d %H:%M:%S.%f")
        startdata_utc = str(datetime(int(utc_now.year),1,1) + timedelta(seconds=startdata*1.0E-10))  #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        BUFFSTART = datetime.strptime(startdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        stopdata_utc = str(datetime(int(utc_now.year),1,1) + timedelta(seconds=CURT*1.0E-10))        #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        BUFFSTOP = datetime.strptime(stopdata_utc,"%Y-%m-%d %H:%M:%S.%f")

#        logging.info( "first HIT ever in current Run on this String in nanoseconds: " + str(startrun) + "\n" +
#        "oldest HIT's time-stamp existing in buffer in nanoseconds: " + str(startdata) +"\n" + 
#        "oldest HIT's time-stamp in UTC: " + str(BUFFSTART) + "\n" +
#        "newest HIT's timestamp in nanoseconds: " + str(CURT) + "\n" +
#        "newest HIT's time-stamp in UTC: " + str(BUFFSTOP) + "\n" +
#        "each hit spool file contains " + str(IVAL) + " * E-10 seconds of data\n" +
#        "duration per file in integer seconds: " + str(IVAL_SEC) + "\n" +
#        "hit spooling writes to " + str(MAXF) + " files per cycle \n" +
#        "HitSpooling writes to newest file: HitSpool-" + str(CURF) + " since " + str(TFILE) + " DAQ units\n" +
#        "HitSpooling is currently writing iteration loop: " + str(HS_LOOP) + "\n" +
#        "The oldest file is: HitSpool-" + str(OLDFILE))        


        # from last Run: 
        LAST_TFILE = (CURT - startrun)%IVAL                 # how long already writing to current file, time in DAQ units 
        LAST_TFILE_SEC = TFILE*1.0E-10  
        LAST_HS_LOOP = int((CURT-startrun)/(MAXF*IVAL))
        if LAST_HS_LOOP == 0:
            LAST_OLDFILE = 0              # file index of oldest in buffer existing file
            last_startdata = last_startrun
        else:
            LAST_OLDFILE = (LAST_CURF+1)
            last_startdata = int(LAST_CURT - (LAST_MAXF-1)*LAST_IVAL - LAST_TFILE)    # oldest, in hit spool buffer existing time stamp in DAQ units         

        #converting the INFO dict's first entry into a datetime object:
        last_startrun_utc = str(datetime(int(utc_now.year),1,1) + timedelta(seconds=last_startrun*1.0E-10))    #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        LAST_RUNSTART = datetime.strptime(last_startrun_utc,"%Y-%m-%d %H:%M:%S.%f")
        last_startdata_utc = str(datetime(int(utc_now.year),1,1) + timedelta(seconds=last_startdata*1.0E-10))  #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        LAST_BUFFSTART = datetime.strptime(last_startdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        last_stopdata_utc = str(datetime(int(utc_now.year),1,1) + timedelta(seconds=LAST_CURT*1.0E-10))        #PDAQ TIME UNITS ARE 0.1 NANOSECONDS
        LAST_BUFFSTOP = datetime.strptime(last_stopdata_utc,"%Y-%m-%d %H:%M:%S.%f")

#        logging.info( "first HIT ever in last Run on this String in nanoseconds: " + str(last_startrun) + "\n" +
#        "oldest HIT's time-stamp existing in buffer in nanoseconds: " + str(last_startdata) +"\n" + 
#        "oldest HIT's time-stamp in UTC: " + str(LAST_BUFFSTART) + "\n" +
#        "newest HIT's timestamp in nanoseconds: " + str(LAST_CURT) + "\n" +
#        "newest HIT's time-stamp in UTC: " + str(LAST_BUFFSTOP) + "\n" +
#        "each hit spool file contains " + str(LAST_IVAL) + " * E-10 seconds of data\n" +
#        "duration per file in integer seconds: " + str(LAST_IVAL_SEC) + "\n" +
#        "hit spooling writes to " + str(LAST_MAXF) + " files per cycle \n" +
#        "HitSpooling writes to newest file: HitSpool-" + str(LAST_CURF) + " since " + str(LAST_TFILE) + " DAQ units\n" +
#        "HitSpooling is currently writing iteration loop: " + str(LAST_HS_LOOP) + "\n" +
#        "The oldest file is: HitSpool-" + str(LAST_OLDFILE)) 
        

        #------ CHECK ALERT DATA LOCATION  -----#
        
        logging.info( "ALERTSTART has format: " + str(ALERTSTART))
        logging.info( "ALERTSTOP has format: " + str(ALERTSTOP))


        if LAST_BUFFSTOP < ALERTSTART < BUFFSTART < ALERTSTOP:
            logging.info( "Data location case 1")          
            hs_sourcedir = hs_sourcedir_current
            logging.info( "HitSpool source data is in directory: " + str(hs_sourcedir))
            sn_start_file = OLDFILE
            logging.warning( "Sn_start doesn't exits in " + str(hs_sourcedir) + " buffer anymore! Start with oldest possible data: HitSpool-" + str(OLDFILE))            
            sn_start_file_str = hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat" 
            timedelta_stop = (ALERTSTOP - BUFFSTART)
            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
            logging.info( "time diff from hit spool buffer start to sn_stop in seconds: " + str(timedelta_stop_seconds))
            sn_stop_file = int(((timedelta_stop_seconds/IVAL_SEC) + OLDFILE) % MAXF)        
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            logging.info( "sn_stops's data is included in file " + str(sn_stop_file_str))    


        elif BUFFSTART < ALERTSTART < ALERTSTOP < BUFFSTOP: 
            logging.info( "Data location case 2")          
            hs_sourcedir = hs_sourcedir_current
            logging.info( "HitSpool source data is in directory: " + str(hs_sourcedir))
            
            
            timedelta_start = (ALERTSTART - BUFFSTART) # should be a datetime.timedelta object 
            #time passed after data_start when sn alert started: sn_start - data_start in seconds:
            timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
            logging.info( "There are " + str(timedelta_start_seconds) + " seconds of data before the Alert started")             
            sn_start_file = int(((timedelta_start_seconds/IVAL_SEC) + OLDFILE) % MAXF)
            sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
            logging.info( "sn_start's data is included in file " + str(sn_start_file_str))    
            timedelta_stop = (ALERTSTOP - BUFFSTART)
            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
            logging.info( "time diff from hit spool buffer start to sn_stop in seconds: " + str(timedelta_stop_seconds))
            sn_stop_file = int(((timedelta_stop_seconds/IVAL_SEC) + OLDFILE) % MAXF)        
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            logging.info( "sn_stops's data is included in file " + str(sn_stop_file_str)) 

        elif LAST_BUFFSTOP < ALERTSTART < ALERTSTOP < BUFFSTART:
            logging.info( "Data location case 3")   
            logging.error("requested data doesn't exist in HitSpool Buffer anymore!Abort request.")
            return None



        elif LAST_BUFFSTART < ALERTSTART < LAST_BUFFSTOP < ALERTSTOP:
            logging.info( "Data location case 4")          
            hs_sourcedir = hs_sourcedir_last
            logging.info( "requested data distributed over both HS Run directories")
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

        elif LAST_BUFFSTART < ALERTSTART < ALERTSTOP < LAST_BUFFSTOP:
            logging.info( "Data location case 5")          
            hs_sourcedir = hs_sourcedir_last
            logging.info( "HS source data is in directory: " + str(hs_sourcedir))  
            timedelta_start = (ALERTSTART - LAST_BUFFSTART) # should be a datetime.timedelta object 
            #time passed after data_start when sn alert started: sn_start - data_start in seconds:
            timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
            sn_start_file = int(((timedelta_start_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)
            sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
            logging.info( "sn_start's data is included in file " + str(sn_start_file_str))                     
            timedelta_stop = (ALERTSTOP - LAST_BUFFSTART)
            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
            sn_stop_file = int(((timedelta_stop_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)        
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            logging.info( "sn_stop's data is included in file " + str(sn_stop_file_str))   


        elif ALERTSTART < LAST_BUFFSTART < ALERTSTOP < LAST_BUFFSTOP:
            logging.info( "Data location case 6")
            hs_sourcedir = hs_sourcedir_last
            logging.info( "HS source data is in directory: " + str(hs_sourcedir))
            sn_start_file = LAST_OLDFILE
            logging.warning("sn_start doesn't exits in" + str( hs_sourcedir) +  "buffer anymore! Start with oldest possible data: HitSpool-"+str(LAST_OLDFILE))
            sn_start_file_str = hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"               
            timedelta_stop = (ALERTSTOP - LAST_BUFFSTART)
            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
            logging.info( "time diff from hit spool buffer start to sn_stop in seconds: " + str(timedelta_stop_seconds))
            sn_stop_file = int(((timedelta_stop_seconds/LAST_IVAL_SEC) + LAST_OLDFILE) % LAST_MAXF)        
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            logging.info( "sn_stops's data is included in file " + str(sn_stop_file_str))

        elif ALERTSTART < ALERTSTOP < LAST_BUFFSTART:
            logging.info( "Data location case 7")
            logging.error( "requested data doesn't exist in HitSpool Buffer anymore! Abort request.")
            return None
                 
        elif ALERTSTOP < ALERTSTART:
            logging.error("sn_start & sn_stop time-stamps inverted. Abort request.")
            return None
    
        elif BUFFSTOP < ALERTSTART:
            #logging.info( "Sn_start & sn_stop time-stamps error. \nAbort request."
            logging.error( "ALERTSTART is in the FUTURE ?!")
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
        truetrigger = ALERTSTART.strftime("%Y%m%d_%H%M%S") 
        truetrigger_dir = "HitSpoolData_" + truetrigger + "_" + src_mchn + "/"
        hs_copydest = hs_copydir + truetrigger_dir
        logging.info( "unique naming for folder: " + str(hs_copydest))

        #move these files aside into subdir /tmp/ to prevent from being overwritten from next hs cycle while copying:
        #make subdirectory "/tmp" . if it exist doesn't already

        if cluster == "localhost":         
            tmp_dir = "/home/david/TESTCLUSTER/testhub/tmp/" + truetrigger + "/"
        else:
            tmp_dir = "/mnt/data/pdaqlocal/tmp/HsRequest_" + truetrigger + "/"
        try:
#            subprocess.check_call("mkdir -p " + tmp_dir, shell=True)
#            logging.info( "created subdir for relevant hs files")
            fabmkdir_tmpdir()        
        
        except subprocess.CalledProcessError:
#            logging.info( "Subdir in /mnt/data/padqlocal/tmp/ already exists") 
            pass
        # -- building file list -- # 
        copy_files_list = []   
        for i in range (n_rlv_files):
            sn_start_file_i = (sn_start_file+i)%MAXF
            next_file = re.sub("HitSpool-" + str(sn_start_file), "HitSpool-" + str(sn_start_file_i), sn_start_file_str)
            hs_tmp_copy = subprocess.check_call("cp -l " + next_file + " " + tmp_dir, shell=True)
            if hs_tmp_copy == 0:
                next_tmpfile = tmp_dir + "HitSpool-" + str(sn_start_file_i) + ".dat"
                copy_files_list.append(next_tmpfile)
                logging.info("linked the file: " + str(next_file) + " to tmp directory")
            else:
                logging.error("failed to link file " + str(sn_start_file_i) + " to tmp dir")      
                
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
                  
                else:
                    logging.error("failed to link hitspool file " + str(sn_stop_file_i) + " to tmp dir")         
                
        except NameError:
            pass
        logging.info("list of relevant files: " + str(copy_files_list))    
        copy_files_str = " ".join(copy_files_list) 

        # ---- Rsync the relevant files to DESTINATION ---- #
                
        # mkdir destination copydir
        fabmkdir_copydest(hs_dest_mchn, cluster, hs_copydir, truetrigger_dir)    
        
        # ------- the REAL rsync command for SPS and SPTS:-----#
        # there is a rsync deamon: hitspool/ points internally to /mnt/data/pdaqlocal/HsDataCopy/ this is set fix on SPTS and SPS by Ralf Auer     
        # is useful when syncing from more than 50 hubs at the same time
        if (cluster == "SPS") or (cluster == "SPTS") :  
            rsync_cmd = "nice rsync -avv --bwlimit=300 --log-format=%i%n%L " + copy_files_str + " " + hs_copydir + truetrigger_dir        
            logging.info( "rsync does:\n " + str(rsync_cmd)) 
        
        #------ the localhost rsync command -----#           
        else:   
            rsync_cmd = "nice rsync -avv --bwlimit=300 --log-format=%i%n%L " + copy_files_str + " " + hs_copydir + truetrigger_dir
            logging.info("rsync command: " + str(rsync_cmd))             
            
        hs_rsync = subprocess.Popen(rsync_cmd, shell=True, bufsize=256, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        hs_rsync_out = hs_rsync.stdout.readlines()
        hs_rsync_err = hs_rsync.stderr.readlines()
        
        # --- catch rsync error --- #
        if len(hs_rsync_err) is not 0:
            logging.error("failed rsync process:\n" + str(hs_rsync_err))
            logging.info("KEEP tmp dir with data in: " + str(tmp_dir))
                        
        # --- proceed if no error --- #
        else:
            logging.info("rsync out:\n" +str(hs_rsync_out))
            logging.info("successful copy of HS data from " + str(hs_sourcedir) + " at " + str(src_mchn) + " to " + str(hs_copydest) +" at " + str(hs_ssh_access))
            rsync_dataload = re.search(r'(?<=total size is )[0-9]*', hs_rsync_out[-1])
            if rsync_dataload is not None:
                dataload_mb = str(float(int(rsync_dataload.group(0))/1024**2))
            else:
                dataload_mb = "TBD"
            logging.info(str("dataload of %s in [MB]:\n%s" % (hs_copydest, dataload_mb )))
            
            # remove tmp dir:
            try:
                remove_tmp_files = "rm -r " + tmp_dir
                subprocess.check_call(remove_tmp_files, shell=True)
                logging.info("tmp dir deleted.")
                
            except subprocess.CalledProcessError:
                logging.error("failed removing tmp files...")                
                pass

        hs_rsync.stdout.flush()
        hs_rsync.stderr.flush()

if __name__=="__main__":
    '''
    For grabbing hs data from hubs independently (without sndaq providing request).
    HsGrabber sends json to HsPublisher to grab hs data from hubs.
    Uses the Hs Interface infrastructure.
    '''
    def usage():
        print >>sys.stderr, """
        usage :: HsGrabberSingleHub.py [options]
            -b         | begin of data: "YYYY-mm-dd HH:MM:SS.[us]" OR DAQ timestamp [0.1 ns from beginning of the year]
            -e         | end of data "YYYY-mm-dd HH:MM:SS.[us]"   OR DAQ timestamp [0.1 ns from beginning of the year]   
            -c         | copydir e.g. "pdaq@2ndbuild:/mnt/data/pdaqlocal/HsDataCopy/"
            
            HsGrabberSingleHub reads UTC timestamps or DAQ timestamps, calculates the requested hitspool file indexes and ships the data to copydir.
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
    print "This HsGrabberSingleHub runs on: " , src_mchn

    if cluster == "localhost":
        logfile = "/home/david/TESTCLUSTER/testhub/logs/HsGrabberSingleHub_" + src_mchn_short + ".log" 
    else:
        logfile = "/mnt/data/pdaqlocal/HsInterface/logs/HsGrabberSingleHub_" + src_mchn_short + ".log"

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
                        level=logging.INFO, stream=sys.stdout, 
                        datefmt= '%Y-%m-%d %H:%M:%S', 
                        filename=logfile)
    logging.info("this Grabber runs on: " + str(src_mchn_short))
    ##take arguments from command line and check for correct input
    opts, args = getopt.getopt(sys.argv[1:], 'hb:e:c:', ['help','request='])
    for o, a in opts:
        if o == '-b':                
            if not a.isdigit():
                request_start = 0 #DAQ timestamp not yet defined
                try:        
                    request_begin_utc = datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S.%f")
                except ValueError, e:
                    #print "Problem with the time-stamp format: ", e
                    try:
                        #print "Try matching format without subsecond precision..."
                        request_begin_short = re.sub(".[0-9]{9}", '', str(a))
                        request_begin_utc = datetime.strptime(request_begin_short, "%Y-%m-%d %H:%M:%S")
                        #print "matched"
                    except ValueError,e:
                        print  "Problem with the time-stamp format: ", e
            else:
                request_start = int(a)
                request_begin_utc = 0
        if o == '-e':
            if not a.isdigit():
                request_stop = 0 #SNDAQ timestamp not yet defined do that later on
                try:        
                    request_end_utc = datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S.%f")
                except ValueError, e:
                    #print "Problem with the time-stamp format: ", e
                    try:
                        #print "Try matching format without subsecond precision..."
                        request_end_short = re.sub(".[0-9]{9}", '', str(a))
                        request_end_utc = datetime.strptime(request_end_short, "%Y-%m-%d %H:%M:%S")
                        #print "matched"
                    except ValueError,e:
                        print  "Problem with the time-stamp format: ", e
#                request_end_utc = datetime.strptime(str(a), "%Y-%m-%d %H:%M:%S.%f")
            else:
                request_stop = int(a)
                request_end_utc = 0
        if o == '-c':
            copydir = str(a)
        elif o == '-h' or o == '--help':
            usage()       

    if len(sys.argv) < 5 :
        print usage()


    # convert to UTC or DAQ whatever direction is needed:
    if (request_start == 0) and (request_stop == 0) :
        daqyearstart = datetime.strptime(str(request_begin_utc.year)+"-01-01", "%Y-%m-%d")
        request_begin_utc_delta = request_begin_utc - daqyearstart
        request_end_utc_delta = request_end_utc - daqyearstart
        request_start = (request_begin_utc_delta.microseconds + (request_begin_utc_delta.seconds + request_begin_utc_delta.days * 24 * 3600) * 10**6) * 10**4
        request_stop = (request_end_utc_delta.microseconds + (request_end_utc_delta.seconds + request_end_utc_delta.days * 24 * 3600) * 10**6) * 10**4        
    
    elif (request_begin_utc == 0) and (request_begin_utc == 0):        
        daqyear = int(datetime.utcnow().year)
        request_begin_utc = datetime.strptime(str(datetime(daqyear, 1, 1) + timedelta(seconds = request_start*1.0E-10)), "%Y-%m-%d %H:%M:%S.%f")
        request_end_utc = datetime.strptime(str(datetime(daqyear, 1, 1) + timedelta(seconds = request_stop*1.0E-10)), "%Y-%m-%d %H:%M:%S.%f")
    
    logging.info("\n NEW REQUEST \n")    
    logging.info( "HS REQUEST DATA BEGIN UTC time: " + str(request_begin_utc))
    logging.info("HS REQUEST DATA END UTC time: " + str(request_end_utc))
    logging.info("HS REQUEST DATA BEGIN DAQ time: "+ str( request_start))
    logging.info("HS REQUEST DATA END DAQ time: "+ str( request_stop))
    logging.info("HS Data Destination: " + str(copydir))

    
    request_parser(request_begin_utc, request_end_utc, request_start, request_stop, copydir, src_mchn, src_mchn_short, cluster)