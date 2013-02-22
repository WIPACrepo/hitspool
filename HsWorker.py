#!/usr/bin/python
#
#
#Hit Spool Worker to be run on hubs
#author: dheereman
#
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
        hs_sourcedir = '/mnt/data/pdaqlocal/currentRun/'
        #hs_copydest_list = list() 
        fsummary = open(logfile, "a")
        packer_start = str(datetime.utcnow())
        print >> fsummary, "\n**********************************************\nStarted HitSpoolWorker on %s at: %s" % (src_mchn, packer_start)
        fsummary.close()
        

#        # --- Parsing alert message JSON ----- :
        
        print "the message is of type: " , type(alert), "and looks like:\n", alert
        
        alert_info = json.loads(alert)
        print "alert_info message in json format: \n" , alert_info , "\nand is of type: \n", type(alert_info)
        start = int(alert_info[0]["start"]) # timestamp in DAQ units as a string
        stop = int(alert_info[0]['stop'])   # timestamp in DAQ units as a string
        hs_user_machinedir = alert_info[0]['copy'] # should be something like: pdaq@expcont:/mnt/data/pdaqlocal/HsDataCopy/



        try:
            sn_start = int(start)
            print "SN START = ", sn_start
            sn_start_utc = str(datetime(2013,1,1) + timedelta(seconds=sn_start*1.0E-10))
            print "DAQ time-stamp in UTC: ", sn_start_utc
            ALERTSTART = datetime.strptime(sn_start_utc,"%Y-%m-%d %H:%M:%S")
            print "ALERTSTART = ", ALERTSTART            
        
        except (TypeError,ValueError):
            print "ERROR in json message: no start timestamp found. Abort request..."
            fsummary = open(logfile, "a")
            print >> fsummary, "No timestamps found in alert message. Aborting request... "
            fsummary.close()  
            sys.exit()
            
        else:
            pass
        
        
        try:
            sn_stop = int(stop)
            print "SN STOP = ", sn_stop
            sn_stop_utc = str(datetime(2013,1,1) + timedelta(seconds=sn_stop*1.0E-10))
            print "DAQ time-stamp in UTC: ", sn_stop_utc
            ALERTSTOP = datetime.strptime(sn_stop_utc,"%Y-%m-%d %H:%M:%S")
            print "ALERTSTOP =", ALERTSTOP
            
        except (TypeError,ValueError):
            print "ERROR in json message: no stop timestamp found. Abort request..."
            fsummary = open(logfile, "a")
            print >> fsummary, "No timestamps found in alert message. Aborting request... "
            fsummary.close()
            sys.exit()
            
            
        try :
            print "HS machinedir = ", hs_user_machinedir
            hs_copydir = re.sub('\w+\@\w+:', '', hs_user_machinedir)
            print " HS COPYDIR = ", hs_copydir
            hs_ssh_access = re.sub(':/\w+/\w+/\w+/\w+/', "", hs_user_machinedir)
            
        except (TypeError, ValueError):
            #"ERROR in json message: no copydir found. Abort request..."
            sys.exit( "ERROR in json message: no copydir found. Abort request...")

        #------Parsing hitspool info.txt to find the requested files-------:        
        # Find the right file(s) that contain the start/stoptime and the actual sn trigger time stamp=sntts
        # useful: datetime.timedelta
        try:
            filename = hs_sourcedir+'info.txt'
            print filename
            fin = open (filename)
        except IOError as (errno, strerror):
            print "cannot open %s" % filename
            print "I/O error({0}): {1}".format(errno, strerror)
            return None
        else:
            infodict= {}
            for line in open(filename):
                (key, val) = line.split()
                infodict[str(key)] = int(val)
            fin.close()
                            
        startdata = infodict['T0']      # oldest, in hit spool buffer existing time stamp in DAQ units
        print "startdata in nanoseconds: " , startdata
        CURT = infodict['CURT']         # current time stamp in DAQ units
        IVAL = infodict['IVAL']         # len of each file in integer 0.1 nanoseconds
        IVAL_SEC = IVAL*1.0E-10         # len of each file in integer seconds
        CURF = infodict['CURF']         # file index of currently active hit spool file
        MAXF = infodict['MAXF']         # number of files per cycle
        HS_LOOP = int((CURT-startdata)/(MAXF*IVAL))
        if HS_LOOP == 0:
            OLDFILE = 0              # file index of oldest in buffer existing file
        else:
            OLDFILE = (CURF+1)
         
        print "get information from %s..." % filename

        #converting the INFO dict's first entry into a datetime object:
        startdata_utc = str(datetime(2013,1,1) + timedelta(seconds=startdata*1.0E-10))
        BUFFSTART = datetime.strptime(startdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        stopdata_utc = str(datetime(2013,1,1) + timedelta(seconds=CURT*1.0E-10))
        BUFFSTOP = datetime.strptime(stopdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        print "oldest hit's time-stamp existing in buffer:\n%s" %(BUFFSTART)
        print "currently written hit with timestamp: %d" % CURT
        print "newest hit's time-stamp existing in buffer:\n%s" %(BUFFSTOP)
        print "each hit spool file contains %d * E-10 seconds of data" % IVAL
        print "duration per file in integer seconds: %d" % IVAL_SEC
        print "hit spooling writes to %d files per cycle " % MAXF
        print "index of oldest file: HitSpool-%d" % OLDFILE
        print "The Hit Spooler is currently writing iteration loop: %d" % HS_LOOP
        fsummary = open(logfile, "a")
        print >> fsummary, "From info.txt:\n BUFFSTART= %s\n BUFFSTOP=  %s\n IVAL_SEC=  %d \n OLDFILE=  HitSpool-%s\n MAXF=     %s\n HS_LOOP= %s\n" %(BUFFSTART, 
                                                                                                                                       BUFFSTOP, 
                                                                                                                                       IVAL_SEC, 
                                                                                                                                       OLDFILE, 
                                                                                                                                       MAXF, 
                                                                                                                                       HS_LOOP)
        fsummary.close()
        
        
    #------ Go to sn_start signal -----:
        
        #print "ALERTSATRT has format: %s" %  ALERTSTART
        #Check if required sn_start data still exists in buffer. If sn request comes in from earlier times --> FAIL
        #for this to be possible I need the time-stamp of the file that is currently recording (--> Kael's or my update)
        #take OLDFILE value to check!
        
        if start < startdata:
            sn_start_file = OLDFILE
            print "Sn_start doesn't exits in buffer anymore! Copy oldest possible data: HitSpool-%s" % OLDFILE
            fsummary = open(logfile, "a")
            print >> fsummary, "Sn_start doesn't exits in buffer anymore! Copy oldest possible data: HitSpool-%s" % OLDFILE
            sn_start_file_str = hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"   

        else: 
            timedelta_start = (ALERTSTART - BUFFSTART) # should be a datetime.timedelta object 
            #time passed after data_start when sn alert started: sn_start - data_start in seconds:
            #print "There is \n%s\n of data in the spool before the requested data" % timedelta_start
            #timedelta_start_seconds = int(timedelta_start.total_seconds())
            timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
            print "There are \n%s\n seconds of data before the Alert started" % timedelta_start_seconds            
            sn_start_file = int(((timedelta_start_seconds/IVAL_SEC) + OLDFILE) % MAXF)
            #sn_start_cycle = int(timedelta_start_seconds / (IVAL_SEC*MAXF))
            sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
        print "sn_start's data is included in file %s" % (sn_start_file_str)    
                  
    #------ sn_stop signal -----:
        print "sn_stop has format: %s" % sn_stop
        #Check if required sn_start data still exists in buffer. If sn request comes in from earlier times --> FAIL
        #for this to be possible I need the time-stamp of the file that is currently recoding (--> Kael's or my update)
        # take OLDFILE value to check!
        
        if ALERTSTOP < BUFFSTART and ALERTSTOP > ALERTSTART:
            print "requested data doesn't exist in HitSpool Buffer anymore! Abort request."
            fsummary = open(logfile, "a")
            print >> fsummary, "requested data doesn't exist in HitSpool Buffer anymore!. Aborting request... "
            fsummary.close() 
            return None
        elif ALERTSTOP < ALERTSTART:
            #print "Sn_start & sn_stop time-stamps error. Abort request."
            fsummary = open(logfile, "a")
            print >> fsummary, "Sn_start & sn_stop time-stamps error. Aborting request... "
            fsummary.close()
            #sys.exit("Sn_start & sn_stop time-stamps error. Abort request.")
            return None
        elif ALERTSTOP > BUFFSTART and ALERTSTOP > ALERTSTART:   # sn_stop > BUFFEND is not possible by definition
            timedelta_stop = (ALERTSTOP - BUFFSTART)
            #timedelta_stop_seconds = int(timedelta_stop.total_seconds())
            timedelta_stop_seconds =  (timedelta_stop.seconds + timedelta_stop.days * 24 * 3600)      
            print "time diff from hit spool buffer start to sn_stop in seconds: %s" % timedelta_stop_seconds
            sn_stop_file = int(((timedelta_stop_seconds/IVAL_SEC) + OLDFILE) % MAXF)        
            #sn_stop_cycle = int(timedelta_stop_seconds / (IVAL_SEC*MAXF))
            sn_stop_file_str = hs_sourcedir +"HitSpool-" + str(sn_stop_file) + ".dat"
            print "sn_stops's data is included in file %s" % (sn_stop_file_str)
        
        fsummary = open(logfile, "a")
        print >> fsummary, "start : %s \nis included in \n%s" % ( sn_start ,sn_start_file_str)
        print >> fsummary, "stop : %s \nis included in \n%s" % (sn_stop, sn_stop_file_str)
        fsummary.close()



    # ---- HitSpoolData Access and Copy ----:
        #how many files n_rlv_files do we have to move and copy:

        if sn_start_file < sn_stop_file:
            n_rlv_files = ((sn_stop_file - sn_start_file) + 1) % MAXF
            print "n_rlv_files = %s " % n_rlv_files
        else:
            n_rlv_files = ((sn_stop_file - sn_start_file)+ MAXF + 1) % MAXF # mod MAXF for the case that sn_start & sn_stop are in the same HS file
            print "n_rlv_files = %s " % n_rlv_files

        sn_start_mod = ALERTSTART.strftime("%Y%m%d_%H%M%S") 
        sn_start_foldername = re.sub(" ","_", str(sn_start_mod))
        hs_copydest = hs_copydir+sn_start_foldername+"_"+src_mchn + "/"
        print "unique naming for folder: %s " % hs_copydest

        #move these files aside into subdir /tmp/ to prevent from being overwritten from next hs cycle while copying:
        #make subdirectory "/tmp" . if it exists already
        tmp_dir = "/mnt/data/pdaqlocal/tmp/SNALERT_"+sn_start_foldername + "/"
        try:
            subprocess.check_call("mkdir -p " + tmp_dir, shell=True)
            print "created subdir for relevant hs files"
        except subprocess.CalledProcessError:
            print "Subdir  in /mnt/data/padqlocal/tmp/ already exists" 
            pass
    
        copy_files_list = []   
        for i in range (n_rlv_files):
            sn_start_file_i = (sn_start_file+i)%MAXF
            next_file = re.sub("HitSpool-" + str(sn_start_file), "HitSpool-" + str(sn_start_file_i), sn_start_file_str)
            #move these files aside to prevent from being overwritten from next hs cycle while copying:    
            hs_tmp_copy = subprocess.check_call("cp -a " + next_file + " " + tmp_dir, shell=True)
            if hs_tmp_copy == 0:
                next_tmpfile = tmp_dir + "HitSpool-" + str(sn_start_file_i) + ".dat"
                #print "\nnext file to copy is: %s" % next_copy
                copy_files_list.append(next_tmpfile)
            else:
                print "ERROR: copy hitspool file  %s to tmp dir failed"      %    str(sn_start_file_i)   
        print "list of relevant files %s" % copy_files_list
        copy_files_str = " ".join(copy_files_list) 
        print "joined string of relevant files :\n %s" % copy_files_str
        print "last relevant is:\n%s" % sn_stop_file_str
        
        # ---- Rsync the relevant files to expcont ---- #
        rsync_cmd = "nice rsync -avv --bwlimit=30 --log-format=%i%n%L " + copy_files_str + " " + hs_ssh_access + ':' + hs_copydest + " >>" + logfile
        
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
            print >> fsummary, "Finished HitSpoolWorker at: %s\n**********************************************\n" % packer_stop
            fsummary.close()
                
        except subprocess.CalledProcessError:
            print "\nError in rsync from %s to %s....\n" %(src_mchn, hs_ssh_access)
            fsummary = open(logfile, "a")
            print >> fsummary, "rsyncing failed !!"
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at: %s\n**********************************************\n" % packer_stop
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
        infile = open(logfile, "r")
        for txtline in infile.readlines():
            txtline = txtline.rstrip()
            new_dl = dl_pattern.findall(txtline) # new_dl is a list with one element 
            new_sn_start = sn_start_pattern.findall(txtline)
            new_sn_stop = sn_stop_pattern.findall(txtline)
            new_copydest = copydest_pattern.findall(txtline)
            
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
        sender.connect("tcp://"+spts_expcont_ip+":55560")
        print "PUSH-Socket to send message to:\nport 55560 on %s" % spts_expcont_ip
        
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
                print "HsWorker sends report JSON to HsSender on expcont..."
                print "\nHS_Worker ready for next alert\n"
            except KeyboardInterrupt:
                print " Interruption received, shutting down..."
                sys.exit()

x = MyHubserver()
x.sync_worker()