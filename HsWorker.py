#!/usr/bin/python
#
#
#Hit Spool Worker to be run on hubs
#author: dheereman
#
"""
"sico_tester"        "HsPublisher"      "HsWorker"        "HsI3liveSink"
-----------        -----------
| sni3daq |        | expcont |         -----------        ------------
| REQUEST | <----->| REPLY   |         | IcHub n |        | expcont  |
-----------        | PUSH    | ------> | PULL   n|        | PULL     |
                   ----------          |PUSH     | ---->  |          |
                                        ---------          -----------
                                                                 
"""

from datetime import datetime, timedelta
import re, sys
import zmq #@UnresolvedImport
import subprocess
import json


p = subprocess.Popen(["hostname"], stdout = subprocess.PIPE)
out, err = p.communicate()
src_mchn = out.rstrip()
print "this Worker is running on: " , src_mchn
summaryfile = "/mnt/data/pdaqlocal/HsInterface/summary_"+src_mchn+".log"

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
        hs_sourcedir = '/mnt/data/pdaqlocal/hs_spts_run29141/'
        #hs_copydest_list = list() 
        fsummary = open(summaryfile, "a")
        packer_start = str(datetime.utcnow())
        print >> fsummary, "\n**********************************************\nStarted HitSpoolWorker on %s at: %s" % (src_mchn, packer_start)
        fsummary.close()

#        # --- Parsing alert message JSON ----- :
        
        alert_info = json.load(alert)
        start = alert_info[0]['start'] # timestamp in DAQ units as a string
        if start == "":
            print "ERROR in json message: no start timestamp found. Abort request..."
            fsummary = open(summaryfile, "a")
            print >> fsummary, "No timestamps found in alert message. Aborting request... "
            fsummary.close()  
            sys.exit()
        else:
            sn_start = int(start)
            print "SN START = ", sn_start
            sn_start_utc = str(datetime(2012,1,1) + timedelta(seconds=sn_start*1.0E-10))
            ALERTSTART = datetime.strptime(sn_start_utc,"%Y-%m-%d %H:%M:%S.%f")
            print "ALERTSTART = ", ALERTSTART
        
        stop = alert_info[0]['stop']   # timestamp in DAQ units as a string
        if stop == "":
            "ERROR in json message: no stop timestamp found. Abort request..."
            fsummary = open(summaryfile, "a")
            print >> fsummary, "No timestamps found in alert message. Aborting request... "
            fsummary.close()
            sys.exit()
        else:
            sn_stop = int(stop)
            print "SN STOP = ", sn_stop
            sn_stop_utc = str(datetime(2012,1,1) + timedelta(seconds=sn_stop*1.0E-10))
            ALERTSTOP = datetime.strptime(sn_stop_utc,"%Y-%m-%d %H:%M:%S.%f")
            print "ALERTSTOP =", ALERTSTOP
            
        hs_user_machinedir = alert_info[0]['copy'] # should be something like: pdaq@expcont:/mnt/data/pdaqlocal/HsDataCopy/
        if hs_user_machinedir == "":
            "ERROR in json message: no copydir found. Abort request..."
            sys.exit()
        else:
            print "HS machinedir = ", hs_user_machinedir
            hs_copydir = re.sub('\w+\@\w+:', '', hs_user_machinedir)
            print " HS COPYDIR = ", hs_copydir
            hs_ssh_access = re.sub(':/\w+/\w+/\w+/\w+/', "", hs_user_machinedir)
        
        

        
        #-----Parsing the alert string message-------:
        
#        pattern = re.compile(r'trigger\s+timestamp\s\=\s[0-9]{4}\-[0-9]{2}\-[0-9]{2}\s(UTC)\s[0-9]{2}\:[0-9]{2}\:[0-9]{2}', flags=re.MULTILINE)
#        trigtime = re.compile(r'[0-9]{4}\-[0-9]{2}\-[0-9]{2}\s+\w+\s[0-9]{2}\:[0-9]{2}\:[0-9]{2}', flags=re.MULTILINE)
#        copydata = re.compile(r'HitSpool\s+copy\s+=\s+\w+\@\w+[\.\w+]+:/[\w+/]+')
#        sntts = pattern.search(alert)
#        sntts_all = trigtime.findall(alert) # Return all non-overlapping matches of trigtime pattern in data, as a list of strings
#        copydir = copydata.search(alert)
#
#        #Parsing the HitSpool Copy directory: user@machine_name:/where/ever/it/should/be/copied/
#        #this message is divided into ssh command (user@machine_name --> ssh_access) and directory 
#        #(--> hs_copydir) to be handled in copy command at the end...
#        if copydir is None:
#            print "HS copy directory parsing failed. Aborting request... "
#            fsummary = open(summaryfile, "a")
#            print >> fsummary, "HS copy directory parsing failed. Aborting request... "
#            fsummary.close()
#            return None
#        else:
#            hs_copydir = re.sub('HitSpool\s+copy\s+=\s+\w+\@\w+:', '', copydir.group(0))
#            hs_user_machinedir = re.sub('HitSpool\s+copy\s+=\s+', '',copydir.group(0))
#            hs_ssh_access = re.sub(':/\w+/\w+/\w+/\w+/', "", hs_user_machinedir)
#            print "HS copied data directory is explicitely:\n%s\n%s\n%s" % (hs_copydir,hs_user_machinedir,hs_ssh_access) 
#        
#        if sntts is None:
#            print "No timestamps found in alert message. Aborting request..."
#            fsummary = open(summaryfile, "a")
#            print >> fsummary, "No timestamps found in alert message. Aborting request... "
#            fsummary.close()            
#            
#        else:
#            print "all timestamps found in alert message are: \n %r" % (sntts_all)
#
#        sn_start = datetime.strptime(sntts_all[0],"%Y-%m-%d UTC %H:%M:%S")
#        sn_stop = datetime.strptime(sntts_all[1],"%Y-%m-%d UTC %H:%M:%S")
        
        
        
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
        startdata_utc = str(datetime(2012,1,1) + timedelta(seconds=startdata*1.0E-10))
        BUFFSTART = datetime.strptime(startdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        stopdata_utc = str(datetime(2012,1,1) + timedelta(seconds=CURT*1.0E-10))
        BUFFSTOP = datetime.strptime(stopdata_utc,"%Y-%m-%d %H:%M:%S.%f")
        print "oldest hit's time-stamp existing in buffer:\n%s" %(BUFFSTART)
        print "currently written hit with timestamp: %d" % CURT
        print "newest hit's time-stamp existing in buffer:\n%s" %(BUFFSTOP)
        print "each hit spool file contains %d * E-10 seconds of data" % IVAL
        print "duration per file in integer seconds: %d" % IVAL_SEC
        print "hit spooling writes to %d files per cycle " % MAXF
        print "index of oldest file: HitSpool-%d" % OLDFILE
        print "The Hit Spooler is currently writing iteration loop: %d" % HS_LOOP
        fsummary = open(summaryfile, "a")
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
            fsummary = open(summaryfile, "a")
            print >> fsummary, "Sn_start doesn't exits in buffer anymore! Copy oldest possible data: HitSpool-%s" % OLDFILE
            sn_start_file_str = hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"   

        else: 
            timedelta_start = (ALERTSTART - BUFFSTART) # should be a datetime.timedelta object 
            #time passed after data_start when sn alert started: sn_start - data_start in seconds:
            print "time-stamp of oldest Hit still existing in HitSpoolBuffer:\n%s" % timedelta_start
            #timedelta_start_seconds = int(timedelta_start.total_seconds())
            timedelta_start_seconds = (timedelta_start.seconds + timedelta_start.days * 24 * 3600)
            print "There are \n%s\n seconds of data before the Alert started" % timedelta_start_seconds            
            sn_start_file = int(((timedelta_start_seconds/IVAL_SEC) + OLDFILE) % MAXF)
            #sn_start_cycle = int(timedelta_start_seconds / (IVAL_SEC*MAXF))
            sn_start_file_str =  hs_sourcedir + "HitSpool-" + str(sn_start_file) + ".dat"
        print "sn_start's data is included in file %s" % (sn_start_file_str)    
                  
        #------ Now go to sn_stop signal -----:
        print "sn_stop has format: %s" % sn_stop
        #Check if required sn_start data still exists in buffer. If sn request comes in from earlier times --> FAIL
        #for this to be possible I need the time-stamp of the file that is currently recoding (--> Kael's or my update)
        # take OLDFILE value to check!
        
        if ALERTSTOP < BUFFSTART and ALERTSTOP > ALERTSTART:
            print "requested data doesn't exist in HitSpool Buffer anymore! Abort request."
            fsummary = open(summaryfile, "a")
            print >> fsummary, "requested data doesn't exist in HitSpool Buffer anymore!. Aborting request... "
            fsummary.close() 
            return None
        elif ALERTSTOP < ALERTSTART:
            #print "Sn_start & sn_stop time-stamps error. Abort request."
            fsummary = open(summaryfile, "a")
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
        
        fsummary = open(summaryfile, "a")
        print >> fsummary, "sn_start: %s\n is included in \n%s" % ( sn_start ,sn_start_file_str)
        print >> fsummary, "sn_stop: %s\n is included in \n%s" % (sn_stop, sn_stop_file_str)
        fsummary.close()

        # ---- HitSpoolData Access and Copy ----:
        #how many files n_copy_files do we have to copy:

        if sn_start_file < sn_stop_file:
            n_copy_files = ((sn_stop_file - sn_start_file) + 1) % MAXF
            print "n_copy_files = %s " % n_copy_files
        else:
            n_copy_files = ((sn_stop_file - sn_start_file)+ MAXF + 1) % MAXF # mod MAXF for the case that sn_start & sn_stop are in the same HS file
            print "n_copy_files = %s " % n_copy_files

        sn_start_mod = ALERTSTART.strftime("%Y%m%d_%Hh%Mm%Ss_") 
        sn_start_foldername = re.sub(" ","_", str(sn_start_mod))
        hs_copydest = hs_copydir+sn_start_foldername+src_mchn
        print "unique naming for folder: %s " % sn_start_foldername

        #move these files aside into subdir /tmp/ to prevent from being overwritten from next hs cycle while copying:
        #make subdirectory "/tmp" . if it exists already
        tmp_dir = hs_sourcedir+"/tmp/"
        try:
            subprocess.check_call("mkdir "+tmp_dir, shell=True)
            print "created subdir for hs files taht should be copied"
        except subprocess.CalledProcessError:
            print "Subdir /tmp already exists" 
            pass
    
        copy_files_list = list()    
        for i in range (n_copy_files):
            sn_start_file_i = (sn_start_file+i)%MAXF
            next_file = re.sub("HitSpool-"+str(sn_start_file), "HitSpool-"+str(sn_start_file_i), sn_start_file_str)
            
            #move these files aside to prevent from being overwritten from next hs cycle while copying:

            hs_tmp_move = subprocess.check_call("mv "+next_file+" "+tmp_dir, shell=True)
            if hs_tmp_move == 0:
                next_tmpfile = tmp_dir+"HitSpool-"+str(sn_start_file_i)+".dat"
                #print "\nnext file to copy is: %s" % next_copy
                copy_files_list.append(next_tmpfile)
            else:
                print "ERROR: moving hitspool file to tmp dir failed"
        print "list of files to copy %s" % copy_files_list
            
        copy_files_str = " ".join(copy_files_list) 
        print "joined string of files to copy:\n %s" % copy_files_str
        print "last file to copy is:\n%s" % sn_stop_file_str
        
        
        
        
        rsync_cmd = "nice rsync -avv --bwlimit=800 --log-format=%i%n%L "+copy_files_str+" "+hs_ssh_access+':'+hs_copydest+" >>"+summaryfile
        
        print "rsync does:\n %s" % rsync_cmd 
        fsummary = open(summaryfile, "a")
        print >> fsummary, "\n *** from rsync process: ***\n"
        fsummary.close()

        
        try:
            hs_rsync = subprocess.check_call(rsync_cmd,shell=True)
            print hs_rsync
            #return hs_rsync
            print "copied data from %s at machine %s to %s at machine %s" % (hs_sourcedir, src_mchn, hs_copydest, hs_ssh_access)
            print "data is copied & stored to %s at %s " % (hs_copydest, hs_ssh_access)
            fsummary = open(summaryfile, "a")
            print >>fsummary, "data is copied & stored to %s at %s " % (hs_copydest, hs_ssh_access)
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at: %s\n**********************************************\n" % packer_stop
            fsummary.close()
                
        except subprocess.CalledProcessError:
            print "\nError in rsync from %s to %s....\n" %(src_mchn, hs_ssh_access)
            fsummary = open(summaryfile, "a")
            print >> fsummary, "rsyncing failed !!"
            packer_stop = str(datetime.utcnow())
            print >> fsummary, "Finished HitSpoolWorker at: %s\n**********************************************\n" % packer_stop
            fsummary.close()


    def info_report(self, summaryfile):
        """
        summerizes what has been done: hostname, dataload copied and timestamps of the alert
        """
        infolist = [src_mchn]
        dl_list = [] #new empty list to store the data load patterns found in the summaryfile
        start_list = []
        stop_list = []
        #print "the infolist contains: ", infolist
        dl_pattern = re.compile(r'total\ssize\sis\s[0-9]*', flags=re.MULTILINE)
        sn_start_pattern = re.compile(r'sn_start\:\s[0-9]{4}\-[0-9]{2}\-[0-9]{2}\s[0-9]{2}\:[0-9]{2}\:[0-9]{2}', flags=re.MULTILINE) 
        sn_stop_pattern = re.compile(r'sn_stop\:\s[0-9]{4}\-[0-9]{2}\-[0-9]{2}\s[0-9]{2}\:[0-9]{2}\:[0-9]{2}', flags=re.MULTILINE)
        infile = open(summaryfile, "r")
        for txtline in infile.readlines():
            txtline = txtline.rstrip()
            new_dl = dl_pattern.findall(txtline) # new_dl is a list with one element 
            new_sn_start = sn_start_pattern.findall(txtline)
            new_sn_stop = sn_stop_pattern.findall(txtline)
            #print txtline
            #infolist.append(dl_list[-1])
            #for word in new_dl:
                #print 'this pattern is found and now contained in the findalllist: ' ,word
            if new_dl is not None and new_dl not in dl_list:
                dl_list.append(new_dl)
                #print 'findall found in total dataload patterns: \n', dl_list
#            for word in new_sn_start:
#                print 'this pattern is found and now contained in the findalllist: ' ,word
            if new_sn_start is not None and new_sn_start not in start_list:
                start_list.append(new_sn_start)
                #print "finadall found sn-start pattern: ", start_list
            if new_sn_stop is not None and new_sn_stop not in stop_list:
                stop_list.append(new_sn_stop) 
        #print "the findall list carries: ", dl_list
        infolist.append(dl_list[-1][0])
        infolist.append(start_list[-1][0])
        infolist.append(stop_list[-1][0])
        print "infolist now contains:\n" , infolist


        #print " now the infolist contains: ", infolist
        #infostring = " ,".join(infolist)
        #print "the infolist joined to a long string likes like this: \n",infostring 
        infostring = str(infolist)
        return infostring
        
context = zmq.Context()
#spts_expcont_ip = "10.2.2.12"
spts_expcont_ip = "expcont"

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
                 
        # Socket to send meassage to :
        sender = context.socket(zmq.PUSH)
        sender.connect("tcp://"+spts_expcont_ip+":55560")
        print "PUSH-Socket to send message to:\nport 55560 on %s" % spts_expcont_ip
        
        # Socket to synchronize the Publisher with the Workers
        syncclient = context.socket(zmq.PUSH)
        syncclient.connect("tcp://"+spts_expcont_ip+":55562")

        # send a synchronization request:
        try:
            syncclient.send("Hi! Hub wants to connect!")
            print "syncservice sended sync request"
        except KeyboardInterrupt:
            sys.exit()
        
        while True:
            try:
                print 'waiting for new alert...'
                
                #alert is encoded as JSON:
                # message = subsrciber.recv_json()
                
                #alert is a regular string:
                message = subscriber.recv()
                
                print "received message"
                self.message = message
                print "HubServer got alert message:\n%s\n from Publisher" % (message)
                
                print "now parsing should start... \n"
                newalert = MyAlert()
                newalert.alert_parser(message, src_mchn)
                #print "parsing and copying finished succesful...\n now summarize the :\n "
                infostr = newalert.info_report(summaryfile)
                print " info_report gives:\n", infostr
                
                # wait for alert parser to be finished and that it provides the infolist!
                # send back: 'DONE' message and information about the copy process: hostname timestamps & dataload copied: infolist[]    
                # print "the infolist contains: " , infolist
                sender.send(infostr)
                print "Thanks for the Alert. Greetings from the HitSpoolHub..."
                print "\nHS_Worker ready for next alert\n"
            except KeyboardInterrupt:
                print " Interruption received, proceeding..."
                sys.exit()

x = MyHubserver()
x.sync_worker()
