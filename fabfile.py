
"""
fabfile.py for hitspool interface on SPTS (default) , SPS or on a localhost machine.
David Heereman , i3.hsinterface@gmail.com
"""

from fabric.api import *
#from fabric.decorators import parallel
import subprocess
#from optparse import OptionParser
from fabric.contrib.project import rsync_project
import sys
import re
from datetime import  datetime
from email.mime.text import MIMEText

###
### detect the system this fab is running on 
###
with settings(hide('running')):
    host = local("hostname -f", capture=True).stdout
    user = local("whoami", capture=True).stdout
            
    if not ".icecube." in host:
        fastprint("This fabfile runs in your own test environment.\nPlease provide the follwing variables:\n")
        SystemName = "LOCALHOST"
        host_short = host
        fastprint("Make sure to have a hitspool data directory structure on your test-system (lastRun & currentRun) with data files.\n")
    else:
        if "pdaq" != user:
            fastprint("Sorry user " + user + ", you are not pdaq. Please try again as pdaq.\n")
            sys.exit(0)
        #check machine
        if not "access" in host:
            fastprint("Wrong machine. Use access machine for SPTS or SPS instead.\n")
            sys.exit(0)
        #check host
        if "wisc.edu" in host:
            SystemName = "SPTS"
            host_short = re.sub(".icecube.wisc.edu", "", host)
        elif "usap.gov" in host:
            SystemName = "SPS"
            host_short = re.sub(".icecube.usap.gov", "", host)
        else:
            fastprint("Wrong host. Use SPTS or SPS instead.\n")
            sys.exit(0)    
            
    fastprint("User " + user + " at " + host + " is running this fab on " + SystemName +"\n")

###
### Set the environment variables according to the cluster:
###
if  SystemName ==  "SPTS" :

    SVN_PATH        = "http://code.icecube.wisc.edu/svn/sandbox/dheereman/HitSpoolScripts/trunk"
    CHECKOUT_PATH   = "/scratch/dheereman/HsInterface/trunk/"
    HSiface_PATH    = "/mnt/data/pdaqlocal/HsInterface/trunk/"
    DEPLOY_TARGET   = [ "2ndbuild" , "ichub21", "ichub29", "expcont"]
    CRONTAB_PATH    = HSiface_PATH + "hs_crontabs_spts.txt"
    CRONTAB_PATH_2ndbuild = HSiface_PATH + "hs_crontabs_spts_2ndbuild.txt"
    CRONTAB_PATH_expcont = HSiface_PATH + "hs_crontabs_spts_expcont.txt"
    LOGPATH         = re.sub("trunk", "logs",HSiface_PATH)
    FABLOGPATH      = re.sub("trunk", "logs",CHECKOUT_PATH)
    FABLOGFILE      = FABLOGPATH +  "/" + "hs_fab.log"
    
    
    StartWorker     = "python " + HSiface_PATH + "HsWorker.py"
    StartPublisher  = "python " + HSiface_PATH + "HsPublisher.py"
    StartSender     = "python " + HSiface_PATH + "HsSender.py"
    StartWatcher    = "python " + HSiface_PATH + "HsWatcher.py"
#    StartController    = "python " + HSiface_PATH + "HsController.py"
    env.parallel = True
    env.disable_known_hosts = True
    env.roledefs = {
            'access' : ['pdaq@access'],
            '2ndbuild' : ['pdaq@2ndbuild'],
            'expcont' : ['pdaq@expcont'],
            'hubs' : ['pdaq@ichub21', 'pdaq@ichub29'],
                    }
    
    do_local = False
#        print "do_local is set to ", do_local
#        return do_local

elif SystemName == "SPS":

    SVN_PATH        = "http://code.icecube.wisc.edu/daq/projects/hitspool/trunk"
    CHECKOUT_PATH   = "/home/pdaq/HsInterface/trunk/"
    HSiface_PATH    = "/mnt/data/pdaqlocal/HsInterface/trunk/"
    DEPLOY_TARGET   = ["ichub%0.2d" %i for i in range(87)[1::]] + ["ithub%0.2d" %i for i in range(12)[1::]] + ["expcont", "2ndbuild"] 
    CRONTAB_PATH    = HSiface_PATH + "hs_crontabs_sps.txt"
    CRONTAB_PATH_2ndbuild = HSiface_PATH + "hs_crontabs_sps_2ndbuild.txt"
    CRONTAB_PATH_expcont  = HSiface_PATH + "hs_crontabs_sps_expcont.txt"

    LOGPATH         = re.sub("trunk", "logs",HSiface_PATH)
    FABLOGPATH         = re.sub("trunk", "logs",CHECKOUT_PATH)
    FABLOGFILE         = FABLOGPATH +  "/" + "hs_fab.log"

    StartWorker     = "python26 " + HSiface_PATH + "HsWorker.py"
    StartPublisher  = "python26 " + HSiface_PATH + "HsPublisher.py"
    StartSender     = "python26 " + HSiface_PATH + "HsSender.py" 
    StartWatcher    = "python26 " + HSiface_PATH + "HsWatcher.py"
#    StartController    = "python " + HSiface_PATH + "HsController.py"
          
    env.parallel = True
    env.disable_known_hosts = True
    env.roledefs = {
            'access' : ['pdaq@access'],
            '2ndbuild' : ['pdaq@2ndbuild'],
            'expcont' : ['pdaq@expcont'],
            'hubs' : ['pdaq@ichub01','pdaq@ichub02', 'pdaq@ichub03', 'pdaq@ichub04', 'pdaq@ichub05', 'pdaq@ichub06', 'pdaq@ichub07', 'pdaq@ichub08', 'pdaq@ichub09', 'pdaq@ichub10', 'pdaq@ichub11',
                          'pdaq@ichub12', 'pdaq@ichub13', 'pdaq@ichub14', 'pdaq@ichub15', 'pdaq@ichub16', 'pdaq@ichub17', 'pdaq@ichub18', 'pdaq@ichub19', 'pdaq@ichub20', 'pdaq@ichub21', 'pdaq@ichub22',
                          'pdaq@ichub23', 'pdaq@ichub24', 'pdaq@ichub25', 'pdaq@ichub26', 'pdaq@ichub26', 'pdaq@ichub27', 'pdaq@ichub28', 'pdaq@ichub29', 'pdaq@ichub30', 'pdaq@ichub31', 'pdaq@ichub32',
                          'pdaq@ichub33', 'pdaq@ichub34', 'pdaq@ichub35', 'pdaq@ichub36', 'pdaq@ichub37', 'pdaq@ichub38', 'pdaq@ichub39', 'pdaq@ichub40', 'pdaq@ichub41', 'pdaq@ichub42', 'pdaq@ichub43',
                          'pdaq@ichub44', 'pdaq@ichub45', 'pdaq@ichub46', 'pdaq@ichub47', 'pdaq@ichub48', 'pdaq@ichub49', 'pdaq@ichub50', 'pdaq@ichub51', 'pdaq@ichub52', 'pdaq@ichub53', 'pdaq@ichub54',
                          'pdaq@ichub55', 'pdaq@ichub56', 'pdaq@ichub57', 'pdaq@ichub58', 'pdaq@ichub59', 'pdaq@ichub60', 'pdaq@ichub61', 'pdaq@ichub62', 'pdaq@ichub63', 'pdaq@ichub64', 'pdaq@ichub65',
                          'pdaq@ichub66', 'pdaq@ichub67', 'pdaq@ichub68', 'pdaq@ichub69', 'pdaq@ichub70', 'pdaq@ichub71', 'pdaq@ichub72', 'pdaq@ichub73', 'pdaq@ichub74', 'pdaq@ichub75', 'pdaq@ichub76',
                          'pdaq@ichub77', 'pdaq@ichub78', 'pdaq@ichub79', 'pdaq@ichub80', 'pdaq@ichub81', 'pdaq@ichub82', 'pdaq@ichub83', 'pdaq@ichub84', 'pdaq@ichub85', 'pdaq@ichub86', 
                          'pdaq@ithub01', 'pdaq@ithub02', 'pdaq@ithub03', 'pdaq@ithub04', 'pdaq@ithub05', 'pdaq@ithub06', 'pdaq@ithub07', 'pdaq@ithub08', 'pdaq@ithub09', 'pdaq@ithub10', 'pdaq@ithub11'], 
                    }
    do_local = False
#        print "do_local is set to ", do_local
#        return do_local

elif SystemName == "LOCALHOST":
    
    rolename = user + '@' + host
    print "This fabfile is running on a local machine.\n\
    This means that there is no real cluster. \n\
    So we'll assume the following:\n\
    SVN_PATH = your development sandbox\n\
    HSiface_PATH = CHECKOUT_PATH\n"
           
    SVN_PATH = "http://code.icecube.wisc.edu/svn/sandbox/dheereman/HitSpoolScripts/trunk"       
    CHECKOUT_PATH   = HSiface_PATH  = str(raw_input("Your local path to the HitSpool Interface: "))
    DEPLOY_TARGET   = ["localhost"]
    CRONTAB_PATH    = HSiface_PATH + "hs_crontabs_test.txt"
    CRONTAB_PATH_2ndbuild = HSiface_PATH + "hs_crontabs_test_2ndbuild.txt"
    CRONTAB_PATH_expcont  = HSiface_PATH + "hs_crontabs_test_expcont.txt"
    LOGPATH         = re.sub("trunk", "logs",HSiface_PATH)
    FABLOGPATH         = re.sub("trunk", "logs",CHECKOUT_PATH)
    FABLOGFILE         = FABLOGPATH +  "/" + "hs_fab.log"
    
    StartWorker     = "python " + HSiface_PATH + "HsWorker.py"
    StartPublisher  = "python " + HSiface_PATH + "HsPublisher.py"
    StartSender     = "python " + HSiface_PATH + "HsSender.py"
    StartWatcher    = "python " + HSiface_PATH + "HsWatcher.py"
#    StartController    = "python " + HSiface_PATH + "HsController.py"
    
    env.use_ssh_config = True
    env.parallel = True
    env.disable_known_hosts = True
    env.roledefs = {
            'access' : [rolename],
            '2ndbuild' : [rolename],
            'expcont' : [rolename],
            'hubs' : [rolename],
                    }
    
    do_local = True
    print "do_local is ",do_local
    #return do_local
else:
    fastprint("Unidentified Cluster. Exit now.\n")
    sys.exit(0) 

###
### ---- general utility functions ---#
###


#--- function for Alert emails ---#
def _sendMail(subj, msgline, msgtype):
    msg = MIMEText(msgline)
#    msg["From"] = "david.heereman@ulb.ac.be"
    msg["To"] = "i3.hsinterface@gmail.com"
    msg["Subject"] = subj + " HsInterface Alert: " + SystemName +" fabric"  
    p = subprocess.Popen(["/usr/sbin/sendmail", "-t"], stdin=subprocess.PIPE)
    p.communicate(msg.as_string())
    _log("Email was sent about " + msgtype + " ...")

##execute command with additional check that output is no digit (= no error code is returned)
##returns the stdout of the command in a list
def _local_return_stdout(command):
    with settings(hide('running','stdout')):
        p           = subprocess.Popen(["ps", "ax"], shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        out, err    = p.communicate()
        returnout   = out.rstrip()
        returnerr   = err.rstrip()
        returnlist    = returnout.split("\n")
    return returnlist

def _log(msg):
    # fabric version  < 2.0 doesn't support any customizable output 
    # nor the redirection of stdout to logging module: https://github.com/fabric/fabric/issues/57
    # logging would write only the std.output but not the fabirc output
    # -> build your logfile format yourself :( 
    open(FABLOGFILE, "a").write(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " INFO "  + str(msg) + "\n")
    fastprint(msg+"\n")
    
def _capture_local(cmd, pty=False):
    """
    Call local() with capture enabled to emulate run() behavior
    """
    return local(cmd, capture=True)
        
#@roles('access')
def hs_checkout(SVN_PATH, CHECKOUT_PATH): 
    """
    SVN co HS interface code.
    """   
    with hide("running", "stdout"):
        _log("checked out source code from %s to %s..." % (SVN_PATH, CHECKOUT_PATH))
        local("svn co " + SVN_PATH + " " + CHECKOUT_PATH + " " )
        _log("check out done.\n")
    pass

#@parallel
#@roles('expcont', '2ndbuild', 'hubs')    
def hs_mk_dir(do_local=False):
    """
    Make HsInterface directory at destination for all.
    """
    for host in DEPLOY_TARGET:
        hs_mk_dir_on_host(host)
    
def hs_mk_dir_on_host(host):
    """
    Make HsInterface directory at destination <host>.
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(host_string=host): 
        with hide("running", "stdout"):   
            if host == "2ndbuild":
                _log("Creating (if not there yet) the HsInterface directories on " + host +" ...")
                frun("mkdir -p " + LOGPATH) 
                _log('LOGPATH: '+ str(LOGPATH) + ' set')                                       
                frun("mkdir -p " + HSiface_PATH)  
                _log('HSiface_PATH: ' + str(HSiface_PATH) + ' set')                
                frun("mkdir -p " + LOGPATH + "workerlogs/")
                _log('WorkerLogsCopyPATH: '+ str(LOGPATH) + "workerlogs/" + ' set')
                frun("mkdir -p /mnt/data/HitSpool/unlimited/")
                _log("SPADE pickup directories set")   
                
                
            else:
                _log("Creating (if not there yet) the HsInterface directories on " + host +" ...")
                frun("mkdir -p " + LOGPATH)                        
                frun("mkdir -p " + HSiface_PATH)  
                _log('HSiface_PATH: ' + str(HSiface_PATH) + ' set')
                _log('LOGPATH: '+ str(LOGPATH) + ' set')
            
                
                
def _deactivate_hsiface_cron():
    for host in [DEPLOY_TARGET]:
        deactivate_hsiface_cron_for_host(host)

def deactivate_hsiface_cron_for_host(host):
    """
    Deactivates the HsWatcher crnjob on <host>
    """
    with settings(host_string=host):
        _log("Deactivating HSiface cronjobs on " + host + "...")
        with hide("running"):
            run("crontab -l |sed '/HSiface/s/^/#/' |crontab -") # deactivate
        _log("done.\n")
        
def _set_up_all_cronjobs():
    for host in DEPLOY_TARGET:
        set_up_cronjobs_for_host(host)
    
def set_up_cronjobs_for_host(host):
    """
    Activate HsInterface cronjobs for host.
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(host_string=host):
        _log("Setting up HSiface cronjobs on " + host + "...")
        if "2ndbuild" in host:
            with hide("running"):
                frun("(cat " + CRONTAB_PATH_2ndbuild + "; echo; crontab -l |grep -v HSiface |grep -v '^$') | crontab -")
        elif "expcont" in host:
            with hide("running"):
                frun("(cat " + CRONTAB_PATH_expcont + "; echo; crontab -l |grep -v HSiface |grep -v '^$') | crontab -")
        else:
            with hide("running"):
                frun("(cat " + CRONTAB_PATH + "; echo; crontab -l |grep -v HSiface |grep -v '^$') | crontab -")
        _log("done.")
            
###    
### ----- HS service functions ---#    
###

#@roles("2ndbild")
#def hs_start_control():
#    """
#    Start HsController service.
#    """
#    if do_local:
#        frun = _capture_local
#    else:
#        frun = run
#    frun(StartController)

@roles('expcont')
def _hs_start_pub():
    """
    Start the HsPublisher service 
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    _log("")
    frun(StartPublisher)

#@roles('expcont')
def hs_start_pub_bkg(host):
    """
    Start the HsPublisher service in bkg  on <host>
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(host_string=host):
        with hide("running"):
            _log("Start Publisher remotely via startup script in bkg")
            frun("source " + HSiface_PATH + "run_pub_bkg.sh " + HSiface_PATH, shell=True, pty=False)
            _log("done.")

    
@roles('2ndbuild')
def _hs_start_sender():
    """
    Start the Sender service 
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    frun(StartSender)                          

#@roles('2ndbuild')
def hs_start_sender_bkg(host):
    """
    Start the Sender service in bkg  on <host>
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(host_string=host):
        with hide("running"):
            _log("Start Sender remotely via startup script in bkg")
            frun("source " + HSiface_PATH + "run_sender_bkg.sh " + HSiface_PATH, shell=True, pty=False)
            _log("done.")
        
#@roles('hubs')    
def _hs_start_worker():
    """
    Start the Worker service 
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    frun(StartWorker)

#@roles('hubs')    
def hs_start_worker_bkg(host):
    """
    Start the Worker service in bkg on <host>
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(host_string=host):
        with hide("running"):
            _log("Start Worker remotely via startup script in bkg")
            frun("source " + HSiface_PATH + "run_worker_bkg.sh " + HSiface_PATH, shell=True, pty=False)
            _log("done.")
    
#def hs_run_watcher_host(host):
#    """
#    Run HsWatcher once on host to see status of service.
#    """
##    if do_local:
##        frun = _capture_local
##    else:
##        frun = run
#    with settings(host_string=host):
#        with hide("running"):
#            _log("run HsWatcher on " + host + " ...")
#            run(StartWatcher)   
#            _log("done.\n")
#
#def hs_run_watcher_once_all():
#    """
#    Run the HsWatcher service once on all machines.
#    The corresponding HsInterface service will be started on the machine if not running. 
#    """
#    for target in DEPLOY_TARGET:
#        hs_run_watcher_host(target)
                
@roles('expcont') 
def hs_stop_pub():
    """
    Stop the Publisher.
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(warn_only=True):
        with hide('running', 'warnings'):  
            result = frun("pkill -f \"" +  StartPublisher + "\"") # see man page of "pkill" for exit staus details
            if result.return_code == 0:
                _log("Found.")
            elif result.return_code == 1:
                _log("No processes matched. Nothing to stop.")
            elif result.return_code == 2:
                _log("Syntax error in the pkill command string")
            else:
                _log(result)
                raise SystemExit()

@roles('2ndbuild')     
def hs_stop_sender():
    """
    Stop the Sender.
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run  
    with settings(warn_only=True):  
        with hide('running', 'warnings'):
            result = frun("pkill -f \"" +  StartSender + "\"") # see man page of "pkill" for exit staus details
            if result.return_code == 0:
                _log("Found.")
            elif result.return_code == 1:
                _log("No processes matched. Nothing to stop.")
            elif result.return_code == 2:
                _log("Syntax error in the pkill command string")
            else:
                _log(result)
                raise SystemExit()
        
#@roles('2ndbuild')     
#def hs_stop_controller():
#    """
#    Stop the Sender.
#    """
#    if do_local:
#        frun = _capture_local
#    else:
#        frun = run    
#    with hide('everything'):
#        frun("pkill -f \"" +  StartController + "\"")
        
@parallel
@roles('hubs')
def hs_stop_workers():
    """
    Stop all Worker in parallel.
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run       
    with settings(warn_only=True):
        with hide('running', 'warnings'):  
            result = frun("pkill -f \"" +  StartWorker + "\"") # see man page of "pkill" for exit staus details
            if result.return_code == 0:
                _log("Found.")
            elif result.return_code == 1:
                _log("No processes matched. Nothing to stop.")
            elif result.return_code == 2:
                _log("Syntax error in the pkill command string")
            else:
                _log(result)
                raise SystemExit()          

def hs_stop_worker_on_host(host):
    """
    Stop the Worker on host.
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run       
    with settings(host_string=host, warn_only=True):
        with hide('running'):  
            result = frun("pkill -f \"" +  StartWorker + "\"") # see man page of "pkill" for exit staus details
            if result.return_code == 0:
                _log("Found.")
            elif result.return_code == 1:
                _log("No processes matched. Nothing to stop.")
            elif result.return_code == 2:
                _log("Syntax error in the pkill command string")
            else:
                _log(result)
                raise SystemExit()
            
def hs_stop_watcher_on_host(host):
    """
    Stop a hanging HsWatcher on <host>
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run       
    with settings(host_string=host, warn_only=True):
        with hide('running'):  
            result = frun("pkill -f \"" +  StartWatcher + "\"") # see man page of "pkill" for exit staus details
            if result.return_code == 0:
                _log("Found.")
            elif result.return_code == 1:
                _log("No processes matched. Nothing to stop.")
            elif result.return_code == 2:
                _log("Syntax error in the pkill command string")
            else:
                _log(result)
                raise SystemExit()
    
    
    

def hs_stage():
    """
    SVN checkout & mkdir 
    """
    _log("Staging the HsInterface components...")
    if SystemName == "SPS":
        hs_checkout(SVN_PATH, CHECKOUT_PATH)
    else:
        _log("No SVN checkout done because not running on SPS. If needed, please do that manually.")
    hs_mk_dir(do_local)
    _log("done.")

def hs_deploy_to_host(host):
    with settings(host_string=host):
        _log("Deploying (rsyncing) HSiface to " + host + "...")
        with hide("running", "stdout"):
            rsync_project(HSiface_PATH , CHECKOUT_PATH, exclude=(".svn"))
        _log("done.\n")

#@roles('expcont', '2ndbuild', 'hubs')    
def hs_deploy():
    """
    Deploy (rsync) HitSpool interface components
    """
    _log("Deploy targets are: " + str(DEPLOY_TARGET))
    for host in DEPLOY_TARGET:
            hs_deploy_to_host(host)
            
def hs_install():
    """
    Installing the HitSPool Interface on the system
    """
    hs_stage()
    hs_deploy()
    
def hs_stop():
    """
    Stopping all HsInterface components.
    """
    for target in DEPLOY_TARGET:
        with settings(host_string=target):
            with hide('running'):
                _log("HsInterface will be stopped on " + target)
                deactivate_hsiface_cron_for_host(target)
                _log("done.")
                if target == "2ndbuild":
                    _log("Looking for running HsSender service on " + target + " ...\n")
                    hs_stop_sender()
                    _log("done.") 
    #                _log("Stopping HsController service on " + target + " ...")
    #                hs_stop_control()
    #                _log("done.") 
                elif target == "expcont":
                    _log("Looking for running HsPublisher service on " + target + " ...\n")
                    hs_stop_pub()
                    _log("done.")
                elif "hub" in target:
                    _log("Looking for running HsWorker service on " + target + " ...\n")                
                    hs_stop_worker_on_host(target)
                    _log("done.")
                elif target == "localhost":
                    _log("Stopping HsInterface components on testcluster localhost...")
                    hs_stop_worker_on_host(target)
                else:
                    _log("unidentified target...")
    _sendMail("STOPPED", "HsInterface services were stopped via fabric " + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) , "stop all")

def hs_start():
    """
    (Re)Starting all HsInterface components. 
    """
    #first stop all components / cronjobs that might still be running:
    _log("All remaining running components will be stopped for doing a clean start afterwards...")
    hs_stop()
    _log("Staring HsInterface components...")
    #launch HsInterface services via executing HsWatcher once (not as cronjobs) on all machines:
    for host in DEPLOY_TARGET:
        if host == "2ndbuild":
            hs_start_sender_bkg(host)
            set_up_cronjobs_for_host(host)
        elif host == "expcont":
            hs_start_pub_bkg(host)
            set_up_cronjobs_for_host(host)
        elif "hub" in host:
            hs_start_worker_bkg(host)
            set_up_cronjobs_for_host(host)
        else:
            _log("unidentified machine in DEPLOY_TARGET. Please Check your Cluster Settings.")
            
    _sendMail("(RE)START", "HsInterface services were (re)started via fabric at " + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) , "(re)start")
    


def hs_status_on_host(host):
    """
    Check status on individual <host>
    """
    with settings(host_string=host):
        with hide("running", "stdout"):
            if "hub" in host:
                proc = run("ps ax")
                # proc = number of running HsWorker instances PLUS 1 (grep command also counts) 
                if "HsWorker" in proc:
                    _log(str(host) + ": HsWorker active")
                else:
                    _log(str(host) + ": HsWorker NOT active")
                        
            elif "2ndbuild" in host:
                proc = run("ps ax | grep HsSender | wc -l")
                # proc = number of running HsSender instances PLUS 1 (grep command also counts) 
                if "HsSender" in proc:
                    _log(str(host) + ": HsSender active")
                else:
                    _log(str(host) + ": HsSender NOT active")

            elif "expcont" in host:
                proc = run("ps ax | grep HsPublisher | wc -l")
                # proc = number of running HsPiblisher instances PLUS 1 (grep command also counts) 
                if "HsPublisher" in proc:
                    _log(str(host) + ": HsPublisher active")
                else:
                    _log(str(host) + ": HsPublisher NOT active")
   
            else:
                _log("Running on wrong host")
            
            #return host, proc
                

def hs_status():
    """
    Summarize how many HsInterface componets are up and runnning.
    """
    ACTIVE_COMP     = [] #list vor active hs services
    INACTIVE_COMP   = [] #list vor active hs services
    
    for host in DEPLOY_TARGET:
        with settings(host_string=host):
            with hide("running", "stdout"):
                processes = run("ps ax")
                if "hub" in host:
                    if "HsWorker" in processes:
                        ACTIVE_COMP.append(host)
                    else:
                        INACTIVE_COMP.append(host)
                elif host == "2ndbuild":
                    if "HsSender" in processes:
                        ACTIVE_COMP.append(host)
                    else:
                        INACTIVE_COMP.append(host)
                elif host == "expcont":
                    if "HsPublisher" in processes:
                        ACTIVE_COMP.append(host)
                    else:
                        INACTIVE_COMP.append(host) 
                else:
                    #wrong host
                    pass
    print len(ACTIVE_COMP) ," HsInterface components are active:\n" + str(ACTIVE_COMP)
    print len(INACTIVE_COMP) ," HsInterface components are NOT active:\n" + str(INACTIVE_COMP)                       
