
"""
fabfile.py for hitspool interface on SPTS (default), SPS or on a localhost machine.
David Heereman, i3.hsinterface@gmail.com
"""

import os
import subprocess
import sys
import re
import zmq

from datetime import  datetime
from email.mime.text import MIMEText
from fabric.api import *
from fabric.contrib.project import rsync_project
#from fabric.decorators import parallel
#from optparse import OptionParser

import HsConstants


###
### detect the system this fab is running on
###
with settings(hide('running')):
    HOST = local("hostname -f", capture=True).stdout
    USER = local("whoami", capture=True).stdout

    if not ".icecube." in HOST:
        fastprint("This fabfile runs in your own test environment.\n"
                  "Please provide the following variables:\n")
        SYSTEM_NAME = "LOCALHOST"
        fastprint("Make sure to have a hitspool data directory structure on"
                  " your test-system (lastRun & currentRun) with data files.\n")
    else:
        if "pdaq" != USER:
            fastprint("Sorry user %s, you are not pdaq."
                      " Please try again as pdaq.\n" % USER)
            sys.exit(0)
        #check machine
        if not "access" and not "expcont" in HOST:
            fastprint("Wrong machine. Use access or expcont machine for SPTS or SPS instead.\n")
            sys.exit(0)
        #check host
        if "wisc.edu" in HOST:
            SYSTEM_NAME = "SPTS"
        elif "usap.gov" in HOST:
            SYSTEM_NAME = "SPS"
        else:
            fastprint("Wrong cluster. Use SPTS or SPS instead.\n")
            sys.exit(0)

    #fastprint("User " + user + " at " + HOST + " is running this fab on " + SYSTEM_NAME +"\n")

###
### Set the environment variables according to the cluster:
###
if  SYSTEM_NAME == "SPTS":

    SVN_PATH = "http://code.icecube.wisc.edu/svn/sandbox/dheereman/HitSpoolScripts/trunk"
    CHECKOUT_PATH = "/scratch/dheereman/HsInterface/trunk"
    HSIFACE_PATH = "/mnt/data/pdaqlocal/HsInterface/trunk"
    DEPLOY_TARGET = ["2ndbuild", "ichub21", "expcont"]
    CRONTAB_NAME = "spts"
    env.parallel = True
    env.disable_known_hosts = True
    env.roledefs = {
        'access' : ['pdaq@access'],
        '2ndbuild' : ['pdaq@2ndbuild'],
        'expcont' : ['pdaq@expcont'],
        'hubs' : ['pdaq@ichub21'],
    }

    DO_LOCAL = False

elif SYSTEM_NAME == "SPS":

    SVN_PATH = "http://code.icecube.wisc.edu/daq/projects/hitspool/trunk"
    CHECKOUT_PATH = "/home/pdaq/HsInterface/trunk"
    HSIFACE_PATH = "/mnt/data/pdaqlocal/HsInterface/trunk"
    DEPLOY_TARGET = ["ichub%0.2d" % i for i in range(1, 87)] + \
                      ["ithub%0.2d" % i for i in range(1, 12)] + \
                      ["expcont", "2ndbuild"]
    CRONTAB_NAME = "sps"

    env.parallel = True
    env.disable_known_hosts = True
    env.roledefs = {
        'access' : ['pdaq@access'],
        '2ndbuild' : ['pdaq@2ndbuild'],
        'expcont' : ['pdaq@expcont'],
        'hubs' : ['pdaq@ichub%02d' % i for i in range(1, 87)] + \
                 ['pdaq@ithub%02d' % i for i in range(1, 12)],
    }
    DO_LOCAL = False

elif SYSTEM_NAME == "LOCALHOST":

    rolename = USER + '@' + HOST
    print "This fabfile is running on a local machine.\n\
    This means that there is no real cluster. \n\
    So we'll assume the following:\n\
    SVN_PATH = your development sandbox\n\
    HSIFACE_PATH = CHECKOUT_PATH\n"

    SVN_PATH = "http://code.icecube.wisc.edu/svn/sandbox/dheereman/HitSpoolScripts/trunk"
    CHECKOUT_PATH = HSIFACE_PATH = str(raw_input("Your local path to the HitSpool Interface: "))
    DEPLOY_TARGET = ["localhost"]
    CRONTAB_NAME = "test"

    env.use_ssh_config = True
    env.parallel = True
    env.disable_known_hosts = True
    env.roledefs = {
        'access' : [rolename],
        '2ndbuild' : [rolename],
        'expcont' : [rolename],
        'hubs' : [rolename],
    }

    DO_LOCAL = True
else:
    fastprint("Unidentified Cluster. Exit now.\n")
    sys.exit(0)

LOGPATH = re.sub("trunk", "logs", HSIFACE_PATH)
FABLOGPATH = re.sub("trunk", "logs", CHECKOUT_PATH)
FABLOGFILE = os.path.join(FABLOGPATH, "hs_fab.log")

START_WORKER_CMD = "python " + os.path.join(HSIFACE_PATH, "HsWorker.py")
START_PUBLISHER_CMD = "python " + os.path.join(HSIFACE_PATH, "HsPublisher.py")
START_SENDER_CMD = "python " + os.path.join(HSIFACE_PATH, "HsSender.py")
START_WATCHER_CMD = "python " + os.path.join(HSIFACE_PATH, "HsWatcher.py")
#START_CONTROLLER_CMD = "python " + os.path.join(HSIFACE_PATH, "HsController.py")


###
### ---- general utility functions ---#
###


#--- function for Alert emails ---#
def _send_mail(subj, msgline, msgtype):
    msg = MIMEText(msgline)
#    msg["From"] = "david.heereman@ulb.ac.be"
    msg["To"] = HsConstants.ALERT_EMAIL_DEV
    msg["Subject"] = subj + " HsInterface Alert: %s fabric" % SYSTEM_NAME
    proc = subprocess.Popen(["/usr/sbin/sendmail", "-t"], stdin=subprocess.PIPE)
    proc.communicate(msg.as_string())
    _log("Email was sent about " + msgtype + " ...")

def _log(msg):
    # fabric version  < 2.0 doesn't support any customizable output
    # nor the redirection of stdout to logging module: https://github.com/fabric/fabric/issues/57
    # logging would write only the std.output but not the fabirc output
    # -> build your logfile format yourself :(
    with open(FABLOGFILE, "a") as logout:
        logout.write("%s  INFO %s\n" %
                     (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))
    fastprint("%s\n" % msg)

def _capture_local(cmd, shell=False, pty=False):
    """
    Call local() with capture enabled to emulate run() behavior
    """
    return local(cmd, capture=True)

#@roles('access')
def hs_checkout(svn_path, checkout_path):
    """
    SVN co HS interface code.
    """
    with hide("running", "stdout"):
        _log("checked out source code from %s to %s..." %
             (svn_path, checkout_path))
        local("svn co %s %s" % (svn_path, checkout_path))
        _log("check out done.\n")

#@parallel
#@roles('expcont', '2ndbuild', 'hubs')
def hs_mk_dir(do_local=False):
    """
    Make HsInterface directory at destination for all.
    """
    for host in DEPLOY_TARGET:
        hs_mk_dir_on_host(host, do_local=do_local)

def hs_mk_dir_on_host(host, do_local=False):
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
                frun("mkdir -p %s" % LOGPATH)
                _log('LOGPATH: %s set' % LOGPATH)
                frun("mkdir -p %s" % HSIFACE_PATH)
                _log('HSIFACE_PATH: %s set' % HSIFACE_PATH)
                wlogdir = os.path.join(LOGPATH, "workerlogs")
                frun("mkdir -p %s" % wlogdir)
                _log('WorkerLogsCopyPATH: %s set' % wlogdir)
                frun("mkdir -p /mnt/data/HitSpool/unlimited")
                _log("SPADE pickup directories set")


            else:
                _log("Creating (if not there yet) the HsInterface directories on " + host +" ...")
                frun("mkdir -p %s" % LOGPATH)
                frun("mkdir -p %s" % HSIFACE_PATH)
                _log('HSIFACE_PATH: %s set' % HSIFACE_PATH)
                _log('LOGPATH: %s set' % LOGPATH)

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

def set_up_cronjobs_for_host(host, do_local=False):
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
            suffix = "_2ndbuild"
        elif "expcont" in host:
            suffix = "_expcont"
        else:
            suffix = ""

        cronfile = os.path.join(HSIFACE_PATH, "hs_crontabs_%s%s.txt" %
                                (CRONTAB_NAME, suffix))
        with hide("running"):
            frun("(cat %s; echo; crontab -l |grep -v HSiface | grep -v '^$')"
                 " | crontab -" % cronfile)
        _log("done.")

###
### ----- HS service functions ---#
###

#@roles("2ndbild")
#def hs_start_control(do_local=False):
#    """
#    Start HsController service.
#    """
#    if do_local:
#        frun = _capture_local
#    else:
#        frun = run
#    frun(START_CONTROLLER_CMD)

@roles('expcont')
def _UNUSED_hs_start_pub():
    """
    Start the HsPublisher service
    """
    if DO_LOCAL:
        frun = _capture_local
    else:
        frun = run
    _log("")
    frun(START_PUBLISHER_CMD)

#@roles('expcont')
def hs_start_pub_bkg(host, do_local=False):
    """
    Start the HsPublisher service in bkg  on <host>
    """
    _hs_start_cmd_bkg(host, "HsPublisher.py", do_local=do_local)

@roles('2ndbuild')
def _hs_start_sender(do_local=False):
    """
    Start the Sender service
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    frun(START_SENDER_CMD)

#@roles('2ndbuild')
def hs_start_sender_bkg(host, do_local=False):
    """
    Start the Sender service in bkg  on <host>
    """
    _hs_start_cmd_bkg(host, "HsSender.py", do_local=do_local)

#@roles('hubs')
def _hs_start_worker(do_local=False):
    """
    Start the Worker service
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    frun(START_WORKER_CMD)

#@roles('hubs')
def hs_start_worker_bkg(host, do_local=False):
    _hs_start_cmd_bkg(host, "HsWorker.py", do_local=do_local)

def _hs_start_cmd_bkg(host, cmd, do_local=False):
    """
    Start the requested component in background on <host>
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(host_string=host):
        with hide("running"):
            _log("Start %s remotely via startup script in bkg" % cmd)
            frun('source "%s" "%s" "%s"' %
                 (os.path.join(HSIFACE_PATH, "run_cmd_bkg.sh"),
                  HSIFACE_PATH, cmd), shell=True, pty=False)
            _log("done.")


@roles('expcont')
def hs_stop_pub():
    """
    Stop the Publisher.
    """
    _hs_kill_script(START_PUBLISHER_CMD, do_local=DO_LOCAL)

@roles('2ndbuild')
def hs_stop_sender():
    """
    Stop the Sender.
    """
    _hs_kill_script(START_SENDER_CMD, do_local=DO_LOCAL)

def _hs_kill_script(script_to_kill, do_local=False):
    """
    Kill a script
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(warn_only=True):
        with hide('running', 'warnings'):
            result = frun("pkill -f \"%s\"" % script_to_kill)
            # see man page of "pkill" for exit staus details
            if result.return_code == 0:
                _log("Found.")
            elif result.return_code == 1:
                _log("No processes matched. Nothing to stop.")
            elif result.return_code == 2:
                _log("Syntax error in the pkill command string")
            else:
                _log(result)
                raise SystemExit()

def hs_stop_all_workers():
    for host in DEPLOY_TARGET:
        hs_stop_worker_on_host(host)

def hs_stop_worker_on_host(host, do_local=DO_LOCAL):
    """
    Stop the Worker on host.
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(host_string=host, warn_only=True):
        with hide('running'):

            result = frun("pkill -f \"%s\"" % START_WORKER_CMD)

            # see man page of "pkill" for exit staus details
            if result.return_code == 0:
                _log("Found running worker on host %s and stopped it" %
                     str(host))
            elif result.return_code == 1:
                _log("No processes matched. Nothing to stop.")
            elif result.return_code == 2:
                _log("Syntax error in the pkill command string")
            else:
                _log(result)
                raise SystemExit()

def hs_stop_watcher_on_host(host, do_local=DO_LOCAL):
    """
    Stop a hanging HsWatcher on <host>
    """
    if do_local:
        frun = _capture_local
    else:
        frun = run
    with settings(host_string=host, warn_only=True):
        with hide('running'):
            result = frun("pkill -f \"%s\"" % START_WATCHER_CMD)
            # see man page of "pkill" for exit status details
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
    if SYSTEM_NAME == "SPS":
        hs_checkout(SVN_PATH, CHECKOUT_PATH)
    else:
        _log("No SVN checkout done because not running on SPS. If needed, please do that manually.")
    hs_mk_dir(do_local=DO_LOCAL)
    _log("done.")

def hs_deploy_to_host(host):
    with settings(host_string=host):
        exclude_list = (".svn", ".hg", ".hgignore", "*.log", "*.pyc", "*.swp")
        hs_mk_dir_on_host(host)
        _log("Deploying (rsyncing) HSiface to " + host + "...")
        with hide("running", "stdout"):
            rsync_project(HSIFACE_PATH, CHECKOUT_PATH, exclude=exclude_list)
        _log("done.\n")


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
                    _log("Looking for running HsSender service on %s ...\n" %
                         target)
                    hs_stop_sender()
                    _log("done.")
    #                _log("Stopping HsController service on " + target + " ...")
    #                hs_stop_control()
    #                _log("done.")
                elif target == "expcont":
                    _log("Looking for running HsPublisher service on %s ...\n" %
                         target)
                    hs_stop_pub()
                    _log("done.")
                elif "hub" in target:
                    _log("Looking for running HsWorker service on %s ...\n" %
                         target)
                    hs_stop_worker_on_host(target)
                    _log("done.")
                elif target == "localhost":
                    _log("Stopping HsInterface components on testcluster localhost...")
                    hs_stop_worker_on_host(target)
                else:
                    _log("unidentified target...")
    _send_mail("STOPPED", "HsInterface services were stopped via fabric %s" %
               datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "stop all")

def hs_start():
    """
    (Re)Starting all HsInterface components.
    """
    #first stop all components / cronjobs that might still be running:
    _log("All remaining running components will be stopped for doing"
         " a clean start afterwards...")
    hs_stop()
    _log("Staring HsInterface components...")
    #launch HsInterface services via executing HsWatcher once
    # (not as cronjobs) on all machines:
    for host in DEPLOY_TARGET:
        if host == "2ndbuild":
            hs_start_sender_bkg(host, do_local=DO_LOCAL)
            set_up_cronjobs_for_host(host, do_local=DO_LOCAL)
        elif host == "expcont":
            hs_start_pub_bkg(host, do_local=DO_LOCAL)
            set_up_cronjobs_for_host(host, do_local=DO_LOCAL)
        elif "hub" in host:
            hs_start_worker_bkg(host, do_local=DO_LOCAL)
            set_up_cronjobs_for_host(host, do_local=DO_LOCAL)
        else:
            _log("unidentified machine in DEPLOY_TARGET. Please Check your Cluster Settings.")

    _send_mail("(RE)START", "HsInterface services were (re)started via fabric"
               " at %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
               "(re)start")


def hs_status():
    """
    Summarize how many HsInterface componets are up and runnning.
    """
    active_comp = [] #list vor active hs services
    inactive_comp = [] #list vor active hs services
    mylocalhost = local("hostname -f", capture=True).stdout
    for targethost in DEPLOY_TARGET:
        with settings(host_string=targethost):
            with hide("running", "stdout", "status"):
                processes = run("ps ax")
                if "hub" in targethost:
                    if "HsWorker" in processes:
                        active_comp.append(targethost)
                    else:
                        inactive_comp.append(targethost)
                elif targethost == "2ndbuild":
                    if "HsSender" in processes:
                        active_comp.append(targethost)
                    else:
                        inactive_comp.append(targethost)
                elif targethost == "expcont":
                    if 'expcont' in mylocalhost:
                        # if this fabfile is running on expcont
                        processes = local('ps ax', capture=True).stdout
                    if "HsPublisher" in processes:
                        active_comp.append(targethost)
                    else:
                        inactive_comp.append(targethost)
                else:
                    #wrong host
                    pass

    ###
    ### define zmq socket for I3Live JSON sending
    ###
    context = zmq.Context()
    i3socket = context.socket(zmq.PUSH)
    i3socket.connect("tcp://expcont:%d" % HsConstants.I3LIVE_PORT)

    if len(inactive_comp) > 0:
        value = "%s of %s components NOT RUNNING: %s" % \
                (len(inactive_comp), len(DEPLOY_TARGET), inactive_comp),
    else:
        value = "%s of %s components RUNNING" % \
                (len(DEPLOY_TARGET), len(DEPLOY_TARGET)),
    i3socket.send_json({"service": "HSiface",
                        "varname": "state",
                        "value": value,
                        "prio": 1})

    print "%d HsInterface components are active:\n%s" % \
        (len(active_comp), active_comp)
    print "%d HsInterface components are NOT active:\n%s" % \
        (len(inactive_comp), inactive_comp)
