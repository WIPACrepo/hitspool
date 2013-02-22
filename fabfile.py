"""
Fabric Script for running the HsInterface on SPTS or SPS
To be run being pdaq @ access
fab hs_start_pub:<numer of hubs connected>
"""




from fabric.api import *
#from fabric.decorators import parallel
import subprocess
from optparse import OptionParser
from fabric.contrib.project import rsync_project

#from fabric.state import env
#from fabric.operations import run


SVN_PATH        =  "http://code.icecube.wisc.edu/daq/projects/hitspool/trunk"
CHECKOUT_PATH   = "/home/pdaq/HsInterface/trunk/"
HSiface_PATH    = "/mnt/data/pdaqlocal/HsInterface/trunk/"

StartWorker     = "python26 " + HSiface_PATH + "HsWorker.py"
StartPublisher  = "python " + HSiface_PATH + "HsPublisher.py -n"
StartSender     = "python " + HSiface_PATH + "HsSender.py"



env.parallel = True
 
env.roledefs = {
                'spts-access' : ['dheereman@access.spts.icecube.wisc.edu'],
                'spts-2ndbuild' : ['pdaq@10.2.2.16'],
                'spts-expcont' : ['pdaq@10.2.2.12'],
                'spts-hubs' : ['pdaq@ichub29', 'pdaq@ichub21'],
                'spts-ichub29' : ['pdaq@ichub29'],
                'sps-access' : ['pdaq@sps-access'],
                'sps-2ndbuild' : ['pdaq@sps-2ndbuild'],
                'sps-expcont' : ['pdaq@sps-expcont'],
                'sps-hubs' : ['pdaq@ichub01','pdaq@ichub02', 'pdaq@ichub03', 'pdaq@ichub04', 'pdaq@ichub05', 'pdaq@ichub06', 'pdaq@ichub07', 'pdaq@ichub08', 'pdaq@ichub09', 'pdaq@ichub10', 'pdaq@ichub11',
                              'pdaq@ichub12', 'pdaq@ichub13', 'pdaq@ichub14', 'pdaq@ichub15', 'pdaq@ichub16', 'pdaq@ichub17', 'pdaq@ichub18', 'pdaq@ichub19', 'pdaq@ichub20', 'pdaq@ichub21', 'pdaq@ichub22',
                              'pdaq@ichub23', 'pdaq@ichub24', 'pdaq@ichub25', 'pdaq@ichub26', 'pdaq@ichub26', 'pdaq@ichub27', 'pdaq@ichub28', 'pdaq@ichub29', 'pdaq@ichub30', 'pdaq@ichub31', 'pdaq@ichub32',
                              'pdaq@ichub33', 'pdaq@ichub34', 'pdaq@ichub35', 'pdaq@ichub36', 'pdaq@ichub37', 'pdaq@ichub38', 'pdaq@ichub39', 'pdaq@ichub40', 'pdaq@ichub41', 'pdaq@ichub42', 'pdaq@ichub43',
                              'pdaq@ichub44', 'pdaq@ichub45', 'pdaq@ichub46', 'pdaq@ichub47', 'pdaq@ichub48', 'pdaq@ichub49', 'pdaq@ichub50', 'pdaq@ichub51', 'pdaq@ichub52', 'pdaq@ichub53', 'pdaq@ichub54',
                              'pdaq@ichub55', 'pdaq@ichub56', 'pdaq@ichub57', 'pdaq@ichub58', 'pdaq@ichub59', 'pdaq@ichub60', 'pdaq@ichub61', 'pdaq@ichub62', 'pdaq@ichub63', 'pdaq@ichub64', 'pdaq@ichub65',
                              'pdaq@ichub66', 'pdaq@ichub67', 'pdaq@ichub68', 'pdaq@ichub69', 'pdaq@ichub70', 'pdaq@ichub71', 'pdaq@ichub72', 'pdaq@ichub73', 'pdaq@ichub74', 'pdaq@ichub75', 'pdaq@ichub76',
                              'pdaq@ichub77', 'pdaq@ichub78', 'pdaq@ichub79', 'pdaq@ichub80', 'pdaq@ichub81', 'pdaq@ichub82', 'pdaq@ichub83', 'pdaq@ichub84', 'pdaq@ichub85', 'pdaq@ichub86', 
                              'pdaq@ithub01', 'pdaq@ithub02', 'pdaq@ithub03', 'pdaq@ithub04', 'pdaq@ithub05', 'pdaq@ithub06', 'pdaq@ithub07', 'pdaq@ithub08', 'pdaq@ithub09', 'pdaq@ithub10', 'pdaq@ithub11'],
                }

#update svn on dheereman@access:/scratch/dheereman/scripts/trunk/
#in a final version of this fabric script, this should be done as --username + icecube with the standard password
#@roles('spts-access')
def hs_checkout():
    global SVN_PATH
    global CHECKOUT_PATH
    local("svn co " + SVN_PATH + " " + CHECKOUT_PATH + " " )
    fastprint("checked out source code from " + SVN_PATH + " to " + CHECKOUT_PATH)
    pass

@roles('spts-expcont', "spts-hubs")    
def hs_deploy():
    rsync_project(HSiface_PATH , CHECKOUT_PATH, exclude=(".svn"))
    fastprint("HitSpoolScripts deployed successful\n")
    pass
    
@roles('spts-expcont')
def hs_start_pub(nworker):
    run(StartPublisher + " %i" % int(nworker))
    fastprint("HsPublisher launched")

@roles('spts-expcont')
def hs_start_sender():
    run(StartSender)                          
    fastprint('HsSender launched')        


@parallel
@roles('spts-ichub29')    
def hs_start_worker():
    run(StartWorker)                          
    fastprint('HsWorker launched')

@roles('spts-expcont') 
def hs_stop_pub(nworker):
    run("pkill -f \"" +  StartPublisher + " %i\"" % int(nworker))
    fastprint("pkilled HsPublisher")

@roles('spts-expcont')     
def hs_stop_sender():
    run("pkill -f \"" +  StartSender + "\"")
    fastprint("pkilled HsSender")

@parallel
@roles('spts-ichub29')
def hs_stop_worker():
    run("pkill -f \"" + StartWorker + "\"")
    fastprint('pkilled HsWorker')

@parallel
def start_all(nworker):
#    with hide('running', 'stdout', 'stderr'):       # hiding the "[hostname] run:" status line and preventing print out 
#                                                    # of stdout & stderr
    
    execute(hs_start_pub(nworker))                   # hs_start_pub takes an argument: hs_start_pub:nworker
    fastprint("Publisher started on expcont")
    execute(hs_start_sender)
    fastprint("Sender started on expcont")
    execute(hs_start_worker)
    fastprint("Worker launched on hubs")    