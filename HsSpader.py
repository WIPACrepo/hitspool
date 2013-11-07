#!/usr/bin/python
#
#Hit Spool SPADE-ing to be run on access
#author: dheereman
#
from datetime import datetime, timedelta
import sys
import getopt
import zmq
import subprocess
import re
import logging
import glob
import os
import errno

def log(msg):
    print msg
    logging.info(msg)

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def spade_pickup_data(indir, pattern, outdir):
        '''
        Create subdir for folder related to the alert
        Move folder in subdir
        tar & bzip folder
        create semaphore file for folder
        move .sem & .dat.tar file in Spade directory
        '''
        log("Preparation for SPADE Pickup of HS datastarted amnually via HsSpader...")
        basedir     = indir
        alertname   = pattern
        datalist    = glob.glob(indir + "*" + alertname + "*")
        log("found HS data:\n" + str(datalist))
        datalistlocal = [re.sub(indir, "", s) for s in datalist]
        spadedir    = outdir
        hubnamelist = ["ichub%0.2d" %i for i in range(87)[1::]] + ["ithub%0.2d" %i for i in range(12)[1::]]
        
        #if outdir doen't exist:
        make_sure_path_exists(outdir)
        
        
        if len(datalist) != 0:
            for hub in hubnamelist:
                tarname     = "HS_SNALERT_" + alertname + "_" + hub + ".dat.tar.bz2"
                semname     = "HS_SNALERT_" + alertname + "_" + hub + ".sem"
                data    = [s for s in datalistlocal if hub in s]
                if len(data) == 1:
                    dataname = data[0]
                    log("data: " + str(dataname) + " will be tarred to: " + str(tarname))
                    subprocess.check_call(['nice', 'tar', '-jcvf', tarname, dataname ], cwd=basedir)
                    log("done")
                    try:
                        move =  subprocess.check_call(['mv', '-v', tarname, spadedir], cwd=basedir)
                        log("moved " + str(tarname) + " to SPADE dir " + str(spadedir))
                        if move == 0:
                            log("create .sem file")
                            subprocess.check_call(["touch", semname], cwd=spadedir)
                            log("Preparation for SPADE Pickup of " + str(tarname) + " DONE")
                        else:
                            log("failed moving the tarred data.")
                            log("Please put the data manually in the SPADE directory")

                    except (IOError,OSError,subprocess.CalledProcessError):
                        log("Loading data in SPADE directory failed")
                        log("Please put the data manually in the SPADE directory")                                     
                else:
                    log("no or ambiguous HS data found in this directory.")
        else:
            log("no HS data found for this alert time pattern.")

if __name__=="__main__":
    '''
    Putting manually the HS data in the SPADE Q n sps-2ndbuild. 
    Creates tarballs and smafore files for each hub's data and moves it to Spade directory /mnt/data/HitSpool/
    '''
    def usage():
        print >>sys.stderr, """
        usage :: HsSpader.py [options]
            -i         | Input dir where hs data is located, e.g. /mnt/data/pdaqlocal/HsDataCopy/
            -p         | time pattern of HS data alert:<yyyymmdd>_<hhmmss> e.g. 20131101_045126
            -o         | Output directory: default is /mnt/data/HitSpool/
            -l         | logfile, e.g. topath HsSenders log: /mnt/data/pdaqlocal/HsInterface/logs/hssender_2ndbuild.sps.log
            """
        sys.exit(1)
    #take arguments from command line and check for correct input
    opts, args = getopt.getopt(sys.argv[1:], 'hi:o:p:l:', ['help','alert='])
    for o, a in opts:    
        if o == "-i":
            indir = str(a)
            print indir
        if o == "-p":
            pattern = str(a)
            print pattern
        if o == "-o":
            outdir = str(a)
            print outdir
        if o == "-l":
            logfile = str(a)
            print logfile
    if len(sys.argv) < 5:
        print usage()
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', 
                level=logging.INFO, stream=sys.stdout, 
                datefmt= '%Y-%m-%d %H:%M:%S', 
                filename=logfile)
    
    spade_pickup_data(indir, pattern, outdir)
    