#!/bin/bash
#Starts the HitSpool Components HsWorker.py on hubs
#To be called remotley via hs fabric file 
#Contact i3.hsinterface@gmail.com for concerns

HSiface_PATH=$1

#echo $HSiface_PATH/HsWorker.py
#start the Hsinterface compnonent 
#in BACKGROUND (" &" added at EOL!) 
#and redirect stderr and stdout
python26 $HSiface_PATH"HsWorker.py" >$HSiface_PATH"hsworker_stderr_stdout.log" 2>&1 &

#now check the status of Worker:
python26 $HSiface_PATH"HsWatcher.py" >$HSiface_PATH"hswatcher_stderr_stdout.log" 2>&1
