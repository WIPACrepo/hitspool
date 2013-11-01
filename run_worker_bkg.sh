#!/bin/bash
#Starts the HitSpool Components HsWorker.py on hubs
#To be called remotley via hs fabric file 
#Contact i3.hsinterface@gmail.com for concerns

HSiface_PATH=$1

#echo $HSiface_PATH/HsWorker.py

#start the Hsinterface compnonent in BACKGROUND (" &" added at EOL!) and redirect stderr and stdout
python $HSiface_PATH"HsWorker.py" >$HSiface_PATH"hsworker_stderr_stdout.log" 2>&1 &

#wait with Watcher for some seconds to give Worker time to set up his connections...
#sleep 3

#now check the status of Worker:
#echo "run watcher"
python $HSiface_PATH"HsWatcher.py"
