#!/bin/bash
#Starts the HitSpool Components HsSender.py on 2ndbuild
#To be called remotley via hs fabric file 
#Contact i3.hsinterface@gmail.com for concerns

HSiface_PATH=$1

#echo $HSiface_PATH"HsSender.py"

#start the Hsinterface component in BACKGROUND (" &" added!)

python26 $HSiface_PATH"HsSender.py" >$HSiface_PATH"hssender_stderr_stdout.log" 2>&1 &


#now check the status of Sender via HsWatcher:
python26 $HSiface_PATH"HsWatcher.py" >$HSiface_PATH"hswatcher_stderr_stdout.log" 2>&1
