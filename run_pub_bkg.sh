#!/bin/bash
#Starts the HitSpool Components HsPublisher on expcont
#To be called remotley via hs fabric file 
#Contact i3.hsinterface@gmail.com for concerns

HSiface_PATH=$1

#echo $HSiface_PATH"HsPublisher.py"

#start the Hsinterface compnonent in BACKGROUND
# (" &" added at EOL) and redirect 
#stderr & stdout to garbage (&> /dev/null)
python26 $HSiface_PATH"HsPublisher.py" >$HSiface_PATH"hspublisher_stderr_stdout.log" 2>&1 &


#now check the status of Publisher via HsWatcher:
python26 $HSiface_PATH"HsWatcher.py" >$HSiface_PATH"hswatcher_stderr_stdout.log" 2>&1
