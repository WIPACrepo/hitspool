#!/bin/bash
#Starts the HitSpool Components HsPublisher on expcont
#To be called remotley via hs fabric file 
#Contact i3.hsinterface@gmail.com for concerns

HSiface_PATH=$1

#echo $HSiface_PATH"HsPublisher.py"

#start the Hsinterface compnonent in BACKGROUND (" &" added at EOL) and redirect stderr & stdout to garbage (&> /dev/null)
python $HSiface_PATH"HsPublisher.py" &> /dev/null &

#wait with Watcher for some seconds to give Publisher time to set up his connections...
#sleep 3

#now check the status of Publisher via HsWatcher:
python $HSiface_PATH"HsWatcher.py"
