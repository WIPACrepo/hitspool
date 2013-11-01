#!/bin/bash
#Starts the HitSpool Components HsSender.py on 2ndbuild
#To be called remotley via hs fabric file 
#Contact i3.hsinterface@gmail.com for concerns

HSiface_PATH=$1

#echo $HSiface_PATH"HsSender.py"

#start the Hsinterface component in BACKGROUND (" &" added!)
#this call survives (varified by trail-and-error-proof) if given an extra redirection parameter ">>" :

python $HSiface_PATH"HsSender.py" &> /dev/null &

#this call gets executed but gets cut off after some seconds: 
#
#python $HSiface_PATH"HsSenderLiveTest.py" &


#wait with Watcher for some seconds to give Sender time to set up his connections...
#sleep 3

#now check the status of Sender via HsWatcher:
python $HSiface_PATH"HsWatcher.py"
