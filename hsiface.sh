#!/bin/bash
# This script is to be used in combination with a fabfile.py to deploy, 
# start and run the HitSpool Interface components in parallel. 
# This hack-around is needed because fabric doesn't offer a possiblity 
# to run nultiple processes in parallel on different machines (background "&" 
# mode isn't working inside fabric)
#additionally, the fabric function are inside another bash script that, if the fab function finishes, shoots me an email
if [ "$1" == "" ] ; then 
	echo "Usage: "
	echo "hsiface.sh [checkout, deploy, start, stop, restart]"   
elif [ "$1" == "checkout" ] ; then
	echo "Update HsInterface components from SVN repoitory"
	fab hs_checkout
elif [ "$1" == "deploy" ] ; then 
	echo "Deploy HsInterface components to the system"
	fab hs_mk_dir
	fab hs_deploy
elif [ "$1" == "start" ] ; then
	echo "Start the HsInterface components on the System: "
	echo "Start HsPublisher on expcont for " $1 "HsWorkers"
	./hs_run_publisher.sh &
	sleep 2
	echo "Start HsSender on expcont"
	./hs_run_worker.sh &
	sleep 2
	echo "Start " $1 "HsWorker on the Hubs"
	./hs_run_sender.sh &
elif [ "$1" == "stop" ] ; then
	echo "Stopping all HsInterface components..." 
	fab hs_stop_pub:1
	sleep 2
	fab hs_stop_sender
	sleep 2 
	fab hs_stop_worker
	
elif [ "$1" == "restart" ] ; then
	echo "Stoppong all HsInterface components..." 
	fab hs_stop_pub:1 &
	sleep 2
	fab hs_stop_sender &
	sleep 2 
	fab hs_stop_worker &
	echo "Start the HsInterface components on the System: "
	echo "Start HsPublisher on expcont for " $1 "HsWorkers"
	fab hs_start_pub:1 &
	sleep 2
	echo "Start HsSender on expcont"
	fab hs_start_sender &
	sleep 2
	echo "Start " $1 "HsWorker on the relevant Hubs"
	fab hs_start_worker &		
else
	echo "Syntax error when providing option for hsiface.sh"
fi
