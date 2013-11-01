#!/bin/bash
#Starts the HitSpool Components Workers on all hubs via fabric..
SUBJECT="Alert from HsInterface: HsWorkers STARTED"
EMAIL="i3.hsinterface@gmail.com"
cTime="$(date)"
echo "HsWorkers started running at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 

fab hs_start_worker

SUBJECT="Alert from HsInterface: HsWorkers STOPPED"
EMAIL="i3.hsinterface@gmail.com"
cTime="$(date)"
echo "HsWorker stopped at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 