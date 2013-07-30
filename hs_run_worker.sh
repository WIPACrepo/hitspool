#!/bin/bash
#Starts the HitSpool HsWorker.py on the hubs.
# before start, send a notification email to dheereman@gmail.com
SUBJECT1="HsWorkers START"
EMAIL="dheereman@gmail.com"
cTime1="$(date)"
echo "HsWorkers started at $cTime1" | /bin/mail -s "$SUBJECT1" "$EMAIL" 

fab hs_start_worker

#if it ever stops, alert via email:
# email subject
SUBJECT="HsWorkers STOP"
# Email To ?
EMAIL="dheereman@gmail.com"
# Email text/message
cTime="$(date)"
# send an email using /bin/mail
echo "HsWorker stopped at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 
