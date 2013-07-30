#!/bin/bash
#Starts the HitSpool Components HsPublisher.py on expcont.
#before start, send a notification email to dheereman@gmail.com
SUBJECT1="HsPublisher START"
EMAIL="dheereman@gmail.com"
cTime1="$(date)"
echo "HsPublisher started at $cTime1" | /bin/mail -s "$SUBJECT1" "$EMAIL" 

fab hs_start_pub:1		#asking for at 1 HsWorker on a random hub to sync to the publisher

#if it ever stops, alert via email:
SUBJECT="HsPublisher STOP"
EMAIL="dheereman@gmail.com"
cTime="$(date)"
echo "HsPublisher stopped at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL"


