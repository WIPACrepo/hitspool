#!/bin/bash
#Starts the HitSpool Components HsSender on 2ndbuild.
# before start, send a notification email to dheereman@gmail.com
SUBJECT1="HsSender START"
EMAIL="dheereman@gmail.com"
cTime1="$(date)"
echo "HsSender started at $cTime1" | /bin/mail -s "$SUBJECT1" "$EMAIL" 

fab hs_start_sender

#if it ever stops, alert via email:
# email subject
SUBJECT="HsSender STOP"
# Email To ?
EMAIL="dheereman@gmail.com"
# Email text/message
cTime="$(date)"
echo "HsSender stopped at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 

