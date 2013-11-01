#!/bin/bash
#Starts the HitSpool Components HsSender on 2ndbuild via fabric.
SUBJECT="Alert from HsInterface: HsSender STARTED"
EMAIL="i3.hsinterface@gmail.com"
cTime="$(date)"
echo "HsSender started running at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 

fab hs_start_sender

SUBJECT="Alert from HsInterface: HsSender STOPPED"
EMAIL="i3.hsinterface@gmail.com"
cTime="$(date)"
echo "HsSender stopped at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 

