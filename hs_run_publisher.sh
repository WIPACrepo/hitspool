#!/bin/bash
#Starts the HitSpool Components HsPublisher on expcont via fabric.
SUBJECT="Alert from HsInterface: HsPublisher STARTED"
EMAIL="i3.hsinterface@gmail.com"
cTime="$(date)"
echo "HsSender started running at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 

fab hs_start_pub

SUBJECT="Alert from HsInterface: HsPublisher STOPPED"
EMAIL="i3.hsinterface@gmail.com"
cTime="$(date)"
echo "HsPublisher stopped at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 

