#!/bin/bash
#Starts the HitSpool Components HsPublisher.oy on ecpxont.
#If it ever finishes, it sends an email to dheereman@gmail.com

fab hs_start_sender

#if it ever stops, alert via email:
# email subject
SUBJECT="ALERT: HsSender stopped"
# Email To ?
EMAIL="dheereman@gmail.com"
# Email text/message
cTime="$(date)"
#echo "HsSender stopped running on HUB at $cTime."> $EMAILMESSAGE
#echo "Sneak in to Pole to relaunch via hsiface.sh restart." >>$EMAILMESSAGE
# send an email using /bin/mail
echo "HsSender stopped at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 

