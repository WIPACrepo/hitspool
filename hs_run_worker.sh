#!/bin/bash
#Starts the HitSpool Components HsPublisher.oy on ecpxont.
#If it ever finishes, it sends an emial to dheereman@gmail.com

fab hs_start_worker

#if it ever stops, alert via email:
# email subject
SUBJECT="ALERT: HsWorker stopped"
# Email To ?
EMAIL="dheereman@gmail.com"
# Email text/message
cTime="$(date)"
#echo "HsWorker stopped running on HUB at $cTime."> $EMAILMESSAGE
#echo "Sneak in to Pole to relaunch via hsiface.sh restart." >>$EMAILMESSAGE
# send an email using /bin/mail
echo "HsWorker stopped at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL" 
