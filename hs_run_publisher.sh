#!/bin/bash
#Starts the HitSpool Components HsPublisher.oy on ecpxont.
#If it ever finishes, it sends an emial to dheereman@gmail.com

fab hs_start_pub:20

#if it ever stops, alert via email:
# email subject
SUBJECT="ALERT: HsPublisher stopped"
# Email To ?
EMAIL="dheereman@gmail.com"
# Email text/message
cTime="$(date)"
#echo "HsPublisher stopped running on expcont at $cTime" > $EMAILMESSAGE
#echo "Sneak in to Pole to relaunch via hsiface.sh restart" >>$EMAILMESSAGE
# send an email using /bin/mail
echo "HsPublisher stopped at $cTime" | /bin/mail -s "$SUBJECT" "$EMAIL"


