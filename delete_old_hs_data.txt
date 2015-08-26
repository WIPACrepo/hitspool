###HSiface Jobs -- Please contact David Heereman: i3.hsinterface@gmail.com for questions 

HSifacePATH=/mnt/data/pdaqlocal/HsInterface/trunk

#delete subfolders in $HSifacePATH that are older than 60 days
#to be run at 12:00 AM on every first day of th month:

0 0 1 * * find $HSifacePATH -mindepth 1 -maxdepth 1 -type d -ctime +60 | xargs rm -rf