#!/bin/bash
#Starts the HitSpool Component
#To be called remotely via hs fabric file
#Contact i3.hsinterface@gmail.com for concerns

HS_PATH="$1"
CMD="$2"

CMDPATH="$HS_PATH/$CMD"
if [ ! -f "$CMDPATH" ]; then
    echo "$0: $CMDPATH does not exist" >&2
    exit 1
fi

WATCHPATH="$HS_PATH/HsWatcher.py"
if [ ! -f "$WATCHPATH" ]; then
    echo "$0: $WATCHPATH does not exist" >&2
    exit 1
fi

cmdlog=`echo $CMD | tr '[:upper:]' '[:lower:]' | sed -e 's/\.py$//'`

#start the Hsinterface component in BACKGROUND and redirect
#stderr & stdout to logfile
python "$CMDPATH" > "$HS_PATH/${cmdlog}_stderr_stdout.log" 2>&1 &


#now check the status of Publisher via HsWatcher:
python "$WATCHPATH" > "$HS_PATH/hswatcher_stderr_stdout.log" 2>&1
