#!/bin/sh
#
# Build a testbed for hitspool scripts (see `HINTS.md`)

ROOTDIR="/tmp/TESTCLUSTER"
FAKE_HOSTS="expcont testhub"

if [ ! -d "$ROOTDIR" ]; then
    mkdir "$ROOTDIR"
fi

for dir in $FAKE_HOSTS; do
    subdir="$ROOTDIR/$dir"
    if [ ! -d "$subdir" ]; then
        mkdir "$ROOTDIR/$dir"
    fi
done

destdir="$ROOTDIR/HsDataCopy"
if [ -d "$destdir" ]; then
    rm -rf "$destdir"
fi
mkdir "$destdir"

explog="$ROOTDIR/expcont/logs"
if [ ! -d "$explog" ]; then
    mkdir "$explog"
fi

hubroot="$ROOTDIR/testhub"
hubtmp="$hubroot/tmp"
if [ -d "$hubtmp" ]; then
    rm -rf "$hubtmp"
fi
mkdir "$hubtmp"

curdir=`pwd`
for hs in last current; do
    rundir="$hubroot/${hs}Run"
    if [ ! -d "$rundir" ]; then
        tarfile="$curdir/testbed/$hs.tgz"
        if [ ! -f "$tarfile" ]; then
            wget "http://www.icecube.wisc.edu/~dglo/hitspool-testdata/$hs.tgz" \
                -O "$tarfile"
            if [ $? -ne 0 ]; then
                echo "$0: Cannot initialize $rundir; missing $hs data" >&2
                exit 1
            fi
        fi

        (cd $hubroot && tar xvzf "$tarfile")
	if [ $? -ne 0 ]; then
            echo "$0: Cannot initialize $rundir; $tarfile is bad" >&2
            exit 1
	fi
    fi
done
if [ ! -d "$hubroot/hitspool" ]; then
    if [ -z "$PDAQ_HOME" ]; then
	# Hack for dglo's laptop
	PDAQ_HOME=$HOME/prj/pdaq-madonna
	export PDAQ_HOME
    fi

    if [ -z "$PYTHONPATH" ]; then
	PYTHONPATH="$PDAQ_HOME/dash:$PDAQ_HOME/PyDOM"
    else
	PYTHONPATH="$PYTHONPATH:$PDAQ_HOME/dash:$PDAQ_HOME/PyDOM"
    fi
    export PYTHONPATH

    testbed/build-hitspool-db.py "$hubroot"
fi

copysrc="$ROOTDIR/copysrc"
if [ ! -d "$copysrc" ]; then
    mkdir -p "$copysrc"
fi

copydst="$ROOTDIR/HsDataCopy"
if [ ! -d "$copydst" ]; then
    mkdir -p "$copydst"
fi
