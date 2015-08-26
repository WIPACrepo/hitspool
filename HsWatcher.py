#!/usr/bin/env python

"""
HsWatcher.py
David Heereman

Watching the HitSpool interface services.
Restarts the watched process in case.
"""
import subprocess
import sys
import re
import logging
import zmq
from datetime import datetime
import HsConstants

class MyWatcher(object):
    def get_host(self):
        """
        Detect cluster and define settings accordingly.
        """
        #global CLUSTER, host, user, host_short
        p = subprocess.Popen(["hostname"], stdout = subprocess.PIPE)
        out,err = p.communicate()
        host = out.rstrip()
        q = subprocess.Popen(["whoami"], stdout = subprocess.PIPE)
        out,err = q.communicate()
        user = out.rstrip()

        if not "icecube" in host:
            #print "This HsWatcher runs in your own test environment."
            CLUSTER = "LOCALHOST"
        else:
            if "pdaq" != user:
                logging.info("Sorry user %s, you are not pdaq."
                             " Please try again as pdaq." % user)
                raise SystemExit(1)

            #check host
            if "wisc.edu" in host:
                CLUSTER = "SPTS"
            elif "usap.gov" in host:
                CLUSTER = "SPS"
            else:
                logging.info("Wrong host. Use SPTS or SPS instead.")
                CLUSTER = None
                raise SystemExit(1)

        if CLUSTER == "SPS":
            host_short = re.sub(".icecube.southpole.usap.gov", "", host)
            logfile = "/mnt/data/pdaqlocal/HsInterface/logs/hswatcher_" + host_short + ".log"

        elif CLUSTER == "SPTS":
            host_short = re.sub(".icecube.wisc.edu", "", host)
            logfile = "/mnt/data/pdaqlocal/HsInterface/logs/hswatcher_" + host_short + ".log"

        else:
            host_short = host
            logfile = "/home/david/TESTCLUSTER/testhub/logs/hswatcher_" + host_short + ".log"

        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                            level=logging.INFO, stream=sys.stdout,
                            datefmt= '%Y-%m-%d %H:%M:%S',
                            filename=logfile)
        #logging.info(str(user) + " @ " + str(host) + " is running this HsWatcher on cluster : " + str(CLUSTER))

        return CLUSTER, host, host_short, logfile

    def myWatch(self, CLUSTER, host, host_short):
        """
        Depending on which machines this HsWatcher runs,
        determine the processes it is responsible to watch.
        Watcher at 2ndbuild -->  HsSender, HsController
        Watcher at expcont -->  HsPublisher
        Watcher at hub -->  HsWorker
        """
        global mywatch, HSiface_PATH, StartWorker, StartPublisher, StartSender

        if  CLUSTER ==  "SPTS" :
            HSiface_PATH    = "/mnt/data/pdaqlocal/HsInterface/trunk/"

        elif CLUSTER == "SPS":
            HSiface_PATH    = "/mnt/data/pdaqlocal/HsInterface/trunk/"

        elif CLUSTER == "LOCALHOST":
            #This means that there is no real HitSpool cluster.
            hspath = subprocess.Popen(["locate", "HsWatcher.py"], stdout = subprocess.PIPE)
            out,err = hspath.communicate()
            hspathlist = out.splitlines()
            for entry  in hspathlist:
                if not ".svn" in entry:
                    hspathdir = re.sub("HsWatcher.py$", "", entry)
            HSiface_PATH = hspathdir# = str(raw_input("Your local path to the HitSpool Interface: "))

        HsWorker            = "python " + HSiface_PATH + "HsWorker.py"
        HsWorker_short       = "HsWorker"
        HsPublisher         = "python " + HSiface_PATH + "HsPublisher.py"
        HsPublisher_short   = "HsPublisher"
        HsSender            = "python " + HSiface_PATH + "HsSender.py"
        HsSender_short      = "HsSender"

        if "2ndbuild" in host:
            mywatch = HsSender
            mywatch_short = HsSender_short
        elif "expcont" in host:
            mywatch = HsPublisher
            mywatch_short = HsPublisher_short
        elif "hub" in host:
            mywatch = HsWorker
            mywatch_short = HsWorker_short
        elif "david" in host:
            mywatch = HsWorker
            mywatch_short = HsWorker_short
        else:
            raise SystemExit("Unrecognized host \"%s\"" % host)

        return mywatch, mywatch_short

    def startProc(self, procstatus, mywatch):
        """
        If necessary, start the mywatched process.
        """
        if not procstatus:
            #start the mywatched process:
            subprocess.Popen([mywatch], shell=True, bufsize=256)
        else:
            pass

    def stopProc(self, procstatus, mywatch):
        """
        Stop the mywatch process.
        """
        if not procstatus:
            logging.info("Nothing to stop.")
            pass
        else:
            subprocess.Popen(["pkill -f \"" +  mywatch + "\""], shell=True)
            i3socket.send_json({"service": "HSiface",
                            "varname": mywatch_short + "@" + host_short,
                            "value": "STOPPING", "prio": 1})
            logging.info("STOPPED")

    def isRunning(self, mywatch, mywatch_short):
        """
        Depending on where this HsWatcher is running, check for running
        services of the HitSpool interface. Only logging , no report to I3Live.
        """
        #global procstatus
        processes = subprocess.Popen(["ps", "ax"], stdout = subprocess.PIPE)
        out, err = processes.communicate()
        procstring = out.rstrip()
        #proclist = procstring.split("\n")

        if mywatch_short in procstring:
            procstatus = True
            logging.info("RUNNING")
        else:
            procstatus = False
            logging.info("STOPPED")

        return procstatus

    def isRunningReport(self, mywatch, mywatch_short):
        """
        Depending on where this HsWatcher is running, check for running
        services of the HitSpool interface. Logging and report to I3Live.
        """
        #global procstatus
        processes_update = subprocess.Popen(["ps", "ax"], stdout = subprocess.PIPE)
        out, err = processes_update.communicate()
        procstring_update = out.rstrip()
        #proclist = procstring_update.split("\n")


        if mywatch_short in procstring_update:
            status_update = True
            #update (March & July 2014):
            #dont report single components anymore to I3Live:
            #fabfile on expcont is reporting SUM of running components

            #i3socket.send_json({"service": "HSiface",
            #                "varname": mywatch_short + "@" + host_short,
            #                "value": "RUNNING", "prio": 3})

            logging.info("RUNNING")
        else:
#            logging.info( str(mywatch) + " not in processes list")
            status_update = False
            #i3socket.send_json({"service": "HSiface",
            #                "varname": mywatch_short + "@" + host_short,
            #                "value": "STOPPED", "prio": 3})

            logging.info("STOPPED")
        return status_update

    def send_alert(self, status_update, logfile):
        '''
        Check for how long HsWatcher is reporting STOPPED state.
        In case service is in STOPPED state for too long: send alert to i3live
        '''
        #Each time HsWatcher is run,
        #2 lines are adedd two the log file baout the status
        #do this reading efficiently via tail-like function:
        #equvalent: tail -n 10 logfile to list:
        nlines = 8 # shows entries from last 4 HsWatcher status report
        logtaillist = [line.rstrip() for line in reversed(open(logfile).readlines())][:nlines:]

        if not status_update:
            #runhist     = [s for s in logtaillist if "RUNNING" in s]
            stophist    = [s for s in logtaillist if "STOPPED" in s]

            # send stopped alert after 4 STOPPED status logs:
            if len(stophist) == nlines:
                last_stop = re.search(r'[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}(?=\sINFO)', stophist[-1])
                last_stop_time = str(last_stop.group(0))
                alertmsg1 = mywatch_short + "@" + host_short + " in STOPPED state more than 1h.\nLast seen running before " + last_stop_time

                alertjson1 = {"service" :   "HSiface",
                                  "varname" :   "alert",
                                  "prio"    :   2,
                                  "time"    :   str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                                  "value"   :   {"condition"    : "STOPPED HsInterface Alert: " + mywatch_short + "@" + host_short,
                                                 "desc"         : "HsInterface service stopped",
                                                 "notifies"     : [{"receiver"      : HsConstants.ALERT_EMAIL_DEV,
                                                                    "notifies_txt"  : alertmsg1,
                                                                    "notifies_header" : "RECOVERY HsInterface Alert: " + mywatch_short + "@" + host_short}],
                                                 "short_subject": "true",
                                                 "quiet"        : "true",}}

                i3socket.send_json(alertjson1)



            # send STOPPED alert if in Stopped status:
            if "STOPPED" in logtaillist[0]:
                alertmsg3 = mywatch_short + "@" + host_short + ":\n" + str(logtaillist[0])

                alertjson3 = {"service" :   "HSiface",
                                  "varname" :   "alert",
                                  "prio"    :   2,
                                  "time"    :   str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                                  "value"   :   {"condition"    : "STOPPED HsInterface Alert: " + mywatch_short + "@" + host_short,
                                                 "desc"         : "HsInterface service stopped",
                                                 "notifies"     : [{"receiver"      : HsConstants.ALERT_EMAIL_DEV,
                                                                    "notifies_txt"  : alertmsg3,
                                                                    "notifies_header" : "STOPPED HsInterface Alert: " + mywatch_short + "@" + host_short}],
                                                 "short_subject": "true",
                                                 "quiet"        :   "true"}}
                i3socket.send_json(alertjson3)

        else:
            # send RECOVERY report when RUNNING follows STOPPED status
            if "RUNNING" in logtaillist[0]:
                if len(logtaillist) > 1 :
                    if "STOPPED" in logtaillist[1]:
                        alertmsg2 = mywatch_short + "@" + host_short + " recovered by HsWatcher:\n" + str(logtaillist[1]) + "\n" + str(logtaillist[0])
                        alertjson2 = {"service" :   "HSiface",
                                      "varname" :   "alert",
                                      "prio"    :   2,
                                      "time"    :   str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                                      "value"   :   {"condition"    : "RECOVERY HsInterface Alert: " + mywatch_short + "@" + host_short,
                                                     "desc"         : "HsInterface service recovery notice",
                                                     "notifies"     : [{"receiver"      : HsConstants.ALERT_EMAIL_DEV,
                                                                        "notifies_txt"  : alertmsg2,
                                                                        "notifies_header" : "RECOVERY HsInterface Alert: " + mywatch_short + "@" + host_short},
                                                                       {"receiver"      : HsConstants.ALERT_EMAIL_SN,
                                                                        "notifies_txt"  : alertmsg2,
                                                                        "notifies_header" : "RECOVERY HsInterface Alert: " + mywatch_short + "@" + host_short}],
                                                     "short_subject": "true",
                                                     "quiet"        : "true"}}

                        i3socket.send_json(alertjson2)
                else:
                    pass

if __name__ == "__main__":

    from HsConstants import I3LIVE_PORT


    newservice = MyWatcher()
    context = zmq.Context()
    i3socket = context.socket(zmq.PUSH) # former ZMQ_DOWNSTREAM is depreciated
    CLUSTER, host, host_short, logfile = newservice.get_host()

    if (CLUSTER == "SPS") or (CLUSTER == "SPTS"):
        expcont = "expcont"
    else:
        expcont = "localhost"
    i3socket.connect("tcp://%s:%d" % (expcont, I3LIVE_PORT))

    mywatch, mywatch_short = newservice.myWatch(CLUSTER, host, host_short)
    procstatus = newservice.isRunning(mywatch, mywatch_short)
    newservice.send_alert(procstatus, logfile)
    newservice.startProc(procstatus, mywatch)
    status_update = newservice.isRunningReport(mywatch, mywatch_short)
    newservice.send_alert(status_update, logfile)
