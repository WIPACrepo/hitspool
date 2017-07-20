#!/usr/bin/env python

# path for SVN release ("trunk" for development, "releases/Name" for releases)
RELEASE = "trunk"


# Email address which received email for the hitspool developer(s)
ALERT_EMAIL_DEV = [
    "dglo+hsdev@icecube.wisc.edu",
    "i3.hsinterface@gmail.com",
]
# Email address which received email for the Supernova group
ALERT_EMAIL_SN = ["icecube-sn-dev@lists.uni-mainz.de", ]


# Location of development sandbox on SPTS
SANDBOX_SPTS = "/home/dglo/prj/hitspool"
SANDBOX_SPTS = "/home/pdaq/HsInterface/current"
# Location of release on sps-access
SANDBOX_SPS = "/home/pdaq/HsInterface/current"
# Location of release on other sps machines
SANDBOX_INSTALLED = "/mnt/data/pdaqlocal/HsInterface/current"


# Common 0MQ ports
ALERT_PORT = 55558
I3LIVE_PORT = 6668
OLDALERT_PORT = 55557
OLDPUB_PORT = 55561
PUBLISHER_PORT = 55559
SENDER_PORT = 55560
