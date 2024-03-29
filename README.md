### Documentation

See https://wiki.icecube.wisc.edu/index.php/HitSpool_Interface_Operation_Manual

### Restarting the detector

In an emergency, log onto access and run:

`cd HsInterface/current && ./hs_admin.py restart`

This will stop all the hitspool programs and restart them.

### Basic architecture

```
    "sndaq"           "HsPublisher"      "HsWorker"           "HsSender"
    -----------        -----------
    | sni3daq |        | access  |         -----------        ------------
    | REQUEST | <----->| REPLY   |         | IcHub n |        | 2ndbuild |
    -----------        | PUB     | ------> | SUB    n|        | PULL     |
                       ----------          |PUSH     | ---->  |          |
                                            ---------          -----------
    This is the NEW HsSender for the HS Interface.
    It's started via `hs_admin.py` on access.
    It receives messages from the HsWorkers and is responsible of putting
    the HitSpool Data in the SPADE queue.
```

### Testing

All-in-one testing:

Run `testbed/test-requests.py`

This creates a fake hitspool directory, spawns separate threads running
HsPublisher, HsWorker, and HsSender, then feeds in requests and validates
validates the resulting files and messages to Live.

Older testbed:

Open 4 terminal tabs and run:

* `testbed/FakeLive.py`
* `testbed/run-publisher`
* `testbed/run-worker`
* `testbed/run-sender`

In another terminal, run `testbed/run-grabber`.

Check `/tmp/sender.log` and `/tmp/worker.log` to verify that everything worked
as expected:
* `worker.log` should include rsync details for the copied HitSpool files
  along with an `rsync_sum` message sent to HsSender, followed by rsync details
  for the hitspool logs and a 'log_done' message sent to HsSender.
* `sender.log` should note that it received the `rsync_sum` message and moved
  the associated files, and that it received and ignored to `log_done` message

### Releasing

PRELIMINARY new procedure. 

From a working repository:
```
 export TAG=<tagname>
 git tag -a $TAG -m "<commit message>"
 git push origin --tags
```

As pdaq@access (note this will be easier with newer git version that can clone a single tag):
```
 cd ~/HsInterface
 export TAG=<tagname>
 git clone git@github.com:WIPACrepo/hitspool.git $TAG
 cd $TAG
 git checkout tags/$TAG -b $TAG
 cd ../; rm current; ln -s $TAG current
```

### Deploying

On access, to deploy a new version to all current hitspool machines:

`cd HsInterface/current && ./hs_admin.py deploy`

To deploy to a newly re-kickstarted machine:

`cd HsInterface/current && ./hs_admin.py -t <hostname> --no-stop deploy && ./hs_admin.py -t <hostname> install_crontab`

### SnDAQ verification

SnDAQ's control program (`access:~pdaq/sndaq/sndaq_current_build/bin/sni3ctl`)
includes a `create_test_alert` command which can be used to verify that SnDAQ
can still talk to the HitSpool code.  The `create_test_alert` command fakes an
alert and triggers sndaq's alert chain thereby raising an hs request as well
as sending all the usual alert emails to the `sn-dev` list.
