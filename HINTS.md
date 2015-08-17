### Documentation

See https://wiki.icecube.wisc.edu/index.php/HitSpool_Interface_Operation_Manual

### Testing

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

### SNDAQ verification
There is an sndaq executable which we use to test the connection to the
interface. It is called `sicoTesterWithHitSpooling`. It creates a fake sn
alert and triggers sndaq's alert chain thereby raising an hs request as well
as sending all the usual alert emails to the `sn-dev` list. You can find it in
the current sndaq install directory on spts. Corresponding sources are in the
svn under `sndaq/trunk/src/offline`.
