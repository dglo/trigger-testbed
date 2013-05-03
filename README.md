This is a testbed for the trigger project.  It requires a directory full of
real data (hitspool files) and at least one run configuration.

In a normal run, it uses the run configuration to determine which hubs are
active in the run, then starts a trigger component and feeds the data from those hubs into the trigger component.  If a trigger output file is present, it will
compare the generated trigger output with the previous output from the file and
report any differences.  Otherwise, it will create a new trigger output file.

The output files have a specific name format, defined in `icecube.daq.testbed.HashedFileName`:

`rc`*md5hash*`-`*trig*`-r`*runNumber*`-h`*numberOfHubs*`-p`*numberOfHits*`.dat`

where the individual pieces are:
<dl>
  <dt>md5hash</dt>
  <dd>MD5 hash of the run configuration name</dd>
  <dt>trig</dt>
  <dd>Trigger component (`iit`, `itt`, or `gt`)</dd>
  <dt>runNumber</dt>
  <dd>Run number which provided the hit data</dd>
  <dt>numberOfHubs</dt>
  <dd>Number of "hubs" which provided data (may be smaller than the
  number specified in the run configuration file)</dd>
  <dt>numberOfHits</dt>
  <dd>Number of hits sent by each "hub"</dd>
</dl>

An actual example might look like
`rcfe911132ed5c2f4b833d7ea3bc8e9058-iit-r120151-h69-p32800.dat`

Similarly, the input files have a specific format.  The files should
be stored in a directory named `run`*runNumber*.  The individual hit
files also have a specific name format, defined in
`icecube.daq.testbed.SimpleHitFilter`:

**hub***hubNumber*`_simplehits_`*runNumber*`_`*sequenceNumber*`_`*firstHit*`_`*lastHit*`.dat`

where the individual pieces are:
<dl>
  <dt>hub</dt>
  <dd>hub type, either `hub` or `ithub`</dd>
  <dt>hubNumber</dt>
  <dd>the hub number (as specified in the run configuration file)</dd>
  <dt>runNumber</dt>
  <dd>the run number, identical to that in the directory name</dd>
  <dt>sequence</dt>
  <dd>The file sequence number, `0` for the first series of hits, `1`
  for the second, etc.</dd>
  <dt>firstHit</dt>
  <dd>The count of the first hit in the file, `0` for the first file</dd>
  <dt>lastHit</dt>
  <dd>The count of the last hit in the file, such that `lastHit -
  firstHit + 1` is the number of hits in the file</dd>

There are a couple of helper scripts used to run the testbed.
`test_trigger.py` runs a single trigger, for example:

    test-trigger.py -C IniceTriggerComponent \
	    -c sps-IC86-remove-Carrot-and-Leif_Eriksson-V218 -r 120151 \
		-t ~/prj/simplehits -n 10000

The more exhaustive `test-all-triggers.py` script uses all the
configurations in the run configuration directory to run all three
trigger components with a couple of different numbers of hits.  This takes a
**LONG* time, so it only prints a brief report and saves the output of
any failing runs to be examined later.
