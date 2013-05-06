# `trigger-testbed`

This is a testbed for the trigger project.  It requires a directory full of
real data (hitspool files) and at least one run configuration.

## Basic description

The `icecube.daq.testbed.TestBed` main method uses the run
configuration to determine which hubs are active in the run (though
this can be decreased), then starts a trigger component and feeds the
data from those hubs into the trigger component.  If no trigger output
file is present, it will write the generated trigger output to a file
with a unique name as specified below.  If a file with the
expected name exists, it will compare the generated trigger output
with the previously saved output and report any differences.

## Output file name format

The format for the trigger output name is defined in
`icecube.daq.testbed.HashedFileName`:

`rc`*&lt;md5hash&gt;*`-`*&lt;trig&gt;*`-r`*&lt;runNumber&gt;*`-h`*&lt;numberOfHubs&gt;*`-p`*&lt;numberOfHits&gt;*`.dat`

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

An actual example might be
`rcfe911132ed5c2f4b833d7ea3bc8e9058-iit-r120151-h69-p32800.dat`

## Input file name format
The input files (which should be stored in a directory
named `run`*&lt;runNumber&gt;*) have a specific name format, defined in
`icecube.daq.testbed.SimpleHitFilter`:

*&lt;hub&gt;*_&lt;hubNumber&gt;_`_simplehits_`*&lt;runNumber&gt;*`_`*&lt;sequenceNumber&gt;*`_`*&lt;firstHit&gt;*`_`*&lt;lastHit&gt;*`.dat`

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

An example would be
`run120151/ithub01_simplehits_120151_1_232482_463955.dat`

## Helper scripts

There are a couple of helper scripts used to run the testbed.

#### `test-trigger.py`

`test_trigger.py` runs a single trigger, for example:

    test-trigger.py -C IniceTriggerComponent \
	    -c sps-IC86-remove-Carrot-and-Leif_Eriksson-V218 -r 120151 \
		-t ~/prj/simplehits -n 10000

It's not much more than a wrapper around
`icecube.daq.testbed.TestBed`.

#### `test-all-triggers.py`

The more exhaustive `test-all-triggers.py` script uses all the
configurations in the run configuration directory to run all three
trigger components with a couple of different numbers of hits.  This takes a
**LONG** time, so it only prints a brief report and saves the output of
any failing runs to be examined later.

This script tries to local output files associated with run
configuration files, so any changes to
`icecube.daq.testbed.HashedFileName` must be mirrored in the code
deep inside MyRunner.run_all()
