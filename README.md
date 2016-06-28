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

## Input file name formats

### Old format

The input files (which should be stored in a directory
named `run`*&lt;runNumber&gt;*) must have a specific name format, defined in
`icecube.daq.testbed.SimpleHitFilter`, which closely matches pDAQ's standard
output file format used by `icecube.daq.io.FileDispatcher`:

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

### New format

The new format is much more forgiving.  Files should be stored in a directory,
preferably named `run#######`.  Files in that directory should be named for the
hub which generated them (e.g. `ichub01`, `ithub11`) and each file should
contain *all* the hits for the run.

## Helper scripts

#### `test-trigger.py`

`test_trigger.py` runs a single trigger, for example:

    test-trigger.py -C IniceTriggerComponent \
	    -c sps-IC86-remove-Carrot-and-Leif_Eriksson-V218 -r 120151 \
		-t ~/prj/simplehits -n 10000

It's not much more than a wrapper around
`icecube.daq.testbed.TestBed`.

In the above case, it will run the in-ice trigger component with the algorithms
specified by the trigger configuration pointed to in the
`sps-IC86-remove-Carrot-and-Leif_Eriksson-V218` run configuration file.  It
will use the first 10000 hits from the files in `~/prj/simplehits/run120151`
as input.

#### `test-all-triggers.py`

The more exhaustive `test-all-triggers.py` script uses all the
configurations in the run configuration directory to run all three
trigger components with a couple of different numbers of hits.  This takes a
**LONG** time, so it only prints a brief report and saves the output of
any failing runs to be examined later.

This script tries to locate output files associated with run
configuration files, so any changes to
`icecube.daq.testbed.HashedFileName` must be mirrored in the code
deep inside MyRunner.run_all()


#### `test-algorithm.py`

The `test-algorithm.py` script tests a single algorithm.  It uses many of the
same arguments as the `test-trigger.py` script.  For example:

	./test-algorithm.py -c sps-IC86-2016-icetop-infill-V257 -T 1006 \
		-d ./127429 -n 10000 -t tgt

This will run the trigger with `triggerConfigId` 1006 (which is
SimpleMajorityTrigger in this run config) using the first 10000 hits from
each file in the `127429` subdirectory as inputs.  If a result file exists in
the `tgt` subdirectory, it will check the new results against those in the file.
If no matching file exists, it will create one.

In the above example, the `-O` option could also be used to start up an
`OldSimpleMajorityTrigger` algorithm alongside the `SimpleMajorityTrigger`
algorithm and compare the results coming out of both to make sure they match.
