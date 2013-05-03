#!/usr/bin/env python

import sys

from runner import JavaRunner

# main class being run
MAIN_CLASS = "icecube.daq.testbed.TestBed"

# Java max memory
JAVA_ARGS = "-Xmx4000m"

# required jar files from subprojects and Maven repository
SUBPROJECT_PKGS = ("daq-common", "splicer", "payload", "daq-io", "juggler",
                   "trigger-common", "oldtrigger", "trigger",
                   "trigger-testbed")
REPO_PKGS = (("log4j", "log4j", "1.2.7"),
             ("commons-logging", "commons-logging", "1.0.4"),
             ("edu/wisc/icecube", "icebucket", "3.0.2"),
             ("dom4j", "dom4j", "1.6.1"),
             ("jaxen", "jaxen", "1.1.1"),
             ("org/zeromq", "jzmq", "1.0.0"),
             ("com/google/code/gson", "gson", "2.1"))

if __name__ == "__main__":
    import os

    runner = JavaRunner(MAIN_CLASS)

    runner.add_subproject_jars(SUBPROJECT_PKGS)
    runner.add_repo_jars(REPO_PKGS)

    try:
        jargs = JAVA_ARGS
    except NameError:
        jargs = None

    debug = "DEBUG" in os.environ

    rundata = runner.run(jargs, sys.argv[1:], debug=debug)
    if rundata.returncode() is not None and rundata.returncode() != 0:
        raise SystemExit(rundata.returncode())
