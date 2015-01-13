#!/usr/bin/env python

import platform
import sys

from runner import JavaRunner, jzmq_native_specifier

# main class being run
MAIN_CLASS = "icecube.daq.testbed.TimeTrigger"

# Java max memory
JAVA_ARGS = "-Xmx4000m"

# required jar files from subprojects and Maven repository
SUBPROJECT_PKGS = ("daq-common", "splicer", "payload", "daq-io", "juggler",
                   "trigger-common", "trigger", "trigger-testbed")
REPO_PKGS = (("log4j", "log4j", "1.2.12"),
             ("commons-logging", "commons-logging", "1.0.4"),
             ("org/zeromq", "jzmq-native", "3.1.1-ICECUBE",
              jzmq_native_specifier()),
             ("org/zeromq", "jzmq", "3.1.1-ICECUBE"),
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
