#!/usr/bin/env python

from __future__ import print_function

import datetime
import os
import select
import signal
import subprocess
import sys

from distutils.version import LooseVersion

class RunnerException(Exception):
    """Exception in Java code runner"""
    pass


class RunData(object):
    """Run statistics (return code, duration, etc.)"""

    def __init__(self, java_args, main_class, sys_args):
        """
        Create a RunData object

        java_args - arguments for the 'java' program
        main_class - fully-qualified name of class whose main() method
                    will be run
        sys_args - arguments for the class being run
        """
        self.__exitsig = False
        self.__killsig = False

        self.__returncode = None
        self.__run_time = None
        self.__wait_time = None

        self.__cmd = ["java", ]
        if java_args is not None:
            if isinstance(java_args, str):
                self.__cmd.append(java_args)
            elif isinstance(java_args, list) or isinstance(java_args, tuple):
                self.__cmd += java_args
            else:
                raise RunnerException("Bad java_args type %s for %s" %
                                      (type(java_args), java_args))

        self.__cmd.append(main_class)
        if sys_args is not None:
            self.__cmd += sys_args

    @classmethod
    def __timediff(cls, start_time, end_time):
        """
        Convert the difference between two times to a floating point value
        """
        diff = end_time - start_time
        return float(diff.seconds) + \
            (float(diff.microseconds) / 1000000.0)

    def command(self):
        """Return the command which was run"""
        return self.__cmd

    def exit_signal(self):
        """Return the signal which caused the program to exit (or None)"""
        return self.__exitsig

    def kill_signal(self):
        """Return the signal which caused the program to be killed (or None)"""
        return self.__killsig

    def process(self, line, is_stderr=False):
        """Process a line of output from the program"""
        if not is_stderr:
            sys.stdout.write(line)
            sys.stdout.flush()
        else:
            sys.stderr.write(line)

    def returncode(self):
        """Return the POSIX return code"""
        return self.__returncode

    def run_time(self):
        """Return the time needed to run the program"""
        return self.__run_time

    def set_exit_signal(self, val):
        """Record the signal which caused the program to exit"""
        self.__exitsig = val

    def set_kill_signal(self, val):
        """Record the signal which caused the program to be killed"""
        self.__killsig = val

    def set_return_code(self, val):
        """Record the POSIX return code"""
        self.__returncode = val

    def set_run_time(self, start_time, end_time):
        """Record the run time"""
        self.__run_time = self.__timediff(start_time, end_time)

    def set_wait_time(self, start_time, end_time):
        """Record the wait time"""
        self.__wait_time = self.__timediff(start_time, end_time)

    def wait_time(self):
        """Return the time spent waiting for the program to finish"""
        return self.__wait_time


def jzmq_native_specifier():
    import platform

    system = platform.system()
    machine = platform.machine()
    if system == "Linux" and machine == "x86_64":
        machine = "amd64"
    return machine + "-" + system


class JavaRunner(object):
    """Wrapper which runs a Java program"""

    # Default DAQ version string
    __DEFAULT_DAQ_RELEASE = "1.0.0-SNAPSHOT"
    # IceCube subdirectory in Maven repository
    __DAQ_REPO_SUBDIR = "edu/wisc/icecube"

    # current distribution directory
    __DIST_DIR = None
    # release used to find current distribution directory
    __DIST_RELEASE = None
    # Maven repository
    __MAVEN_REPO = None
    # $PDAQ_HOME envvar
    __PDAQ_HOME = None

    def __init__(self, main_class):
        """Create a JavaRunner instance"""
        self.__main_class = main_class
        self.__classes = []
        self.__classpath = None

        self.__proc = None
        self.__killsig = None
        self.__exitsig = None

    @classmethod
    def __build_jar_name(cls, name, vers, extra=None):
        """Build a versioned jar file name"""
        if extra is None:
            return name + "-" + vers + ".jar"
        return name + "-" + vers + "-" + extra + ".jar"

    @classmethod
    def __distribution_path(cls, daq_release):
        """Distribution directory"""
        if cls.__DIST_RELEASE is None or cls.__DIST_RELEASE != daq_release:
            # clear cached path
            cls.__DIST_DIR = None

            pdaq_home = cls.__pdaq_home()
            if pdaq_home is not None:
                tmpDir = os.path.join(pdaq_home, "target",
                                      "pDAQ-" + daq_release + "-dist", "lib")
                if os.path.exists(tmpDir):
                    cls.__DIST_RELEASE = daq_release
                    cls.__DIST_DIR = tmpDir
        return cls.__DIST_DIR

    def __find_maven_jar(self, proj, name, vers, extra):
        """
        Find a jar file in the Maven repository which is at or after the
        version specified by 'vers'
        """
        repo_dir = self.__maven_repository_path()
        if repo_dir is None:
            return None

        jarname = self.__build_jar_name(name, vers, extra)

        projdir = os.path.join(repo_dir, proj, name)
        if os.path.exists(projdir):
            tmpjar = os.path.join(projdir, vers, jarname)
            if os.path.exists(tmpjar):
                return tmpjar

            overs = LooseVersion(vers)
            for entry in os.listdir(projdir):
                nvers = LooseVersion(entry)
                if overs < nvers:
                    tmpname = self.__build_jar_name(name, entry)
                    tmpjar = os.path.join(projdir, entry, tmpname)
                    if os.path.exists(tmpjar):
                        print("WARNING: Using %s version %s" \
                            " instead of requested %s" % (name, entry, vers), file=sys.stderr)
                        return tmpjar

        dist_dir = self.__distribution_path(self.__DEFAULT_DAQ_RELEASE)
        if dist_dir is not None:
            tmpjar = os.path.join(dist_dir, jarname)
            if os.path.exists(tmpjar):
                return tmpjar

            overs = LooseVersion(vers)
            namedash = name + "-"
            for entry in os.listdir(dist_dir):
                if entry.startswith(namedash):
                    jarext = entry.find(".jar")
                    if jarext > 0:
                        vstr = entry[len(namedash):jarext]
                        nvers = LooseVersion(vstr)
                        if overs <= nvers:
                            print("WARNING: Using %s version %s" \
                                " instead of requested %s" % (name, vstr, vers), file=sys.stderr)
                            return os.path.join(dist_dir, entry)

        return None

    def __find_subproject_classes(self, proj, daq_release):
        jarname = self.__build_jar_name(proj, daq_release)

        # check target/foo-X.Y.Z.jar (if we're in subproject dir)
        tgtjar = os.path.join("target", jarname)
        if os.path.exists(tgtjar):
            return tgtjar

        # check foo/target/foo-X.Y.Z.jar (if we're in project dir)
        projjar = os.path.join(proj, tgtjar)
        if os.path.exists(projjar):
            return projjar

        # check ../foo/target/foo-X.Y.Z.jar (if we're in another subproject dir)
        parentjar = os.path.join("..", projjar)
        if os.path.exists(parentjar):
            return parentjar

        # check foo/target/classes (if we're in project dir)
        tgtcls = os.path.join(proj, "target", "classes")
        if os.path.exists(tgtcls):
            return tgtcls

        # check ../foo/target/classes (if we're in subproject dir)
        projcls = os.path.join("..", tgtcls)
        if os.path.exists(projcls):
            return projcls

        pdaq_home = self.__pdaq_home()
        if pdaq_home is not None:
            # check $PDAQHOME/foo/target/foo-X.Y.Z.jar (possibly older version)
            tmpjar = os.path.join(pdaq_home, projjar)
            if os.path.exists(tmpjar):
                return tmpjar

        dist_dir = self.__distribution_path(self.__DEFAULT_DAQ_RELEASE)
        if dist_dir is not None:
            # dist_dir is usually $PDAQHOME/target/pDAQ-X.Y.Z-dist/lib
            tmpjar = os.path.join(dist_dir, jarname)
            if os.path.exists(tmpjar):
                return tmpjar

        repo_dir = self.__maven_repository_path()
        if repo_dir is not None:
            # check ~/.m2/repository/edu/wisc/icecube/foo/X.Y.Z/foo-X.Y.Z.jar
            tmpjar = os.path.join(repo_dir, self.__DAQ_REPO_SUBDIR, proj,
                                  daq_release, jarname)
            if os.path.exists(tmpjar):
                return tmpjar

        return None

    @classmethod
    def __maven_repository_path(cls):
        """Maven repository directory"""
        if cls.__MAVEN_REPO is None and "HOME" in os.environ:
            tmpDir = os.path.join(os.environ["HOME"], ".m2", "repository")
            if tmpDir is not None and os.path.exists(tmpDir):
                cls.__MAVEN_REPO = tmpDir
        return cls.__MAVEN_REPO

    @classmethod
    def __pdaq_home(cls):
        """Current active pDAQ directory"""
        if cls.__PDAQ_HOME is None and "PDAQ_HOME" in os.environ:
            tmpDir = os.environ["PDAQ_HOME"]
            if tmpDir is not None and os.path.exists(tmpDir):
                cls.__PDAQ_HOME = tmpDir
        return cls.__PDAQ_HOME

    def __run_command(self, data, debug=False):
        """Run the Java program, tracking relevant run-related statistics"""
        self.__killsig = None
        self.__exitsig = None

        if debug:
            print(" ".join(data.command()))

        start_time = datetime.datetime.now()

        self.__proc = subprocess.Popen(data.command(), stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       preexec_fn=os.setsid)
        num_err = 0
        while True:
            reads = [self.__proc.stdout.fileno(), self.__proc.stderr.fileno()]
            try:
                ret = select.select(reads, [], [])
            except select.error:
                 # ignore a single interrupt
                if num_err > 0:
                    break
                num_err += 1
                continue

            for fd in ret[0]:
                if fd == self.__proc.stdout.fileno():
                    line = self.__proc.stdout.readline()
                    data.process(line, False)
                if fd == self.__proc.stderr.fileno():
                    line = self.__proc.stderr.readline()
                    data.process(line, True)

            if self.__proc.poll() is not None:
                break

        self.__proc.stdout.close()
        self.__proc.stderr.close()

        end_time = datetime.datetime.now()

        self.__proc.wait()

        wait_time = datetime.datetime.now()

        data.set_return_code(self.__proc.returncode)

        data.set_run_time(start_time, end_time)
        data.set_wait_time(end_time, wait_time)

        data.set_exit_signal(self.__exitsig)
        data.set_kill_signal(self.__killsig)

        self.__proc = None

    def add_subproject_jars(self, pkgs, daq_release=None):
        """Add pDAQ jar files to CLASSPATH"""
        if daq_release is None:
            daq_release = self.__DEFAULT_DAQ_RELEASE
        pdaq_home = self.__pdaq_home()
        dist_dir = self.__distribution_path(daq_release)
        repo_dir = self.__maven_repository_path()

        for pkg in pkgs:
            jar = self.__find_subproject_classes(pkg, daq_release)
            if jar is None:
                raise RunnerException("Cannot find %s jar file" % pkg)

            self.__classes.append(jar)
            self.__classpath = None

    def add_repo_jars(self, rpkgs):
        """Add Maven repository jar files to CLASSPATH"""
        for t in rpkgs:
            if len(t) == 3:
                (proj, name, version) = t
                extra = None
            elif len(t) == 4:
                (proj, name, version, extra) = t
            else:
                raise RunnerException("Bad repository tuple %s" % (t))

            jar = self.__find_maven_jar(proj, name, version, extra)
            if jar is None:
                raise RunnerException(("Cannot find %s in local Maven" +
                                       " repository (%s)") %
                                      (name, self.__maven_repository_path()))
            self.__classes.append(jar)
            self.__classpath = None

    def kill(self, sig):
        self.send_signal(sig, None)
        self.__killsig = sig

    def quickexit(self, sig, frame):
        """Kill the program if we get an interrupt signal"""
        self.send_signal(sig, frame)
        self.__exitsig = sig

    def run(self, java_args=None, sys_args=None, debug=False):
        """Run the Java program, handling ^C or ^\ as appropriate"""
        # set CLASSPATH if it hasn't been set yet
        if self.__classpath is None:
            self.__classpath = ":".join(self.__classes)
            os.environ["CLASSPATH"] = self.__classpath

            if debug:
                print("export CLASSPATH=\"%s\"" % os.environ["CLASSPATH"])

        signal.signal(signal.SIGINT, self.quickexit)
        signal.signal(signal.SIGQUIT, self.send_signal)

        try:
            rundata = RunData(java_args, self.__main_class, sys_args)
            self.__run_command(rundata, debug)
        finally:
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGQUIT, signal.SIG_DFL)

        return rundata

    def send_signal(self, sig, frame):
        """Send a signal to the process"""
        if self.__proc is not None:
            os.killpg(self.__proc.pid, sig)
