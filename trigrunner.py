#!/usr/bin/env python
#
# Run one or more trigger components

import hashlib
import os
import signal
import sys
import threading

from lxml import etree
from runner import JavaRunner


###
### This following probably don't need to be customized
###

# main class being run
MAIN_CLASS = "icecube.daq.testbed.TestBed"

# Java max memory
JAVA_ARGS = ("-Xmx4000m", "-Dicecube.sndaq.ignore", )

# required jar files from subprojects and Maven repository
SUBPROJECT_PKGS = ("daq-common", "splicer", "payload", "daq-io", "juggler",
                   "trigger", "trigger-testbed")
REPO_PKGS = (("log4j", "log4j", "1.2.7"),
             ("commons-logging", "commons-logging", "1.0.4"),
             ("edu/wisc/icecube", "icebucket", "3.0.2"),
             ("dom4j", "dom4j", "1.6.1"),
             ("jaxen", "jaxen", "1.1.1"),
             ("org/zeromq", "jzmq", "1.0.0"),
             ("com/google/code/gson", "gson", "2.1"))


class SkipList(object):
    "List of run configuration files to skip"
    def __init__(self):
        self.__dict = {}

    def add(self, name):
        self.__dict[name] = 1

    def contains(self, path):
        name = os.path.basename(path)
        if name.endswith(".xml"):
            name = name[:-4]
        return name in self.__dict


class RunConfigException(Exception):
    pass


class RunConfig(object):
    "Run configuration details"

    SKIP_LIST = None

    def __init__(self, filename):
        self.__filename = filename
        self.__stringhubs = []
        self.__icetophubs = []
        self.__comps = []
        self.__trig_cfg = None
        self.__hash = None

        if self.SKIP_LIST is None:
            self.SKIP_LIST = self.__read_skip_list()
        self.__skip = self.SKIP_LIST.contains(filename)

        if not self.__skip:
            self.__parse()

    @classmethod
    def __hashname(cls, name):
        if name.endswith(".xml"):
            name = name[:-4]

        m = hashlib.md5()
        m.update(name)
        return m.hexdigest().lstrip("0")

    def __parse(self):
        try:
            allcfg = etree.parse(self.__filename)
        except:
            raise RunConfigException("Cannot parse %s" % self.__filename)

        for dc in allcfg.findall("//domConfigList"):
            if "hub" in dc.attrib:
                hub = int(dc.attrib["hub"])
                if hub % 1000 < 200:
                    self.__stringhubs.append(hub)
                elif hub % 1000 < 300:
                    self.__icetophubs.append(hub)
        for tc in allcfg.findall("//triggerConfig"):
            if self.__trig_cfg is not None:
                raise RunConfigException("Found multiple trigger" +
                                         " configurations in %s" %
                                         self.__filename)

            path = os.path.join(os.path.dirname(self.__filename),
                                "trigger", tc.text)
            if not os.path.isfile(path):
                if not path.endswith(".xml"):
                    path += ".xml"
                    if not os.path.isfile(path):
                        raise RunConfigException(("Cannot find trigger" +
                                                 " configuration \"%s\"" +
                                                 " in %s ") %
                                                 (path, self.__filename))
                    self.__trig_cfg = TriggerConfig(path)
                    continue

        for rc in allcfg.findall("//runComponent"):
            if "name" not in rc.attrib:
                raise RunConfigException("Found nameless runComponent in %s" %
                                         self.__filename)
            self.__comps.append(rc.attrib["name"])

    @classmethod
    def __read_skip_list(cls):
        skip = SkipList()
        if os.path.isfile("skip-list"):
            try:
                fd = open("skip-list", "r")
                for line in fd:
                    skip.add(line.rstrip())
            finally:
                fd.close()
        return skip

    def __str__(self):
        return "%s[hubs*%d ithubs*%d comps*%d tc=%s]%s" % \
            (os.path.basename(self.__filename), len(self.__stringhubs),
             len(self.__icetophubs), len(self.__comps), self.__trig_cfg,
             self.hash())

    def dirname(self):
        return os.path.dirname(self.__filename)

    def global_hubs(self):
        num = 0
        if len(self.__icetophubs) > 0:
            num += 1
        if len(self.__stringhubs) > 0:
            num += 1
        return num

    def hash(self):
        if self.__hash is None:
            self.__hash = self.__hashname(os.path.basename(self.__filename))
        return self.__hash

    def hubtypes(self):
        realhub = 0
        simhub = 0
        testhub = 0

        for h in self.__stringhubs + self.__icetophubs:
            if h < 0:
                print "Illegal hub#%s in %s" % (h, self)
            elif h < 1000:
                realhub += 1
            elif h < 2000:
                simhub += 1
            elif h < 3000:
                testhub += 1
            else:
                print "Unknown hub#%s in %s" % (h, self)

        return [realhub, simhub, testhub]

    def icetop(self):
        try:
            return self.__comps.index("iceTopTrigger") >= 0
        except:
            return False

    def icetop_hubs(self):
        return len(self.__icetophubs)

    def inice(self):
        try:
            return self.__comps.index("inIceTrigger") >= 0
        except:
            return False

    def inice_hubs(self):
        return len(self.__stringhubs)

    def name(self):
        name = os.path.basename(self.__filename)
        if name.endswith(".xml"):
            name = name[:-4]
        return name

    def skip(self):
        return self.__skip

    def usable(self):
        if len(self.__stringhubs) == 0 and len(self.__icetophubs) == 0:
            return False
        for h in self.__stringhubs + self.__icetophubs:
            if h >= 1000:
                return False
        return self.icetop() or self.inice()


class TriggerAlgorithm(object):
    "Trigger algorithm details"
    def __init__(self):
        self.__name = None
        self.__srcid = None

    def is_global(self):
        return self.__srcid == 6000

    def is_inice(self):
        return self.__srcid == 4000

    def is_icetop(self):
        return self.__srcid == 5000

    def is_valid(self):
        return self.__name is not None and self.__srcid is not None

    def problem_string(self):
        if self.__name is None:
            if self.__srcid is None:
                return "Found empty triggerConfig entry"
            return "Found unnamed triggerConfig entry"
        elif self.__srcid is None:
            return "%s is missing source ID" % self.__name
        return None

    def set_name(self, name):
        self.__name = name

    def set_source(self, srcid):
        self.__srcid = srcid


class TriggerConfig(object):
    "Trigger configuration file details"
    def __init__(self, filename):
        self.__filename = filename
        self.__algorithms = []

        self.__parse()

    def __str__(self):
        counts = [0, 0, 0, 0]
        for a in self.__algorithms:
            if a.is_inice():
                counts[0] += 1
            elif a.is_icetop():
                counts[1] += 1
            elif a.is_global():
                counts[2] += 1
            else:
                counts[3] += 1

        prefix = ("ii", "it", "gl", "??")

        tstr = None
        for i in xrange(len(counts)):
            if counts[i] > 0:
                if tstr is None:
                    tstr = "%s=%d" % (prefix[i], counts[i])
                else:
                    tstr += " %s=%d" % (prefix[i], counts[i])

        return "%s[%s]" % (os.path.basename(self.__filename), tstr)

    def __parse(self):
        allcfg = etree.parse(self.__filename)
        for tc in allcfg.findall("//triggerConfig"):
            algorithm = TriggerAlgorithm()

            src = tc.find("sourceId")
            if src is not None:
                algorithm.set_source(int(src.text))

            nm = tc.find("triggerName")
            if nm is not None:
                algorithm.set_name(nm.text)

            if algorithm.is_valid():
                self.__algorithms.append(algorithm)

        if len(self.__algorithms) == 0:
            raise RunConfigException(self.__filename + " is not a trigger" +
                                     " configuration file")


class RunConfigLister(object):
    "Return a list of RunConfig objects read from the configuration directory"
    def __init__(self, configdir=None):
        self.__configdir = configdir
        self.__list = None

    def __list_files(self):
        if self.__list is not None:
            for f in self.__list:
                yield f
        else:
            for f in os.listdir(self.__configdir):
                if f.startswith(".") or f.endswith(".cfg") or \
                        f == "default-dom-geometry.xml" or \
                        f == "nicknames.txt":
                    continue

                path = os.path.join(self.__configdir, f)
                if not os.path.isfile(path):
                    continue

                yield path

    def add(self, f):
        path = os.path.join(self.__configdir, f)
        if not os.path.isfile(path):
            if not path.endswith(".xml"):
                path += ".xml"
                if not os.path.isfile(path):
                    raise RunConfigException("Cannot find run configuration" +
                                             " \"%s\" in %s" %
                                             (f, self.__configdir))

        if self.__list is None:
            self.__list = [path, ]
        else:
            self.__list.append(path)

    def list(self):
        for f in self.__list_files():
            try:
                yield RunConfig(f)
            except RunConfigException, rce:
                print >> sys.stderr, "Ignoring bad %s (%s)" % (f, rce)


class RunMinder(object):
    "Watch a testbed thread, killing it if it takes a long time"
    def __init__(self, runner):
        """
        Thread which watches for unterminated runs
        runner - DAQ run manager
        wait_time - maximum number of seconds allowed for a run (flt point)
        """
        self.__runner = runner
        self.__wait_time = 1800.0

        self.__condition = threading.Condition()
        self.__running = False
        self.__stopping = False
        self.__ready = False
        self.__run = None

        self.__thread = threading.Thread(target=self.run, name="RunMinder")
        self.__thread.start()

    def run(self):
        self.__running = True

        self.__condition.acquire()
        while not self.__stopping:

            # wait for a run to be started
            self.__ready = True
            while self.__run is None and not self.__stopping:
                self.__condition.wait()
            self.__ready = False

            if self.__run is None and self.__stopping:
                break

            # now wait for the run to finish
            run = self.__run
            self.__condition.wait(self.__wait_time)

            # if the run is still going, kill it
            next_sig = signal.SIGTERM
            stacktrace = True
            while self.__run is not None and run == self.__run:
                if stacktrace:
                    # grab a stack trace for debugging
                    self.__runner.send_signal(signal.SIGQUIT, None)
                    stacktrace = False

                # send a signal and wait a minute for it to take effect
                self.__runner.kill(next_sig)
                self.__condition.wait(60.0)
                if next_sig == signal.SIGTERM:
                    next_sig = signal.SIGINT
                elif next_sig == signal.SIGINT:
                    next_sig = signal.SIGKILL
                else:
                    print >> sys.stderr, "Run %d seems to be unkillable!" % run

        self.__condition.release()

        self.__running = False

    def set_wait_time(self, secs):
        self.__wait_time = secs

    def start_run(self, run_num):
        self.__condition.acquire()
        self.__run = run_num
        self.__condition.notify()
        self.__condition.release()

    def stop_run(self):
        self.__condition.acquire()
        self.__run = None
        self.__condition.notify()
        self.__condition.release()

    def stop_thread(self):
        self.__condition.acquire()
        self.__stopping = True
        self.__condition.notify()
        self.__condition.release()


class TriggerRunner(JavaRunner):
    "Manage a set of one or more testbed runs"

    # trigger types
    (IN_ICE, ICETOP, GLOBAL) = (111, 222, 333)

    def __init__(self, main_class=MAIN_CLASS):
        self.__rpt = None
        self.__run_num = 0
        self.__thread = RunMinder(self)

        self.__wrapname = "wrap.p%d" % os.getpid()

        self.__fd = None
        self.__curcfg = None

        super(TriggerRunner, self).__init__(main_class)

        self.add_subproject_jars(SUBPROJECT_PKGS)
        self.add_repo_jars(REPO_PKGS)

    def __backup_output(self, comp, num_hits, rcname, suffix):
        if not os.path.exists(self.__wrapname):
            return None

        newname = "wrap-%s-%d-%s.%s" % (comp, num_hits, rcname, suffix)
        os.rename(self.__wrapname, newname)
        return newname

    def __cleanup_output(self):
        if os.path.exists(self.__wrapname):
            os.remove(self.__wrapname)

    def __is_empty_output(self):
        if os.path.exists(self.__wrapname) and \
            os.path.getsize(self.__wrapname) > 0:
            return False
        return True

    @classmethod
    def __print(cls, log, msg):
        print msg
        if log is not None:
            print >> log, msg
            log.flush()

    def process(self, line, is_stderr=False):
        if not is_stderr:
            if line.find("Consumer ") >= 0 and \
                (line.find("compared") > 0 or line.find("wrote") or
                 line.find("failed") > 0):
                self.__rpt = line.rstrip()

        self.__fd.write(line)

    def report(self):
        return self.__rpt

    def run(self, sys_args=None, java_args=None, debug=False):
        self.__rpt = None
        self.__fd = open(self.__wrapname, "w")

        self.__run_num += 1
        self.__thread.start_run(self.__run_num)

        try:
            return super(TriggerRunner, self).run(sys_args, java_args,
                                                  debug=debug)
        finally:
            self.__thread.stop_run()
            self.__fd.close()

    def run_all(self, log, cfg_lister, target_dir, type_list, run_num,
                num_hits_list, run_old=False, verbose=False, debug=False):
        for rc in cfg_lister.list():
            if rc.skip():
                if verbose:
                    self.__print(log, "Skip " + rc.name())
                continue
            if not rc.usable():
                continue

            for ttype in type_list:
                if ttype is None:
                    continue

                if ttype == self.IN_ICE:
                    comptype = "in-ice"
                    filetype = "iit"
                    maxhubs = rc.inice_hubs()
                    comp_in_cfg = rc.inice()
                    comp = "IniceTriggerComponent"
                elif ttype == self.ICETOP:
                    comptype = "icetop"
                    filetype = "itt"
                    maxhubs = rc.icetop_hubs()
                    comp_in_cfg = rc.icetop()
                    comp = "IcetopTriggerComponent"
                elif ttype == self.GLOBAL:
                    comptype = "global"
                    filetype = "glbl"
                    maxhubs = rc.global_hubs()
                    comp_in_cfg = True
                    comp = "GlobalTriggerComponent"
                else:
                    print "Unknown trigger type " + str(ttype)
                    break

                if not comp_in_cfg:
                    continue

                for num_hits in num_hits_list:
                    dataname = "rc%s-%s-r%d-h%d-p%d.dat" % \
                        (rc.hash(), filetype, run_num, maxhubs, num_hits)
                    datapath = os.path.join(target_dir, dataname)
                    self.run_one(log, target_dir, rc, ttype, run_num,
                                 num_hits, debug=debug)

    def run_one(self, log, target_dir, rc, trigtype, run_num,
                num_hits, debug=False):
        try:
            java_args = JAVA_ARGS
        except NameError:
            java_args = None

        if trigtype == self.IN_ICE:
            comp = "IniceTriggerComponent"
            comptype = "inice"
        elif trigtype == self.ICETOP:
            comp = "IcetopTriggerComponent"
            comptype = "icetop"
        elif trigtype == self.GLOBAL:
            comp = "GlobalTriggerComponent"
            comptype = "global"
        else:
            print "Unknown trigger type %s" % trigtype
            return

        args = ["-C", comp,
                "-c", rc.name(),
                "-D", rc.dirname(),
                "-n", str(num_hits),
                "-r", str(run_num),
                "-t", target_dir]

        self.__thread.set_wait_time(float(num_hits) / 25.0)

        if self.__curcfg != rc.name():
            self.__curcfg = rc.name()
            self.__print(log, rc.name())

        rundata = self.run(java_args, args, debug=debug)

        if rundata.exit_signal is not None:
            print "EMERGENCY EXIT"
            self.__cleanup_output()
            raise SystemExit(1)

        rpt = self.report()
        if rpt is None:
            rpt = "No report"
            if not self.__is_empty_output():
                self.__backup_output(comptype, num_hits, rc.name(), "norpt")
        else:
            if rpt.startswith("Consumer "):
                rpt = rpt[9:]
            if rpt.endswith(", not stopped"):
                rpt = rpt[:-13]

        if rundata.returncode is None:
            failed = ""
        elif rundata.returncode == 0:
            failed = ""
        else:
            if rundata.returncode == 1:
                failed = "  !!FAILED!!"
            else:
                failed = "  !!FAIL %d!!" % rundata.returncode
            self.__backup_output(comptype, num_hits, rc.name(), "fail")

        self.__cleanup_output()

        if rundata.run_time is None:
            run_time = 0.0
        else:
            run_time = rundata.run_time

        if rundata.wait_time is None or \
           rundata.wait_time < 2:
            waitstr = ""
        else:
            waitstr = ", waited %.2f" % rundata.wait_time

        if rundata.kill_signal is None:
            killed = ""
        else:
            killed = "  !!KILLED %s!!" % rundata.kill_signal

        msg = "    %s %d hits %.0f secs%s: %s  %s%s" % \
              (comptype, num_hits, run_time, waitstr, rpt, killed, failed)
        self.__print(log, msg)

    def stop_threads(self):
        self.__thread.stop_thread()


class FakeRunData(object):
    def __init__(self): pass
    def exit_signal(self): return None
    def kill_signal(self): return None
    def returncode(self): return None
    def run_time(self): return None
    def wait_time(self): return None
