#!/usr/bin/env python
#
# For all existing trigger testbed output files, run the current trigger
# code and compare it against the previously saved data

import os
import re
import sys

from trigrunner import RunConfigLister, TriggerRunner


# directory holding simple hit files
TARGET_DIR = os.path.join(os.environ["HOME"], "prj", "simplehits")


class DataFile(object):
    def __init__(self, path, runcfg, comptype, run, hubs, hits):
        self.__path = path
        self.__runcfg = runcfg
        self.__comptype = comptype
        self.__run = run
        self.__hubs = hubs
        self.__hits = hits

    def __str__(self):
        return "%s-%s-r%d-h%d-p%d" % (self.__runcfg, self.__comp, self.__run,
                                      self.__hubs, self.__hits)

    def run(self, runner, log, target_dir, debug=False):
        runner.run_one(log, target_dir, self.__runcfg, False, self.__comptype,
                       self.__run, self.__hits, debug=debug)


class DataFileLister(object):
    PAT = re.compile("^rc([^-\s]+)-([^-\s]+)-r(\d+)-h(\d+)-p(\d+)\.dat$")

    def __init__(self, datadir, cfg_lister):
        self.__datadir = datadir
        self.__cfghash = self.__build_hash(cfg_lister)

    def __build_hash(self, cfg_lister):
        cfghash = {}
        for rc in cfg_lister.list():
            if not cfghash.has_key(rc.hash()):
                cfghash[rc.hash()] = rc
            else:
                print >>sys.stderr, "Collision: %s overrides %s" % \
                    (cfghash[rc.hash()].name(), rc.name())
        return cfghash

    def list(self, inice_opt, icetop_opt, global_opt):
        for f in os.listdir(self.__datadir):
            m = self.PAT.match(f)
            if m is None:
                continue

            rchash = m.group(1)

            if not self.__cfghash.has_key(rchash):
                continue

            runcfg = self.__cfghash[rchash]
            if runcfg.skip() or not runcfg.usable():
                continue

            comp = m.group(2)

            if comp == "iit":
                comptype = inice_opt
                comp_in_cfg = runcfg.inice()
            elif comp == "iit":
                comptype = icetop_opt
                comp_in_cfg = runcfg.icetop()
            elif comp == "glbl":
                comptype = global_opt
                comp_in_cfg = True
            if comptype is None:
                continue
            if not comp_in_cfg:
                print "Ignoring %s (%s not in runcfg)" % (f, comp)
                continue

            path = os.path.join(self.__datadir, f)
            run = int(m.group(3))
            hubs = int(m.group(4))
            hits = int(m.group(5))

            yield DataFile(path, runcfg, comptype, run, hubs, hits)


if __name__ == "__main__":
    import optparse
    p = optparse.OptionParser()
    p.add_option("-c", "--config", action="append",
                 dest="cfglist", type="string",
                 help="One or more configurations to try" +
                      " (defaults to all configs)")
    p.add_option("-D", "--config-dir", action="store",
                 dest="config_dir", type="string",
                 help="pDAQ configuration directory")
    p.add_option("-l", "--logfile", action="store",
                 dest="logfile", type="string",
                 help="Log file where output is written")
    p.add_option("--no-global", action="store_true",
                 dest="no_global", default=False,
                 help="Do not run global trigger")
    p.add_option("--no-icetop", action="store_true",
                 dest="no_icetop", default=False,
                 help="Do not run icetop trigger")
    p.add_option("--no-inice", action="store_true",
                 dest="no_inice", default=False,
                 help="Do not run in-ice trigger")
    p.add_option("-t", "--target-dir", action="store",
                 dest="target_dir", type="string", default=TARGET_DIR,
                 help="Directory holding simple hit data and" + \
                 " previous run results")
    p.add_option("-v", "--verbose", action="store_true",
                 dest="verbose", default=False,
                 help="Print periodic run status")
    p.add_option("-x", "--debug", action="store_true",
                 dest="debug", default=False,
                 help="Print debugging data")

    opt, args = p.parse_args()

    if len(args):
        raise SystemExit("Found extra command-line arguments: " + str(args))

    if opt.config_dir is not None:
        cfgdir = opt.config_dir
    elif os.environ.has_key("PDAQ_CONFIG"):
        cfgdir = os.environ["PDAQ_CONFIG"]
    else:
        cfgdir = os.path.join(os.environ["HOME"], "config")

    cfg_lister = RunConfigLister(cfgdir)
    if opt.cfglist is not None:
        for f in opt.cfglist:
            cfg_lister.add(f)

    if opt.no_inice:
        inice_opt = None
    else:
        inice_opt = TriggerRunner.IN_ICE
    if opt.no_icetop:
        icetop_opt = None
    else:
        icetop_opt = TriggerRunner.ICETOP
    if opt.no_global:
        global_opt = None
    else:
        global_opt = TriggerRunner.GLOBAL
    type_list = (inice_opt, icetop_opt, global_opt)

    runner = TriggerRunner()

    if opt.logfile is None:
        logfile = "cmp-configs.log"
    else:
        logfile = opt.logfile

    if os.path.exists(logfile):
        # save the old log
        oldfile = logfile + ".old"
        os.rename(logfile, oldfile)

    fd = open(logfile, "w")

    dflister = DataFileLister(opt.target_dir, cfg_lister)
    try:
        for df in dflister.list(inice_opt, icetop_opt, global_opt):
            df.run(runner, fd, opt.target_dir, debug=opt.debug)
    finally:
        fd.close()
        runner.stop_threads()
