#!/usr/bin/env python
#
# Run trigger components with all run configurations

import os

from trigrunner import RunConfigLister, TriggerRunner


# run number used for test data
RUN_NUMBER = 120151

# number of hits used for each set of tests
TEST_NUM_HITS = (1000, 32800)

# directory holding simple hit files
TARGET_DIR = os.path.join(os.environ["HOME"], "prj", "simplehits")


if __name__ == "__main__":
    import optparse
    p = optparse.OptionParser()
    p.add_option("-a", "--always-run", action="store_true",
                 dest="run_old", default=False,
                 help="Run old trigger even if data file exists")
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
    p.add_option("-n", "--num-hits", action="append",
                 dest="num_hits", type="int",
                 help="Number of hits (can specify multiple numbers)")
    p.add_option("--no-global", action="store_true",
                 dest="no_global", default=False,
                 help="Do not run global trigger")
    p.add_option("--no-icetop", action="store_true",
                 dest="no_icetop", default=False,
                 help="Do not run icetop trigger")
    p.add_option("--no-inice", action="store_true",
                 dest="no_inice", default=False,
                 help="Do not run in-ice trigger")
    p.add_option("--no-new", action="store_true",
                 dest="no_new", default=False,
                 help="Do not run new trigger")
    p.add_option("--no-old", action="store_true",
                 dest="no_old", default=False,
                 help="Do not run old trigger")
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

    if opt.num_hits is None or len(opt.num_hits) == 0:
        num_hits_list = TEST_NUM_HITS
    else:
        num_hits_list = opt.num_hits

    if opt.config_dir is not None:
        cfgdir = opt.config_dir
    elif "PDAQ_CONFIG" in os.environ:
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
        logfile = "all-configs.log"
    else:
        logfile = opt.logfile

    if os.path.exists(logfile):
        # save the old log
        oldfile = logfile + ".old"
        os.rename(logfile, oldfile)

    fd = open(logfile, "w")

    try:
        runner.run_all(fd, cfg_lister, opt.target_dir, type_list, RUN_NUMBER,
                       num_hits_list, run_old=opt.run_old,
                       verbose=opt.verbose, debug=opt.debug)
    finally:
        fd.close()
        runner.stop_threads()
