package icecube.daq.testbed;

import icecube.daq.common.ANSIEscapeCode;
import icecube.daq.trigger.exceptions.ConfigException;
import icecube.daq.util.DOMRegistry;
import icecube.daq.util.LocatePDAQ;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * DAQ component test bed.
 */
public class TestBed
{
    private static final ColoredAppender APPENDER =
        new ColoredAppender(/*org.apache.log4j.Level.ALL).setVerbose(true*/);

    private static final Level DEFAULT_LOGLEVEL = Level.ERROR;
    private static final int MAX_FAILURES = 4;

    private File configDir;
    private WrappedComponent comp;
    private boolean dumpSplicer;
    private Level logLevel = DEFAULT_LOGLEVEL;
    private File monOutFile;
    private int numSrcs;
    private int numToProcess;
    private int numToSkip;
    private int maxFailures = MAX_FAILURES;
    private Configuration runCfg;
    private int runNumber;
    private File srcDir;
    private File targetDir;
    private boolean verbose;
    private boolean waitForInput;

    private DOMRegistry registry;

    /**
     * Create a testbed object.
     *
     * @param args command-line arguments
     *
     * @throws IOException if there is a problem
     */
    public TestBed(String[] args)
        throws IOException
    {
        processArgs(args);
    }

    /**
     * Perform a DAQ run.
     *
     * @return <tt>true</tt> if the run was successful
     */
    public boolean run()
    {
        // set log level
        Logger.getRootLogger().setLevel(logLevel);
        APPENDER.setLevel(logLevel);

        if (waitForInput) {
            System.out.print("Hit [RETURN] to start: ");
            System.out.flush();

            try {
                BufferedReader bufferRead =
                    new BufferedReader(new InputStreamReader(System.in));
                bufferRead.readLine();
            } catch (IOException ioe) {
                // ignore errors
            }
        }

        System.err.println("Running " + comp);

        boolean rtnval;
        try {
            rtnval = comp.run(runCfg, numSrcs, runNumber, srcDir, numToProcess,
                              numToSkip, maxFailures, targetDir, monOutFile,
                              verbose, dumpSplicer);
        } catch (Exception ex) {
            System.err.println("Run failed for " + runCfg.getName());
            ex.printStackTrace();
            rtnval = false;
        } finally {
            System.err.println("Destroying " + comp);
            comp.destroy();
        }

        return rtnval;
    }

    /**
     * Process command-line arguments.
     *
     * @param args command-line arguments
     */
    private void processArgs(String[] args)
    {
        String compName = null;
        String runCfgName = null;

        boolean usage = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].length() > 1 && args[i].charAt(0) == '-') {
                switch(args[i].charAt(1)) {
                case 'C':
                    i++;
                    compName = args[i];
                    break;
                case 'c':
                    i++;
                    runCfgName = args[i];
                    break;
                case 'D':
                    i++;
                    File tmpCfgDir = new File(args[i]);
                    if (!tmpCfgDir.isDirectory()) {
                        System.err.println("Bad config directory \"" +
                                           tmpCfgDir + "\"");
                        usage = true;
                    } else {
                        configDir = tmpCfgDir;
                    }
                    break;
                case 'd':
                    i++;
                    File tmpSource = new File(args[i]);
                    if (!tmpSource.isDirectory()) {
                        System.err.println("Bad source directory \"" +
                                           tmpSource + "\"");
                        usage = true;
                    } else {
                        srcDir = tmpSource;
                    }
                    break;
                case 'F':
                    i++;

                    int tmpFail;
                    try {
                        tmpFail = Integer.parseInt(args[i]);
                        maxFailures = tmpFail;
                    } catch (NumberFormatException e) {
                        System.err.println("Bad maximum number of" +
                                           " failures \"" + args[i] + "\"");
                        usage = true;
                        break;
                    }

                    break;
                case 'h':
                    i++;

                    int tmpNum;
                    try {
                        tmpNum = Integer.parseInt(args[i]);
                        numSrcs = tmpNum;
                    } catch (NumberFormatException e) {
                        System.err.println("Bad number of sources \"" +
                                           args[i] + "\"");
                        usage = true;
                        break;
                    }

                    break;
                case 'l':
                    i++;
                    Level tmpLevel;
                    if (args[i].equalsIgnoreCase("off") ||
                        args[i].equalsIgnoreCase("none"))
                    {
                        tmpLevel = Level.OFF;
                    } else if (args[i].equalsIgnoreCase("fatal")) {
                        tmpLevel = Level.FATAL;
                    } else if (args[i].equalsIgnoreCase("error")) {
                        tmpLevel = Level.ERROR;
                    } else if (args[i].equalsIgnoreCase("warn")) {
                        tmpLevel = Level.WARN;
                    } else if (args[i].equalsIgnoreCase("info")) {
                        tmpLevel = Level.INFO;
                    } else if (args[i].equalsIgnoreCase("debug")) {
                        tmpLevel = Level.DEBUG;
                        //} else if (args[i].equalsIgnoreCase("trace")) {
                        //    tmpLevel = Level.TRACE;
                    } else if (args[i].equalsIgnoreCase("all")) {
                        tmpLevel = Level.ALL;
                    } else {
                        System.err.println("Bad log level \"" + args[i] +
                                           "\"");
                        tmpLevel = null;
                    }

                    if (tmpLevel != null) {
                        logLevel = tmpLevel;
                    }

                    break;
                case 'm':
                    i++;
                    monOutFile = new File(args[i]);
                    break;
                case 'n':
                    i++;

                    int tmpProc;
                    try {
                        tmpProc = Integer.parseInt(args[i]);
                        numToProcess = tmpProc;
                    } catch (NumberFormatException e) {
                        System.err.println("Bad number to process \"" +
                                           args[i] + "\"");
                        usage = true;
                        break;
                    }

                    break;
                case 'r':
                    i++;

                    int tmpRun;
                    try {
                        tmpRun = Integer.parseInt(args[i]);
                        runNumber = tmpRun;
                    } catch (NumberFormatException e) {
                        System.err.println("Bad run number \"" + args[i] +
                                           "\"");
                        usage = true;
                        break;
                    }

                    break;
                case 'S':
                    dumpSplicer = true;
                    break;
                case 's':
                    i++;

                    int tmpSkip;
                    try {
                        tmpSkip = Integer.parseInt(args[i]);
                        numToSkip = tmpSkip;
                    } catch (NumberFormatException e) {
                        System.err.println("Bad number to skip \"" + args[i] +
                                           "\"");
                        usage = true;
                        break;
                    }

                    break;
                case 't':
                    i++;
                    File tmpTarget = new File(args[i]);
                    if (!tmpTarget.isDirectory()) {
                        System.err.println("Bad target directory \"" +
                                           targetDir + "\"");
                        usage = true;
                    } else {
                        targetDir = tmpTarget;
                    }
                    break;
                case 'v':
                    verbose = true;
                    break;
                case 'w':
                    waitForInput = true;
                    break;
                default:
                    System.err.println("Unknown option '" + args[i] + "'");
                    usage = true;
                    break;
                }
            } else if (args[i].length() > 0) {
                System.err.println("Unknown argument '" + args[i] + "'");
                usage = true;
            }
        }

        if (srcDir == null && runNumber != 0) {
            String tmpPath =
                String.format("prj/simplehits/run%05d", runNumber);
            srcDir = new File(System.getenv("HOME"), tmpPath);
            if (!srcDir.isDirectory()) {
                System.err.println("Cannot find default source directory " +
                                   srcDir);
                System.err.println("Please specify source directory (-s)");
                usage = true;
            }
        }

        if (numToProcess <= 0) {
            System.err.println("Please specify number of payloads" +
                               " to write (-n)");
            usage = true;
        }

        if (targetDir == null) {
            targetDir = new File(System.getenv("HOME"), "prj/simplehits");
            if (!targetDir.isDirectory()) {
                System.err.println("Cannot find default target directory " +
                                   targetDir);
                System.err.println("Please specify target directory (-t)");
                usage = true;
            }
        }

        if (configDir == null) {
            try {
                configDir = LocatePDAQ.findConfigDirectory();
            } catch (IllegalArgumentException iae) {
                System.err.println("Cannot find configuration directory");
                System.err.println("Please specify config directory (-D)");
                configDir = null;
            }
        }

        try {
            registry = DOMRegistry.loadRegistry(configDir);
        } catch (Exception ex) {
            System.err.println("Cannot load DOM registry");
            ex.printStackTrace();
            usage = true;
        }

        if (runCfgName == null) {
            System.err.println("Please specify run configuration name");
            usage = true;
        } else if (configDir != null) {
            try {
                Configuration tmpCfg =
                    new Configuration(configDir, runCfgName, registry);
                runCfg = tmpCfg;
            } catch (ConfigException ce) {
                ce.printStackTrace();
                usage = true;
            }
        }

        if (compName == null) {
            System.err.println("Please specify trigger component (-C)");
            usage = true;
        } else if (!usage) {
            try {
                comp = WrappedComponentFactory.create(compName);
            } catch (Error err) {
                err.printStackTrace();
                usage = true;
            }
        }

        if (runCfg != null && comp != null) {
            int maxSrcs;
            try {
                maxSrcs = runCfg.getNumberOfSources(comp.getSourceID());
            } catch (ConfigException ce) {
                System.err.println(ce.getMessage());
                maxSrcs = numSrcs;
                usage = true;
            }

            if (maxSrcs <= 0) {
                System.err.printf("Run configuration %s does not contain" +
                                  " any entries for source ID #%d\n",
                                  runCfg.getName(), comp.getSourceID());
                usage = true;
            } else if (numSrcs <= 0) {
                numSrcs = maxSrcs;
            } else if (numSrcs > maxSrcs) {
                System.err.printf("Number of sources %d is greater than" +
                                  " maximum %d for %s from %s\n", numSrcs,
                                  maxSrcs, comp.getName(), runCfg.getName());
                usage = true;
            }
        }

        if (usage) {
            String usageMsg = "java " + getClass().getName() +
                " [-C componentClass]" +
                " [-c runConfig]" +
                " [-D configDir]" +
                " [-d sourceDirectory]" +
                " [-F maxFailures]" +
                " [-h numberOfSources]" +
                " [-l logLevel]" +
                " [-n numberToProcess]" +
                " [-m monitoringOutputFile]" +
                " [-r runNumber]" +
                " [-S(plicerDump)]" +
                " [-s numberToSkip]" +
                " [-t targetDirectory]" +
                " [-v(erbose)]" +
                " [-w(aitForInput)]" +
                "";

            if (comp != null) {
                try {
                    comp.destroy();
                } catch (Exception ex) {
                    System.err.println("Cannot destroy " + compName);
                    ex.printStackTrace();
                }
            }

            throw new IllegalArgumentException(usageMsg);
        }

        System.out.print(ANSIEscapeCode.BG_GREEN + ANSIEscapeCode.FG_BLUE);
        System.out.println("Component: " + comp.getName());
        System.out.println("Data source directory: " + srcDir);
        System.out.println("Data target directory: " + targetDir);
        System.out.println("Number of sources: " + numSrcs);
        System.out.println("Number of payloads to skip: " + numToSkip);
        System.out.println("Number of payloads to process: " + numToProcess);
        System.out.println("--");
        System.out.println("Run configuration: " + runCfg);
        System.out.println("Run number: " + runNumber);
        System.out.println("Log level: " + logLevel);
        if (monOutFile != null) {
            System.out.println("Monitoring: " + monOutFile);
        }
        System.out.print(ANSIEscapeCode.OFF);
        System.out.println("=====================================");
    }

    /**
     * Main program.
     *
     * @param args command-line arguments
     *
     * @throws Exception if there is a problem
     */
    public static final void main(String[] args)
        throws Exception
    {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure(APPENDER);

        TestBed testbed = new TestBed(args);
        if (!testbed.run()) {
            System.exit(1);
        }
    }
}
