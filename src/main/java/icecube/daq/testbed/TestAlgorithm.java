package icecube.daq.testbed;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.payload.impl.DOMHit;
import icecube.daq.payload.impl.PayloadFactory;
import icecube.daq.payload.impl.SimpleHit;
import icecube.daq.payload.impl.SimplerHit;
import icecube.daq.payload.impl.TriggerRequestFactory;
import icecube.daq.splicer.HKN1Splicer;
import icecube.daq.splicer.SplicedAnalysis;
import icecube.daq.splicer.Splicer;
import icecube.daq.splicer.SplicerChangedEvent;
import icecube.daq.splicer.SplicerException;
import icecube.daq.splicer.SplicerListener;
import icecube.daq.splicer.StrandTail;
import icecube.daq.trigger.algorithm.AlgorithmStatistics;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.control.PayloadSubscriber;
import icecube.daq.trigger.control.TriggerManager;
import icecube.daq.trigger.control.TriggerThread;
import icecube.daq.trigger.exceptions.ConfigException;
import icecube.daq.util.DOMRegistryFactory;
import icecube.daq.util.IDOMRegistry;
import icecube.daq.util.LocatePDAQ;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

class PayloadFileToSplicerBridge
    extends AbstractPayloadFileListBridge
{
    private StrandTail node;
    private PayloadFactory factory;
    private IDOMRegistry registry;

    PayloadFileToSplicerBridge(String name, File[] files, StrandTail node)
    {
        super(name, files);

        this.node = node;

        factory = new PayloadFactory(null);
    }

    /**
     * Close the output channel.
     */
    @Override
    void finishThreadCleanup()
    {
        node.close();
    }

    void setDOMRegistry(IDOMRegistry registry)
    {
        this.registry = registry;
    }

    @Override
    public void write(ByteBuffer buf)
        throws IOException
    {
        IPayload payload;
        if (buf.limit() == 4 && buf.getInt(0) == 4) {
            payload = TriggerManager.FLUSH_PAYLOAD;
        } else {
            try {
                payload = factory.getPayload(buf, 0);
            } catch (PayloadException pe) {
                throw new IOException("Cannot load payload", pe);
            }
        }

        // if this isn't a simple hit, try to convert it
        if (payload.getPayloadType() != 1 &&
            payload != TriggerManager.FLUSH_PAYLOAD)
        {
            IPayload simple = null;
            PayloadException simpleEx = null;
            if (payload instanceof DOMHit) {
                DOMHit domHit = (DOMHit) payload;

                IByteBufferCache cache = factory.getByteBufferCache();
                try {
                    ByteBuffer simpleBuf =
                        domHit.getHitBuffer(cache, registry);
                    simple = factory.getPayload(simpleBuf, 0);
                } catch (PayloadException pe) {
                    simpleEx = pe;
                }
            }

            if (simple == null) {
                String msg = "Cannot build simple hit from " + payload +
                    " (type " + payload.getPayloadType() + ")";
                if (simpleEx == null) {
                    throw new IOException(msg);
                } else {
                    throw new IOException(msg, simpleEx);
                }
            }

            payload = simple;
        }

        try {
            node.push(payload);
        } catch (SplicerException se) {
            throw new IOException("Cannot push payload", se);
        }
    }
}

public class TestAlgorithm
{
    private static final ColoredAppender APPENDER =
        new ColoredAppender(/*org.apache.log4j.Level.ALL).setVerbose(true*/);

    private static final Logger LOG = Logger.getLogger(TestAlgorithm.class);

    private static final Level DEFAULT_LOGLEVEL = Level.ERROR;
    private static final int MAX_FAILURES = 4;

    private File configDir;
    private ITriggerAlgorithm oldAlgorithm;
    private ITriggerAlgorithm algorithm;
    private boolean dumpSplicer;
    private int numSrcs;
    private int numToProcess;
    private int numToSkip;
    private int maxFailures = MAX_FAILURES;
    private Configuration runCfg;
    private int runNumber;
    private File srcDir;
    private File targetDir;
    private boolean verbose;
    private boolean compareOld;

    private IDOMRegistry registry;

    /**
     * Create an algorithm testing object.
     *
     * @param args command-line arguments
     *
     * @throws IOException if there is a problem
     */
    public TestAlgorithm(String[] args)
        throws IOException
    {
        processArgs(args);
    }

    private PayloadFileToSplicerBridge[] buildBridges(Splicer splicer)
        throws IOException
    {
        List<Integer> hubs;
        try {
            hubs = runCfg.getHubs(algorithm.getSourceId());
        } catch (ConfigException ce) {
            throw new IOException("Cannot get hubs from " + runCfg, ce);
        }

        if (hubs == null || hubs.size() < numSrcs) {
            throw new IOException("Asked for " + numSrcs + " hub" +
                                  (numSrcs == 1 ? "" : "s") + ", but only " +
                                  (hubs == null ? 0 : hubs.size()) +
                                  " available in " + runCfg.getName());
        }

        PayloadFileToSplicerBridge[] bridges =
            new PayloadFileToSplicerBridge[numSrcs];
        for (int h = 0; h < numSrcs; h++) {
            final int hubId = hubs.get(h);
            final String hubName = SimpleHitFilter.getHubName(hubId);

            File[] files = SimpleHitFilter.listFiles(srcDir, hubId, runNumber);

            PayloadFileToSplicerBridge bridge =
                new PayloadFileToSplicerBridge(hubName, files,
                                               splicer.beginStrand());
            bridge.setDOMRegistry(registry);
            bridge.setNumberToSkip(numToSkip);
            bridge.setMaximumPayloads(numToProcess);
            bridge.setWriteDelay(1, 10);
            bridges[h] = bridge;
        }

        return bridges;
    }

    public TriggerConsumer connectToConsumer(File targetDir, String runCfgName,
                                             int runNumber, int numSrcs,
                                             int numToSkip, int numToProcess)
        throws IOException
    {
        ConsumerHandler handler;

        final int trigId = algorithm.getTriggerConfigId();
        final String name = HashedFileName.getName(runCfgName,
                                                   algorithm.getSourceId(),
                                                   runNumber, trigId, numSrcs,
                                                   numToSkip, numToProcess);
        File outFile = new File(targetDir, name);
        if (outFile.exists()) {
            handler = new CompareHandler(outFile);
            System.err.println("*** Comparing output with " + outFile);
        } else {
            handler = new OutputHandler(outFile);
            System.err.println("*** Writing output to " + outFile);
        }
        handler.configure(algorithm);

        TriggerConsumer consumer =
            new TriggerConsumer(algorithm, handler, registry);
        //consumer.start();

        return consumer;
    }

    private String[] listValidRunNumbers(File hitDir)
    {
        ArrayList<String> numbers = new ArrayList<String>();

        for (String name : hitDir.list()) {
            if (!name.startsWith("run")) {
                continue;
            }

            File path = new File(hitDir, name);
            if (!path.isDirectory()) {
                continue;
            }

            numbers.add(name.substring(3));
        }

        return numbers.toArray(new String[0]);
    }

    /**
     * Process command-line arguments.
     *
     * @param args command-line arguments
     */
    private void processArgs(String[] args)
    {
        int configId = Integer.MIN_VALUE;
        Level logLevel = DEFAULT_LOGLEVEL;
        String runCfgName = null;

        boolean usage = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].length() > 1 && args[i].charAt(0) == '-') {
                switch(args[i].charAt(1)) {
                case 'c':
                    i++;
                    runCfgName = args[i];
                    break;
                case 'C':
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
                case 'D':
                    String propStr;
                    if (args[i].length() > 2) {
                        propStr = args[i].substring(2);
                    } else {
                        i++;
                        propStr = args[i];
                    }

                    String[] tmpProp = propStr.split(Pattern.quote("="), 2);
                    if (tmpProp == null || tmpProp.length != 2) {
                        System.err.println("Bad property \"" + propStr + "\"");
                        usage = true;
                    } else {
                        System.setProperty(tmpProp[0], tmpProp[1]);
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
                case 'O':
                    compareOld = true;
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
                                           args[i] + "\"");
                        usage = true;
                    } else {
                        targetDir = tmpTarget;
                    }
                    break;
                case 'T':
                    i++;

                    int tmpCfgId;
                    try {
                        tmpCfgId = Integer.parseInt(args[i]);
                        configId = tmpCfgId;
                    } catch (NumberFormatException e) {
                        System.err.println("Bad trigger config ID \"" +
                                           args[i] + "\"");
                        usage = true;
                        break;
                    }

                    break;
                case 'v':
                    verbose = true;
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

        if (srcDir == null) {
            if (runNumber == 0) {
                System.err.println("Please specify \"-d sourceDir\" and/or" +
                                   " \"-r runNumber\"");
                 usage = true;
            } else {
                File hitDir =
                    new File(System.getenv("HOME"), "prj/simplehits");
                if (!hitDir.isDirectory()) {
                    System.err.println("Cannot find top-level SimpleHit" +
                                       " directory " + hitDir);
                    usage = true;
                } else {
                    final String subName = String.format("run%05d", runNumber);
                    srcDir = new File(hitDir, subName);
                    if (!srcDir.isDirectory()) {
                        System.err.println("Cannot find hit directory" +
                                           " directory " + srcDir);
                        String[] numbers = listValidRunNumbers(hitDir);
                        if (numbers != null && numbers.length > 0) {
                            System.err.println("Valid run numbers:");
                            for (String number : numbers) {
                                System.err.println("\t" + number);
                            }
                        }
                        usage = true;
                    }
                }
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
            registry = DOMRegistryFactory.load(configDir);
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

        if (configId == Integer.MIN_VALUE) {
            System.err.println("Please specify trigger config ID (-T)");

            // try to fetch the list of trigger config IDs
            ConfigData[] list;
            if (runCfg == null) {
                list = null;
            } else {
                try {
                    list = runCfg.getConfigData();
                } catch (ConfigException cex) {
                    System.err.println("Cannot get configuration data");
                    cex.printStackTrace();
                    list = null;
                }
            }

            // if we got a list of trigger config IDs, print it
            if (list != null && list.length > 0) {
                System.err.println("Valid config IDs:");
                for (ConfigData cd : list) {
                    System.err.println("\t" + cd.getConfigId() + " (" +
                                       cd.getSourceName() + " " +
                                       cd.getName() + ")");
                }
            }

            usage = true;
        } else if (!usage) {
            try {
                algorithm = runCfg.getTriggerAlgorithm(configId);
            } catch (ConfigException ce) {
                ce.printStackTrace();
                usage = true;
            }

            if (algorithm == null) {
                System.err.println("Cannot find trigger config #" + configId +
                                   " in \"" + runCfgName + "\"");
                usage = true;
            } else if (compareOld) {
                // look for an "OldXXX" version of class "XXX"
                try {
                    oldAlgorithm = runCfg.getTriggerAlgorithm(configId, true);
                } catch (ConfigException ce) {
                    ce.printStackTrace();
                    usage = true;
                }
            }
        }

        if (!usage && algorithm == null) {
            System.err.println("Please specify a valid trigger" +
                               " configuration ID");
                usage = true;
        }

        if (runCfg != null && algorithm != null) {
            int maxSrcs;
            try {
                maxSrcs = runCfg.getNumberOfSources(algorithm.getSourceId());
            } catch (ConfigException ce) {
                System.err.println(ce.getMessage());
                maxSrcs = numSrcs;
                usage = true;
            }

            if (maxSrcs <= 0) {
                final int srcId = algorithm.getSourceId();
                final String compName =
                    SourceIdRegistry.getDAQNameFromSourceID(srcId);
                System.err.printf("Run configuration %s does not contain" +
                                  " any entries for component #%d (%s)\n",
                                  runCfg.getName(), srcId, compName);
                usage = true;
            } else if (numSrcs <= 0) {
                numSrcs = maxSrcs;
            } else if (numSrcs > maxSrcs) {
                System.err.printf("Number of sources %d is greater than" +
                                  " maximum %d for %s from %s\n", numSrcs,
                                  maxSrcs, algorithm.getTriggerName(),
                                  runCfg.getName());
                usage = true;
            }
        }

        if (usage) {
            String usageMsg = "java " + getClass().getName() +
                " [-c runConfig]" +
                " [-C configDir]" +
                " [-d sourceDirectory]" +
                " [-F maxNumberOfFailures]" +
                " [-h numberOfSources]" +
                " [-l logLevel]" +
                " [-n numberToProcess]" +
                " [-O(ldAlgorithmCompare)]" +
                " [-r runNumber]" +
                " [-S(plicerDump)]" +
                " [-s numberToSkip]" +
                " [-t targetDirectory]" +
                " [-T triggerConfigID]" +
                " [-v(erbose)]" +
                "";
            throw new IllegalArgumentException(usageMsg);
        }

        // set log level
        Logger.getRootLogger().setLevel(logLevel);
        APPENDER.setLevel(logLevel);
    }

    private static boolean report(TriggerThread thread,
                                  AlgorithmMonitor activity,
                                  TriggerConsumer consumer, double startSecs,
                                  AlgorithmDeathmatch deathmatch)
    {
        final double now = ((double) System.nanoTime()) / 1000000000.0;

        System.out.println(thread.toString());
        for (AlgorithmStatistics stats : activity.getAlgorithmStatistics()) {
            System.out.println(stats.toString());
        }

        System.err.println("-------------------- REPORT --------------------");
        boolean rtnval = report(consumer, deathmatch, now - startSecs);
        if (deathmatch != null) {
            System.out.println(deathmatch.getStats());
        }

        return rtnval;
    }

    private static boolean report(TriggerConsumer consumer,
                                  AlgorithmDeathmatch deathmatch,
                                  double clockSecs)
    {
        final int numWritten, numFailed;
        if (deathmatch == null) {
            numWritten = consumer.getNumberWritten();
            numFailed = consumer.getNumberFailed();
        } else {
            numWritten = deathmatch.getNumberWritten();
            numFailed = deathmatch.getNumberFailed();
        }

        String success;
        if (numWritten > 0) {
            success = "successfully ";
        } else {
            success = "";
        }

        final ConsumerHandler handler = consumer.getHandler();
        final int numExtra = handler.getNumberExtra();
        final int numMissed = handler.getNumberMissed();
        final boolean forcedStop = consumer.forcedStop();
        final boolean sawStop = handler.sawStop();

        System.out.println("Consumer " + success + handler.getReportVerb() +
                           " " + numWritten + " payloads" +
                           (numMissed == 0 ? "" : ", " + numMissed +
                            " missed") +
                           (numExtra == 0 ? "" : ", " + numExtra +
                            " extra") +
                           (numFailed == 0 ? "" : ", " + numFailed +
                            " failed") +
                           (forcedStop ? ", FORCED TO STOP" :
                            (sawStop ? "" : ", not stopped")));

        handler.reportTime(clockSecs);

        return (numMissed == 0 && numFailed == 0 && !forcedStop);
    }

    /**
     * Perform a DAQ run.
     *
     * @return <tt>true</tt> if the run was successful
     */
    private boolean run()
        throws IOException
    {
        // initialize DOM registry for simple hit classes
        SimpleHit.setDOMRegistry(registry);
        SimplerHit.setDOMRegistry(registry);

        AlgorithmDeathmatch deathmatch = null;
        if (oldAlgorithm != null) {
            deathmatch = new AlgorithmDeathmatch(algorithm, oldAlgorithm);
            algorithm = deathmatch;
        }

        TriggerConsumer consumer =
            connectToConsumer(targetDir, runCfg.getName(), runNumber, numSrcs,
                              numToSkip, numToProcess);
        algorithm.setTriggerManager(consumer);
        algorithm.setTriggerCollector(consumer);

        SplicerSubscriber subscriber = new SplicerSubscriber("Subscriber");
        algorithm.setSubscriber(subscriber);

        TriggerRequestFactory factory = new TriggerRequestFactory(null);
        algorithm.setTriggerFactory(factory);

        TriggerThread thread = new TriggerThread(0, algorithm);

        HKN1Splicer splicer =
            new HKN1Splicer<IPayload>(subscriber, new PayloadComparator(),
                                      TriggerManager.FLUSH_PAYLOAD);

        PayloadFileToSplicerBridge[] bridges = buildBridges(splicer);

        final double startTime = ((double) System.nanoTime()) / 1000000000.0;

        thread.start();
        splicer.start();
        startBridges(bridges);

        AlgorithmMonitor activity = new AlgorithmMonitor(algorithm, bridges,
                                                         subscriber, consumer,
                                                         maxFailures);
        if (LOG.isInfoEnabled()) {
            LOG.info("Waiting");
        }

        activity.waitForStasis(20, numToProcess, 2, verbose, dumpSplicer,
                               null);

        algorithm.flush();

        if (verbose) {
            System.out.println("Waiting for final flush...");
        }
        activity.waitForStasis(20, numToProcess, 3, verbose, dumpSplicer,
                               null);

        if (verbose) {
            System.out.println("Stopping...");
        }
        splicer.stop();

        if (verbose) {
            System.out.println("Waiting for splicer...");
        }
        activity.waitForStasis(20, numToProcess, 3, verbose, dumpSplicer,
                               null);

        thread.stop();
        thread.join();

        if (verbose) {
            System.out.println("Stopped...");
        }

        for (AbstractPayloadFileListBridge bridge : bridges) {
            bridge.stopThread();
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Checking");
        }

        boolean rtnval = report(thread, activity, consumer, startTime,
                                deathmatch);

        //final boolean noOutput = consumer.getNumberWritten() == 0 &&
        //    consumer.getNumberFailed() == 0;
        //try {
        //    checkTriggerCaches(comp, noOutput, verbose);
        //} catch (Throwable thr) {
        //    LOG.error("Problem with trigger caches", thr);
        //    rtnval = false;
        //}

        return rtnval;
    }

    /**
     * Start the file bridges.
     *
     * @param bridges list of file bridges
     */
    public static void startBridges(AbstractPayloadFileListBridge[] bridges)
    {
        if (bridges != null) {
            for (int i = 0; i < bridges.length; i++) {
                bridges[i].start();
            }
        }
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

        TestAlgorithm tstalgo = new TestAlgorithm(args);
        if (!tstalgo.run()) {
            System.exit(1);
        }
    }
}
