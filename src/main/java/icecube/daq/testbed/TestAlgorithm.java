package icecube.daq.testbed;

import icecube.daq.payload.IPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.impl.PayloadFactory;
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
import icecube.daq.util.DOMRegistry;
import icecube.daq.util.LocatePDAQ;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

class PayloadComparator
    implements Comparator<IPayload>
{
    public int compare(IPayload p1, IPayload p2)
    {
        if (p1 == null) {
            if (p2 == null) {
                return 0;
            }

            return 1;
        } else if (p2 == null) {
            return -1;
        }

        final long diff = p1.getUTCTime() - p2.getUTCTime();
        if (diff < 0) {
            return -1;
        } else if (diff > 0) {
            return 1;
        } else {
            return 0;
        }
    }

    public boolean equals(Object obj)
    {
        return obj == this;
    }
}

class PayloadFileToSplicerBridge
    extends AbstractPayloadFileListBridge
{
    private StrandTail node;
    private PayloadFactory factory;

    PayloadFileToSplicerBridge(String name, File[] files, StrandTail node)
    {
        super(name, files);

        this.node = node;

        factory = new PayloadFactory(null);
    }

    /**
     * Close the output channel.
     */
    void finishThreadCleanup()
    {
        node.close();
    }

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

        try {
            node.push(payload);
        } catch (SplicerException se) {
            throw new IOException("Cannot push payload", se);
        }
    }
}

class AlgorithmMonitor
    extends ActivityMonitor
{
    private ITriggerAlgorithm algorithm;
    private AbstractPayloadFileListBridge[] bridges;
    private PayloadSubscriber subscriber;

    private int[] bridgeCounts;
    private long prevQueued;
    private long prevWritten;

    private ArrayList<AlgorithmStatistics> statsList =
        new ArrayList<AlgorithmStatistics>();

    AlgorithmMonitor(ITriggerAlgorithm algorithm,
                     AbstractPayloadFileListBridge[] bridges,
                     PayloadSubscriber subscriber, Consumer consumer,
                     int maxFailures)
    {
        super(bridges, consumer, maxFailures);

        this.algorithm = algorithm;
        this.bridges = bridges;
        this.subscriber = subscriber;

        bridgeCounts = new int[bridges.length];
    }

    public boolean checkMonitoredObject()
    {
        boolean changed = false;
        if (!changed) {
            for (int i = 0; i < bridges.length; i++) {
                if (bridges[i].getNumberWritten() > bridgeCounts[i]) {
                    bridgeCounts[i] = bridges[i].getNumberWritten();
                    changed = true;
                    break;
                }
            }
        }

        if (!changed) {
            if (prevQueued != subscriber.size()) {
                prevQueued = subscriber.size();
                changed = true;
            }
        }

        if (!changed) {
            if (prevWritten != algorithm.getTriggerCounter()) {
                prevWritten = algorithm.getTriggerCounter();
                changed = true;
            }
        }

        return changed;
    }

    public void dumpMonitoring(PrintStream out, int rep)
    {
        throw new Error("Unimplemented");
    }

    public void forceStop()
    {
        throw new Error("Unimplemented");
    }

    public Iterable<AlgorithmStatistics> getAlgorithmStatistics()
    {
        statsList.clear();
        statsList.add(new AlgorithmStatistics(algorithm));
        return statsList;
    }

    public String getName()
    {
        return algorithm.getTriggerName();
    }

    public Splicer getSplicer()
    {
        throw new Error("Unimplemented");
    }

    public String getMonitoredName()
    {
        return getName();
    }

    public boolean isInputPaused()
    {
        return false;
    }

    public boolean isInputStopped()
    {
        for (AbstractPayloadFileListBridge bridge : bridges) {
            if (bridge.isRunning()) {
                return false;
            }
        }

        return true;
    }

    public boolean isOutputStopped()
    {
        if (algorithm.getInputQueueSize() > 0) {
            return false;
        }

        if (algorithm.getNumberOfCachedRequests() > 0) {
            return false;
        }

        return true;
    }

    public void pauseInput()
    {
        System.err.println("Not pausing INPUT");
    }

    public void resumeInput()
    {
        System.err.println("Not resuming INPUT");
    }
}

class SplicerSubscriber
    implements PayloadSubscriber, SplicedAnalysis<IPayload>,
               SplicerListener<IPayload>
{
    private static final Logger LOG =
        Logger.getLogger(SplicerSubscriber.class);

    private String name;
    private ArrayList<IPayload> list = new ArrayList<IPayload>();
    private boolean stopping;
    private boolean stopped;

    SplicerSubscriber(String name)
    {
        this.name = name;
    }

    public void analyze(List<IPayload> splicedObjects)
    {
        synchronized (list) {
            list.addAll(splicedObjects);
            list.notify();
        }
    }

    public void disposed(SplicerChangedEvent<IPayload> event)
    {
        LOG.error("Got SplicerDisposed event: " + event);
    }

    public void failed(SplicerChangedEvent<IPayload> event)
    {
        LOG.error("Got SplicerFailed event: " + event);
    }

    /**
     * Get subscriber name
     *
     * @return name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Is there data available?
     *
     * @return <tt>true</tt> if there are more payloads available
     */
    public boolean hasData()
    {
        return list.size() > 0;
    }

    /**
     * Has this list been stopped?
     *
     * @return <tt>true</tt> if the list has been stopped
     */
    public boolean isStopped()
    {
        return stopped;
    }

    /**
     * Return the next available payload.  Note that this may block if there
     * are no payloads queued.
     *
     * @return next available payload.
     */
    public IPayload pop()
    {
        synchronized (list) {
            while (!stopping && list.size() == 0) {
                try {
                    list.wait();
                } catch (InterruptedException ie) {
                    return null;
                }
            }

            if (stopping && list.size() == 0) {
                stopped = true;
                return null;
            }

            return list.remove(0);
        }
    }

    /**
     * Add a payload to the queue.
     *
     * @param pay payload
     */
    public void push(IPayload pay)
    {
        throw new Error("New payloads should only be added by the splicer!");
    }

    /**
     * Get the number of queued payloads
     *
     * @return size of internal queue
     */
    public int size()
    {
        return list.size();
    }

    public void started(SplicerChangedEvent<IPayload> event)
    {
        // ignored
    }

    public void starting(SplicerChangedEvent<IPayload> event)
    {
        // ignored
    }

    /**
     * No more payloads will be collected
     */
    public void stop()
    {
        synchronized (list) {
            if (!stopping && !stopped) {
                stopping = true;
                list.notify();
            }
        }
    }

    public void stopped(SplicerChangedEvent<IPayload> event)
    {
        LOG.error("SplicerStopped: subscriber list has " + list.size() +
                  " entries");
    }

    public void stopping(SplicerChangedEvent<IPayload> event)
    {
        stopping = true;
    }

    public String toString()
    {
        final String sgstr = stopping ? ",stopping" : "";
        final String sdstr = stopped ? ",stopped" : "";
        return String.format("%s*%d%s%s", name, list.size(), sgstr, sdstr);
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
    private Level logLevel = DEFAULT_LOGLEVEL;
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

    private DOMRegistry registry;

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

        final String name = HashedFileName.getName(runCfgName,
                                                   algorithm.getSourceId(),
                                                   runNumber, numSrcs,
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

    /**
     * Process command-line arguments.
     *
     * @param args command-line arguments
     */
    private void processArgs(String[] args)
    {
        int configId = Integer.MIN_VALUE;
        String runCfgName = null;

        boolean usage = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].length() > 1 && args[i].charAt(0) == '-') {
                switch(args[i].charAt(1)) {
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
                String tmpPath =
                    String.format("prj/simplehits/run%05d", runNumber);
                srcDir = new File(System.getenv("HOME"), tmpPath);
                if (!srcDir.isDirectory()) {
                    System.err.println("Cannot find default source " +
                                       " directory " + srcDir);
                    System.err.println("Please specify source directory (-s)");
                    usage = true;
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

        if (configId == Integer.MIN_VALUE) {
            System.err.println("Please specify trigger config ID (-T)");
            usage = true;
        } else if (!usage) {
            try {
                algorithm = runCfg.getTriggerAlgorithm(configId);
            } catch (ConfigException ce) {
                ce.printStackTrace();
                usage = true;
            }

            if (compareOld) {
                // look for an "OldXXX" version of class "XXX"
                try {
                    oldAlgorithm = runCfg.getTriggerAlgorithm(configId, true);
                } catch (ConfigException ce) {
                    ce.printStackTrace();
                    usage = true;
                }
            }
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
                System.err.printf("Run configuration %s does not contain" +
                                  " any entries for component #%d\n",
                                  runCfg.getName(), algorithm.getSourceId());
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
                " [-D configDir]" +
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
    }

    /**
     * Perform a DAQ run.
     *
     * @return <tt>true</tt> if the run was successful
     */
    public boolean run()
        throws IOException
    {
        // set log level
        Logger.getRootLogger().setLevel(logLevel);
        APPENDER.setLevel(logLevel);

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

        if (verbose) {
            System.out.println("Stopping...");
        }
        splicer.stop();

        if (verbose) {
            System.out.println("Waiting for final flush...");
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

        final double endTime = ((double) System.nanoTime()) / 1000000000.0;

        if (LOG.isInfoEnabled()) {
            LOG.info("Checking");
        }

        boolean rtnval = consumer.report(endTime - startTime);
        if (deathmatch != null) {
            System.out.println(deathmatch.getStats());
        }

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
