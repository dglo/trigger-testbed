package icecube.daq.testbed;

import icecube.daq.io.DAQComponentIOProcess;
import icecube.daq.io.DAQComponentOutputProcess;
import icecube.daq.juggler.alert.AlertQueue;
import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.component.DAQTriggerComponent;
import icecube.daq.trigger.control.ITriggerManager;
import icecube.daq.trigger.exceptions.ConfigException;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Base class for DAQ component wrapper.
 */
public abstract class WrappedComponent
{
    private static final Logger LOG = Logger.getLogger(WrappedComponent.class);

    private DAQTriggerComponent comp;
    private String prefix;

    private Pipe[] tails;

    WrappedComponent(DAQTriggerComponent comp, String prefix)
    {
        this.comp = comp;
        this.prefix = prefix;
    }

    /**
     * Verify that there are no buffer leaks.
     *
     * @param comp component
     * @param noOutput <tt>true</tt> if there was no output
     * @param debug <tt>true</tt> if debugging output should be printed
     *
     * @throws DAQCompException if there is a problem
     */
    public static void checkTriggerCaches(DAQTriggerComponent comp,
                                          boolean noOutput,
                                          boolean debug)
        throws DAQCompException
    {
        IByteBufferCache inCache = comp.getInputCache();
        if (debug) {
            System.err.println(comp.getName() + " INcache " + inCache);
        }
        if (!inCache.isBalanced()) {
            throw new Error(comp.getName() + " input buffer cache" +
                            " is unbalanced (" + inCache + ")");
        }
        if (!noOutput && inCache.getTotalBuffersAcquired() <= 0) {
            throw new Error(comp.getName() + " input buffer cache" +
                            " was unused (" + inCache + ")");
        }

        IByteBufferCache outCache = comp.getOutputCache();
        if (debug) {
            System.err.println(comp.getName() + " OUTcache " + outCache);
        }
        if (!outCache.isBalanced()) {
            throw new Error(comp.getName() + " output buffer cache" +
                            " is unbalanced (" + outCache + ")");
        }
        if (comp.getPayloadsSent() > 0 &&
            outCache.getTotalBuffersAcquired() <= 0)
        {
            throw new Error(comp.getName() + " output buffer cache" +
                            " was unused (" + outCache + ", " +
                            comp.getPayloadsSent() + " sent)");
        }
        if (comp.getPayloadsSent() == outCache.getTotalBuffersAcquired()) {
            // cache matched the number of payloads sent
        } else if (comp.getPayloadsSent() ==
                   outCache.getTotalBuffersAcquired() + 1)
        {
            // output cache almost certainly includes a stop message
        } else {
            throw new Error(comp.getName() + " mismatch between triggers" +
                            " allocated (" +
                            outCache.getTotalBuffersAcquired() +
                            ") and sent (" + comp.getPayloadsSent() + ")");
        }
    }

    /**
     * Connect to the payload consumer.
     *
     * @param targetDir output file directory (may not be needed)
     * @param runCfgName run configuration file name
     * @param runNumber run number
     * @param numSrcs number of sources feeding in data
     * @param numToSkip initial number of payloads to skip past
     * @param numToProcess number of input payloads
     *
     * @throws IOException if there was a problem
     *
     * @return newly created payload consumer
     */
    private Consumer connectToConsumer(File targetDir, String runCfgName,
                                       int runNumber, int numSrcs,
                                       int numToSkip, int numToProcess)
        throws IOException
    {
        Pipe outPipe = Pipe.open();

        Pipe.SinkChannel sinkOut = outPipe.sink();
        sinkOut.configureBlocking(false);

        Pipe.SourceChannel srcOut = outPipe.source();
        srcOut.configureBlocking(true);

        DAQComponentOutputProcess out = comp.getWriter();
        out.addDataChannel(sinkOut, comp.getOutputCache(), getName());

        final int trigId;

        List<ITriggerAlgorithm> algorithms = comp.getAlgorithms();
        if (algorithms == null || algorithms.size() == 0) {
            throw new IOException("List of algorithms cannot be null or" +
                                  " empty");
        } else if (algorithms.size() == 1) {
            trigId = algorithms.get(0).getTriggerConfigId();
        } else {
            trigId = -1;
        }

        ConsumerHandler handler;

        final String name = HashedFileName.getName(runCfgName, getSourceID(),
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
        handler.configure(algorithms);

        ChannelConsumer consumer =
            new ChannelConsumer(outFile.getName(), srcOut, handler);
        consumer.start();

        return consumer;
    }

    /**
     * Connect the simple hit files to the input engine.
     *
     * @param tails input channels
     * @param srcDir source directory
     * @param runNum run number
     * @param cfg run configuration
     * @param numSrcs number of sources to connect
     * @param numToSkip initial number of payloads to skip past
     * @param numToProcess number of input payloads
     *
     * @return list of connected payload file bridges
     */
    PayloadFileListBridge[] connectToSimpleHitFiles(Pipe[] tails, File srcDir,
                                                    int runNum,
                                                    Configuration cfg,
                                                    int numSrcs, int numToSkip,
                                                    int numToProcess)
        throws IOException
    {
        final int srcId = getSourceID();

        List<Integer> hubs;
        try {
            hubs = cfg.getHubs(srcId);
        } catch (ConfigException ce) {
            throw new IOException("Cannot get hubs for " + srcId, ce);
        }

        if (hubs == null || hubs.size() < numSrcs) {
            throw new IOException("Asked for " + numSrcs + " hub" +
                                  (numSrcs == 1 ? "" : "s") + ", but only " +
                                  (hubs == null ? 0 : hubs.size()) +
                                  " available in " + cfg.getName());
        }

        PayloadFileListBridge[] bridges = new PayloadFileListBridge[numSrcs];

        for (int h = 0; h < numSrcs; h++) {
            final int hubId = hubs.get(h);
            File[] files = SimpleHitFilter.listFiles(srcDir, hubId, runNum);

            PayloadFileListBridge bridge =
                new PayloadFileListBridge(SimpleHitFilter.getHubName(hubId),
                                          files, tails[h].sink());
            bridge.setNumberToSkip(numToSkip);
            bridge.setMaximumPayloads(numToProcess);
            bridge.setWriteDelay(1, 10);
            bridges[h] = bridge;
        }

        return bridges;
    }

    /**
     * Connect the trigger files to the input engine.
     *
     * @param tails input channels
     * @param srcDir source directory
     * @param runNum run number
     * @param cfg run configuration
     * @param numSrcs number of sources to connect
     * @param numToSkip initial number of payloads to skip past
     * @param numToProcess number of input payloads
     *
     * @return list of connected payload file bridges
     */
    PayloadFileListBridge[] connectToTriggerFiles(Pipe[] tails, File srcDir,
                                                  int runNum,
                                                  Configuration cfg,
                                                  int numSrcs, int numToSkip,
                                                  int numToProcess)
        throws IOException
    {
        PayloadFileListBridge[] bridges = new PayloadFileListBridge[numSrcs];

        for (int i = 0; i < tails.length; i++) {
            int subSrcId;
            if (i == 0) {
                subSrcId = SourceIdRegistry.INICE_TRIGGER_SOURCE_ID;
            } else {
                subSrcId = SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID;
            }

            final int subSrcs;
            try {
                subSrcs = cfg.getNumberOfSources(subSrcId);
            } catch (ConfigException ce) {
                throw new IOException("Cannot find number of sources for " +
                                      subSrcId, ce);
            }

            final String name =
                HashedFileName.getName(cfg.getName(), subSrcId, runNum,
                                       subSrcs, numToSkip, numToProcess);
            File[] files = new File[] { new File(srcDir, name), };

            PayloadFileListBridge bridge =
                new PayloadFileListBridge("trigOut", files, tails[i].sink());
            bridge.setWriteDelay(1, 10);
            bridges[i] = bridge;
        }

        return bridges;
    }

    public void destroy()
    {
        ITriggerManager mgr = comp.getTriggerManager();
        mgr.stopThread();
        for (int i = 0; i < 100; i++) {
            if (mgr.isStopped()) {
                break;
            }
            Thread.yield();
        }

        AlertQueue aq = mgr.getAlertQueue();
        if (aq != null) {
            aq.stop();
            for (int i = 0; i < 100; i++) {
                if (aq.isStopped()) {
                    break;
                }
                Thread.yield();
            }
        }

        if (tails != null) {
            DAQTestUtil.closePipeList(tails);
        }

        comp.getReader().destroyProcessor();
        comp.getWriter().destroyProcessor();

        try {
            comp.destroy();
        } catch (DAQCompException dce) {
            System.err.println("Failed to destroy " + comp);
            dce.printStackTrace();
        }
    }

    /**
     * Get the component name.
     *
     * @return component name
     */
    public String getName()
    {
        return prefix + " " + comp.getName();
    }

    /**
     * Get component source ID
     *
     * @return source ID
     */
    public int getSourceID()
    {
        return comp.getTriggerManager().getSourceId();
    }

    /**
     * Run this component.
     *
     * @param runCfg run configuration
     * @param numSrcs number of sources in the run
     * @param runNum run number
     * @param srcDir input data directory
     * @param numToProcess number of input payloads
     * @param numToSkip initial number of payloads to skip past
     * @param targetDir output data directory
     * @param maxFailures maximum number of comparison failures
     * @param monitoringOutput if non-null, name of monitoring output file
     * @param verbose <tt>true</tt> if regular status reports should be printed
     * @param dumpSplicer <tt>true</tt> if regular splicer status reports
     *                    should be printed
     *
     * @throws Exception if there is a problem
     */
    public boolean run(Configuration runCfg, int numSrcs, int runNum,
                       File srcDir, int numToProcess, int numToSkip,
                       int maxFailures, File targetDir, File monitoringOutput,
                       boolean verbose, boolean dumpSplicer)
        throws Exception
    {
        comp.setGlobalConfigurationDir(runCfg.getParent());
        comp.setAlerter(new MockAlerter());

        comp.start(false);
        comp.configuring(runCfg.getName());

        tails = DAQTestUtil.connectToReader(comp.getReader(),
                                            comp.getInputCache(), numSrcs);

        PayloadFileListBridge[] bridges;
        if (getSourceID() == SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID) {
            bridges = connectToTriggerFiles(tails, targetDir, runNum, runCfg,
                                            numSrcs, numToSkip, numToProcess);
        } else {
            bridges =
                connectToSimpleHitFiles(tails, srcDir, runNum, runCfg, numSrcs,
                                        numToSkip, numToProcess);
        }

        Consumer consumer = connectToConsumer(targetDir,
                                              runCfg.getName(), runNum,
                                              numSrcs, numToSkip,
                                              numToProcess);

        if (comp.getWriter().getChannel() == null) {
            throw new Error("Output engine has no channels");
        }

        final double startTime = ((double) System.nanoTime()) / 1000000000.0;

        comp.starting(runNum);

        startComponentIO(comp.getReader(), comp.getWriter());
        startBridges(bridges);

        comp.started(runNum);

        String abbreviation;
        if (getSourceID() == SourceIdRegistry.INICE_TRIGGER_SOURCE_ID) {
            abbreviation = "II";
        } else if (getSourceID() ==
                   SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID)
        {
            abbreviation = "IT";
        } else if (getSourceID() ==
                   SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID)
        {
            abbreviation = "GL";
        } else {
            abbreviation = "XX#" + getSourceID();
        }

        ComponentMonitor activity = new ComponentMonitor(comp, abbreviation,
                                                         bridges, consumer,
                                                         maxFailures);

        if (LOG.isInfoEnabled()) {
            LOG.info("Waiting");
        }

        PrintStream monOut;
        if (monitoringOutput == null) {
            monOut = null;
        } else {
            monOut = new PrintStream(monitoringOutput);
        }

        activity.waitForStasis(20, numToProcess, 2, verbose, dumpSplicer,
                               monOut);

        if (monOut != null) {
            monOut.close();
        }

        if (verbose) {
            System.out.println("Flushing...");
        }
        comp.flushTriggers();

        if (verbose) {
            System.out.println("Stopping...");
        }
        comp.stopping();

        if (verbose) {
            System.out.println("Sending stops...");
        }
        DAQTestUtil.sendStops(tails, true);

        if (verbose) {
            System.out.println("Waiting for final flush...");
        }
        activity.waitForStasis(20, numToProcess, 3, verbose, dumpSplicer,
                               null);

        if (verbose) {
            System.out.println("Stopped...");
        }
        comp.stopped();

        final double endTime = ((double) System.nanoTime()) / 1000000000.0;

        if (LOG.isInfoEnabled()) {
            LOG.info("Checking");
        }

        boolean rtnval = consumer.report(endTime - startTime);

        final boolean noOutput = consumer.getNumberWritten() == 0 &&
            consumer.getNumberFailed() == 0;
        try {
            checkTriggerCaches(comp, noOutput, verbose);
        } catch (Throwable thr) {
            LOG.error("Problem with trigger caches", thr);
            rtnval = false;
        }

        return rtnval;
    }

    /**
     * Start the file bridges.
     *
     * @param bridges list of file bridges
     */
    public static void startBridges(PayloadFileListBridge[] bridges)
    {
        if (bridges != null) {
            for (int i = 0; i < bridges.length; i++) {
                bridges[i].start();
            }
        }
    }

    /**
     * Start the input and output engines.
     *
     * @param rdr input engine
     * @param out output engine
     */
    public static void startComponentIO(DAQComponentIOProcess rdr,
                                        DAQComponentIOProcess out)
    {
        ArrayList<DAQComponentIOProcess> procList =
            new ArrayList<DAQComponentIOProcess>();

        procList.add(rdr);
        procList.add(out);

        for (DAQComponentIOProcess proc : procList) {
            if (!proc.isRunning()) {
                proc.startProcessing();
            }
        }

        for (DAQComponentIOProcess proc : procList) {
            if (!proc.isRunning()) {
                DAQTestUtil.waitUntilRunning(proc);
            }
        }
    }

    @Override
    public String toString()
    {
        return comp.toString();
    }
}
