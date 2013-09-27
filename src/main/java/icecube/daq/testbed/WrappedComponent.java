package icecube.daq.testbed;

import icecube.daq.io.DAQComponentIOProcess;
import icecube.daq.io.DAQComponentOutputProcess;
import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.trigger.common.DAQTriggerComponent;
import icecube.daq.trigger.common.ITriggerAlgorithm;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A complex object which can be used to filter and sort hit file names.
 */
class SimpleHitFilter
    implements Comparator, FilenameFilter
{
    private String baseName;

    /**
     * Create a hit file filter.
     *
     * @param hubName hub name
     * @param runNum run number
     * @param hubNum hub number
     */
    SimpleHitFilter(String hubName, int runNum, int hubNum)
    {
        baseName = String.format("%s%02d_simplehits_%d_", hubName, hubNum,
                                 runNum);
    }

    /**
     * Does this file name start with the expected pattern?
     *
     * @return <tt>true</tt> if the file name starts with the expected pattern
     */
    public boolean accept(File dir, String name)
    {
        return name.startsWith(baseName);
    }

    /**
     * Compare two objects.
     *
     * @param o1 first object
     * @param o2 second object
     *
     * @return the usual comparison values
     */
    public int compare(Object o1, Object o2)
    {
        if (!(o1 instanceof File) || !(o2 instanceof File)) {
            return o1.getClass().getName().compareTo(o2.getClass().getName());
        }

        String s1 = ((File) o1).getName();
        String s2 = ((File) o2).getName();

        if (!s1.startsWith(baseName)) {
            if (!s2.startsWith(baseName)) {
                return s1.compareTo(s2);
            }

            return 1;
        } else if (!s2.startsWith(baseName)) {
            return -1;
        }

        int end1 = s1.indexOf("_", baseName.length() + 1);
        String sub1 = s1.substring(baseName.length(), end1);
        int num1;
        try {
            num1 = Integer.parseInt(sub1);
        } catch (NumberFormatException nfe) {
            throw new Error(String.format("Cannot parse \"%s\" from \"%s\"",
                                          sub1, s1));
        }

        int end2 = s2.indexOf("_", baseName.length() + 1);
        String sub2 = s2.substring(baseName.length(), end2);
        int num2;
        try {
            num2 = Integer.parseInt(sub2);
        } catch (NumberFormatException nfe) {
            throw new Error(String.format("Cannot parse \"%s\" from \"%s\"",
                                          sub2, s2));
        }

        return num1 - num2;
    }

    /**
     * Do the objects implement the same class?
     *
     * @return <tt>true</tt> if they are the same class
     */
    public boolean equals(Object obj)
    {
        return obj.getClass().getName().equals(getClass().getName());
    }

    /**
     * Sort the list of files.
     *
     * @param files list of files
     */
    public void sort(File[] files)
    {
        Arrays.sort(files, this);
    }
}

/**
 * Base class for DAQ component wrapper.
 */
public abstract class WrappedComponent
{
    private static final Log LOG = LogFactory.getLog(WrappedComponent.class);

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
     * @param out output engine
     * @param outCache output buffer cache
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
    public Consumer connectToConsumer(DAQComponentOutputProcess out,
                                      IByteBufferCache outCache,
                                      List<ITriggerAlgorithm> algorithms,
                                      File targetDir, String runCfgName,
                                      int runNumber, int numSrcs,
                                      int numToSkip, int numToProcess)
        throws IOException
    {
        Pipe outPipe = Pipe.open();

        Pipe.SinkChannel sinkOut = outPipe.sink();
        sinkOut.configureBlocking(false);

        Pipe.SourceChannel srcOut = outPipe.source();
        srcOut.configureBlocking(true);

        out.addDataChannel(sinkOut, outCache);

        Consumer consumer;

        final String name = HashedFileName.getName(runCfgName, getSourceID(),
                                                   runNumber, numSrcs,
                                                   numToSkip, numToProcess);
        File outFile = new File(targetDir, name);
        if (outFile.exists()) {
            consumer = new CompareConsumer(outFile, srcOut);
            System.err.println("*** Comparing output with " + outFile);
        } else {
            consumer = new OutputConsumer(outFile, srcOut);
            System.err.println("*** Writing output to " + outFile);
        }
        consumer.configure(algorithms);
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
            throw new IOException(ce.getMessage());
        }

        String hubName;
        int hubBase;

        switch (srcId) {
        case SourceIdRegistry.INICE_TRIGGER_SOURCE_ID:
            hubName = "hub";
            hubBase = 0;
            break;
        case SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID:
            hubName = "ithub";
            hubBase = 200;
            break;
        case SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID:
            throw new Error("Cannot write hits into global trigger");
        default:
            throw new Error("Cannot write hits into " +
                            SourceIdRegistry.getDAQNameFromSourceID(srcId));
        }

        if (hubs == null || hubs.size() < numSrcs) {
            throw new IOException("Asked for " + numSrcs + " " + hubName +
                                  (numSrcs == 1 ? "" : "s") + ", but only " +
                                  (hubs == null ? 0 : hubs.size()) +
                                  " available in " + cfg.getName());
        }

        PayloadFileListBridge[] bridges = new PayloadFileListBridge[numSrcs];

        for (int h = 0; h < numSrcs; h++) {
            SimpleHitFilter filter =
                new SimpleHitFilter(hubName, runNum, hubs.get(h) - hubBase);
            File[] files = srcDir.listFiles(filter);
            if (files.length == 0) {
                String msg =
                    String.format("Cannot find hit files for run %d %s %d",
                                  runNum, hubName, hubs.get(h));
                throw new IOException(msg);
            }

            // make sure fies are in the correct order
            filter.sort(files);

            PayloadFileListBridge bridge =
                new PayloadFileListBridge(files, tails[h].sink());
            bridge.setNumberToSkip(numToSkip);
            bridge.setMaximumPayloads(numToProcess);
            bridge.setWriteDelay(1, 10);
            bridge.start();
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
                new PayloadFileListBridge(files, tails[i].sink());
            bridge.setWriteDelay(1, 10);
            bridge.start();
            bridges[i] = bridge;
        }

        return bridges;
    }

    public void destroy()
    {
        comp.getTriggerManager().stopThread();

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

        Consumer consumer = connectToConsumer(comp.getWriter(),
                                              comp.getOutputCache(),
                                              comp.getAlgorithms(), targetDir,
                                              runCfg.getName(), runNum,
                                              numSrcs, numToSkip,
                                              numToProcess);

        if (comp.getWriter().getChannel() == null) {
            throw new Error("Output engine has no channels");
        }

        comp.setRunNumber(runNum);

        final double startTime = ((double) System.nanoTime()) / 1000000000.0;

        comp.starting();

        startComponentIO(comp.getReader(), comp.getWriter());

        comp.started();

        String prefix;
        if (getSourceID() == SourceIdRegistry.INICE_TRIGGER_SOURCE_ID) {
            prefix = "II";
        } else if (getSourceID() ==
                   SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID)
        {
            prefix = "IT";
        } else if (getSourceID() ==
                   SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID)
        {
            prefix = "GL";
        } else {
            prefix = "XX#" + getSourceID();
        }

        ActivityMonitor activity = new ActivityMonitor(comp, prefix, bridges,
                                                       consumer, maxFailures);

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

        if (LOG.isInfoEnabled()) {
            LOG.info("Stopping");
        }

        comp.flushTriggers();

        comp.stopping();

        DAQTestUtil.sendStops(tails, true);

        activity.waitForStasis(20, numToProcess, 3, verbose, dumpSplicer,
                               null);

        comp.stopped();

        final double endTime = ((double) System.nanoTime()) / 1000000000.0;

        if (LOG.isInfoEnabled()) {
            LOG.info("Checking");
        }

        boolean rtnval = consumer.report(endTime - startTime);

        final boolean noOutput = consumer.getNumberWritten() == 0 &&
            consumer.getNumberFailed() == 0;
        checkTriggerCaches(comp, noOutput, verbose);

        return rtnval;
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

    public String toString()
    {
        return comp.toString();
    }
}
