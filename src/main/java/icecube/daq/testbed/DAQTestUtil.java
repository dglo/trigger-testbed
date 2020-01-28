package icecube.daq.testbed;

import icecube.daq.io.DAQComponentIOProcess;
import icecube.daq.io.DAQStreamReader;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.splicer.Splicer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Debugging data for an output channel.
 */
class ChannelData
{
    private static final Log LOG = LogFactory.getLog(ChannelData.class);

    private String name;
    private java.nio.channels.Channel chan;
    private Error stack;

    ChannelData(String name, java.nio.channels.Channel chan)
    {
        this.name = name;
        this.chan = chan;

        try {
            throw new Error("StackTrace");
        } catch (Error err) {
            stack = err;
        }
    }

    void logOpen()
    {
        if (chan.isOpen()) {
            LOG.error(toString() + " has not been closed");
            try {
                chan.close();
            } catch (IOException ioe) {
                // ignore errors
            }
        }
    }

    /**
     * Return a debugging string.
     *
     * @return debugging string
     */
    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder("Channel[");
        buf.append(name).append('/').append(chan.toString());

        if (stack != null) {
            buf.append("/ opened at ");
            buf.append(stack.getStackTrace()[1].toString());
        }

        return buf.append(']').toString();
    }
}

/**
 * Test utility methods.
 */
public abstract class DAQTestUtil
{
    private static final Log LOG = LogFactory.getLog(DAQTestUtil.class);

    /** Container for channel debugging data */
    private static ArrayList<ChannelData> chanData =
        new ArrayList<ChannelData>();

    private static final int REPS = 1000;
    private static final int SLEEP_TIME = 10;

    private static ByteBuffer stopMsg;

    /**
     * Generate a minimal run configuration file pointing at a
     * trigger configuration file
     *
     * @param rsrcDirName directory which contains the <tt>config</tt>
     *                    directory
     * @param trigConfigName trigger configuration file name
     *
     * @return newly created run configuration file
     *
     * @throws IOException if there is a problem
     */
    public static File buildConfigFile(String rsrcDirName,
                                       String trigConfigName)
        throws IOException
    {
        File configDir = new File(rsrcDirName, "config");
        if (!configDir.isDirectory()) {
            throw new Error("Config directory \"" + configDir +
                            "\" does not exist");
        }

        File trigCfgDir = new File(configDir, "trigger");
        if (!trigCfgDir.isDirectory()) {
            throw new Error("Trigger config directory \"" + trigCfgDir +
                            "\" does not exist");
        }

        String baseName;
        if (trigConfigName.endsWith(".xml")) {
            baseName =
                trigConfigName.substring(0, trigConfigName.length() - 4);
        } else {
            baseName = trigConfigName;
        }

        File tempFile = File.createTempFile("tmpconfig-", ".xml", configDir);
        tempFile.deleteOnExit();

        FileWriter out = new FileWriter(tempFile);
        try {
            out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            out.write("<runConfig>\n");
            out.write("<triggerConfig>" + baseName + "</triggerConfig>\n");
            out.write("</runConfig>\n");
        } finally {
            out.close();
        }

        return tempFile;
    }

    /**
     * Clear all the channels.
     */
    public static void clearCachedChannels()
    {
        chanData.clear();
    }

    /**
     * Close all the pipes.
     *
     * @param list list of pipes to close
     */
    public static void closePipeList(Pipe[] list)
    {
        for (int i = 0; i < list.length; i++) {
            try {
                list[i].sink().close();
            } catch (IOException ioe) {
                // ignore errors on close
            }
            try {
                list[i].source().close();
            } catch (IOException ioe) {
                // ignore errors on close
            }
        }
    }

    /**
     * Connect one or more pipes to the payload reader and optionally start
     * the reader.
     *
     * @param rdr payload reader
     * @param cache input buffer cache
     * @param numTails number of pipes to connect
     *
     * @return list of pipes to be used to send payloads to the reader
     *
     * @throws IOException if there is a problem
     */
    public static Pipe[] connectToReader(DAQStreamReader rdr,
                                         IByteBufferCache cache,
                                         int numTails)
        throws IOException
    {
        Pipe[] chanList = new Pipe[numTails];

        for (int i = 0; i < chanList.length; i++) {
            chanList[i] = connectToReader(rdr, cache);
        }

        return chanList;
    }

    /**
     * Connect a pipe to the payload reader and optionally start the reader.
     *
     * @param rdr payload reader
     * @param cache input buffer cache
     *
     * @return pipe to be used to send payloads to the reader
     *
     * @throws IOException if there is a problem
     */
    public static Pipe connectToReader(DAQStreamReader rdr,
                                       IByteBufferCache cache)
        throws IOException
    {
        Pipe testPipe = Pipe.open();

        WritableByteChannel sinkChannel = testPipe.sink();
        chanData.add(new ChannelData("rdrSink", sinkChannel));
        testPipe.sink().configureBlocking(true);

        Pipe.SourceChannel sourceChannel = testPipe.source();
        chanData.add(new ChannelData("rdrSrc", sourceChannel));
        sourceChannel.configureBlocking(false);

        rdr.addDataChannel(sourceChannel, "rdrSink", cache, 1024);

        return testPipe;
    }

    /**
     * Try to initialize the payload reader.
     *
     * @param rdr payload reader
     * @param splicer splicer
     * @param rdrName payload reader name
     */
    public static void initReader(DAQStreamReader rdr, Splicer splicer,
                                   String rdrName)
    {
        rdr.start();
        waitUntilStopped(rdr, splicer, "creation");
        if (!rdr.isStopped()) {
            throw new Error(rdrName + " in " + rdr.getPresentState() +
                            ", not Idle after creation");
        }
    }

    /**
     * For any open channels, log an ERROR message and attempt to close them.
     */
    public static void logOpenChannels()
    {
        for (ChannelData cd : chanData) {
            cd.logOpen();
        }
    }

    /**
     * Write a DAQ stop message the the channel.
     *
     * @param chan output channel
     * @param quietStop if <tt>true</tt>, don't log error if stop cannot be
     *                  sent
     *
     * @throws IOException if there is a problem
     */
    public static void sendStopMsg(WritableByteChannel chan, boolean quietStop)
        throws IOException
    {
        if (stopMsg == null) {
            stopMsg = ByteBuffer.allocate(4);
            stopMsg.putInt(0, 4);
            stopMsg.limit(4);
        }

        synchronized (stopMsg) {
            if (chan.isOpen()) {
                stopMsg.position(0);
                chan.write(stopMsg);
            } else if (LOG.isErrorEnabled() && !quietStop) {
                LOG.error("Channel closed; cannot send stop message to " +
                         chan);
            }
        }
    }

    /**
     * Send stop messages to all input channels.
     *
     * @param tails list of input channels
     * @param quietStop if <tt>true</tt>, don't log error if stop cannot be
     *                  sent
     *
     * @throws IOException if there is a problem
     */
    public static void sendStops(Pipe[] tails, boolean quietStop)
        throws IOException
    {
        for (int i = 0; i < tails.length; i++) {
            sendStopMsg(tails[i].sink(), quietStop);
        }
    }

    private static void startIOProcess(DAQComponentIOProcess proc)
    {
        if (!proc.isRunning()) {
            proc.startProcessing();
            waitUntilRunning(proc);
        }
    }

    /**
     * Pretend to be a unit test.
     *
     * @param msg error message
     * @param val value to check
     */
    private static void assertTrue(String msg, boolean val)
    {
        if (!val) {
            throw new AssertionError(msg);
        }
    }

    /**
     * Wait until the input/output engine is running.
     *
     * @param proc input/output engine
     */
    public static void waitUntilRunning(DAQComponentIOProcess proc)
    {
        for (int i = 0; i < REPS && !proc.isRunning(); i++) {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        assertTrue("IOProcess in " + proc.getPresentState() +
                   ", not Running after StartSig", proc.isRunning());
    }

    /**
     * Wait until the input/output engine has stopped.
     *
     * @param proc input/output engine
     * @param splicer splicer
     * @param action current action (running, stopping, etc.)
     */
    public static void waitUntilStopped(DAQComponentIOProcess proc,
                                        Splicer splicer,
                                        String action)
    {
        waitUntilStopped(proc, splicer, action, "");
    }

    /**
     * Wait until the input/output engine has stopped.
     *
     * @param proc input/output engine
     * @param splicer splicer (if <tt>proc</tt> is an input engine)
     * @param action current action (running, stopping, etc.)
     * @param extra extra description string
     */
    private static void waitUntilStopped(DAQComponentIOProcess proc,
                                         Splicer splicer,
                                         String action,
                                         String extra)
    {
        waitUntilStopped(proc, splicer, action, extra, REPS, SLEEP_TIME);
    }

    /**
     * Wait until the input/output engine has stopped.
     *
     * @param proc input/output engine
     * @param splicer splicer (if <tt>proc</tt> is an input engine)
     * @param action current action (running, stopping, etc.)
     * @param extra extra description string
     * @param maxReps maximum number of checks before returning
     * @param sleepTime milliseconds to sleep between checks
     */
    public static void waitUntilStopped(DAQComponentIOProcess proc,
                                        Splicer splicer,
                                        String action,
                                        String extra,
                                        int maxReps, int sleepTime)
    {
        int numReps = 0;
        while (numReps < maxReps &&
               ((proc != null && !proc.isStopped()) ||
                (splicer != null && splicer.getState() !=
                 Splicer.State.STOPPED)))
        {
            numReps++;

            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }

        if (proc != null) {
            assertTrue("IOProcess in " + proc.getPresentState() +
                       ", not Idle after " + action + extra, proc.isStopped());
        }
        if (splicer != null) {
            assertTrue("Splicer in " + splicer.getState().name() +
                       ", not STOPPED after " + numReps + " reps of " +
                       action + extra,
                       splicer.getState() == Splicer.State.STOPPED);
        }
    }
}
