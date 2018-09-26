package icecube.daq.testbed;

import icecube.daq.common.ANSIEscapeCode;
import icecube.daq.juggler.mbean.MemoryStatistics;
import icecube.daq.splicer.HKN1Splicer;
import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.algorithm.AlgorithmStatistics;

import java.io.PrintStream;

/**
 * Monitor all activity in this component.
 */
public abstract class ActivityMonitor
{
    private static final long MAX_QUEUED = 100000;

    private static final long MAX_TIME_DIFF = 10000000000L;

    private static final int PROGRESS_FREQUENCY = 100;
    private static final int MONITOR_FREQUENCY = 4;

    protected Consumer consumer;

    private AbstractPayloadFileListBridge[] bridges;
    private int maxFailures;

    private long received;
    private long queuedIn;
    private long processed;
    private long queuedOut;
    private long sent;
    private boolean stopped;
    private boolean forcedStop;
    private boolean summarized;

    private MemoryStatistics memoryStats = new MemoryStatistics();

    ActivityMonitor(AbstractPayloadFileListBridge[] bridges, Consumer consumer,
                    int maxFailures)
    {
        this.bridges = bridges;
        this.consumer = consumer;
        this.maxFailures = maxFailures;
    }

    /**
     * Check for activity.
     *
     * @return <tt>false</tt> if everything is stagnant
     */
    private boolean check()
    {
        if (isStopped() != summarized) {
            summarized = isStopped();
        }

        boolean newStopped = isInputStopped() && isOutputStopped();

        boolean changed;
        if (isSummarized()) {
            changed = false;
        } else {
            changed = checkMonitoredObject();
        }

        setStopped(newStopped);

        if (!isStopped()) {
            checkBridges();
        }

        if (consumer.getNumberFailed() > maxFailures && !forcedStop) {
            // pause everything
            for (AbstractPayloadFileListBridge bridge : bridges) {
                bridge.stopThread();
            }

            forceStop();

            consumer.setForcedStop();

            forcedStop = true;
        }

        return changed;
    }

    public abstract boolean checkMonitoredObject();

    void checkBridges()
    {
        long earliestTime = Long.MAX_VALUE;
        long latestTime = 0;
        for (AbstractPayloadFileListBridge bridge : bridges) {
            if (bridge.getLastTime() > 0 &&
                bridge.getLastTime() < Long.MAX_VALUE)
            {
                if (bridge.getLastTime() < earliestTime) {
                    earliestTime = bridge.getLastTime();
                }

                if (bridge.getLastTime() > latestTime) {
                    latestTime = bridge.getLastTime();
                }
            }
        }

        if (earliestTime < Long.MAX_VALUE && latestTime > 0) {
            for (AbstractPayloadFileListBridge bridge : bridges) {
                if (bridge.isPaused() &&
                    bridge.getLastTime() - latestTime < MAX_TIME_DIFF)
                {
                    bridge.unpause();
                }

                if (!bridge.isPaused() &&
                    bridge.getLastTime() - earliestTime > MAX_TIME_DIFF)
                {
                    bridge.pause();
                }
            }
        }
    }

    public abstract void dumpMonitoring(PrintStream out, int rep);

    private void dumpProgress(PrintStream out, int rep, boolean dumpSplicers)
    {
        StringBuilder buf = new StringBuilder();
        buf.append('#').
            append(rep).
            append(':').
            append(ANSIEscapeCode.BG_GREEN).
            append(toString()).
            append(ANSIEscapeCode.OFF);

        for (AlgorithmStatistics stat : getAlgorithmStatistics()) {
            buf.append("\n        ").
                append(ANSIEscapeCode.BG_MAGENTA).
                append(stat.toString()).
                append(ANSIEscapeCode.OFF);
        }

        out.println(buf.toString());

        if (dumpSplicers) {
            dumpSplicer(out, getName(), getSplicer());
        }
    }

    private void dumpSplicer(PrintStream out, String title, Splicer splicer)
    {
        out.println("*********************");
        out.println("*** " + title + " Splicer");
        out.println("*********************");
        String[] desc = ((HKN1Splicer) splicer).dumpDescription();
        for (int d = 0; d < desc.length; d++) {
            out.println("  " + desc[d]);
        }
    }

    /**
     * Force component to stop.
     */
    public abstract void forceStop();

    /**
     * Format the trigger counts into a string.
     *
     * @return trigger counts string
     */
    public abstract Iterable<AlgorithmStatistics> getAlgorithmStatistics();

    public abstract String getMonitoredName();

    public abstract String getName();

    long getNumberOfQueuedInputs()
    {
        return queuedIn;
    }

    long getNumberOfQueuedOutputs()
    {
        return queuedOut;
    }

    long getNumberProcessed()
    {
        return processed;
    }

    long getNumberReceived()
    {
        return received;
    }

    /**
     * Get the number of payloads sent.
     *
     * @return number of payloads sent
     */
    long getNumberSent()
    {
        return sent;
    }

    /**
     * Get the component's splicer.
     *
     * @return splicer
     */
    public abstract Splicer getSplicer();

    public abstract boolean isInputPaused();

    public abstract boolean isInputStopped();

    public abstract boolean isOutputStopped();

    /**
     * Is everything stopped?
     *
     * @return <tt>true</tt> if everything has stopped
     */
    public boolean isStopped()
    {
        return stopped;
    }

    public boolean isSummarized()
    {
        return summarized;
    }

    public abstract void pauseInput();

    public abstract void resumeInput();

    void setNumberOfQueuedInputs(long value)
    {
        queuedIn = value;
    }

    void setNumberOfQueuedOutputs(long value)
    {
        queuedOut = value;
    }

    void setNumberProcessed(long value)
    {
        processed = value;
    }

    void setNumberReceived(long value)
    {
        received = value;
    }

    void setNumberSent(long value)
    {
        sent = value;
    }

    /**
     * Set "stopped" state
     *
     * @param val <tt>true</tt> if the monitored object has stopped
     */
    public void setStopped(boolean val)
    {
        stopped = val;
    }

    boolean waitForStasis(int staticReps, int maxReps, int stoppedReps,
                          boolean verbose, boolean dumpSplicers,
                          PrintStream monOut)
    {
        final int sleepMSec = 100;

        final PrintStream out = System.out;

        int numStatic = 0;
        int numStopped = 0;
        for (int i = 0; i < maxReps; i++) {
            boolean changed = check();
            if (changed) {
                numStatic = 0;
                numStopped = 0;
            } else if (isStopped()) {
                numStopped++;
            } else {
                numStatic++;
            }

            if (changed && (queuedIn > MAX_QUEUED || queuedOut > MAX_QUEUED) &&
                !isInputPaused())
            {
                pauseInput();
                if (verbose) {
                    System.err.println("!! Pausing reader");
                }
            } else if (changed &&
                       (queuedIn <= MAX_QUEUED && queuedOut <= MAX_QUEUED) &&
                       isInputPaused())
            {
                resumeInput();
                if (verbose) {
                    System.err.println("!! Unpausing reader");
                }
            }

            if (verbose && i % PROGRESS_FREQUENCY == 0) {
                dumpProgress(out, i, dumpSplicers);
            }

            if (monOut != null && i % MONITOR_FREQUENCY == 0) {
                dumpMonitoring(monOut, i);
            }

            if (numStatic >= staticReps || numStopped >= stoppedReps) {
                System.out.println(getMonitoredName() + " was static for " +
                                   numStatic + " reps" +
                                   (numStopped == 0 ? "" :
                                    ", stopped for " + numStopped + " reps"));
                break;
            }

            try {
                Thread.sleep(sleepMSec);
            } catch (InterruptedException ie) {
                // ignore errors
            }
        }

        return numStatic >= staticReps;
    }

    /**
     * Return a debugging string.
     *
     * @return debugging string
     */
    @Override
    public String toString()
    {
        String rdStopped = isInputStopped() ? " inStop" : "";
        String wrStopped = isOutputStopped() ? " outStop" : "";

        if (isSummarized()) {
            return " " + getName() + " stopped";
        }

        long[] stats = memoryStats.getMemoryStatistics();

        summarized = stopped;
        return String.format(" %s%s %d->%d->%d->%d->%d%s | %d / %d",
                             getName(), rdStopped, getNumberReceived(),
                             getNumberOfQueuedInputs(), getNumberProcessed(),
                             getNumberOfQueuedOutputs(), getNumberSent(),
                             wrStopped, stats[0], stats[1]);
    }
}
