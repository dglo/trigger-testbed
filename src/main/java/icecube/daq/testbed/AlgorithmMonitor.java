package icecube.daq.testbed;

import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.algorithm.AlgorithmStatistics;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.control.PayloadSubscriber;

import java.io.PrintStream;
import java.util.ArrayList;

class AlgorithmMonitor
    extends ActivityMonitor
{
    private ITriggerAlgorithm algorithm;
    private AbstractPayloadFileListBridge[] bridges;
    private PayloadSubscriber subscriber;

    private long[] bridgeCounts;
    private long prevQueued;
    private long prevWritten;
    private long prevCached;
    private long prevSent;

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

        bridgeCounts = new long[bridges.length];
    }

    public boolean checkMonitoredObject()
    {
        boolean changed = false;
        long total = 0;
        for (int i = 0; i < bridges.length; i++) {
            final long value = bridges[i].getNumberWritten();
            total += value;

            if (value > bridgeCounts[i]) {
                bridgeCounts[i] = value;
                changed = true;
            }
        }
        setNumberReceived(total);

        if (prevQueued != subscriber.size()) {
            prevQueued = subscriber.size();
            setNumberOfQueuedInputs(prevQueued);
            changed = true;
        }

        if (prevCached != algorithm.getNumberOfCachedRequests()) {
            prevCached = algorithm.getNumberOfCachedRequests();
            setNumberOfQueuedOutputs(prevCached);
            changed = true;
        }

        if (prevWritten != algorithm.getTriggerCounter()) {
            prevWritten = algorithm.getTriggerCounter();
            changed = true;
        }

        if (prevSent != consumer.getNumberWritten()) {
            prevSent = consumer.getNumberWritten();
            setNumberSent(prevSent);
            changed = true;
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

    public String getMonitoredName()
    {
        return getName();
    }

    public String getName()
    {
        return algorithm.getTriggerName();
    }

    public Splicer getSplicer()
    {
        throw new Error("Unimplemented");
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
