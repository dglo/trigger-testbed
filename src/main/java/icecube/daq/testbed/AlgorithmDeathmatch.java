package icecube.daq.testbed;

import icecube.daq.io.DAQComponentOutputProcess;
import icecube.daq.juggler.alert.AlertQueue;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.impl.SimpleHit;
import icecube.daq.payload.impl.SimplerHit;
import icecube.daq.payload.impl.TriggerRequestFactory;
import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.algorithm.AlgorithmStatistics;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.control.ITriggerCollector;
import icecube.daq.trigger.control.ITriggerManager;
import icecube.daq.trigger.control.Interval;
import icecube.daq.trigger.control.PayloadSubscriber;
import icecube.daq.trigger.control.SubscribedList;
import icecube.daq.trigger.exceptions.ConfigException;
import icecube.daq.trigger.exceptions.IllegalParameterValueException;
import icecube.daq.trigger.exceptions.TriggerException;
import icecube.daq.trigger.exceptions.UnknownParameterException;
import icecube.daq.util.CodeTimer;
import icecube.daq.util.DOMRegistryFactory;
import icecube.daq.util.IDOMRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

class ManagerWrapper
    implements ITriggerManager
{
    private ITriggerManager mgr;

    ManagerWrapper(ITriggerManager mgr)
    {
        this.mgr = mgr;
    }

    @Override
    public void addTrigger(ITriggerAlgorithm x0)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void addTriggers(Iterable<ITriggerAlgorithm> x0)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void flush()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public AlertQueue getAlertQueue()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public Iterable<AlgorithmStatistics> getAlgorithmStatistics()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public IDOMRegistry getDOMRegistry()
    {
        try {
            return DOMRegistryFactory.load();
        } catch (Exception ex) {
            throw new Error("Cannot load DOM registry", ex);
        }
    }

    @Override
    public int getNumOutputsQueued()
    {
        throw new Error("Unimplemented");
    }

    public int getNumRequestsQueued()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public Map<String, Integer> getQueuedInputs()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public int getSourceId()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public long getTotalProcessed()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public boolean isStopped()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void setDOMRegistry(IDOMRegistry x0)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void setEarliestPayloadOfInterest(IPayload payload)
    {
        // ignored, since TriggerManager doesn't do anything with it
    }

    @Override
    public void setOutputEngine(DAQComponentOutputProcess x0)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void setSplicer(Splicer x0)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void stopThread()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void switchToNewRun(int i0)
    {
        throw new Error("Unimplemented");
    }
}

public class AlgorithmDeathmatch
    implements ITriggerAlgorithm, ITriggerCollector
{
    private static final Logger LOG =
        Logger.getLogger(AlgorithmDeathmatch.class);

    private Object runLock = new Object();

    private ITriggerAlgorithm newAlgorithm;
    private List<ITriggerRequestPayload> newReleased =
        new ArrayList<ITriggerRequestPayload>();
    private CodeTimer newTimer = new CodeTimer(100);

    private ITriggerAlgorithm oldAlgorithm;
    private List<ITriggerRequestPayload> oldReleased =
        new ArrayList<ITriggerRequestPayload>();
    private CodeTimer oldTimer = new CodeTimer(100);

    private PayloadSubscriber subscriber;

    private Random random = new Random();

    private int numFailed;
    private int numWritten;

    public AlgorithmDeathmatch(ITriggerAlgorithm newAlgorithm,
                               ITriggerAlgorithm oldAlgorithm)
    {
        this.newAlgorithm = newAlgorithm;
        this.oldAlgorithm = oldAlgorithm;
    }

    private ITriggerAlgorithm getRandomAlgorithm(boolean oldFirst, int index)
    {
        if (oldFirst) {
            return (index == 0 ? oldAlgorithm : newAlgorithm);
        }

        return (index == 0 ? newAlgorithm : oldAlgorithm);
    }

    private CodeTimer getMatchingTimer(ITriggerAlgorithm algorithm)
    {
        if (algorithm == oldAlgorithm) {
            return oldTimer;
        } else if (algorithm != newAlgorithm) {
            throw new Error("Unknown algorithm \"" +
                            algorithm.getClass().getName() + "\"");
        }

        return newTimer;
    }

    @Override
    public void addParameter(String name, String value)
        throws UnknownParameterException, IllegalParameterValueException
    {
        final int pos = 0;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.addParameter(name, value);
            } finally {
                timer.stop(pos);
            }
        }
    }

    @Override
    public void addReadout(int rdoutType, int offset, int minus, int plus)
    {
        final int pos = 1;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.addReadout(rdoutType, offset, minus, plus);
            } finally {
                timer.stop(pos);
            }
        }
    }

    @Override
    public void checkTriggerType(int type)
        throws ConfigException
    {
        oldAlgorithm.checkTriggerType(type);
    }

    @Override
    public int compareTo(ITriggerAlgorithm algorithm)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void flush()
    {
        final int pos = 2;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.flush();
            } finally {
                timer.stop(pos);
            }
        }
    }

    public ITriggerAlgorithm getAlgorithm()
    {
        return newAlgorithm;
    }

    @Override
    public IPayload getEarliestPayloadOfInterest()
    {
        final int pos = 3;

        IPayload oldPay = null;
        IPayload newPay = null;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                IPayload pay = algo.getEarliestPayloadOfInterest();
                if (algo == oldAlgorithm) {
                    oldPay = pay;
                } else {
                    newPay = pay;
                }
            } finally {
                timer.stop(pos);
            }
        }

        if ((newPay == null && oldPay != null) || !newPay.equals(oldPay)) {
            System.err.println("MISMATCH in getEarliestPayloadOfInterest:" +
                               " old " + oldPay + ", new " + newPay);
        }

        return oldPay;
    }

    @Override
    public int getInputQueueSize()
    {
        if (subscriber == null) {
            return -1;
        }

        return subscriber.size();
    }

    @Override
    public Interval getInterval(Interval interval)
    {
        final int pos = 5;

        Interval oldIval = null;
        Interval newIval = null;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            Interval tmpIval = new Interval();
            tmpIval.start = interval.start;
            tmpIval.end = interval.end;

            timer.start(pos);
            try {
                Interval ival = algo.getInterval(interval);
                if (algo == oldAlgorithm) {
                    oldIval = ival;
                } else {
                    newIval = ival;
                }
            } finally {
                timer.stop(pos);
            }
        }

        if ((newIval == null && oldIval != null) || !newIval.equals(oldIval)) {
            System.err.println("MISMATCH in getInterval:" +
                               " old " + oldIval + ", new " + newIval);
        }

        return oldIval;
    }

    @Override
    public long getLatency()
    {
        final int pos = 6;

        long oldLatency = 0L;
        long newLatency = 0L;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                long latency = algo.getLatency();
                if (algo == oldAlgorithm) {
                    oldLatency = latency;
                } else {
                    newLatency = latency;
                }
            } finally {
                timer.stop(pos);
            }
        }

        if (newLatency != oldLatency) {
            System.err.println("MISMATCH in getLatency:" +
                               " old " + oldLatency + ", new " + newLatency);
        }

        return oldLatency;
    }

    @Override
    public String getMonitoringName()
    {
        return oldAlgorithm.getMonitoringName();
    }

    public ITriggerAlgorithm getNewAlgorithm()
    {
        return newAlgorithm;
    }

    /**
     * Get the number of bad payloads.
     *
     * @return number of bad payloads
     */
    public int getNumberFailed()
    {
        return numFailed;
    }

    @Override
    public int getNumberOfCachedRequests()
    {
        final int pos = 7;

        int oldVal = Integer.MIN_VALUE, newVal = Integer.MIN_VALUE;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                int val = algo.getNumberOfCachedRequests();
                if (algo == oldAlgorithm) {
                    oldVal = val;
                } else {
                    newVal = val;
                }
            } finally {
                timer.stop(pos);
            }
        }

        final int diff = Math.abs(newVal - oldVal);
        if (diff != 0 && diff != 1) {
            System.err.println("MISMATCH in getNumberOfCachedRequests:" +
                               " old " + oldVal + ", new " + newVal);
        }

        return oldVal;
    }

    /**
     * Get the number of successfully consumed payloads.
     *
     * @return number of payloads
     */
    public int getNumberWritten()
    {
        return numWritten;
    }

    public ITriggerAlgorithm getOldAlgorithm()
    {
        return oldAlgorithm;
    }

    @Override
    public long getReleaseTime()
    {
        return oldAlgorithm.getReleaseTime();
    }

    @Override
    public long getSentTriggerCount()
    {
        final int pos = 9;

        long oldVal = Long.MIN_VALUE, newVal = Long.MIN_VALUE;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                long val = algo.getSentTriggerCount();
                if (algo == oldAlgorithm) {
                    oldVal = val;
                } else {
                    newVal = val;
                }
            } finally {
                timer.stop(pos);
            }
        }

        final long diff = Math.abs(newVal - oldVal);
        if (diff != 0L && diff != 1L) {
            System.err.println("MISMATCH in getSentTriggerCount:" +
                               " old " + oldVal + ", new " + newVal);
        }

        return oldVal;
    }

    @Override
    public int getSourceId()
    {
        return oldAlgorithm.getSourceId();
    }

    public String getStats()
    {
        final double oldTotal = (double) oldTimer.getTotalTime();
        final double newTotal = (double) newTimer.getTotalTime();

        final long oldTime =
            oldAlgorithm.getEarliestPayloadOfInterest().getUTCTime();
        final long newTime =
            newAlgorithm.getEarliestPayloadOfInterest().getUTCTime();
        String timeStr;
        if (oldTime == newTime) {
            timeStr = "";
        } else {
            timeStr =
                String.format("\nNew time %d != old time %d (%d difference)",
                              newTime, oldTime, newTime - oldTime);
        }

        return oldTimer.getStats(oldAlgorithm.getClass().getName()) + "\n" +
            newTimer.getStats(newAlgorithm.getClass().getName()) + "\n" +
            String.format("Old algorithm takes %.2f%% as long as new",
                          (oldTotal / newTotal) * 100.0) + timeStr;
    }

    @Override
    public PayloadSubscriber getSubscriber()
    {
        return subscriber;
    }

    @Override
    public int getTriggerConfigId()
    {
        return oldAlgorithm.getTriggerConfigId();
    }

    @Override
    public int getTriggerCounter()
    {
        final int pos = 13;

        int oldVal = Integer.MIN_VALUE, newVal = Integer.MIN_VALUE;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                int val = algo.getTriggerCounter();
                if (algo == oldAlgorithm) {
                    oldVal = val;
                } else {
                    newVal = val;
                }
            } finally {
                timer.stop(pos);
            }
        }

        final int diff = Math.abs(newVal - oldVal);
        if (diff != 0 && diff != 1) {
            System.err.println("MISMATCH in getTriggerCounter:" +
                               " old " + oldVal + ", new " + newVal);
        }

        return oldVal;
    }

    @Override
    public Map<String, Object> getTriggerMonitorMap()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public String getTriggerName()
    {
        return oldAlgorithm.getTriggerName();
    }

    @Override
    public int getTriggerType()
    {
        return oldAlgorithm.getTriggerType();
    }

    @Override
    public boolean hasCachedRequests()
    {
        final int pos = 16;

        boolean oldVal = false, newVal = false;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                boolean val = algo.hasCachedRequests();
                if (algo == oldAlgorithm) {
                    oldVal = val;
                } else {
                    newVal = val;
                }
            } finally {
                timer.stop(pos);
            }
        }

        if (newVal != oldVal) {
            System.err.println("MISMATCH in hasCachedRequests:" +
                               " old " + oldVal + ", new " + newVal);
        }

        return oldVal;
    }

    @Override
    public boolean hasData()
    {
        if (subscriber == null) {
            return false;
        }

        return subscriber.hasData();
    }

    @Override
    public boolean hasValidMultiplicity()
    {
        final int pos = 18;

        boolean oldVal = false, newVal = false;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                boolean val = algo.hasValidMultiplicity();
                if (algo == oldAlgorithm) {
                    oldVal = val;
                } else {
                    newVal = val;
                }
            } finally {
                timer.stop(pos);
            }
        }

        if (newVal != oldVal) {
            System.err.println("MISMATCH in hasValidMultiplicity:" +
                               " old " + oldVal + ", new " + newVal);
        }

        return oldVal;
    }

    @Override
    public boolean isConfigured()
    {
        final int pos = 19;

        boolean oldVal = false, newVal = false;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                boolean val = algo.isConfigured();
                if (algo == oldAlgorithm) {
                    oldVal = val;
                } else {
                    newVal = val;
                }
            } finally {
                timer.stop(pos);
            }
        }

        if (newVal != oldVal) {
            System.err.println("MISMATCH in isConfigured:" +
                               " old " + oldVal + ", new " + newVal);
        }

        return oldVal;
    }

    @Override
    public void recycleUnusedRequests()
    {
        final int pos = 20;
        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.recycleUnusedRequests();
            } finally {
                timer.stop(pos);
            }
        }
    }

    @Override
    public int release(Interval interval,
                       List<ITriggerRequestPayload> released)
    {
        throw new Error("Unimplemented");
    }

    @Override
    public void resetAlgorithm()
    {
        final int pos = 22;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.resetAlgorithm();
            } finally {
                timer.stop(pos);
            }
        }
    }

    @Override
    public void resetUID()
    {
        final int pos = 23;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.resetUID();
            } finally {
                timer.stop(pos);
            }
        }
    }

    @Override
    public void runTrigger(IPayload payload)
        throws TriggerException
    {
        final int pos = 30;

        synchronized (runLock) {
            final boolean oldFirst = random.nextBoolean();
            for (int i = 0; i < 2; i++) {
                ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
                CodeTimer timer = getMatchingTimer(algo);

                timer.start(pos);
                try {
                    algo.runTrigger(payload);
                } finally {
                    timer.stop(pos);
                }
            }
        }
    }

    @Override
    public void sendLast()
    {
        final int pos = 40;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.sendLast();
            } finally {
                timer.stop(pos);
            }
        }
    }

    private static int releaseAll(ITriggerAlgorithm algorithm,
                                  List<ITriggerRequestPayload> released)
    {
        int numReleased = 0;
        while (true) {
            Interval ival = algorithm.getInterval(new Interval());
            if (ival == null) {
                break;
            }

            final int len = released.size();
            final int num = algorithm.release(ival, released);
            numReleased += num;

            // if no new hits were released, we're done
            if (released.size() == len) {
                break;
            }
        }
        return numReleased;
    }

    @Override
    public void setChanged()
    {
        final int pos = 42;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            List<ITriggerRequestPayload> released;
            if (algo == oldAlgorithm) {
                released = oldReleased;
            } else {
                released = newReleased;
            }

            timer.start(pos);
            try {
                final int num = releaseAll(algo, released);
                if (i == 1) {
                    numWritten += num;
                }
            } finally {
                timer.stop(pos);
            }
        }

        while (!oldReleased.isEmpty() && !newReleased.isEmpty()) {
            ITriggerRequestPayload oldReq = oldReleased.remove(0);
            ITriggerRequestPayload newReq = newReleased.remove(0);
            if (!PayloadComparison.compareTriggerRequest(oldReq, newReq, true,
                                                         false))
            {
                throw new Error("Expected " + oldReq + ", got " + newReq);
            }
        }
    }

    @Override
    public void setSourceId(int srcId)
    {
        final int pos = 43;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.setSourceId(srcId);
            } finally {
                timer.stop(pos);
            }
        }
    }

    @Override
    public void setSubscriber(PayloadSubscriber subscriber)
    {
        if (this.subscriber != null) {
            throw new Error(getTriggerName() +
                            " is already subscribed to an input queue");
        }

        this.subscriber = subscriber;
    }

    @Override
    public void setTriggerCollector(ITriggerCollector collector)
    {
        oldAlgorithm.setTriggerCollector(this);
        newAlgorithm.setTriggerCollector(this);
    }

    @Override
    public void setTriggerConfigId(int cfgId)
    {
        final int pos = 46;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.setTriggerConfigId(cfgId);
            } finally {
                timer.stop(pos);
            }
        }
    }

    @Override
    public void setTriggerFactory(TriggerRequestFactory factory)
    {
        final int pos = 47;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.setTriggerFactory(factory);
            } finally {
                timer.stop(pos);
            }
        }
    }

    @Override
    public void setTriggerManager(ITriggerManager mgr)
    {
        ManagerWrapper mock = new ManagerWrapper(mgr);
        oldAlgorithm.setTriggerManager(mock);
        newAlgorithm.setTriggerManager(mock);
    }

    @Override
    public void setTriggerName(String name)
    {
        final int pos = 49;

        final boolean oldFirst = random.nextBoolean();
        for (int i = 0; i < 2; i++) {
            ITriggerAlgorithm algo = getRandomAlgorithm(oldFirst, i);
            CodeTimer timer = getMatchingTimer(algo);

            timer.start(pos);
            try {
                algo.setTriggerName(name);
            } finally {
                timer.stop(pos);
            }
        }
    }

    public void unsubscribe(SubscribedList list)
    {
        if (subscriber == null) {
            LOG.warn(getTriggerName() +
                     " is not subscribed to the input queue");
            return;
        }

        if (!list.unsubscribe(subscriber)) {
            LOG.warn(getTriggerName() +
                     " was not fully unsubscribed from the input queue");
        }

        subscriber = null;
    }

    @Override
    public String toString()
    {
        return "Deathmatch[" + oldAlgorithm + " <=> " + newAlgorithm + "]";
    }
}
