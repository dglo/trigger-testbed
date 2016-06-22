package icecube.daq.testbed;

import icecube.daq.io.DAQComponentOutputProcess;
import icecube.daq.juggler.alert.AlertQueue;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.impl.TriggerRequestFactory;
import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.algorithm.AlgorithmStatistics;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.control.ITriggerCollector;
import icecube.daq.trigger.control.ITriggerManager;
import icecube.daq.trigger.control.Interval;
import icecube.daq.trigger.control.PayloadSubscriber;
import icecube.daq.trigger.control.SubscribedList;
import icecube.daq.trigger.exceptions.IllegalParameterValueException;
import icecube.daq.trigger.exceptions.TriggerException;
import icecube.daq.trigger.exceptions.UnknownParameterException;
import icecube.daq.util.CodeTimer;
import icecube.daq.util.DOMRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

class ManagerWrapper
    implements ITriggerManager
{
    private ITriggerManager mgr;

    ManagerWrapper(ITriggerManager mgr)
    {
        this.mgr = mgr;
    }

    public void addTrigger(ITriggerAlgorithm x0)
    {
        throw new Error("Unimplemented");
    }

    public void addTriggers(Iterable<ITriggerAlgorithm> x0)
    {
        throw new Error("Unimplemented");
    }

    public void flush()
    {
        throw new Error("Unimplemented");
    }

    public AlertQueue getAlertQueue()
    {
        throw new Error("Unimplemented");
    }

    public Iterable<AlgorithmStatistics> getAlgorithmStatistics()
    {
        throw new Error("Unimplemented");
    }

    public DOMRegistry getDOMRegistry()
    {
        try {
            return DOMRegistry.loadRegistry();
        } catch (Exception ex) {
            throw new Error("Cannot load DOM registry", ex);
        }
    }

    public int getNumInputsQueued()
    {
        throw new Error("Unimplemented");
    }

    public int getNumOutputsQueued()
    {
        throw new Error("Unimplemented");
    }

    public int getNumRequestsQueued()
    {
        throw new Error("Unimplemented");
    }

    public int getSourceId()
    {
        throw new Error("Unimplemented");
    }

    public long getTotalProcessed()
    {
        throw new Error("Unimplemented");
    }

    public boolean isStopped()
    {
        throw new Error("Unimplemented");
    }

    public void setDOMRegistry(DOMRegistry x0)
    {
        throw new Error("Unimplemented");
    }

    public void setEarliestPayloadOfInterest(IPayload payload)
    {
        // ignored, since TriggerManager doesn't do anything with it
    }

    public void setOutputEngine(DAQComponentOutputProcess x0)
    {
        throw new Error("Unimplemented");
    }

    public void setSplicer(Splicer x0)
    {
        throw new Error("Unimplemented");
    }

    public void stopThread()
    {
        throw new Error("Unimplemented");
    }

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

    private ITriggerAlgorithm newAlgorithm;
    private List<ITriggerRequestPayload> newReleased =
        new ArrayList<ITriggerRequestPayload>();
    private CodeTimer newTimer = new CodeTimer(100);

    private ITriggerAlgorithm oldAlgorithm;
    private List<ITriggerRequestPayload> oldReleased =
        new ArrayList<ITriggerRequestPayload>();
    private CodeTimer oldTimer = new CodeTimer(100);

    private PayloadSubscriber subscriber;
    private ITriggerManager mgr;

    public AlgorithmDeathmatch(ITriggerAlgorithm newAlgorithm,
                               ITriggerAlgorithm oldAlgorithm)
    {
        this.newAlgorithm = newAlgorithm;
        this.oldAlgorithm = oldAlgorithm;
    }

    public void addParameter(String name, String value)
        throws UnknownParameterException, IllegalParameterValueException
    {
final int pos = 0;
newTimer.start(pos); try {
        newAlgorithm.addParameter(name, value);
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.addParameter(name, value);
} finally { oldTimer.stop(pos); }
    }

    public void addReadout(int rdoutType, int offset, int minus, int plus)
    {
final int pos = 1;
newTimer.start(pos); try {
        newAlgorithm.addReadout(rdoutType, offset, minus, plus);
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.addReadout(rdoutType, offset, minus, plus);
} finally { oldTimer.stop(pos); }
    }

    public int compareTo(ITriggerAlgorithm algorithm)
    {
        throw new Error("Unimplemented");
    }

    public void flush()
    {
final int pos = 2;
newTimer.start(pos); try {
        newAlgorithm.flush();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.flush();
} finally { oldTimer.stop(pos); }
    }

    public IPayload getEarliestPayloadOfInterest()
    {
        IPayload oldPay, newPay;
final int pos = 3;
newTimer.start(pos); try {
        newPay = newAlgorithm.getEarliestPayloadOfInterest();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldPay = oldAlgorithm.getEarliestPayloadOfInterest();
} finally { oldTimer.stop(pos); }
        if ((newPay == null && oldPay != null) || !newPay.equals(oldPay)) {
            System.err.println("MISMATCH in getEarliestPayloadOfInterest:" +
                               " old " + oldPay + ", new " + newPay);
        }
        return oldPay;
    }

    public int getInputQueueSize()
    {
        if (subscriber == null) {
            return -1;
        }

        return subscriber.size();
    }

    public Interval getInterval(Interval interval)
    {
        Interval oldIval, newIval;
final int pos = 5;
        Interval saved = new Interval();
        saved.start = interval.start;
        saved.end = interval.end;
newTimer.start(pos); try {
        newIval = newAlgorithm.getInterval(interval);
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldIval = oldAlgorithm.getInterval(saved);
} finally { oldTimer.stop(pos); }
        if ((newIval == null && oldIval != null) || !newIval.equals(oldIval)) {
            System.err.println("MISMATCH in getInterval:" +
                               " old " + oldIval + ", new " + newIval);
        }
        return oldIval;
    }

    public String getMonitoringName()
    {
        return oldAlgorithm.getMonitoringName();
    }

    public int getNumberOfCachedRequests()
    {
        int oldVal, newVal;
final int pos = 7;
newTimer.start(pos); try {
        newVal = newAlgorithm.getNumberOfCachedRequests();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldVal = oldAlgorithm.getNumberOfCachedRequests();
} finally { oldTimer.stop(pos); }
        if (newVal != oldVal) {
            System.err.println("MISMATCH in getNumberOfCachedRequests:" +
                               " old " + oldVal + ", new " + newVal);
        }
        return oldVal;
    }

    public IPayload getReleaseTime()
    {
        return oldAlgorithm.getReleaseTime();
    }

    public long getSentTriggerCount()
    {
        long oldVal, newVal;
final int pos = 9;
newTimer.start(pos); try {
        newVal = newAlgorithm.getSentTriggerCount();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldVal = oldAlgorithm.getSentTriggerCount();
} finally { oldTimer.stop(pos); }
        if (newVal != oldVal) {
            System.err.println("MISMATCH in getSentTriggerCount:" +
                               " old " + oldVal + ", new " + newVal);
        }
        return oldVal;
    }

    public int getSourceId()
    {
        return oldAlgorithm.getSourceId();
    }

    public String getStats()
    {
        final double oldTotal = (double) oldTimer.getTotalTime();
        final double newTotal = (double) newTimer.getTotalTime();

        return oldTimer.getStats(oldAlgorithm.getClass().getName()) + "\n" +
            newTimer.getStats(newAlgorithm.getClass().getName()) + "\n" +
            String.format("Old algorithm takes %.2f%% as long as new",
                          (oldTotal / newTotal) * 100.0);
    }

    public PayloadSubscriber getSubscriber()
    {
        return subscriber;
    }

    public int getTriggerConfigId()
    {
        return oldAlgorithm.getTriggerConfigId();
    }

    public int getTriggerCounter()
    {
        int oldVal, newVal;
final int pos = 13;
newTimer.start(pos); try {
        newVal = newAlgorithm.getTriggerCounter();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldVal = oldAlgorithm.getTriggerCounter();
} finally { oldTimer.stop(pos); }
        if (newVal != oldVal) {
            System.err.println("MISMATCH in getTriggerCounter:" +
                               " old " + oldVal + ", new " + newVal);
        }
        return oldVal;
    }

    public Map<String, Object> getTriggerMonitorMap()
    {
        throw new Error("Unimplemented");
    }

    public String getTriggerName()
    {
        return oldAlgorithm.getTriggerName();
    }

    public int getTriggerType()
    {
        return oldAlgorithm.getTriggerType();
    }

    public boolean hasCachedRequests()
    {
        boolean oldVal, newVal;
final int pos = 16;
newTimer.start(pos); try {
        newVal = newAlgorithm.hasCachedRequests();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldVal = oldAlgorithm.hasCachedRequests();
} finally { oldTimer.stop(pos); }
        if (newVal != oldVal) {
            System.err.println("MISMATCH in hasCachedRequests:" +
                               " old " + oldVal + ", new " + newVal);
        }
        return oldVal;
    }

    public boolean hasData()
    {
        if (subscriber == null) {
            return false;
        }

        return subscriber.hasData();
    }

    public boolean hasValidMultiplicity()
    {
        boolean oldVal, newVal;
final int pos = 18;
newTimer.start(pos); try {
        newVal = newAlgorithm.hasValidMultiplicity();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldVal = oldAlgorithm.hasValidMultiplicity();
} finally { oldTimer.stop(pos); }
        if (newVal != oldVal) {
            System.err.println("MISMATCH in hasValidMultiplicity:" +
                               " old " + oldVal + ", new " + newVal);
        }
        return oldVal;
    }

    public boolean isConfigured()
    {
        boolean oldVal, newVal;
final int pos = 19;
newTimer.start(pos); try {
        newVal = newAlgorithm.isConfigured();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
       oldVal = oldAlgorithm.isConfigured();
} finally { oldTimer.stop(pos); }
        if (newVal != oldVal) {
            System.err.println("MISMATCH in isConfigured:" +
                               " old " + oldVal + ", new " + newVal);
        }
        return oldVal;
    }

    public void recycleUnusedRequests()
    {
final int pos = 20;
newTimer.start(pos); try {
        newAlgorithm.recycleUnusedRequests();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.recycleUnusedRequests();
} finally { oldTimer.stop(pos); }
    }

    public int release(Interval interval,
                       List<ITriggerRequestPayload> released)
    {
        throw new Error("Unimplemented");
    }

    public void resetAlgorithm()
    {
final int pos = 22;
newTimer.start(pos); try {
        newAlgorithm.resetAlgorithm();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.resetAlgorithm();
} finally { oldTimer.stop(pos); }
    }

    public void resetUID()
    {
final int pos = 23;
newTimer.start(pos); try {
        newAlgorithm.resetUID();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.resetUID();
} finally { oldTimer.stop(pos); }
    }

private Object runLock = new Object();

    public void runTrigger(IPayload payload)
        throws TriggerException
    {
        synchronized (runLock) {
final int pos = 30;
newTimer.start(pos); try {
        newAlgorithm.runTrigger(payload);
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.runTrigger(payload);
} finally { oldTimer.stop(pos); }
        }
    }

    public void sendLast()
    {
final int pos = 40;
newTimer.start(pos); try {
        newAlgorithm.sendLast();
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.sendLast();
} finally { oldTimer.stop(pos); }
    }

    private static void releaseAll(ITriggerAlgorithm algorithm,
                                   List<ITriggerRequestPayload> released)
    {
        while (true) {
            Interval ival = algorithm.getInterval(new Interval());
            if (ival == null) {
                break;
            }

            final int len = released.size();
            algorithm.release(ival, released);
            if (released.size() == len) {
                break;
            }
        }
    }

    public void setChanged()
    {
        releaseAll(oldAlgorithm, oldReleased);
        releaseAll(newAlgorithm, newReleased);

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

    public void setSourceId(int srcId)
    {
final int pos = 41;
newTimer.start(pos); try {
        newAlgorithm.setSourceId(srcId);
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.setSourceId(srcId);
} finally { oldTimer.stop(pos); }
    }

    public void setSubscriber(PayloadSubscriber subscriber)
    {
        if (this.subscriber != null) {
            throw new Error(getTriggerName() +
                            " is already subscribed to an input queue");
        }

        this.subscriber = subscriber;
    }

    public void setTriggerCollector(ITriggerCollector collector)
    {
        oldAlgorithm.setTriggerCollector(this);
        newAlgorithm.setTriggerCollector(this);
    }

    public void setTriggerConfigId(int cfgId)
    {
final int pos = 44;
newTimer.start(pos); try {
        newAlgorithm.setTriggerConfigId(cfgId);
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.setTriggerConfigId(cfgId);
} finally { oldTimer.stop(pos); }
    }

    public void setTriggerFactory(TriggerRequestFactory factory)
    {
final int pos = 45;
newTimer.start(pos); try {
        newAlgorithm.setTriggerFactory(factory);
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.setTriggerFactory(factory);
} finally { oldTimer.stop(pos); }
    }

    public void setTriggerManager(ITriggerManager mgr)
    {
        ManagerWrapper mock = new ManagerWrapper(mgr);
        oldAlgorithm.setTriggerManager(mock);
        newAlgorithm.setTriggerManager(mock);
    }

    public void setTriggerName(String name)
    {
final int pos = 47;
newTimer.start(pos); try {
        newAlgorithm.setTriggerName(name);
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.setTriggerName(name);
} finally { oldTimer.stop(pos); }
    }

    public void setTriggerType(int trigType)
    {
final int pos = 98;
newTimer.start(pos); try {
        newAlgorithm.setTriggerType(trigType);
} finally { newTimer.stop(pos); }
oldTimer.start(pos); try {
        oldAlgorithm.setTriggerType(trigType);
} finally { oldTimer.stop(pos); }
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

    public String toString()
    {
        return "Deathmatch[" + oldAlgorithm + " <=> " + newAlgorithm + "]";
    }
}
