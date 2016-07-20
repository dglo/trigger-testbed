package icecube.daq.testbed;

import icecube.daq.io.DAQComponentOutputProcess;
import icecube.daq.juggler.alert.AlertQueue;
import icecube.daq.payload.IPayload;
import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.algorithm.AlgorithmStatistics;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.control.ITriggerManager;
import icecube.daq.util.IDOMRegistry;

public class MockManager
    implements ITriggerManager
{
    private IDOMRegistry reg;

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

    public IDOMRegistry getDOMRegistry()
    {
        return reg;
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

    public void setDOMRegistry(IDOMRegistry reg)
    {
        this.reg = reg;
    }

    public void setEarliestPayloadOfInterest(IPayload pay)
    {
        // ignored
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
