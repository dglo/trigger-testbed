package icecube.daq.testbed;

import icecube.daq.io.DAQComponentOutputProcess;
import icecube.daq.juggler.alert.AlertQueue;
import icecube.daq.payload.IPayload;
import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.algorithm.AlgorithmStatistics;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.control.ITriggerManager;
import icecube.daq.util.IDOMRegistry;

import java.util.Map;

public class MockManager
    implements ITriggerManager
{
    private IDOMRegistry reg;

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
        return reg;
    }

    @Override
    public Map<String, Integer> getQueuedInputs()
    {
        throw new Error("Unimplemented");
    }

    @Override
    public int getNumOutputsQueued()
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
    public void setDOMRegistry(IDOMRegistry reg)
    {
        this.reg = reg;
    }

    @Override
    public void setEarliestPayloadOfInterest(IPayload pay)
    {
        // ignored
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
