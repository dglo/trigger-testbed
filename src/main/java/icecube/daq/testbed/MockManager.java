package icecube.daq.testbed;

import icecube.daq.io.DAQComponentOutputProcess;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.control.ITriggerManager;
import icecube.daq.util.DOMRegistry;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MockManager
    implements ITriggerManager
{
    private DOMRegistry reg;
    private IPayload earliest;

    public void addTrigger(ITriggerAlgorithm x0)
    {
        throw new Error("Unimplemented");
    }

    public void addTriggers(List x0)
    {
        throw new Error("Unimplemented");
    }

    public void flush()
    {
        throw new Error("Unimplemented");
    }

    public DOMRegistry getDOMRegistry()
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

    public int getSourceId()
    {
        throw new Error("Unimplemented");
    }

    public long getTotalProcessed()
    {
        throw new Error("Unimplemented");
    }

    public Map getTriggerCounts()
    {
        throw new Error("Unimplemented");
    }

    public void setDOMRegistry(DOMRegistry reg)
    {
        this.reg = reg;
    }

    public void setEarliestPayloadOfInterest(IPayload pay)
    {
        earliest = pay;
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
