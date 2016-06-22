package icecube.daq.testbed;

import icecube.daq.io.DAQComponentOutputProcess;
import icecube.daq.juggler.alert.AlertQueue;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.algorithm.AlgorithmStatistics;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.control.ITriggerCollector;
import icecube.daq.trigger.control.ITriggerManager;
import icecube.daq.trigger.control.Interval;
import icecube.daq.util.DOMRegistry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TriggerConsumer
    implements Consumer, ITriggerCollector, ITriggerManager
{
    private ITriggerAlgorithm algorithm;
    private ConsumerHandler handler;
    private DOMRegistry domRegistry;

    private ArrayList<AlgorithmStatistics> statsList =
        new ArrayList<AlgorithmStatistics>(1);

    private int numWritten;
    private int numFailed;
    private boolean forcedStop;

    public TriggerConsumer(ITriggerAlgorithm algorithm,
                           ConsumerHandler handler, DOMRegistry domRegistry)
    {
        this.algorithm = algorithm;
        this.handler = handler;
        this.domRegistry = domRegistry;
    }

    public void addTrigger(ITriggerAlgorithm algorithm)
    {
        throw new Error("Unimplemented");
    }

    public void addTriggers(Iterable<ITriggerAlgorithm> algorithms)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Configure consumer using list of active trigger algorithms.
     *
     * @param algorithms list of active trigger algorithms
     */
    public void configure(Iterable<ITriggerAlgorithm> algorithms)
    {
        throw new Error("Unimplemented");
    }

    public void flush()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Was the handler forced to stop?
     * @return <tt>true</tt> if handler was forced to stop
     */
    public boolean forcedStop()
    {
        return forcedStop;
    }

    public AlertQueue getAlertQueue()
    {
        throw new Error("Unimplemented");
    }

    public Iterable<AlgorithmStatistics> getAlgorithmStatistics()
    {
        statsList.clear();
        statsList.add(new AlgorithmStatistics(algorithm));
        return statsList;
    }

    public DOMRegistry getDOMRegistry()
    {
        return domRegistry;
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

    /**
     * Get the number of bad payloads.
     *
     * @return number of bad payloads
     */
    public int getNumberFailed()
    {
        return numFailed;
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

    public boolean report(double clockSecs)
    {
        String success;
        if (numWritten > 0) {
            success = "successfully ";
        } else {
            success = "";
        }

        final int numExtra = handler.getNumberExtra();
        final int numMissed = handler.getNumberMissed();
        final boolean sawStop = handler.sawStop();
        System.out.println("Consumer " + success + handler.getReportVerb() +
                           " " + numWritten + " payloads" +
                           (numMissed == 0 ? "" : ", " + numMissed +
                            " missed") +
                           (numExtra == 0 ? "" : ", " + numExtra +
                            " extra") +
                           (numFailed == 0 ? "" : ", " + numFailed +
                            " failed") +
                           (forcedStop ? ", FORCED TO STOP" :
                            (sawStop ? "" : ", not stopped")));

        handler.reportTime(clockSecs);

        return (numMissed == 0 && numFailed == 0 && !forcedStop);
    }

    public void setChanged()
    {
        List<ITriggerRequestPayload> released = null;
        while (true) {
            Interval ival = algorithm.getInterval(new Interval());
            if (ival == null) {
                break;
            }

            if (released == null) {
                released = new ArrayList<ITriggerRequestPayload>();
            }

            algorithm.release(ival, released);
            if (released.size() == 0) {
                break;
            }

            numWritten += released.size();
            released.clear();
        }
    }

    public void setDOMRegistry(DOMRegistry x0)
    {
        throw new Error("Unimplemented");
    }

    public void setEarliestPayloadOfInterest(IPayload payload)
    {
        // do nothing
    }

    /**
     * Handler should forceably stop processing inputs.
     */
    public void setForcedStop()
    {
        forcedStop = true;
    }

    public void setOutputEngine(DAQComponentOutputProcess x0)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Set the splicer associated with this trigger manager
     *
     * @param splicer splicer
     * @deprecated
     */
    public void setSplicer(Splicer splicer)
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

    /**
     * Receive a payload.
     *
     * @param buf payload bytes
     *
     * @throws IOException if there is a problem
     */
    void write(ByteBuffer buf)
        throws IOException
    {
        try {
            handler.handle(buf);
        } catch (IOException ioe) {
            numFailed++;
            throw ioe;
        }
    }
}
