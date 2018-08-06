package icecube.daq.testbed;

import icecube.daq.payload.IPayload;
import icecube.daq.splicer.SplicedAnalysis;
import icecube.daq.splicer.SplicerChangedEvent;
import icecube.daq.splicer.SplicerListener;
import icecube.daq.trigger.control.PayloadSubscriber;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

class SplicerSubscriber
    implements PayloadSubscriber, SplicedAnalysis<IPayload>,
               SplicerListener<IPayload>
{
    private static final Logger LOG =
        Logger.getLogger(SplicerSubscriber.class);

    private String name;
    private ArrayList<IPayload> list = new ArrayList<IPayload>();
    private boolean stopping;
    private boolean stopped;

    SplicerSubscriber(String name)
    {
        this.name = name;
    }

    public void analyze(List<IPayload> splicedObjects)
    {
        synchronized (list) {
            list.addAll(splicedObjects);
            list.notify();
        }
    }

    public void disposed(SplicerChangedEvent<IPayload> event)
    {
        LOG.error("Got SplicerDisposed event: " + event);
    }

    public void failed(SplicerChangedEvent<IPayload> event)
    {
        LOG.error("Got SplicerFailed event: " + event);
    }

    /**
     * Get subscriber name
     *
     * @return name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Is there data available?
     *
     * @return <tt>true</tt> if there are more payloads available
     */
    public boolean hasData()
    {
        return list.size() > 0;
    }

    /**
     * Has this list been stopped?
     *
     * @return <tt>true</tt> if the list has been stopped
     */
    public boolean isStopped()
    {
        return stopped;
    }

    /**
     * Return the next available payload.  Note that this may block if there
     * are no payloads queued.
     *
     * @return next available payload.
     */
    public IPayload pop()
    {
        synchronized (list) {
            while (!stopping && list.size() == 0) {
                try {
                    list.wait();
                } catch (InterruptedException ie) {
                    return null;
                }
            }

            if (stopping && list.size() == 0) {
                stopped = true;
                return null;
            }

            return list.remove(0);
        }
    }

    /**
     * Add a payload to the queue.
     *
     * @param pay payload
     */
    public void push(IPayload pay)
    {
        throw new Error("New payloads should only be added by the splicer!");
    }

    /**
     * Get the number of queued payloads
     *
     * @return size of internal queue
     */
    public int size()
    {
        return list.size();
    }

    public void started(SplicerChangedEvent<IPayload> event)
    {
        // ignored
    }

    public void starting(SplicerChangedEvent<IPayload> event)
    {
        // ignored
    }

    /**
     * No more payloads will be collected
     */
    public void stop()
    {
        synchronized (list) {
            if (!stopping && !stopped) {
                stopping = true;
                list.notify();
            }
        }
    }

    public void stopped(SplicerChangedEvent<IPayload> event)
    {
        LOG.error("SplicerStopped: subscriber list has " + list.size() +
                  " entries");
    }

    public void stopping(SplicerChangedEvent<IPayload> event)
    {
        stopping = true;
    }

    public String toString()
    {
        final String sgstr = stopping ? ",stopping" : "";
        final String sdstr = stopped ? ",stopped" : "";
        return String.format("%s*%d%s%s", name, list.size(), sgstr, sdstr);
    }
}
