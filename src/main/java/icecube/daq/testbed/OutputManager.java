package icecube.daq.testbed;

import icecube.daq.io.DAQComponentOutputProcess;
import icecube.daq.io.OutputChannel;
import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.ITriggerRequestPayload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Manage the output channel.
 */
public class OutputManager
{
    private static final Logger LOG = Logger.getLogger(OutputManager.class);

    private DAQComponentOutputProcess outProc;
    private OutputChannel outChan;

    private IByteBufferCache outCache;

    /**
     * Create an output manager.
     *
     * @param outProc output engine
     */
    OutputManager(DAQComponentOutputProcess outProc)
    {
        this.outProc = outProc;
    }

    /**
     * Write all requests in the list.
     *
     * @param list list of trigger requests.
     */
    public void pushAll(List<ITriggerRequestPayload> list)
    {
        // need to merge together adjoining requests
        for (ITriggerRequestPayload tr : list) {
            writePayload(tr);
        }
    }

    /**
     * Start processing (actually only need to extract the output channel)
     */
    public void start()
    {
        if (outChan != null) {
            throw new Error("OutputManager has already been started");
        }

        outChan = outProc.getChannel();
        if (outChan == null) {
            throw new Error("Output channel has not been set in " +
                            outProc);
        }
    }

    private void writePayload(IPayload payload)
    {
        int bufLen = payload.length();

        // allocate ByteBuffer
        ByteBuffer buf;
        if (outCache != null) {
            buf = outCache.acquireBuffer(bufLen);
        } else {
            buf = ByteBuffer.allocate(bufLen);
        }

        // write trigger to a ByteBuffer
        try {
            payload.writePayload(false, 0, buf);
        } catch (IOException ioe) {
            LOG.error("Couldn't create payload", ioe);
            buf = null;
        }

        if (buf != null) {
            outChan.receiveByteBuffer(buf);
        }
    }
}
