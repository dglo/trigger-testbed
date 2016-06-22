package icecube.daq.testbed;

import icecube.daq.trigger.algorithm.ITriggerAlgorithm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Payload consumer.
 */
public class ChannelConsumer
    implements Consumer, Runnable
{
    private static final Log LOG = LogFactory.getLog(ChannelConsumer.class);

    private String inputName;
    private ReadableByteChannel chanIn;
    private ConsumerHandler handler;

    private Thread thread;
    private int numWritten;
    private int numFailed;
    private boolean forcedStop;

    /**
     * Create a consumer.
     *
     * @param inputName consumer name
     * @param chanIn input channel
     */
    public ChannelConsumer(String inputName, ReadableByteChannel chanIn,
                           ConsumerHandler handler)
    {
        this.inputName = inputName;
        this.chanIn = chanIn;
        this.handler = handler;

        if (chanIn instanceof SelectableChannel) {
            SelectableChannel selChan = (SelectableChannel) chanIn;
            if (!selChan.isBlocking()) {
                throw new Error("Got non-blocking channel");
            }
        }
    }

    /**
     * Close the input channel.
     *
     * @throws IOException if there is a problem
     */
    void close()
        throws IOException
    {
        chanIn.close();
    }

    /**
     * Was the handler forced to stop?
     * @return <tt>true</tt> if handler was forced to stop
     */
    public boolean forcedStop()
    {
        return forcedStop;
    }

    /**
     * Get the consumer name.
     *
     * @return name
     */
    public String getName()
    {
        return inputName;
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

    /**
     * Is the consumer thread running?
     *
     * @return <tt>true</tt> if the thread is running
     */
    public boolean isRunning()
    {
        return thread != null;
    }

    /**
     * Write a consumer report to the standard output.
     *
     * @param clockSecs time required to process all the data
     *
     * @return <tt>true</tt> if all payloads were found
     */
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

    /**
     * Run the consumer thread.
     */
    public void run()
    {
        ByteBuffer lenBuf = ByteBuffer.allocate(4);

        while (true) {
            lenBuf.rewind();
            int numBytes;
            try {
                numBytes = chanIn.read(lenBuf);
            } catch (IOException ioe) {
                throw new Error("Couldn't read length from " + inputName, ioe);
            }

            if (numBytes < 4) {
                break;
            }

            final int len = lenBuf.getInt(0);
            if (len < 4) {
                throw new Error("Bad length " + len);
            }

            ByteBuffer buf = ByteBuffer.allocate(len);
            buf.putInt(len);

            while (buf.position() != len) {
                try {
                    chanIn.read(buf);
                } catch (IOException ioe) {
                    throw new Error("Couldn't read data from " + inputName,
                                    ioe);
                }
            }

            buf.flip();

            try {
                handler.handle(buf);
            } catch (IOException ioe) {
                LOG.error("Couldn't write " + len + " bytes from " +
                          inputName, ioe);
                numFailed++;
                continue;
            }

            // don't overwhelm other threads
            Thread.yield();
            if (handler.sawStop()) {
                break;
            }

            numWritten++;
        }

        try {
            close();
        } catch (IOException ioe) {
            // ignore errors on close
        }

        thread = null;
    }

    /**
     * Handler should forceably stop processing inputs.
     */
    public void setForcedStop()
    {
        forcedStop = true;
    }

    /**
     * Start the consumer thread.
     */
    public void start()
    {
        numWritten = 0;

        thread = new Thread(this);
        thread.setName(inputName);
        thread.start();
    }

    /**
     * Return a debugging string.
     *
     * @return debugging string
     */
    public String toString()
    {
        return inputName + "#" + numWritten + (isRunning() ? "" : "(stopped)");
    }
}
