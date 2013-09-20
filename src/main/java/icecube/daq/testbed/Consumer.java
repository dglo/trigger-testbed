package icecube.daq.testbed;

import icecube.daq.trigger.common.ITriggerAlgorithm;

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
public abstract class Consumer
    implements Runnable
{
    public static final int STOP_MESSAGE_LENGTH = 4;

    private static final Log LOG = LogFactory.getLog(Consumer.class);

    private String inputName;
    private ReadableByteChannel chanIn;
    private Thread thread;
    private int numWritten;
    private int numMissed;
    private int numExtra;
    private int numFailed;
    private boolean sawStop;
    private boolean forcedStop;

    /**
     * Create a consumer.
     *
     * @param inputName consumer name
     * @param chanIn input channel
     */
    public Consumer(String inputName, ReadableByteChannel chanIn)
    {
        this.inputName = inputName;
        this.chanIn = chanIn;

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
     * Configure consumer using list of active trigger algorithms.
     *
     * @param algorithms list of active trigger algorithms
     */
    public abstract void configure(List<ITriggerAlgorithm> algorithms);

    /**
     * Found an extra payload which was sent to the consumer but not expected.
     */
    void foundExtra()
    {
        numExtra++;
    }

    /**
     * Found a payload which was expected but not sent to the consumer.
     */
    void foundMissed()
    {
        numMissed++;
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

    abstract String getReportVerb();

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
     * Is this byte buffer a stop message?
     *
     * @param buf payload bytes
     *
     * @returns <tt>true</tt> if this is a stop message
     */
    boolean isStopMessage(ByteBuffer buf)
    {
        return buf.limit() == STOP_MESSAGE_LENGTH &&
            buf.getInt(0) == STOP_MESSAGE_LENGTH;
    }

    /**
     * Write a consumer report to the standard output.
     *
     * @return <tt>true</tt> if all payloads were found
     */
    public boolean report()
    {
        String success;
        if (numWritten > 0) {
            success = "successfully ";
        } else {
            success = "";
        }

        System.out.println("Consumer " + success + getReportVerb() + " " +
                           numWritten + " payloads" +
                           (numMissed == 0 ? "" : ", " + numMissed +
                            " missed") +
                           (numExtra == 0 ? "" : ", " + numExtra +
                            " extra") +
                           (numFailed == 0 ? "" : ", " + numFailed +
                            " failed") +
                           (forcedStop ? ", FORCED TO STOP" :
                            (sawStop ? "" : ", not stopped")));

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
                write(buf);
            } catch (IOException ioe) {
                LOG.error("Couldn't write " + len + " bytes from " +
                          inputName, ioe);
                numFailed++;
                continue;
            }

            // don't overwhelm other threads
            Thread.yield();

            if (isStopMessage(buf)) {
                sawStop = true;
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
     * Component was forced to stop.
     */
    public void setForcedStop()
    {
        forcedStop = true;
    }

    /**
     * Consumer saw a stop message
     */
    public void setSawStop()
    {
        sawStop = true;
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
     * Receive a payload.
     *
     * @param buf payload bytes
     *
     * @throws IOException if there is a problem
     */
    abstract void write(ByteBuffer buf)
        throws IOException;

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
