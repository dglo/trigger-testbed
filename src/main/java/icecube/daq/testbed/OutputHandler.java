package icecube.daq.testbed;

import icecube.daq.trigger.algorithm.ITriggerAlgorithm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Consume payloads in some way (write to a file, compare against an
 * existing file)
 */
public class OutputHandler
    implements ConsumerHandler
{
    private WritableByteChannel outChan;

    private long firstTime = Long.MIN_VALUE;
    private long lastTime = Long.MIN_VALUE;
    private boolean sawStop;

    /**
     * Create a payload output handler.
     *
     * @param outFile output file name
     *
     * @throws IOException if there is a problem
     */
    public OutputHandler(File outFile)
        throws IOException
    {
        outChan = openFile(outFile);
    }

    @Override
    public void close()
        throws IOException
    {
        outChan.close();
    }

    @Override
    public void configure(ITriggerAlgorithm algorithm)
    {
        // nothing to do
    }

    @Override
    public void configure(Iterable<ITriggerAlgorithm> algorithms)
    {
        // nothing to do
    }

    /**
     * Get number of payloads which were unexpected.
     * @return number of extra payloads
     */
    @Override
    public int getNumberExtra()
    {
        return 0;
    }

    /**
     * Get the number of leftover payloads which were not seen.
     * @return number of missed payloads
     */
    @Override
    public int getNumberMissed()
    {
        return 0;
    }

    @Override
    public String getReportVerb()
    {
        return "wrote";
    }

    @Override
    public void handle(ByteBuffer buf)
        throws IOException
    {
        if (Util.isStopMessage(buf)) {
            sawStop = true;
        }

        if (buf.limit() >= 16) {
            setLastUTCTime(buf.getLong(8));
        }

        buf.position(0);
        int numWritten = outChan.write(buf);
        if (numWritten != buf.limit()) {
            throw new IOException("Expected to write " + buf.limit() +
                                  " bytes, not " + numWritten);
        }
    }

    private static WritableByteChannel openFile(File file)
        throws IOException
    {
        FileOutputStream out = new FileOutputStream(file.getPath());
        return out.getChannel();
    }

    @Override
    public void reportTime(double clockSecs)
    {
        if (firstTime != Long.MIN_VALUE && lastTime != Long.MIN_VALUE) {
            final double daqTicksPerSec = 10000000000.0;

            final double firstSecs = ((double) firstTime) / daqTicksPerSec;
            final double lastSecs = ((double) lastTime) / daqTicksPerSec;

            System.out.format("Processed %.2f seconds of data in" +
                              " %.2f real seconds\n", lastSecs - firstSecs,
                              clockSecs);
        }
    }

    @Override
    public boolean sawStop()
    {
        return sawStop;
    }

    public void setLastUTCTime(long time)
    {
        if (firstTime == Long.MIN_VALUE) {
            firstTime = time;
        }

        lastTime = time;
    }
}
