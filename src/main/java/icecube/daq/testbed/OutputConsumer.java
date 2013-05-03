package icecube.daq.testbed;

import icecube.daq.trigger.common.ITriggerAlgorithm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * Consume payloads in some way (write to a file, compare against an
 * existing file)
 */
public class OutputConsumer
    extends Consumer
{
    private WritableByteChannel outChan;

    /**
     * Create an output payload consumer.
     *
     * @param outFile output file name
     * @param chanIn channel from which payloads are read
     *
     * @throws IOException if there is a problem
     */
    public OutputConsumer(File outFile, ReadableByteChannel chanIn)
        throws IOException
    {
        super(outFile.getName(), chanIn);

        outChan = openFile(outFile);
    }

    void close()
        throws IOException
    {
        try {
            outChan.close();
        } finally {
            super.close();
        }
    }

    public void configure(List<ITriggerAlgorithm> algorithms)
    {
        // nothing to do
    }

    String getReportVerb()
    {
        return "wrote";
    }

    private static WritableByteChannel openFile(File file)
        throws IOException
    {
        FileOutputStream out = new FileOutputStream(file.getPath());
        return out.getChannel();
    }

    void write(ByteBuffer buf)
        throws IOException
    {
        buf.position(0);

        int numWritten = outChan.write(buf);
        if (numWritten != buf.limit()) {
            throw new IOException("Expected to write " + buf.limit() +
                                  " bytes, not " + numWritten);
        }
    }
}
