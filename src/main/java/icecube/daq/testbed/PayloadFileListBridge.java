package icecube.daq.testbed;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.WritableByteChannel;

/**
 * An output bridge which reads payloads from a list of files qnd writes
 * them to an output channel.
 */
public class PayloadFileListBridge
    extends AbstractPayloadFileListBridge
{
    private WritableByteChannel chanOut;

    /**
     * Create an output bridge which writes payloads from a list of files.
     *
     * @param name source name
     * @param files list of files to write
     * @param chanOut output channel
     */
    public PayloadFileListBridge(String name, File[] files,
                                 WritableByteChannel chanOut)
    {
        super(name, files);

        this.chanOut = chanOut;
        if (chanOut instanceof SelectableChannel &&
            !((SelectableChannel) chanOut).isBlocking())
        {
            throw new Error("Output channel should be blocking");
        }
    }

    /**
     * Close the output channel.
     */
    void finishThreadCleanup()
    {
        try {
            chanOut.close();
        } catch (IOException ioe) {
            // ignore errors on close
        }

        chanOut = null;
    }

    public void write(ByteBuffer buf)
        throws IOException
    {
        int lenOut = chanOut.write(buf);
        if (lenOut != buf.limit()) {
            throw new Error("Expected to write " + buf.limit() +
                            " bytes, not " + lenOut);
        }
    }
}
