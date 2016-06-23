package icecube.daq.testbed;

import java.nio.ByteBuffer;

public final class Util
{
    public static final int STOP_MESSAGE_LENGTH = 4;

    /**
     * Build a stop message in the provided byte buffer.
     *
     * @param stopBuf buffer used to create stop message
     *
     * @return stop message
     */
    public static ByteBuffer buildStopMessage(ByteBuffer stopBuf)
    {
        if (stopBuf == null || stopBuf.capacity() < STOP_MESSAGE_LENGTH) {
            stopBuf = ByteBuffer.allocate(STOP_MESSAGE_LENGTH);
        }
        stopBuf.limit(STOP_MESSAGE_LENGTH);

        stopBuf.putInt(0, STOP_MESSAGE_LENGTH);

        stopBuf.position(0);

        return stopBuf;
    }

    /**
     * Is this byte buffer a stop message?
     *
     * @param buf payload bytes
     *
     * @return <tt>true</tt> if this is a stop message
     */
    public static boolean isStopMessage(ByteBuffer buf)
    {
        return buf.limit() == STOP_MESSAGE_LENGTH &&
            buf.getInt(0) == STOP_MESSAGE_LENGTH;
    }
}
