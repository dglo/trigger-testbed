package icecube.daq.testbed;

import java.nio.ByteBuffer;

/**
 * Payload validator methods.
 */
public interface PayloadValidator
{
    /**
     * Validate a payload byte buffer.
     *
     * @param buf payload bytes
     */
    void validate(ByteBuffer buf);
}
