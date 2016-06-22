package icecube.daq.testbed;

import icecube.daq.trigger.algorithm.ITriggerAlgorithm;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ConsumerHandler
{
    void close()
        throws IOException;

    void configure(Iterable<ITriggerAlgorithm> algorithms);

    void configure(ITriggerAlgorithm algorithm);

    /**
     * Get number of payloads which were unexpected.
     * @return number of extra payloads
     */
    int getNumberExtra();

    /**
     * Get the number of leftover payloads which were not seen.
     * @return number of missed payloads
     */
    int getNumberMissed();

    String getReportVerb();
    void handle(ByteBuffer buf)
        throws IOException;

    void reportTime(double clockSecs);
    boolean sawStop();
}
