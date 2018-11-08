package icecube.daq.testbed;

import icecube.daq.io.PayloadByteReader;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.impl.BasePayload;
import icecube.daq.payload.impl.PayloadFactory;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

/**
 * Compare new payloads against a previously generated file.
 */
public class CompareHandler
    extends PayloadComparison
    implements ConsumerHandler
{
    /** Log object for this class */
    private static final Logger LOG = Logger.getLogger(CompareHandler.class);

    private PayloadByteReader rdr;
    private int payloadCount;

    private PayloadFactory factory;

    private long firstTime = Long.MIN_VALUE;
    private long lastTime = Long.MIN_VALUE;
    private boolean sawStop;

    private int numExtra;
    private int numMissed;

    /**
     * Create a comparison handler.
     *
     * @param payloadFile file containing good payloads
     *
     * @throws IOException if there is a problem
     */
    public CompareHandler(File payloadFile)
        throws IOException
    {
        rdr = new PayloadByteReader(payloadFile);
    }

    @Override
    public void close()
        throws IOException
    {
        // count the number of expected payloads which were not sent
        for (ByteBuffer buf = rdr.next();
             buf != null && !Util.isStopMessage(buf);
             buf = rdr.next())
        {
            numMissed++;
        }

        rdr.close();
    }

    private boolean comparePayloads(PrintStream out, ByteBuffer expBuf,
                                    ByteBuffer gotBuf)
    {
        ITriggerRequestPayload exp = getPayload(expBuf);
        ITriggerRequestPayload got = getPayload(gotBuf);
        setLastUTCTime(got.getUTCTime());
        return comparePayloads(out, exp, got);
    }

    @Override
    public void configure(ITriggerAlgorithm algorithm)
    {
        if (algorithm.getMonitoringName().equals("THROUGHPUT")) {
            setThroughputType(algorithm.getTriggerType());
        }
    }

    @Override
    public void configure(Iterable<ITriggerAlgorithm> algorithms)
    {
        for (ITriggerAlgorithm algo : algorithms) {
            configure(algo);
        }
    }

    /**
     * Get number of payloads which were unexpected.
     * @return number of extra payloads
     */
    @Override
    public int getNumberExtra()
    {
        return numExtra;
    }

    /**
     * Get the number of leftover payloads which were not seen.
     * @return number of missed payloads
     */
    @Override
    public int getNumberMissed()
    {
        return numMissed;
    }

    @Override
    public String getReportVerb()
    {
        return "compared";
    }

    private ITriggerRequestPayload getPayload(ByteBuffer buf)
    {
        if (factory == null) {
            factory = new PayloadFactory(null);
        }

        ITriggerRequestPayload pay;
        try {
            pay = (ITriggerRequestPayload) factory.getPayload(buf, 0);
        } catch (PayloadException ex) {
            LOG.error("Cannot get payload from " +
                      BasePayload.toHexString(buf, 0), ex);
            return null;
        }

        try {
            pay.loadPayload();
        } catch (Exception ex) {
            LOG.error("Cannot load payload", ex);
            return null;
        }

        return pay;
    }

    @Override
    public void handle(ByteBuffer buf)
        throws IOException
    {
        final PrintStream out = System.out;

        if (buf == null) {
            throw new IOException("Cannot write null payload");
        }

        if (buf.limit() < 4) {
            throw new IOException("Payload #" + payloadCount + " should be" +
                                  " at least 4 bytes long");
        }

        payloadCount++;

        ByteBuffer expBuf = rdr.next();
        if (expBuf == null) {
            if (Util.isStopMessage(buf)) {
                // we're at the end of the file and saw a stop message
            } else {
                // got an extra payload
                numExtra++;
            }
        } else if (Util.isStopMessage(buf)) {
            // check for stop message
            if (!Util.isStopMessage(expBuf)) {
                throw new IOException("Payload #" + payloadCount +
                                      " is a premature stop message");
            }

            sawStop = true;
        } else if (!comparePayloads(out, expBuf, buf)) {
            throw new IOException("Payload #" + payloadCount +
                                  " comparison failed");
        }
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
