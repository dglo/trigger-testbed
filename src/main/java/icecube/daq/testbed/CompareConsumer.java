package icecube.daq.testbed;

import icecube.daq.io.PayloadByteReader;
import icecube.daq.payload.IHitPayload;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.IReadoutRequest;
import icecube.daq.payload.IReadoutRequestElement;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.payload.impl.PayloadFactory;
import icecube.daq.payload.impl.ReadoutRequestElement;
import icecube.daq.trigger.common.ITriggerAlgorithm;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Consumer which compares payloads against a previously generated file.
 */
public class CompareConsumer
    extends Consumer
{
    /** Log object for this class */
    private static final Log LOG = LogFactory.getLog(CompareConsumer.class);

    /** Indentation for parts of trigger request string */
    private static final String INDENT_STEP = "    ";

    private PayloadByteReader rdr;
    private int payloadCount;

    private PayloadFactory factory;

    private int throughputType = Integer.MIN_VALUE;

    /**
     * Create a comparison consumer.
     *
     * @param outFile output file
     * @param chanIn input channel
     *
     * @throws IOException if there is a problem
     */
    public CompareConsumer(File outFile, ReadableByteChannel chanIn)
        throws IOException
    {
        super(outFile.getName(), chanIn);

        rdr = new PayloadByteReader(outFile);
    }

    void close()
        throws IOException
    {
        // count the number of expected payloads which were not sent
        for (ByteBuffer buf = rdr.next(); buf != null && !isStopMessage(buf);
             buf = rdr.next())
        {
            foundMissed();
        }

        try {
            rdr.close();
        } finally {
            super.close();
        }
    }

    private boolean compareHit(IHitPayload exp, IHitPayload got,
                               boolean exact, boolean reportError)
    {
        if (!compareLong("CompareHitTime", exp.getHitTimeUTC().longValue(),
                         got.getHitTimeUTC().longValue(), reportError))
        {
            return false;
        }

        if (exact) {
            if (!compareLong("CompareHitDOM", exp.getDOMID().longValue(),
                             got.getDOMID().longValue(), reportError))
            {
                return false;
            }

            if (!compareInt("SourceId", exp.getSourceID().getSourceID(),
                            got.getSourceID().getSourceID(), reportError))
            {
                return false;
            }
        }

        return true;
    }

    private boolean compareInt(String fldName, int exp, int got,
                               boolean reportError)
    {
        if (exp != got) {
            if (reportError) {
                LOG.error("Compare" + fldName + ": exp " + exp +
                          " got " + got);
            }

            return false;
        }

        return true;
    }

    private boolean compareLong(String fldName, long exp, long got,
                                boolean reportError)
    {
        if (exp != got) {
            if (reportError) {
                LOG.error("Compare" + fldName + ": exp " + exp +
                          " got " + got);
            }

            return false;
        }

        return true;
    }

    private boolean comparePayloads(PrintStream out, ByteBuffer expBuf,
                                    ByteBuffer gotBuf)
    {
        return dumpPayloads(out, expBuf, gotBuf, true);
    }

    private boolean compareReadoutRequest(IReadoutRequest exp,
                                          IReadoutRequest got,
                                          boolean checkUID,
                                          boolean ignoreLastTimeErrors,
                                          boolean reportError)
    {
        if (checkUID && !compareInt("RReqUID", exp.getUID(), got.getUID(),
                                    reportError))
        {
            return false;
        }

        if (!compareInt("RReqSrcId", exp.getSourceID().getSourceID(),
                        got.getSourceID().getSourceID(), reportError))
        {
            return false;
        }

        List expList = exp.getReadoutRequestElements();
        List gotList = got.getReadoutRequestElements();
        if (expList == null || gotList == null) {
            if (expList != gotList) {
                if (reportError) {
                    LOG.error("CompareRdoutReqElems: exp " + expList +
                              " got " + gotList);
                }

                return false;
            }
        } else {
            List merged;
            if (expList.size() <= gotList.size()) {
                merged = expList;
            } else {
                merged = mergeElements(expList);
            }

            if (merged.size() != gotList.size()) {
                if (reportError) {
                    LOG.error("CompareRdoutReqElems: expLen " + merged.size() +
                              " got " + gotList.size());
                }

                return false;
            } else {
                ArrayList gotCopy = new ArrayList(gotList);
                for (int i = 0; i < merged.size(); i++) {
                    IReadoutRequestElement expElem =
                        (IReadoutRequestElement) merged.get(i);

                    boolean found = false;
                    for (int j = 0; j < gotCopy.size(); j++) {
                        IReadoutRequestElement gotElem =
                            (IReadoutRequestElement) gotCopy.get(j);

                        if (compareReadoutRequestElement(expElem, gotElem,
                                                         ignoreLastTimeErrors,
                                                         false))
                        {
                            gotCopy.remove(j);
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        if (reportError) {
                            LOG.error("CompareRdoutReqElems: cannot find " +
                                      expElem + " in " + got);
                        }

                        return false;
                    }
                }
            }
        }

        return true;
    }

    private boolean compareReadoutRequestElement(IReadoutRequestElement exp,
                                                 IReadoutRequestElement got,
                                                 boolean ignoreLastTimeErrors,
                                                 boolean reportError)
    {
        if (!compareInt("CompareRREType", exp.getReadoutType(),
                        got.getReadoutType(), reportError))
        {
            return false;
        }

        if (!compareInt("CompareRRESrcId", exp.getSourceID().getSourceID(),
                        got.getSourceID().getSourceID(), reportError))
        {
            return false;
        }

        if (!compareLong("CompareRREFirst", exp.getFirstTimeUTC().longValue(),
                         got.getFirstTimeUTC().longValue(), reportError))
        {
            return false;
        }

        if (!ignoreLastTimeErrors &&
            !compareLong("CompareRRELast", exp.getLastTimeUTC().longValue(),
                         got.getLastTimeUTC().longValue(), reportError))
        {
            return false;
        }

        if (!compareLong("CompareRREDOM", exp.getDomID().longValue(),
                         got.getDomID().longValue(), reportError))
        {
            return false;
        }

        return true;
    }

    private boolean compareTriggerRequest(ITriggerRequestPayload exp,
                                          ITriggerRequestPayload got,
                                          boolean reportError)
    {
        if (!compareLong("UTCTime", exp.getUTCTime(), got.getUTCTime(),
                         reportError))
        {
            return false;
        }

        if (!compareInt("TrigType", exp.getTriggerType(),
                         got.getTriggerType(), reportError))
        {
            return false;
        }

        if (!compareInt("ConfigId", exp.getTriggerConfigID(),
                         got.getTriggerConfigID(), reportError))
        {
            return false;
        }

        boolean checkUID = true;
        if (exp.getUID() >= 0 &&
            exp.getTriggerConfigID() == -1 &&
            (exp.getTriggerType() == -1 ||
             exp.getTriggerType() == throughputType))
        {
            final int GLOBAL_TRIGGER =
                SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID;

            if (exp.getTriggerType() == -1 &&
                exp.getSourceID().getSourceID() == GLOBAL_TRIGGER &&
                !compareInt("UID", exp.getUID(), got.getUID(),
                            reportError))
            {
                return false;
            } else if (exp.getTriggerType() == throughputType &&
                       got.getUID() < 0)
            {
                // throughput trigger UIDs may not match but should be positive
                return false;
            }

            checkUID = false;
        } else if (!compareInt("UID", exp.getUID(), got.getUID(),
                               reportError))
        {
            return false;
        }

        if (!compareInt("SourceId", exp.getSourceID().getSourceID(),
                        got.getSourceID().getSourceID(), reportError))
        {
            return false;
        }

        if (!compareLong("FirstTime", exp.getFirstTimeUTC().longValue(),
                         got.getFirstTimeUTC().longValue(), reportError))
        {
            return false;
        }

        if (!compareLong("LastTime", exp.getLastTimeUTC().longValue(),
                         got.getLastTimeUTC().longValue(), reportError))
        {
            return false;
        }

        boolean ignoreLastTimeErrors = exp.getTriggerType() == throughputType;
        if (!compareReadoutRequest(exp.getReadoutRequest(),
                                   got.getReadoutRequest(),
                                   checkUID, ignoreLastTimeErrors,
                                   reportError))
        {
            return false;
        }

        List expList;
        List gotList;
        try {
            expList = exp.getPayloads();
            gotList = got.getPayloads();
        } catch (Exception ex) {
            LOG.error("Cannot get payloads", ex);
            return false;
        }

        if (expList == null || gotList == null) {
            if (expList != gotList) {
                if (reportError) {
                    LOG.error("ComparePayloads: exp " + expList +
                              " got " + gotList);
                }

                return false;
            }
        } else if (expList.size() != gotList.size()) {
            if (reportError) {
                LOG.error("ComparePayloads: expLen " + expList.size() +
                          " got " + gotList.size());
            }

            return false;
        } else {
            // report errors for immediate subpayloads of merged or
            // throughput trigger requests
            final boolean reportTrigError =
                !(exp.getUID() >= 0 && exp.getTriggerConfigID() == -1 &&
                  (exp.getTriggerType() == -1 ||
                   exp.getTriggerType() == throughputType));

            ArrayList gotCopy = new ArrayList(gotList);
            for (int i = 0; i < expList.size(); i++) {
                IPayload expPay = (IPayload) expList.get(i);

                boolean found = false;
                if (expPay instanceof ITriggerRequestPayload) {
                    ITriggerRequestPayload expReq =
                        (ITriggerRequestPayload) expPay;

                    for (int j = 0; j < gotCopy.size(); j++) {
                        IPayload gotPay = (IPayload) gotCopy.get(j);
                        if (gotPay instanceof ITriggerRequestPayload) {
                            ITriggerRequestPayload gotReq =
                                (ITriggerRequestPayload) gotPay;

                            if (lookSimilar(expReq, gotReq) &&
                                compareTriggerRequest(expReq, gotReq,
                                                      reportTrigError))
                            {
                                gotCopy.remove(j);
                                found = true;
                                break;
                            }
                        }
                    }
                } else if (expPay instanceof IHitPayload) {
                    IHitPayload expHit = (IHitPayload) expPay;

                    for (int j = 0; j < gotCopy.size(); j++) {
                        IPayload gotPay = (IPayload) gotCopy.get(j);
                        if (gotPay instanceof IHitPayload) {
                            IHitPayload gotHit = (IHitPayload) gotPay;

                            if (compareHit(expHit, gotHit, true, false)) {
                                gotCopy.remove(j);
                                found = true;
                                break;
                            } else if (compareHit(expHit, gotHit, false,
                                                  false))
                            {
                                final String fmt = "Loose match for hit %d:" +
                                    " expected %s DOM %s, got %s DOM %s";
                                LOG.error(String.format(fmt,
                                                        expHit.getUTCTime(),
                                                        expHit.getSourceID(),
                                                        expHit.getDOMID(),
                                                        gotHit.getSourceID(),
                                                        gotHit.getDOMID()));
                                gotCopy.remove(j);
                                found = true;
                                break;
                            }
                        }
                    }
                } else {
                    if (reportError) {
                        LOG.error("ComparePayloads: Found unknown payload " +
                                  expPay);
                    }

                    return false;
                }

                if (!found) {
                    if (reportError) {
                        LOG.error("ComparePayloads: cannot find " +
                                  expPay);
                    }

                    return false;
                }
            }
        }

        return true;
    }

    public void configure(List<ITriggerAlgorithm> algorithms)
    {
        for (ITriggerAlgorithm a : algorithms) {
            if (a.getTriggerName().startsWith("Throughput")) {
                throughputType = a.getTriggerType();
            }
        }
    }

    private boolean dumpPayloads(PrintStream out, ByteBuffer expBuf,
                                 ByteBuffer gotBuf, boolean compare)
    {
        ITriggerRequestPayload exp = getPayload(expBuf);
        try {
            ITriggerRequestPayload got = getPayload(gotBuf);

            try {
                if (compare && (exp == null || got == null)) {
                    if (exp != got) {
                        LOG.error("DumpPayloads: exp " + exp +
                                  " got " + got);
                        return false;
                    }

                    return true;
                }

                if (!compare || !compareTriggerRequest(exp, got, true)) {
                    out.println("EXP ----\n" + getTrigReqString(exp));
                    out.println("GOT ----\n" + getTrigReqString(got));

                    return false;
                }
            } finally {
                if (got != null) {
                    got.recycle();
                }
            }
        } finally {
            if (exp != null) {
                exp.recycle();
            }
        }

        return true;
    }

    String getReportVerb()
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
            LOG.error("Cannot get payload", ex);
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

    private String getTrigReqString(ITriggerRequestPayload tr)
    {
        return getTrigReqString(tr, INDENT_STEP);
    }

    private String getTrigReqString(ITriggerRequestPayload tr, String indent)
    {
        if (tr == null) {
            return indent + "!!! NULL TriggerRequest !!!";
        }

        StringBuilder buf = new StringBuilder(indent);
        buf.append(tr.toString());

        List list;
        try {
            list = tr.getPayloads();
        } catch (Exception ex) {
            buf.append('\n').append(INDENT_STEP).append(indent).
                append("Cannot get payloads: ").append(ex);
            list = null;
        }

        if (list != null) {
            for (Object obj : list) {
                if (obj instanceof ITriggerRequestPayload) {
                    ITriggerRequestPayload kid = (ITriggerRequestPayload) obj;
                    buf.append('\n').append(INDENT_STEP).
                        append(getTrigReqString(kid, indent + INDENT_STEP));
                } else {
                    buf.append("\n").append(INDENT_STEP).append(INDENT_STEP).
                        append(indent).append(obj);
                }
            }
        }

        return buf.toString();
    }

    /**
     * Trigger requests look similar if they have the same type,
     * configuration ID, and times. Note that UIDs may be different.
     *
     * @param exp expected request
     * @param got received request
     *
     * @return <tt>true</tt> if the requests look similar.
     */
    private boolean lookSimilar(ITriggerRequestPayload exp,
                                ITriggerRequestPayload got)
    {
        if (!compareLong("UTCTime", exp.getUTCTime(), got.getUTCTime(),
                         false))
        {
            return false;
        }

        if (!compareInt("TrigType", exp.getTriggerType(),
                         got.getTriggerType(), false))
        {
            return false;
        }

        if (!compareInt("ConfigId", exp.getTriggerConfigID(),
                         got.getTriggerConfigID(), false))
        {
            return false;
        }

        if (!compareLong("FirstTime", exp.getFirstTimeUTC().longValue(),
                         got.getFirstTimeUTC().longValue(), false))
        {
            return false;
        }

        if (!compareLong("LastTime", exp.getLastTimeUTC().longValue(),
                         got.getLastTimeUTC().longValue(), false))
        {
            return false;
        }

        return true;
    }

    private static List mergeElements(List expList)
    {
        final int II_GLOBAL = IReadoutRequestElement.READOUT_TYPE_II_GLOBAL;
        final int IT_GLOBAL = IReadoutRequestElement.READOUT_TYPE_IT_GLOBAL;

        final int NO_STRING = IReadoutRequestElement.NO_STRING;
        final long NO_DOM = IReadoutRequestElement.NO_DOM;

        long iiFirst = Long.MAX_VALUE;
        long iiLast = Long.MIN_VALUE;
        long itFirst = Long.MAX_VALUE;
        long itLast = Long.MIN_VALUE;

        for (Object obj : expList) {
            if (!(obj instanceof IReadoutRequestElement)) {
                return expList;
            }

            IReadoutRequestElement elem = (IReadoutRequestElement) obj;
            if (elem.getReadoutType() == IT_GLOBAL) {
                final long first = elem.getFirstTimeUTC().longValue();
                if (first < itFirst) {
                    itFirst = first;
                }
                final long last = elem.getLastTimeUTC().longValue();
                if (last > itLast) {
                    itLast = last;
                }
            } else if (elem.getReadoutType() == II_GLOBAL) {
                final long first = elem.getFirstTimeUTC().longValue();
                if (first < iiFirst) {
                    iiFirst = first;
                }
                final long last = elem.getLastTimeUTC().longValue();
                if (last > iiLast) {
                    iiLast = last;
                }
            } else {
                return expList;
            }
        }

        List merged = new ArrayList();
        merged.add(new ReadoutRequestElement(II_GLOBAL, NO_STRING, iiFirst,
                                             iiLast, NO_DOM));
        merged.add(new ReadoutRequestElement(IT_GLOBAL, NO_STRING, itFirst,
                                             itLast, NO_DOM));

        return merged;
    }

    void write(ByteBuffer buf)
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
            if (isStopMessage(buf)) {
                // we're at the end of the file and saw a stop message
            } else {
                // got an extra payload
                foundExtra();
            }
        } else if (isStopMessage(buf)) {
            // check for stop message
            if (!isStopMessage(expBuf)) {
                throw new IOException("Payload #" + payloadCount +
                                      " is a premature stop message");
            }

            setSawStop();
        } else if (!comparePayloads(out, expBuf, buf)) {
            throw new IOException("Payload #" + payloadCount +
                                  " comparison failed");
        }
    }
}
