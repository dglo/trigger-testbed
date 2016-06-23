package icecube.daq.testbed;

import icecube.daq.payload.IHitPayload;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.IReadoutRequest;
import icecube.daq.payload.IReadoutRequestElement;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.payload.impl.ReadoutRequestElement;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class PayloadComparison
{
    private static final Logger LOG =
        Logger.getLogger(PayloadComparison.class);

    /** Indentation for parts of trigger request string */
    private static final String INDENT_STEP = "    ";

    private static int throughputType = Integer.MIN_VALUE;

    private static boolean compareHit(IHitPayload exp, IHitPayload got,
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

    private static boolean compareInt(String fldName, int exp, int got,
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

    private static boolean compareLong(String fldName, long exp, long got,
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

    public static boolean comparePayloads(PrintStream out,
                                          ITriggerRequestPayload exp,
                                          ITriggerRequestPayload got)
    {
        final boolean reportError = true;
        final boolean reportLooseMatch = false;

        try {
            try {
                if (!compareTriggerRequest(exp, got, reportError,
                                           reportLooseMatch))
                {
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


    private static boolean compareReadoutRequest(IReadoutRequest exp,
                                                 IReadoutRequest got,
                                                 boolean checkUID,
                                                 boolean ignoreLastTimeErrors,
                                                 boolean reportError)
    {
        final boolean mergeHack = false;

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
            if (!mergeHack || expList.size() <= gotList.size()) {
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

    private static boolean
        compareReadoutRequestElement(IReadoutRequestElement exp,
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

    public static boolean compareTriggerRequest(ITriggerRequestPayload exp,
                                                ITriggerRequestPayload got,
                                                boolean reportError,
                                                boolean reportLooseMatch)
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
                                                      reportTrigError,
                                                      reportLooseMatch))
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
                                if (reportLooseMatch) {
                                    final String fmt = "Loose match for" +
                                        " hit %d: expected %s DOM %s," +
                                        " got %s DOM %s";
                                    LOG.error(String.format(fmt,
                                                            expHit.getUTCTime(),
                                                            expHit.getSourceID(),
                                                            expHit.getDOMID(),
                                                            gotHit.getSourceID(),
                                                            gotHit.getDOMID()));
                                }

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

    private static String getRdoutReqString(IReadoutRequest rr, String indent)
    {
        StringBuilder buf = new StringBuilder();

        if (rr == null) {
            buf.append('\n').append(indent).
                append("!!! NULL ReadoutRequest !!!");
        } else {
            List list;
            try {
                list = rr.getReadoutRequestElements();
            } catch (Exception ex) {
                buf.append('\n').append(indent).append(INDENT_STEP).
                    append("Cannot get elements: ").append(ex);
                list = null;
            }

            if (list != null) {
                for (Object obj : list) {
                    buf.append('\n').append(indent).append(INDENT_STEP).
                        append(obj);
                }
            }
        }

        return buf.toString();
    }

    private static String getTrigReqString(ITriggerRequestPayload tr)
    {
        return getTrigReqString(tr, INDENT_STEP);
    }

    private static String getTrigReqString(ITriggerRequestPayload tr,
                                           String indent)
    {
        if (tr == null) {
            return indent + "!!! NULL TriggerRequest !!!";
        }

        StringBuilder buf = new StringBuilder(indent);
        buf.append(tr.toString());
        buf.append(getRdoutReqString(tr.getReadoutRequest(), indent));

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
    private static boolean lookSimilar(ITriggerRequestPayload exp,
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

    public static void setThroughputType(int val)
    {
        throughputType = val;
    }
}
