package icecube.daq.testbed;

import icecube.daq.common.ANSIEscapeCode;
import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.juggler.mbean.MemoryStatistics;
import icecube.daq.splicer.HKN1Splicer;
import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.common.DAQTriggerComponent;

import java.io.PrintStream;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Monitor all activity in this component.
 */
public class ActivityMonitor
{
    private static final Log LOG = LogFactory.getLog(ActivityMonitor.class);

    private static final long MAX_QUEUED = 100000;

    private static final long MAX_TIME_DIFF = 10000000000L;

    private static final int PROGRESS_FREQUENCY = 100;
    private static final int MONITOR_FREQUENCY = 4;

    private DAQTriggerComponent comp;
    private String prefix;
    private PayloadFileListBridge[] bridges;
    private Consumer consumer;
    private int maxFailures;

    private long received;
    private long queuedIn;
    private long processed;
    private long queuedOut;
    private long sent;
    private boolean stopped;
    private boolean forcedStop;
    private boolean summarized;

    private MemoryStatistics memoryStats = new MemoryStatistics();

    ActivityMonitor(DAQTriggerComponent comp, String prefix,
                    PayloadFileListBridge[] bridges, Consumer consumer,
                    int maxFailures)
    {
        this.comp = comp;
        this.prefix = prefix;
        this.bridges = bridges;
        this.consumer = consumer;
        this.maxFailures = maxFailures;
    }

    /**
     * Check for activity.
     *
     * @return <tt>false</tt> if everything is stagnant
     */
    private boolean check()
    {
        if (stopped != summarized) {
            summarized = stopped;
        }

        boolean newStopped = (comp == null ||
                              (!comp.getReader().isRunning() &&
                               comp.getWriter().isStopped()));

        boolean changed = false;
        if (comp != null && !summarized) {
            if (received != comp.getPayloadsReceived()) {
                received = comp.getPayloadsReceived();
                changed = true;
            }
            if (queuedIn != comp.getTriggerManager().getNumInputsQueued()) {
                queuedIn = comp.getTriggerManager().getNumInputsQueued();
                changed = true;
            }
            if (processed != comp.getTriggerManager().getTotalProcessed()) {
                processed = comp.getTriggerManager().getTotalProcessed();
                changed = true;
            }
            if (queuedOut != comp.getTriggerManager().getNumOutputsQueued()) {
                queuedOut = comp.getTriggerManager().getNumOutputsQueued();
                changed = true;
            }
            if (sent != comp.getPayloadsSent()) {
                sent = comp.getPayloadsSent();
                changed = true;
            }
        }

        if (stopped != newStopped) {
            stopped = newStopped;
        }

        if (!stopped) {
            long earliestTime = Long.MAX_VALUE;
            long latestTime = 0;
            for (PayloadFileListBridge bridge : bridges) {
                if (bridge.getLastTime() > 0 &&
                    bridge.getLastTime() < Long.MAX_VALUE)
                {
                    if (bridge.getLastTime() < earliestTime) {
                        earliestTime = bridge.getLastTime();
                    }

                    if (bridge.getLastTime() > latestTime) {
                        latestTime = bridge.getLastTime();
                    }
                }
            }

            if (earliestTime < Long.MAX_VALUE && latestTime > 0) {
                for (PayloadFileListBridge bridge : bridges) {
                    if (bridge.isPaused() &&
                        bridge.getLastTime() - latestTime < MAX_TIME_DIFF)
                    {
                        bridge.unpause();
                    }

                    if (!bridge.isPaused() &&
                        bridge.getLastTime() - earliestTime > MAX_TIME_DIFF)
                    {
                        bridge.pause();
                    }
                }
            }
        }

        if (consumer.getNumberFailed() > maxFailures && !forcedStop) {
            // pause everything
            for (PayloadFileListBridge bridge : bridges) {
                bridge.stopThread();
            }

            // now force component to stop
            try {
                comp.forcedStop();
            } catch (DAQCompException dce) {
                LOG.error("Cannot forceStop " + comp.getName(), dce);
            }

            consumer.setForcedStop();

            forcedStop = true;
        }

        return changed;
    }

    private void dumpMBeanDynamic(PrintStream out,
                                  javax.management.DynamicMBean mbean,
                                  String indent)
    {
        javax.management.MBeanInfo info = mbean.getMBeanInfo();
        javax.management.MBeanAttributeInfo[] attrs = info.getAttributes();
        for (int i = 0; i < attrs.length; i++) {
            if (!attrs[i].isReadable()) {
                out.println(indent + "!" + attrs[i].getName());
            } else {
                Object rtnval;
                try {
                    rtnval = mbean.getAttribute(attrs[i].getName());
                } catch (Exception ex) {
                    ex.printStackTrace(out);
                    continue;
                }

                out.println(indent + attrs[i].getName() + ": " +
                            getMBeanValueString(rtnval));
            }
        }
    }

    private void dumpMBeanData(PrintStream out, Object obj, String indent)
    {
        Class cls = obj.getClass();

        Class[] ifaces = cls.getInterfaces();
        for (int i = 0; i < ifaces.length; i++) {
            if (ifaces[i].getName().endsWith("MBean")) {
                if (ifaces[i].getName().endsWith("DynamicMBean")) {
                    dumpMBeanDynamic(out, (javax.management.DynamicMBean) obj,
                                     indent);
                } else {
                    dumpMBeanInterface(out, obj, ifaces[i], indent);
                }
                break;
            }
        }
    }

    private void dumpMBeanInterface(PrintStream out, Object obj, Class iface,
                                    String indent)
    {
        Method[] methods = iface.getMethods();
        for (int i = 0; i < methods.length; i++) {
            Class[] params = methods[i].getParameterTypes();
            if (params != null && params.length > 0) {
                out.println(indent + "*** " + obj.getClass().getName() +
                            " MBean method " + iface.getName() +
                            " should not have any parameters");
                continue;
            }

            String name = methods[i].getName();
            if (name.startsWith("get")) {
                name = name.substring(3);
            } else if (name.startsWith("is")) {
                name = name.substring(2);
            } else {
                out.println(indent + "*** " + obj.getClass().getName() +
                            " MBean method " + iface.getName() +
                            " does not start with \"get\" or \"is\"");
                continue;
            }

            Object rtnval;
            try {
                rtnval = methods[i].invoke(obj);
            } catch (Exception ex) {
                ex.printStackTrace(out);
                continue;
            }

            out.println(indent + name + ": " + getMBeanValueString(rtnval));
        }
    }

    private void dumpMonitoring(PrintStream out, int rep)
    {
        Set<String> names = comp.listMBeans();
        if (names == null || names.size() == 0) {
            return;
        }

        String dateStr = getFakeDateString(rep);
        for (String name : names) {
            //out.print(ANSIEscapeCode.FG_YELLOW + ANSIEscapeCode.BG_BLUE);
            out.println(name + ": " + dateStr + ":");
            try {
                dumpMBeanData(out, comp.getMBean(name), "    ");
            } catch (DAQCompException dce) {
                dce.printStackTrace(out);
            }
            out.println();
        }
        //out.println(ANSIEscapeCode.OFF);
    }

    private void dumpProgress(PrintStream out, int rep, boolean dumpSplicers)
    {
        final String tcRaw = getTriggerCountsString();

        String tcStr;
        if (tcRaw == null || tcRaw.length() == 0) {
            tcStr = "";
        } else {
            tcStr = "\n        " + ANSIEscapeCode.BG_MAGENTA + tcRaw +
                ANSIEscapeCode.OFF;
        }

        out.println("#" + rep + ":" +
                    ANSIEscapeCode.BG_GREEN + toString() +
                    ANSIEscapeCode.OFF + tcStr);

        if (dumpSplicers) {
            dumpSplicer(out, prefix, comp.getSplicer());
        }
    }

    private void dumpSplicer(PrintStream out, String title, Splicer splicer)
    {
        out.println("*********************");
        out.println("*** " + title + " Splicer");
        out.println("*********************");
        String[] desc = ((HKN1Splicer) splicer).dumpDescription();
        for (int d = 0; d < desc.length; d++) {
            out.println("  " + desc[d]);
        }
    }

    private String getFakeDateString(int rep)
    {
        int min = rep * 5;

        int hour;
        if (min < 60) {
            hour = 0;
        } else {
            hour = min / 60;
            min %= 60;
        }

        int day;
        if (hour < 24) {
            day = 1;
        } else {
            day = (hour / 24) + 1;
            hour %= 24;
        }

        int month = 0;
        int year = 2012;

        int[] monDays = {
            31, 28, 31, 30, 31, 30, 31, 31, 31, 30, 31, 30, 31,
        };
        while (day > monDays[month]) {
            day -= monDays[month];
            month++;

            if (month >= 12) {
                year++;
                month %= 12;
            }
        }

        final String fmt = "%04d-%02d-%02d %02d:%02d:%02d.%06d";
        return String.format(fmt, year, month + 1, day, hour, min, 0, 0);
    }

    private String getMBeanValueString(Object obj)
    {
        if (obj == null) {
            return "null";
        } else if (obj.getClass().isArray()) {
            StringBuilder strBuf = new StringBuilder("[");
            final int len = Array.getLength(obj);
            for (int i = 0; i < len; i++) {
                if (strBuf.length() > 1) {
                    strBuf.append(", ");
                }
                strBuf.append(getMBeanValueString(Array.get(obj, i)));
            }
            strBuf.append("]");
            return strBuf.toString();
        } else if (obj.getClass().equals(HashMap.class)) {
            StringBuilder strBuf = new StringBuilder("{");
            HashMap map = (HashMap) obj;
            for (Object key : map.keySet()) {
                if (strBuf.length() > 1) {
                    strBuf.append(", ");
                }
                strBuf.append('\'').append(getMBeanValueString(key));
                strBuf.append("': ").append(getMBeanValueString(map.get(key)));
            }
            strBuf.append("}");
            return strBuf.toString();
        } else {
            return obj.toString();
        }
    }

    /**
     * Get the number of payloads sent.
     *
     * @return number of payloads sent
     */
    public long getSent()
    {
        return sent;
    }

    /**
     * Get the component's splicer.
     *
     * @return splicer
     */
    public Splicer getSplicer()
    {
        return comp.getSplicer();
    }

    /**
     * Format the trigger counts into a string.
     *
     * @return trigger counts string
     */
    private String getTriggerCountsString()
    {
        Map<String, Long> trigCounts =
            comp.getTriggerManager().getTriggerCounts();

        StringBuilder buf = new StringBuilder();
        for (String key : trigCounts.keySet()) {
            int idx = key.lastIndexOf("Trigger");

            String name;
            if (idx < 0) {
                name = key;
            } else if (idx + 7 == key.length()) {
                name = key.substring(0, idx);
            } else {
                name = key.substring(0, idx) + key.substring(idx + 8);
            }
            if (name.endsWith("1")) {
                name = name.substring(0, name.length() - 1);
            }

            buf.append(" ").append(name).append(":").
                append(trigCounts.get(key));
        }

        return buf.toString();
    }

    /**
     * Is everything stopped?
     *
     * @return <tt>true</tt> if everything has stopped
     */
    public boolean isStopped()
    {
        return stopped;
    }

    /**
     * Return a debugging string.
     *
     * @return debugging string
     */
    public String toString()
    {
        if (comp == null) {
            return "";
        }

        String rdStopped = comp.getReader().isRunning() ? "" : " inStop";
        String wrStopped = comp.getWriter().isStopped() ? " outStop" : "";

        if (summarized) {
            return " " + prefix + " stopped";
        }

        long[] stats = memoryStats.getMemoryStatistics();

        summarized = stopped;
        return String.format(" %s%s %d->%d->%d->%d->%d%s | %d / %d",
                             prefix, rdStopped, received, queuedIn, processed,
                             queuedOut, sent, wrStopped, stats[0], stats[1]);
    }

    boolean waitForStasis(int staticReps, int maxReps, int stoppedReps,
                          boolean verbose, boolean dumpSplicers,
                          PrintStream monOut)
    {
        final int sleepMSec = 100;

        final PrintStream out = System.out;

        int numStatic = 0;
        int numStopped = 0;
        for (int i = 0; i < maxReps; i++) {
            boolean changed = check();
            if (changed) {
                numStatic = 0;
                numStopped = 0;
            } else if (isStopped()) {
                numStopped++;
            } else {
                numStatic++;
            }

            if (changed && (queuedIn > MAX_QUEUED || queuedOut > MAX_QUEUED) &&
                !comp.getReader().isPaused())
            {
                comp.getReader().pause();
                if (verbose) {
                    System.err.println("!! Pausing reader");
                }
            } else if (changed &&
                       (queuedIn <= MAX_QUEUED && queuedOut <= MAX_QUEUED) &&
                       comp.getReader().isPaused())
            {
                comp.getReader().unpause();
                if (verbose) {
                    System.err.println("!! Unpausing reader");
                }
            }

            if (verbose && i % PROGRESS_FREQUENCY == 0) {
                dumpProgress(out, i, dumpSplicers);
            }

            if (monOut != null && i % MONITOR_FREQUENCY == 0) {
                dumpMonitoring(monOut, i);
            }

            if (numStatic >= staticReps || numStopped >= stoppedReps) {
                System.out.println("Component " + comp + " was static for " +
                                   numStatic + " reps" +
                                   (numStopped == 0 ? "" :
                                    ", stopped for " + numStopped + " reps"));
                break;
            }

            try {
                Thread.sleep(sleepMSec);
            } catch (InterruptedException ie) {
                // ignore errors
            }
        }

        return numStatic >= staticReps;
    }
}
