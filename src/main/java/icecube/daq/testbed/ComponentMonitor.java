package icecube.daq.testbed;

import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.splicer.Splicer;
import icecube.daq.trigger.algorithm.AlgorithmStatistics;
import icecube.daq.trigger.component.DAQTriggerComponent;
import icecube.daq.trigger.control.ITriggerManager;

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
public class ComponentMonitor
    extends ActivityMonitor
{
    private static final Log LOG = LogFactory.getLog(ComponentMonitor.class);

    private DAQTriggerComponent comp;
    private String prefix;

    ComponentMonitor(DAQTriggerComponent comp, String prefix,
                     PayloadFileListBridge[] bridges, Consumer consumer,
                     int maxFailures)
    {
        super(bridges, consumer, maxFailures);

        this.comp = comp;
        this.prefix = prefix;
    }

    public boolean checkMonitoredObject()
    {
        boolean changed = false;
        if (comp != null) {
            if (getNumberReceived() != comp.getPayloadsReceived()) {
                setNumberReceived(comp.getPayloadsReceived());
                changed = true;
            }
            if (getNumberOfQueuedInputs() !=
                getNumInputsQueued(comp.getTriggerManager()))
            {
                final long val = getNumInputsQueued(comp.getTriggerManager());
                setNumberOfQueuedInputs(val);
                changed = true;
            }
            if (getNumberProcessed() !=
                comp.getTriggerManager().getTotalProcessed())
            {
                final long val = comp.getTriggerManager().getTotalProcessed();
                setNumberProcessed(val);
                changed = true;
            }
            if (getNumberOfQueuedOutputs() !=
                comp.getTriggerManager().getNumOutputsQueued())
            {
                final long val = comp.getTriggerManager().getNumOutputsQueued();
                setNumberOfQueuedOutputs(val);
                changed = true;
            }
            if (getNumberSent() != comp.getPayloadsSent()) {
                setNumberSent(comp.getPayloadsSent());
                changed = true;
            }
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

    public void dumpMonitoring(PrintStream out, int rep)
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

    /**
     * Force component to stop.
     */
    public void forceStop()
    {
        try {
            comp.forcedStop();
        } catch (DAQCompException dce) {
            LOG.error("Cannot forceStop " + comp.getName(), dce);
        }
    }

    /**
     * Format the trigger counts into a string.
     *
     * @return trigger counts string
     */
    public Iterable<AlgorithmStatistics> getAlgorithmStatistics()
    {
        return comp.getTriggerManager().getAlgorithmStatistics();
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

    public String getMonitoredName()
    {
        return comp.toString();
    }

    private static long getNumInputsQueued(ITriggerManager mgr)
    {
        Map<String, Integer> map = mgr.getQueuedInputs();

        long total = 0;
        for (Integer val : map.values()) {
            total += val;
        }

        return total;
    }

    public String getName()
    {
        return prefix;
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

    public boolean isInputPaused()
    {
        return comp.getReader().isPaused();
    }

    public boolean isInputStopped()
    {
        return comp == null || !comp.getReader().isRunning();
    }

    public boolean isOutputStopped()
    {
        return comp == null || comp.getWriter().isStopped();
    }

    public void pauseInput()
    {
        comp.getReader().pause();
    }

    public void resumeInput()
    {
        comp.getReader().unpause();
    }
}
