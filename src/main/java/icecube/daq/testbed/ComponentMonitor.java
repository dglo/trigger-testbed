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

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

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

    @Override
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

    private void dumpMBeanData(DumpState state, Object obj, Class cls)
    {
        Class[] ifaces = cls.getInterfaces();
        for (int i = 0; i < ifaces.length; i++) {
            if (ifaces[i].getName().endsWith("MBean")) {
                if (ifaces[i].getName().endsWith("DynamicMBean")) {
                    dumpMBeanDynamic(state, (DynamicMBean) obj);
                } else {
                    dumpMBeanInterface(state, obj, ifaces[i]);
                }
                break;
            }
        }

        Class clsSuper = cls.getSuperclass();
        if (clsSuper != null) {
            dumpMBeanData(state, obj, clsSuper);
        }
    }

    private void dumpMBeanDynamic(DumpState state, DynamicMBean mbean)
    {
        MBeanInfo info = mbean.getMBeanInfo();
        MBeanAttributeInfo[] attrs = info.getAttributes();
        for (int i = 0; i < attrs.length; i++) {
            if (!attrs[i].isReadable()) {
                final String beanName = mbean.getMBeanInfo().getClassName();
                System.err.println("ERROR: Unreadable " + beanName + " attr#" +
                                   i + ": " + attrs[i].getName());
            } else {
                Object rtnval;
                try {
                    rtnval = mbean.getAttribute(attrs[i].getName());
                } catch (Exception ex) {
                    ex.printStackTrace();
                    continue;
                }

                state.add(attrs[i].getName(), rtnval);
            }
        }
    }

    private void dumpMBeanInterface(DumpState state, Object obj, Class iface)
    {
        Method[] methods = iface.getMethods();
        for (int i = 0; i < methods.length; i++) {
            Class[] params = methods[i].getParameterTypes();
            if (params != null && params.length > 0) {
                System.err.println("ERROR: " + obj.getClass().getName() +
                                   " MBean method " + iface.getName() +
                                   " should not have any parameters");
                continue;
            }

            String mthdName = methods[i].getName();
            if (mthdName.startsWith("get")) {
                mthdName = mthdName.substring(3);
            } else if (mthdName.startsWith("is")) {
                mthdName = mthdName.substring(2);
            } else {
                System.err.println("ERROR: " + obj.getClass().getName() +
                                   " MBean method " + iface.getName() +
                                   " does not start with \"get\" or \"is\"");
                continue;
            }

            Object rtnval;
            try {
                rtnval = methods[i].invoke(obj);
            } catch (Exception ex) {
                ex.printStackTrace();
                continue;
            }

            state.add(mthdName, rtnval);
        }
    }

    public void dumpMonitoring(PrintStream out, int rep)
    {
        Set<String> names = comp.listMBeans();
        if (names == null || names.size() == 0) {
            return;
        }

        DumpState state = new DumpState(out, getFakeDateString(rep));
        for (String name : names) {
            state.setName(name);
            Object obj;
            try {
                obj = comp.getMBean(name);
            } catch (DAQCompException dce) {
                dce.printStackTrace();
                continue;
            }

            dumpMBeanData(state, obj, obj.getClass());
            state.finish();
        }
    }

    /**
     * Force component to stop.
     */
    @Override
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
    @Override
    public Iterable<AlgorithmStatistics> getAlgorithmStatistics()
    {
        return comp.getTriggerManager().getAlgorithmStatistics();
    }

    /**
     * Return a fake date string based on the interation counter
     *
     * @return monitoring date string
     */
    private String getFakeDateString(int count)
    {
        int min = count * 5;

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

    @Override
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

    @Override
    public String getName()
    {
        return prefix;
    }

    /**
     * Get the component's splicer.
     *
     * @return splicer
     */
    @Override
    public Splicer getSplicer()
    {
        return comp.getSplicer();
    }

    @Override
    public boolean isInputPaused()
    {
        return comp.getReader().isPaused();
    }

    @Override
    public boolean isInputStopped()
    {
        return comp == null || !comp.getReader().isRunning();
    }

    @Override
    public boolean isOutputStopped()
    {
        return comp == null || comp.getWriter().isStopped();
    }

    @Override
    public void pauseInput()
    {
        comp.getReader().pause();
    }

    @Override
    public void resumeInput()
    {
        comp.getReader().unpause();
    }

    class DumpState
    {
        private String indent = "    ";

        private PrintStream out;
        private String dateStr;

        private String objectName;
        private boolean printed;

        DumpState(PrintStream out, String dateStr)
        {
            this.out = out;
            this.dateStr = dateStr;

            objectName = "??UNSET??";
        }

        void add(String name, Object value)
        {
            if (!printed) {
                //out.print(ANSIEscapeCode.FG_YELLOW + ANSIEscapeCode.BG_BLUE);
                out.println(objectName + ": " + dateStr + ":");
                printed = true;
            }
            //out.print(ANSIEscapeCode.FG_YELLOW + ANSIEscapeCode.BG_BLUE);
            out.println(indent + name + ": " + formatMBeanValue(value));
            //out.println(ANSIEscapeCode.OFF);
        }

        void finish()
        {
            if (printed) {
                out.println();
            }
        }

        private String formatMBeanValue(Object obj)
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
                    strBuf.append(formatMBeanValue(Array.get(obj, i)));
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
                    final Object value = map.get(key);
                    strBuf.append('\'').append(formatMBeanValue(key));
                    strBuf.append("': ").append(formatMBeanValue(value));
                }
                strBuf.append("}");
                return strBuf.toString();
            } else {
                return obj.toString();
            }
        }

        void setName(String name)
        {
            objectName = name;
            printed = false;
        }
    }
}
