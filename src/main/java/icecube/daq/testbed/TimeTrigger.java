package icecube.daq.testbed;

import icecube.daq.trigger.config.DomSetFactory;
import icecube.daq.io.PayloadFileReader;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.payload.impl.TriggerRequestFactory;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;
import icecube.daq.trigger.config.DomSetFactory;
import icecube.daq.trigger.control.ITriggerCollector;
import icecube.daq.trigger.exceptions.TriggerException;
import icecube.daq.util.CodeTimer;
import icecube.daq.util.DOMRegistry;
import icecube.daq.util.LocatePDAQ;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

class MockCollector
    implements ITriggerCollector
{
    public void setChanged()
    {
        // do nothing
    }
}

/**
 * Compare running times for different trigger implementations
 *
 * Run as `java icecube.daq.testbed.TimeTrigger ATrigger BTrigger CTrigger`
 */
public class TimeTrigger
{
    private static final ColoredAppender APPENDER =
        new ColoredAppender(/*org.apache.log4j.Level.ALL).setVerbose(true*/);

    private static final Logger LOG = Logger.getLogger(TimeTrigger.class);

    private static final String HITFILE = "sortedhits.dat";

    private static final String PKG_PREFIX = "icecube.daq.trigger.algorithm.";

    private ITriggerAlgorithm[] list;

    public TimeTrigger(ITriggerAlgorithm[] list)
    {
        this.list = list;
    }

    public boolean configure()
    {
        final File cfgDir = LocatePDAQ.findConfigDirectory();

        DOMRegistry reg;
        try {
            reg = DOMRegistry.loadRegistry(cfgDir);
        } catch (Exception ex) {
            LOG.error("Cannot load DOM registry", ex);
            return false;
        }

        DomSetFactory.setDomRegistry(reg);

        try {
            DomSetFactory.setConfigurationDirectory(cfgDir.getPath());
        } catch (TriggerException tex) {
            LOG.error("Cannot init DomSetFactory", tex);
            return false;
        }

        MockManager mgr = new MockManager();
        mgr.setDOMRegistry(reg);

        MockCollector coll = new MockCollector();

        TriggerRequestFactory factory = new TriggerRequestFactory(null);

        for (ITriggerAlgorithm a : list) {
            a.setSourceId(SourceIdRegistry.INICE_TRIGGER_SOURCE_ID);
            a.setTriggerConfigId(1007);
            a.setTriggerType(14);

            try {
                a.addParameter("multiplicity", "5");
                a.addParameter("coherenceLength", "7");
                a.addParameter("timeWindow", "1500");
                a.addParameter("domSet", "2");
            } catch (TriggerException te) {
                LOG.error("Cannot configure " + a, te);
            }

            a.addReadout(1, 0, 10000, 10000);
            a.addReadout(2, 0, 4000, 6000);

            a.setTriggerCollector(coll);
            a.setTriggerFactory(factory);
            a.setTriggerManager(mgr);
        }

        return true;
    }

    private boolean process()
    {
        PayloadFileReader rdr;
        try {
            rdr = new PayloadFileReader(HITFILE);
        } catch (IOException ioe) {
            LOG.error("Cannot open " + HITFILE, ioe);
            return false;
        }

        CodeTimer timer = new CodeTimer(list.length);

        int count = 0;
        for (IPayload pay : rdr) {
            for (int i = 0; i < list.length; i++) {
                timer.start(i);
                try {
                    list[i].runTrigger(pay);
                    timer.stop(i);
                } catch (TriggerException te) {
                    timer.stop(i);
                    LOG.error(list[i].getTriggerName() +
                              " cannot process hit #" + count, te);
                }
            }
            count++;
        }

        for (ITriggerAlgorithm a : list) {
            a.flush();
        }

        report(timer, count);

        return true;
    }

    private void report(CodeTimer timer, int count)
    {
        System.out.println("Read " + count + " payloads from " + HITFILE);

        String[] names = new String[list.length];
        for (int i = 0; i < list.length; i++) {
            names[i] = list[i].getTriggerName();
            System.out.printf("#%d: %s\n", i, list[i]);
        }
        System.out.println(timer.getStats(names));

    }

    public boolean run()
    {
        if (!configure()) {
            return false;
        }

        return process();
    }

    public static final void main(String[] args)
        throws Exception
    {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure(APPENDER);

        Logger.getRootLogger().setLevel(Level.ERROR);
        APPENDER.setLevel(Level.ERROR);

        ArrayList<ITriggerAlgorithm> alist = new ArrayList<ITriggerAlgorithm>();
        for (int i = 0; i < args.length; i++) {
            Class cls;
            try {
                cls = Class.forName(PKG_PREFIX + args[i]);
            } catch (Exception ex) {
                System.err.println("Cannot find algorithm \"" + args[i] +
                                   "\"");
                continue;
            }

            ITriggerAlgorithm algo;
            try {
                algo = (ITriggerAlgorithm) cls.newInstance();
            } catch (Exception ex) {
                System.err.println("\"" + args[i] +
                                   "\" is not an ITriggerAlgorithm");
                continue;
            }

            String clsname = cls.getName();
            if (!clsname.startsWith(PKG_PREFIX)) {
                System.err.println("I am *so* confused right now! (clsname=" +
                                   clsname + ")");
                continue;
            }

            algo.setTriggerName(clsname.substring(PKG_PREFIX.length()));

            alist.add(algo);
        }

        ITriggerAlgorithm[] list = new ITriggerAlgorithm[alist.size()];
        for (int i = 0; i < list.length; i++) {
            list[i] = alist.get(i);
        }

        TimeTrigger tt = new TimeTrigger(list);
        if (!tt.run()) {
            System.exit(1);
        }
    }
}
