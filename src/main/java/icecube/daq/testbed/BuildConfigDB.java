package icecube.daq.testbed;

import icecube.daq.trigger.exceptions.ConfigException;
import icecube.daq.util.DOMRegistryException;
import icecube.daq.util.DOMRegistryFactory;
import icecube.daq.util.IDOMRegistry;

import java.io.File;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class BuildConfigDB
{
    private static Logger LOG = Logger.getLogger(BuildConfigDB.class);

    private static final String SQL_DRIVER = "org.sqlite.JDBC";
    private static final String DB_NAME = "confighash.db";

    static {
        BasicConfigurator.configure();

        try {
            // load the sqlite-JDBC driver using the current class loader
            Class.forName(SQL_DRIVER);
        } catch (Exception ex) {
            LOG.error("Cannot load \"" + SQL_DRIVER + "\"", ex);
        }
    }

    private File cfgDir;
    private File dataDir;

    private PreparedStatement tcSelStmt;
    private PreparedStatement tcInsStmt;

    /**
     * Build the configuration hash database.
     *
     * @param args command-line arguments
     *
     * @throws IOException if there is a problem
     */
    public BuildConfigDB(String[] args)
        throws IOException
    {
        processArgs(args);
    }


    /**
     * Process command-line arguments.
     *
     * @param args command-line arguments
     */
    private void processArgs(String[] args)
    {
        boolean usage = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].length() > 1 && args[i].charAt(0) == '-') {
                switch(args[i].charAt(1)) {
                case 'c':
                    i++;
                    File tmpCfg = new File(args[i]);
                    if (!tmpCfg.isDirectory()) {
                        System.err.println("Bad config directory \"" +
                                           tmpCfg + "\"");
                        usage = true;
                    } else {
                        cfgDir = tmpCfg;
                    }
                    break;
                case 'd':
                    i++;
                    File tmpData = new File(args[i]);
                    if (!tmpData.isDirectory()) {
                        System.err.println("Bad data directory \"" +
                                           dataDir + "\"");
                        usage = true;
                    } else {
                        dataDir = tmpData;
                    }
                    break;
                default:
                    System.err.println("Unknown option '" + args[i] + "'");
                    usage = true;
                    break;
                }
            } else if (args[i].length() > 0) {
                System.err.println("Unknown argument '" + args[i] + "'");
                usage = true;
            }
        }

        if (cfgDir == null) {
            System.err.println("Please use '-c' to specify config directory");
            usage = true;
        } else if (!cfgDir.isDirectory()) {
            System.err.println("\"" + cfgDir + "\" is not a directory");
            usage = true;
        }

        if (dataDir == null) {
            System.err.println("Please use '-d' to specify data directory");
            usage = true;
        } else if (!dataDir.isDirectory()) {
            System.err.println("\"" + dataDir + "\" is not a directory");
            usage = true;
        }

        if (usage) {
            String usageMsg = "java " + getClass().getName() + " " +
                " [-c configDirectory]" +
                " [-d dataDirectory]" +
                "";

            throw new IllegalArgumentException(usageMsg);
        }
    }

    /**
     * Perform a DAQ run.
     *
     * @return <tt>true</tt> if the run was successful
     */
    public boolean run()
    {
        IDOMRegistry reg;
        try {
            reg = DOMRegistryFactory.load();
        } catch (DOMRegistryException drex) {
            LOG.error("Failed to load DOMRegistry while updating hashDB",
                      drex);
            return false;
        }

        File[] entries = cfgDir.listFiles();
        for (int i = 0; i < entries.length; i++) {
            String name = entries[i].getName();
            if (!name.endsWith(".xml")) {
                continue;
            }

            // remove the trailing ".xml"
            name = name.substring(0, name.length() - 4);

            // ignore default DOM geometry
            if (name.equals("default-dom-geometry")) {
                continue;
            }

            Configuration cfg;
            try {
                cfg = new Configuration(cfgDir, entries[i].getName(), reg);
            } catch (ConfigException ce) {
                if (!ce.getMessage().contains("No hub specified")) {
                    LOG.error("Ignoring " + name, ce);
                }
                continue;
            }

            final String hash = HashedFileName.hashName(name);
            try {
                ConfigHashDB.stashHash(dataDir, name, hash,
                                       cfg.getTriggerConfigName());
            } catch (SQLException sex) {
                LOG.error("Cannot update config database entry for \"" +
                          name + "\"", sex);
            }
        }

        return true;
    }

    /**
     * Main program.
     *
     * @param args command-line arguments
     *
     * @throws Exception if there is a problem
     */
    public static final void main(String[] args)
        throws Exception
    {
        BuildConfigDB buildDB = new BuildConfigDB(args);
        if (!buildDB.run()) {
            System.exit(1);
        }
    }
}
