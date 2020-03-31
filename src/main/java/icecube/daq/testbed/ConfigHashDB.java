package icecube.daq.testbed;

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

public class ConfigHashDB
{
    /** Log object for this class */
    private static final Logger LOG = Logger.getLogger(ConfigHashDB.class);

    private static final String SQL_DRIVER = "org.sqlite.JDBC";
    private static final String DB_NAME = ".confighash.db";

    private static final Object DB_LOCK = new Object();
    private static Connection DB_CONN;

    private static PreparedStatement SELECT_TRIGCFG;
    private static PreparedStatement INSERT_TRIGCFG;

    static {
        BasicConfigurator.configure();

        try {
            // load the sqlite-JDBC driver using the current class loader
            Class.forName(SQL_DRIVER);
        } catch (Exception ex) {
            LOG.error("Cannot load \"" + SQL_DRIVER + "\"", ex);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run()
                {
                    synchronized (DB_LOCK) {
                        if (DB_CONN != null) {
                            try {
                                DB_CONN.close();
                            } catch (SQLException sex) {
                                LOG.error("Cannot close " + DB_NAME, sex);
                            }
                        }
                    }
                }
            });
    }

    private static final Statement getStatement()
        throws SQLException
    {
        if (DB_CONN == null) {
            throw new SQLException(DB_NAME + " database has not been opened");
        }

        Statement stmt;
        synchronized (DB_CONN) {
            try {
                stmt = DB_CONN.createStatement();
                stmt.setQueryTimeout(30);
            } catch (SQLException sex) {
                throw new SQLException("Cannot create " + DB_NAME +
                                       " statement", sex);
            }
        }

        return stmt;
    }

    private static final int getTriggerConfigID(String trigCfgName)
        throws SQLException
    {
        if (DB_CONN == null) {
            throw new SQLException(DB_NAME + " database has not been opened");
        }

        if (SELECT_TRIGCFG == null) {
            synchronized (DB_CONN) {
                SELECT_TRIGCFG =
                    DB_CONN.prepareStatement("select id from trigconfig" +
                                             " where name=?");
            }
        }

        int id = Integer.MIN_VALUE;

        // try to find this trigger configuration name
        synchronized (SELECT_TRIGCFG) {
            SELECT_TRIGCFG.setString(1, trigCfgName);
            ResultSet rs = SELECT_TRIGCFG.executeQuery();
            while (rs.next()) {
                id = rs.getInt(1);
            }
            try {
                rs.close();
            } catch (SQLException se) {
                // ignore errors on close
            }
        }

        // if we found the name, return the ID
        if (id > Integer.MIN_VALUE) {
            return id;
        }

        if (INSERT_TRIGCFG == null) {
            synchronized (DB_CONN) {
                INSERT_TRIGCFG =
                    DB_CONN.prepareStatement("insert or ignore into" +
                                             " trigconfig(name) values (?)");
            }
        }

        synchronized (INSERT_TRIGCFG) {
            INSERT_TRIGCFG.setString(1, trigCfgName);
            INSERT_TRIGCFG.executeUpdate();
        }

        return getTriggerConfigID(trigCfgName);
    }

    private static final void initializeDatabase()
        throws SQLException
    {
        Statement stmt;
        try {
            stmt = getStatement();
        } catch (SQLException sex) {
            throw new SQLException("Cannot initialize " + DB_NAME, sex);
        }

        try {
            try {
                stmt.executeUpdate("pragma foreign_keys = ON");
            } catch (SQLException sex) {
                LOG.error("Cannot enable SQLite foreign key support for " +
                          DB_NAME, sex);
            }

            boolean createdTrigCfg;
            try {
                stmt.executeUpdate("create table if not exists" +
                                   " trigconfig(" +
                                   "id integer primary key autoincrement," +
                                   "name string)");
                createdTrigCfg = true;
            } catch (SQLException sex) {
                LOG.error("Cannot create trigconfig database in " +
                          DB_NAME, sex);
                createdTrigCfg = false;
            }

            if (createdTrigCfg) {
                try {
                    stmt.executeUpdate("create unique index" +
                                       " if not exists tc_index" +
                                       " on trigconfig(name)");
                } catch (SQLException sex) {
                    LOG.error("Cannot create trigconfig index in " +
                              DB_NAME, sex);
                }

                try {
                    stmt.executeUpdate("create table if not exists " +
                                       " runconfig(" +
                                       " id integer primary key" +
                                       "  autoincrement," +
                                       "name string," +
                                       "hash string," +
                                       "trigconfig_id integer," +
                                       "foreign key(trigconfig_id)" +
                                       " references trigconfig(id))");
                } catch (SQLException sex) {
                    LOG.error("Cannot create runconfig database in " +
                              DB_NAME, sex);
                }
            }

            boolean createdRunCfg;
            try {
                stmt.executeUpdate("create unique index if not exists" +
                                   " rc_nameindex on runconfig(name)");
                createdRunCfg = true;
            } catch (SQLException sex) {
                LOG.error("Cannot create runconfig name index in " +
                          DB_NAME, sex);
                createdRunCfg = false;
            }

            if (createdRunCfg) {
                try {
                    stmt.executeUpdate("create unique index" +
                                       " if not exists rc_hashindex" +
                                       " on runconfig(hash)");
                } catch (SQLException sex) {
                    LOG.error("Cannot create runconfig hash index in " +
                              DB_NAME, sex);
                }
            }
        } finally {
            try {
                stmt.close();
            } catch (SQLException sex) {
                LOG.error("Ignoring error while initializing " + DB_NAME, sex);
            }
        }
    }

    private static final void openDatabase(File dataDir)
        throws SQLException
    {
        synchronized (DB_LOCK) {
            if (DB_CONN == null) {
                File dataFile = new File(dataDir, DB_NAME);

                String path;
                try {
                    path = dataFile.getCanonicalPath();
                } catch (IOException ioe) {
                    throw new SQLException("Cannot build canonical path" +
                                           "  for \"" + dataFile + "\"", ioe);
                }

                try {
                    DB_CONN = DriverManager.getConnection("jdbc:sqlite:" +
                                                          path);
                } catch (SQLException sex) {
                    throw new SQLException("Cannot open " + DB_NAME, sex);
                }
            }

            initializeDatabase();
        }
    }

    public static final void stashHash(File dataDir, String runCfgName,
                                       String hash, String trigCfgName)
        throws SQLException
    {
        if (DB_CONN == null) {
            openDatabase(dataDir);
        }

        PreparedStatement pstmt =
            DB_CONN.prepareStatement("insert or ignore into runconfig" +
                                     "(name, hash, trigconfig_id)" +
                                     " values (?, ?, ?)");

        pstmt.setString(1, runCfgName);
        pstmt.setString(2, hash);
        if (trigCfgName == null) {
            pstmt.setNull(3, java.sql.Types.INTEGER);
        } else {
            final int tcid = getTriggerConfigID(trigCfgName);
            pstmt.setInt(3, tcid);
        }

        pstmt.execute();
        pstmt.close();
    }
}
