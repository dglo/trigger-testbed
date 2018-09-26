package icecube.daq.testbed;

import icecube.daq.common.ANSIEscapeCode;
import icecube.daq.common.IDAQAppender;

import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Mock log4j appender.
 */
public class ColoredAppender
    implements IDAQAppender
{
    /** minimum level of log messages which will be print. */
    private Level minLevel;

    /**
     * Create a ColoredAppender which ignores everything below the WARN level.
     */
    public ColoredAppender()
    {
        this(Level.WARN);
    }

    /**
     * Create a ColoredAppender which ignores everything
     * below the specified level.
     *
     * @param minLevel minimum level
     */
    public ColoredAppender(Level minLevel)
    {
        this.minLevel = minLevel;
    }

    /**
     * Unimplemented.
     *
     * @param x0 ???
     */
    public void addFilter(Filter x0)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Unimplemented.
     */
    @Override
    public void clearFilters()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Nothing needs to be done here.
     */
    @Override
    public void close()
    {
        // don't need to do anything
    }

    /**
     * Handle a logging event.
     *
     * @param evt logging event
     */
    public void doAppend(LoggingEvent evt)
    {
        if (evt.getLevel().toInt() >= minLevel.toInt()) {
            dumpEvent(evt);
        }
    }

    /**
     * Dump a logging event to System.out
     *
     * @param evt logging event
     */
    private void dumpEvent(LoggingEvent evt)
    {
        LocationInfo loc = evt.getLocationInformation();

        System.out.println(ANSIEscapeCode.BG_RED + ANSIEscapeCode.FG_YELLOW +
                           ANSIEscapeCode.ITALIC_ON + evt.getLoggerName() +
                           " " + evt.getLevel() + ANSIEscapeCode.ITALIC_OFF +
                           " [" + loc.fullInfo + "] " + evt.getMessage() +
                           ANSIEscapeCode.OFF);

        String[] stack = evt.getThrowableStrRep();
        for (int i = 0; stack != null && i < stack.length; i++) {
            System.out.println(ANSIEscapeCode.BG_RED +
                               ANSIEscapeCode.FG_YELLOW + "> " + stack[i] +
                               ANSIEscapeCode.OFF);
        }
    }

    /**
     * Unimplemented.
     *
     * @return ???
     */
    public ErrorHandler getErrorHandler()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Unimplemented.
     *
     * @return ???
     */
    public Filter getFilter()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Unimplemented.
     *
     * @return ???
     */
    public Layout getLayout()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Get logging level.
     *
     * @return logging level
     */
    @Override
    public Level getLevel()
    {
        return minLevel;
    }

    /**
     * Unimplemented.
     *
     * @return ???
     */
    @Override
    public String getName()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Is this appender sending log messages?
     *
     * @return <tt>true</tt> if this appender is connected
     */
    @Override
    public boolean isConnected()
    {
        return true;
    }

    /**
     * Is this appender sending log messages to the specified host and port.
     *
     * @param logHost DAQ host name/IP address
     * @param logPort DAQ port number
     * @param liveHost I3Live host name/IP address
     * @param livePort I3Live port number
     *
     * @return <tt>true</tt> if this appender uses the host:port
     */
    public boolean isConnected(String logHost, int logPort, String liveHost,
                               int livePort)
    {
        return true;
    }

    /**
     * Reconnect to the remote socket.
     */
    @Override
    public void reconnect()
    {
        // do nothing
    }

    /**
     * Unimplemented.
     *
     * @return ???
     */
    @Override
    public boolean requiresLayout()
    {
        throw new Error("Unimplemented");
    }

    /**
     * Unimplemented.
     *
     * @param x0 ???
     */
    public void setErrorHandler(ErrorHandler x0)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Unimplemented.
     *
     * @param x0 ???
     */
    public void setLayout(Layout x0)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Set logging level.
     *
     * @param lvl logging level
     *
     * @return this object (so commans can be chained)
     */
    public ColoredAppender setLevel(Level lvl)
    {
        minLevel = lvl;

        return this;
    }

    /**
     * Unimplemented.
     *
     * @param s0 ???
     */
    @Override
    public void setName(String s0)
    {
        throw new Error("Unimplemented");
    }
}
