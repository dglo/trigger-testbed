package icecube.daq.testbed;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An output bridge which reads payloads from a list of files qnd writes
 * them to an output channel.
 */
public class PayloadFileListBridge
    implements Runnable
{
    private static final Log LOG =
        LogFactory.getLog(PayloadFileListBridge.class);

    private static final int STOP_MESSAGE_LENGTH = 4;

    private int bundleSize;
    private int writeDelay;
    private int writeCount;

    private File[] files;
    private int curIndex;

    private Thread thread;
    private int numSkipped;
    private int numWritten;
    private int numToSkip;
    private int maxToWrite;

    private boolean paused;
    private Object pauseLock = new Object();

    private boolean stopped;

    private long lastTime;

    private WritableByteChannel chanOut;

    /**
     * Create an output bridge which writes payloads from a list of files.
     *
     * @param files list of files to write
     * @param chanOut output channel
     */
    public PayloadFileListBridge(File[] files, WritableByteChannel chanOut)
    {
        this.files = files;

        if (files == null || files.length == 0) {
            throw new Error("No files found");
        }

        curIndex = 0;

        this.chanOut = chanOut;
        if (chanOut instanceof SelectableChannel &&
            !((SelectableChannel) chanOut).isBlocking())
        {
            throw new Error("Output channel should be blocking");
        }
    }

    /**
     * Build a stop message in the provided byte buffer.
     *
     * @param stopBuf buffer used to create stop message
     *
     * @return stop message
     */
    ByteBuffer buildStopMessage(ByteBuffer stopBuf)
    {
        if (stopBuf == null || stopBuf.capacity() < STOP_MESSAGE_LENGTH) {
            stopBuf = ByteBuffer.allocate(STOP_MESSAGE_LENGTH);
        }
        stopBuf.limit(STOP_MESSAGE_LENGTH);

        stopBuf.putInt(0, STOP_MESSAGE_LENGTH);

        stopBuf.position(0);

        return stopBuf;
    }

    /**
     * Close the output channel.
     */
    void finishThreadCleanup()
    {
        try {
            chanOut.close();
        } catch (IOException ioe) {
            // ignore errors on close
        }

        chanOut = null;
    }

    /**
     * Get the most recent payload time.
     *
     * @return most recent payload time
     */
    public long getLastTime()
    {
        return lastTime;
    }

    /**
     * Get the number of initial payloads which should be skipped.
     *
     * @return number of payloads to skip at the start of the stream
     */
    public int getNumberSkipped()
    {
        return numSkipped;
    }

    /**
     * Get the number of payloads written to the channel.
     *
     * @return number of payloads written
     */
    public int getNumberWritten()
    {
        return numWritten;
    }

    /**
     * Is the input thread paused?
     *
     * @return <tt>true</tt> if the thread is paused
     */
    public boolean isPaused()
    {
        return paused;
    }

    /**
     * Is the input thread running?
     *
     * @return <tt>true</tt> if the input thread is running
     */
    public boolean isRunning()
    {
        return thread != null;
    }

    /**
     * Does this buffer contain a DAQ stop message?
     *
     * @return <tt>true</tt> if this is a stop message
     */
    boolean isStopMessage(ByteBuffer buf)
    {
        return buf.limit() == STOP_MESSAGE_LENGTH &&
            buf.getInt(0) == STOP_MESSAGE_LENGTH;
    }

    /**
     * Pause the input thread.
     */
    public void pause()
    {
        if (!paused) {
            synchronized (pauseLock) {
                paused = true;
                pauseLock.notify();
            }
        }
    }

    /**
     * Read in payloads from a set of files and write them to the channel.
     */
    public void run()
    {
        ByteBuffer lenBuf = ByteBuffer.allocate(4);

        boolean sendStop = true;
        boolean running = true;
        for (int i = 0; running && i < files.length; i++) {
            ReadableByteChannel chanIn;
            try {
                chanIn = new FileInputStream(files[i]).getChannel();
            } catch (FileNotFoundException fnfe) {
                throw new Error("Cannot open \"" + files[i] + "\"", fnfe);
            }

            if (LOG.isInfoEnabled()) {
                LOG.info("Opened " + files[i]);
            }

            if (!sendStop) {
                throw new Error("Found file \"" + files[i] + "\" after stop");
            }

            while (true) {
                lenBuf.rewind();

                while (paused) {
                    synchronized (pauseLock) {
                        try {
                            pauseLock.wait();
                        } catch (InterruptedException ie) {
                            // ignore interrupts
                        }
                    }
                }

                if (stopped) {
                    running = false;
                    break;
                }

                int numBytes;
                try {
                    numBytes = chanIn.read(lenBuf);
                } catch (IOException ioe) {
                    throw new Error("Couldn't read length from " +
                                    files[curIndex].getName(), ioe);
                }

                if (numBytes <= 0) {
                    break;
                }

                if (numBytes < 4) {
                    throw new Error("Incomplete payload (" + numBytes +
                                    " bytes)");
                }

                final int len = lenBuf.getInt(0);
                if (len < 4) {
                    throw new Error("Bad length " + len);
                }

                ByteBuffer buf = ByteBuffer.allocate(len);
                buf.putInt(len);

                while (buf.position() != len) {
                    try {
                        chanIn.read(buf);
                    } catch (IOException ioe) {
                        throw new Error("Couldn't read data from " +
                                        files[curIndex].getName(), ioe);
                    }
                }

                if (len >= 16) {
                    lastTime = buf.getLong(8);
                }

                buf.flip();

                if (numSkipped < numToSkip) {
                    numSkipped++;
                    continue;
                }

                try {
                    write(buf);
                } catch (IOException ioe) {
                    throw new Error("Couldn't write " + len + " bytes from " +
                                    files[curIndex].getName(), ioe);
                }

                // don't overwhelm other threads
                Thread.yield();

                if (isStopMessage(buf)) {
                    sendStop = false;
                    running = false;
                    break;
                }

                numWritten++;

                if (maxToWrite > 0 && numWritten > maxToWrite) {
                    running = false;
                    break;
                }
            }

            try {
                chanIn.close();
            } catch (IOException ioe) {
                // ignore errors on close
            }
        }

        if (sendStop) {
            ByteBuffer buf = buildStopMessage(null);
            if (buf != null) {
                try {
                    write(buf);
                } catch (IOException ioe) {
                    throw new Error("Couldn't write " +
                                    files[curIndex].getName() +
                                    " stop message", ioe);
                }
            }
        }

        lastTime = Long.MAX_VALUE;

        finishThreadCleanup();

        thread = null;
    }

    /**
     * Set the maximum number of payloads to write.
     *
     * @param max maximum number of payloads to write
     */
    public void setMaximumPayloads(int max)
    {
        maxToWrite = max;
    }

    /**
     * Set the number of payloads to skip at the front of the file.
     *
     * @param num number of payloads to skip
     */
    public void setNumberToSkip(int num)
    {
        numToSkip = num;
    }

    /**
     * Set the number of milliseconds to sleep after writing a set of payloads
     *
     * @param count number of payloads to write
     * @param msecSleep milliseconds to sleep after <tt>count</tt> payloads
     */
    public void setWriteDelay(int count, int msecSleep)
    {
        bundleSize = count;
        writeDelay = msecSleep;
    }

    /**
     * Start the input thread.
     */
    public void start()
    {
        numWritten = 0;

        thread = new Thread(this);
        thread.setName(files[curIndex].getName());
        thread.start();
    }

    /**
     * Stop the input thread.
     */
    public void stopThread()
    {
        stopped = true;
        unpause();
    }

    /**
     * Unpause the input thread.
     */
    public void unpause()
    {
        if (paused) {
            synchronized (pauseLock) {
                paused = false;
                pauseLock.notify();
            }
        }
    }

    void write(ByteBuffer buf)
        throws IOException
    {
        if (bundleSize > 0 && writeCount++ > bundleSize) {
            writeCount = 0;
            try {
                Thread.sleep(writeDelay);
            } catch (Exception ex) {
                // do nothing
            }
        }

        int lenOut = chanOut.write(buf);
        if (lenOut != buf.limit()) {
            throw new Error("Expected to write " + buf.limit() +
                            " bytes, not " + lenOut);
        }

        Thread.yield();
    }

    /**
     * Return a debugging string.
     *
     * @return debugging string
     */
    public String toString()
    {
        return files[curIndex].getName() + "#" + numWritten +
            (isPaused() ? ":paused" : "") + (isRunning() ? "" : ":stopped");
    }
}
