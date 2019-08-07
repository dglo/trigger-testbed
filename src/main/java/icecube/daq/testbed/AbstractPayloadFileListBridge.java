package icecube.daq.testbed;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

/**
 * An output bridge which reads payloads from a list of files qnd writes
 * them to an output channel.
 */
public abstract class AbstractPayloadFileListBridge
    implements Runnable
{
    private static final Logger LOG =
        Logger.getLogger(AbstractPayloadFileListBridge.class);

    private int bundleSize;
    private int writeDelay;
    private int writeCount;

    private String name;
    private File[] files;
    private int curIndex;

    private Thread thread;
    private int numSkipped;
    private int numWritten;
    private int numToSkip;
    private int maxToWrite;

    private boolean paused;
    private Object pauseLock = new Object();

    private boolean stopping;
    private boolean stopped;

    private long lastTime;

    /**
     * Create an output bridge which writes payloads from a list of files.
     *
     * @param name source name
     * @param files list of files to write
     */
    public AbstractPayloadFileListBridge(String name, File[] files)
    {
        this.name = name;
        this.files = files;

        if (files == null || files.length == 0) {
            throw new Error("No files found for " + name);
        }

        curIndex = 0;
    }

    /**
     * Close the output channel.
     */
    void finishThreadCleanup()
    {
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
        return !stopped;
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
    @Override
    public void run()
    {
        ByteBuffer lenBuf = ByteBuffer.allocate(4);

        boolean sendStop = true;
        boolean running = true;
        for (int i = 0; running && i < files.length; i++) {
            ReadableByteChannel chanIn;

            FileInputStream fin;
            try {
                fin = new FileInputStream(files[i]);
            } catch (FileNotFoundException fnfe) {
                throw new Error("Cannot open \"" + files[i] + "\"", fnfe);
            }

            if (files[i].getName().endsWith(".gz")) {
                try {
                    chanIn = Channels.newChannel(new GZIPInputStream(fin));
                } catch (IOException ioe) {
                    throw new Error("Cannot open \"" + files[i] + "\"", ioe);
                }
            } else {
                chanIn = fin.getChannel();
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

                if (stopping) {
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
                    writeWithDelay(buf);
                } catch (IOException ioe) {
                    throw new Error("Couldn't write " + len + " bytes from " +
                                    files[curIndex].getName(), ioe);
                }

                // don't overwhelm other threads
                Thread.yield();

                if (Util.isStopMessage(buf)) {
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
            ByteBuffer buf = Util.buildStopMessage(null);
            if (buf != null) {
                try {
                    writeWithDelay(buf);
                } catch (IOException ioe) {
                    throw new Error("Couldn't write " +
                                    files[curIndex].getName() +
                                    " stop message", ioe);
                }
            }
        }

        lastTime = Long.MAX_VALUE;

        finishThreadCleanup();

        stopped = true;
        stopping = false;
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
        stopping = true;
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

    public abstract void write(ByteBuffer buf)
        throws IOException;

    void writeWithDelay(ByteBuffer buf)
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

        write(buf);

        Thread.yield();
    }

    /**
     * Return a debugging string.
     *
     * @return debugging string
     */
    @Override
    public String toString()
    {
        return name + ":" + files[curIndex].getName() + "#" + numWritten +
            (isPaused() ? ":paused" : "") + (isRunning() ? "" : ":stopped");
    }
}
