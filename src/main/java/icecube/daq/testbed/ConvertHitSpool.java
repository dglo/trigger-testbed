package icecube.daq.testbed;

import icecube.daq.common.DAQCmdInterface;
import icecube.daq.io.BufferWriter;
import icecube.daq.io.DispatchException;
import icecube.daq.io.PayloadByteReader;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.MiscUtil;
import icecube.daq.payload.PayloadException;
import icecube.daq.payload.PayloadRegistry;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.payload.impl.PayloadFactory;
import icecube.daq.payload.impl.DOMHit;
import icecube.daq.payload.impl.SimpleHit;
import icecube.daq.util.DOMRegistryFactory;
import icecube.daq.util.DOMInfo;
import icecube.daq.util.IDOMRegistry;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Identify all files for a single in-ice or icetop hub
 */
class HubPayloadFilter
    implements Comparator, FilenameFilter
{
    private static final int NO_NUMBER = Integer.MIN_VALUE;

    private int hubNumber;
    private String hubName;

    HubPayloadFilter(int hubNumber)
        throws IllegalArgumentException
    {
        this.hubNumber = hubNumber;
        this.hubName = MiscUtil.formatHubID(hubNumber);
    }

    /**
     * Does this file name start with the expected pattern?
     *
     * @return <tt>true</tt> if the file name starts with the expected pattern
     */
    public boolean accept(File dir, String name)
    {
        // we've got a hitspool directory
        if (name.startsWith("HitSpool-")) {
            return true;
        }

        // find the hub name within the file name
        int nameStart = -1;
        int nameLen = -1;
        nameStart = name.indexOf(hubName);
        if (nameStart >= 0) {
            nameLen = hubName.length();
        } else if (hubName.startsWith("ic")) {
            nameStart = name.indexOf(hubName.substring(2));
            if (nameStart >= 0) {
                if (nameStart >= 2 &&
                    name.substring(nameStart - 2, 2).equals("it"))
                {
                    return false;
                }

                nameLen = hubName.length() - 2;
            }
        }

        // if there's no hub name, reject the file
        if (nameLen < 0) {
            return false;
        }

        return true;
    }

    /**
     * Compare two objects.
     *
     * @param o1 first object
     * @param o2 second object
     *
     * @return the usual comparison values
     */
    @Override
    public int compare(Object o1, Object o2)
    {
        if (!(o1 instanceof File) || !(o2 instanceof File)) {
            return o1.getClass().getName().compareTo(o2.getClass().getName());
        }

        String s1 = ((File) o1).getName();
        String s2 = ((File) o2).getName();

        final int n1 = extractFileNumber(s1);
        final int n2 = extractFileNumber(s2);
        if (n1 == NO_NUMBER) {
            if (n2 == NO_NUMBER) {
                return s1.compareTo(s2);
            }

            return 1;
        } else if (n2 == NO_NUMBER) {
            return -1;
        }

        return n1 - n2;
    }

    private int extractFileNumber(String name)
    {
        final String hsBase = "HitSpool";

        String numStr;

        // find start of numeric string
        final int idx;
        if (name.startsWith(hubName)) {
            idx = hubName.length();
        } else if (name.startsWith(hsBase)) {
            idx = hsBase.length();
        } else if (name.startsWith("ic" + hubName)) {
            idx = hubName.length() + 2;
        } else {
            idx = -1;
        }

        if (idx <= 0) {
            return NO_NUMBER;
        }

        // find end of numeric string
        int end = name.indexOf("_", idx + 1);
        if (end < 0) {
            end = name.indexOf(".", idx + 1);
            if (end < 0) {
                throw new Error("Cannot find end of file number in" +
                                "  \"" + name + "\" (hub \"" +
                                hubName + "\" hs \"" + hsBase + "\")");
            }
        }

        // extract numeric string
        numStr = name.substring(idx + 1, end);

        // convert string to integer value
        try {
            return Integer.parseInt(numStr);
        } catch (NumberFormatException nfe) {
            throw new Error("Cannot parse file number \"" + numStr +
                            "\" from \"" + name +
                            "\" (hub \"" + hubName +
                            "\" hs \"" + hsBase + "\")");
        }
    }

    public String getHubName()
    {
        return hubName;
    }

    public static File[] listFiles(File srcDir, int hubId)
        throws IOException
    {
        HubPayloadFilter filter = new HubPayloadFilter(hubId);

        File[] hitfiles = srcDir.listFiles(filter);
        if (hitfiles.length > 0) {
            // make sure files are in the correct order
            filter.sort(hitfiles);
        }

        return hitfiles;
    }

    /**
     * Sort the list of files.
     *
     * @param files list of files
     */
    public void sort(File[] files)
    {
        Arrays.sort(files, this);
    }
}

public class ConvertHitSpool
{
    private static final Logger LOG = Logger.getLogger(ConvertHitSpool.class);

    private static final String INDENT = "   ";

    // DAQ ticks per second
    private static final long TICKS_PER_SECOND = 10000000000L;

    // allow 0.1 second between the end of one file and start of the next
    private static final long MAX_UTC_DIFFERENCE = TICKS_PER_SECOND / 10L;

    private File srcDir;
    private File destDir;
    private int hubNumber = Integer.MAX_VALUE;
    private int runNumber = Integer.MAX_VALUE;
    private long maxPayloads = Long.MAX_VALUE;
    private long startUTC = Long.MAX_VALUE;
    private long endUTC = Long.MAX_VALUE;
    private boolean includeLC0Hits;
    private boolean verbose;
    private boolean printIntervals;
    private String hubName;
    private File[] files;

    private IDOMRegistry registry;
    private PayloadFactory factory;

    private void finishSourceDirectory()
        throws IllegalArgumentException
    {
        File newDir = srcDir;

        // if there's a run subdirectory under the source directory, add that
        final String runStr = Integer.toString(runNumber);
        File subDir = new File(srcDir, runStr);
        if (subDir.isDirectory()) {
            newDir = subDir;
        } else {
            subDir = new File(srcDir, "run" + runStr);
            if (subDir.isDirectory()) {
                newDir = subDir;
            }
        }

        // if there's a hub subdirectory under the source directory, add that
        final String hubName = MiscUtil.formatHubID(hubNumber);
        subDir = new File(newDir, hubName);
        if (subDir.isDirectory()) {
            newDir = subDir;
        } else if (hubName.startsWith("ic")) {
            subDir = new File(newDir, hubName.substring(2));
            if (subDir.isDirectory()) {
                newDir = subDir;
            }
        }

        // reassign the source directory in case we modified it
        srcDir = newDir;
    }

    private void dumpSettings()
    {
        System.out.println("Source: " + srcDir);
        System.out.println("Destination: " + destDir);
        System.out.println("Run: " + runNumber);
        System.out.println("Hub#" + hubNumber + ": " + hubName);

        if (printIntervals) {
            System.out.println("Print intervals");
        } else {
            if (maxPayloads != Long.MAX_VALUE) {
                System.out.println("Max payloads: " + maxPayloads);
            } else {
                System.out.println("Max payloads: NONE");
            }
            if (startUTC != Long.MAX_VALUE) {
                System.out.println("Start time: " + startUTC);
            } else {
                System.out.println("Start time: NONE");
            }
            if (endUTC != Integer.MAX_VALUE) {
                System.out.println("Duration: " + (endUTC - startUTC));
            } else {
                System.out.println("Duration: NONE");
            }
            System.out.println("Include LC0 hits: " +
                               (includeLC0Hits ? "yes" : "NO"));
        }

        System.out.println("Verbose: " + (verbose ? "yes" : "NO"));
        System.out.println("Files:");
        for (File file : files) {
            System.out.println("\t" + file.getName());
        }
        System.out.flush();
    }

    private void initialize()
        throws IllegalArgumentException
    {
        // load default DOM geometry file
        try {
            registry = DOMRegistryFactory.load();
        } catch (Throwable thr) {
            LOG.error("Cannot load DOM registry");
            thr.printStackTrace();
            System.exit(1);
        }

        // create a factory which will be used to build hit payloads
        factory = new PayloadFactory(null);
    }

    private void printIntervals()
    {
        long lastUTC = Long.MIN_VALUE;
        File lastFile = null;

        int numFiles = 0;
        for (File file : files) {
            if (verbose) {
                System.out.printf("File %d of %d: %s\n", ++numFiles,
                                  files.length, file.getName());
                System.out.flush();
            }

            PayloadByteReader rdr;
            try {
                rdr = new PayloadByteReader(file);
            } catch (IOException ioe) {
                System.err.println("Cannot read \"" + file.getName() + "\":");
                ioe.printStackTrace();
                continue;
            }

            long firstUTC = Long.MIN_VALUE;

            long numPayloads = 0;
            for (ByteBuffer buf : rdr) {
                ILoadablePayload pay;
                try {
                    pay = (ILoadablePayload) factory.getPayload(buf, 0);
                    if (!(pay instanceof DOMHit)) {
                        System.err.println("Payload #" + numPayloads +
                                           " is not a hit: " +
                                           pay.getClass().getName());
                        continue;
                    }

                    try {
                        pay.loadPayload();
                    } catch (IOException ioe) {
                        System.err.println("Cannot load payload#" +
                                           numPayloads);
                        ioe.printStackTrace();
                        continue;
                    }
                } catch (PayloadException pex) {
                    System.err.println("For payload #" + numPayloads + ":");
                    pex.printStackTrace();
                    continue;
                }

                DOMHit hit = (DOMHit) pay;

                // check for gaps between files
                if (firstUTC == Long.MIN_VALUE) {
                    firstUTC = hit.getUTCTime();

                    // if we have the last time from a previous file...
                    if (lastUTC != Long.MIN_VALUE) {
                        final long diff = firstUTC - lastUTC;
                        if (diff > MAX_UTC_DIFFERENCE) {
                            final String diffStr =
                                MiscUtil.formatDurationTicks(diff);
                            final String msg = "WARNING: Significant gap (" +
                                diffStr + ") between the end of " +
                                lastFile.getName() + " and the start of " +
                                file.getName();
                            System.err.println(msg);
                        }
                    }
                }

                // save this hit's UTC time in case it's the last payload
                lastUTC = hit.getUTCTime();
            }

            final String durStr =
                MiscUtil.formatDurationTicks(lastUTC - firstUTC);
            System.out.println(file.getName() + ": " + firstUTC + "-" +
                               lastUTC + " (" + durStr + ")");

            // remember this file in case we need it for error messages
            lastFile = file;
        }
    }

    private void processArgs(String[] args)
    {
        String destStr = null;
        String srcStr = null;
        int duration = Integer.MAX_VALUE;

        ArrayList<File> fileList = new ArrayList<File>();

        boolean getDestDir = false;
        boolean getDuration = false;
        boolean getHubNum = false;
        boolean getMax = false;
        boolean getRun = false;
        boolean getSrcDir = false;
        boolean getStartTime = false;

        boolean usage = false;

        for (int i = 0; i < args.length; i++) {
            if (getDestDir) {
                destStr = args[i];
                getDestDir = false;
                continue;
            }

            if (getDuration) {
                try {
                    int tmp = MiscUtil.extractDurationSeconds(args[i]);
                    duration = tmp;
                } catch (NumberFormatException nfe) {
                    System.err.println("Bad duration \"" + args[i] + "\"");
                    usage = true;
                }

                getDuration = false;
                continue;
            }

            if (getHubNum) {
                try {
                    int tmp = Integer.parseInt(args[i]);
                    hubNumber = tmp;
                } catch (NumberFormatException nfe) {
                    System.err.println("Bad hub number \"" + args[i] + "\"");
                    usage = true;
                }

                getHubNum = false;
                continue;
            }

            if (getMax) {
                try {
                    long tmp = Long.parseLong(args[i]);
                    maxPayloads = tmp;
                } catch (NumberFormatException nfe) {
                    System.err.println("Bad number of payloads \"" + args[i] +
                                       "\"");
                    usage = true;
                }

                getMax = false;
                continue;
            }

            if (getRun) {
                try {
                    int tmp = Integer.parseInt(args[i]);
                    runNumber = tmp;
                } catch (NumberFormatException nfe) {
                    System.err.println("Bad run number \"" + args[i] + "\"");
                    usage = true;
                }

                getRun = false;
                continue;
            }

            if (getSrcDir) {
                srcStr = args[i];
                getSrcDir = false;
                continue;
            }

            if (getStartTime) {
                try {
                    long tmp = Long.parseLong(args[i]);
                    startUTC = tmp;
                } catch (NumberFormatException nfe) {
                    System.err.println("Bad start time \"" + args[i] +
                                       "\", only DAQ ticks are allowed");
                    usage = true;
                }

                getStartTime = false;
                continue;
            }

            if (args[i].length() > 1 && args[i].charAt(0) == '-') {
                switch(args[i].charAt(1)) {
                case 'D':
                    if (args[i].length() == 2) {
                        getDestDir = true;
                    } else {
                        destStr = args[i].substring(2);
                    }
                    break;
                case 'H':
                    if (args[i].length() == 2) {
                        getHubNum = true;
                    } else {
                        try {
                            int tmp = Integer.parseInt(args[i].substring(2));
                            hubNumber = tmp;
                        } catch (NumberFormatException nfe) {
                            System.err.println("Bad hub number \"" +
                                               args[i].substring(2) + "\"");
                            usage = true;
                        }
                    }
                    break;
                case 'I':
                    printIntervals = true;
                    break;
                case 'S':
                    if (args[i].length() == 2) {
                        getSrcDir = true;
                    } else {
                        srcStr = args[i].substring(2);
                    }
                    break;
                case 'd':
                    if (args[i].length() == 2) {
                        getDuration = true;
                    } else {
                        try {
                            int tmp = Integer.parseInt(args[i].substring(2));
                            duration = tmp;
                        } catch (NumberFormatException nfe) {
                            System.err.println("Bad duration \"" +
                                               args[i].substring(2) + "\"");
                            usage = true;
                        }
                    }
                    break;
                case 'f':
                    includeLC0Hits = true;
                    break;
                case 'n':
                    if (args[i].length() == 2) {
                        getMax = true;
                    } else {
                        try {
                            long tmp = Long.parseLong(args[i].substring(2));
                            maxPayloads = tmp;
                        } catch (NumberFormatException nfe) {
                            System.err.println("Bad number of payloads \"" +
                                               args[i].substring(2) + "\"");
                            usage = true;
                        }
                    }
                    break;
                case 'r':
                    if (args[i].length() == 2) {
                        getRun = true;
                    } else {
                        try {
                            int tmp = Integer.parseInt(args[i].substring(2));
                            runNumber = tmp;
                        } catch (NumberFormatException nfe) {
                            System.err.println("Bad run number \"" +
                                               args[i].substring(2) + "\"");
                            usage = true;
                        }
                    }
                    break;
                case 's':
                    if (args[i].length() == 2) {
                        getStartTime = true;
                    } else {
                        try {
                            long tmp = Long.parseLong(args[i].substring(2));
                            startUTC = tmp;
                        } catch (NumberFormatException nfe) {
                            System.err.println("Bad start time \"" +
                                               args[i].substring(2) + "\"");
                            usage = true;
                        }
                    }
                    break;
                case 'v':
                    verbose = true;
                    break;
                default:
                    System.err.println("Bad option \"" + args[i] + "\"");
                    usage = true;
                    break;
                }

                continue;
            }

            File f = new File(args[i]);
            if (f.exists()) {
                fileList.add(f);
            } else {
                System.err.println("Ignoring nonexistent file \"" + f + "\"");
                usage = true;
            }
        }

        if (hubNumber == Integer.MAX_VALUE) {
            System.err.println("Please specify the hub number for these hits");
            usage = true;
        } else {
            final int tmpNum = hubNumber % 1000;
            if (tmpNum >= 200 && tmpNum <= 211) {
                hubName = String.format("ithub%02d", tmpNum - 200);
            } else if (tmpNum > 0 && tmpNum <= 86) {
                hubName = String.format("ichub%02d", tmpNum);
            } else {
                System.err.println("Bad hub number " + hubNumber);
                usage = true;
            }
        }

        if (!usage && runNumber == Integer.MAX_VALUE) {
            System.err.println("Please specify the run number");
            usage = true;
        }

        if (srcStr == null) {
            System.err.println("Please specify the source directory");
            usage = true;
        } else {
            srcDir = new File(srcStr);
            if (!srcDir.isDirectory()) {
                System.err.println("Source directory \"" + srcStr +
                                   "\" does not exist");
                usage = true;
            }
        }

        if (destStr != null) {
            destDir = new File(destStr);
            if (!destDir.isDirectory()) {
                System.err.println("Destination directory \"" + destStr +
                                   "\" does not exist");
                usage = true;
            }
        } else {
            destDir = new File(System.getProperty("user.dir"));
            if (!destDir.isDirectory()) {
                System.err.println("User's home directory \"" + destDir +
                                   "\" does not exist!");
                usage = true;
            } else {
                System.err.println("NOTE: Writing output files to \"" +
                                   destDir + "\"");
            }
        }

        if ((startUTC != Long.MAX_VALUE && duration == Integer.MAX_VALUE) ||
            (startUTC == Long.MAX_VALUE && duration != Integer.MAX_VALUE))
        {
            System.err.println("Both start time and duration must be" +
                               " specified (or neither)");
            usage = true;
        } else if (startUTC != Long.MAX_VALUE) {
            endUTC = startUTC + ((long) duration * TICKS_PER_SECOND);
        }

        // add run/hub subdirectories to srcDir
        if (!usage) {
            finishSourceDirectory();

            if (fileList.size() > 0) {
                files = fileList.toArray(new File[0]);
            } else {
                try {
                    files = HubPayloadFilter.listFiles(srcDir, hubNumber);
                    if (files.length == 0) {
                        System.err.println("Cannot find files for hub #" +
                                           hubNumber + " in " + srcDir);
                        usage = true;
                    }
                } catch (IOException ioe) {
                    System.err.println("Cannot find files for hub #" +
                                       hubNumber);
                    ioe.printStackTrace();
                    usage = true;
                }
            }
        }

        if (usage) {
            System.err.print("Usage: ");
            System.err.print("java ConvertHitSpool");
            System.err.print(" -D destinationDirectory");
            System.err.print(" -H hubNumber");
            System.err.print(" [-I(ntervalsOnly)]");
            System.err.print(" -S sourceDirectory");
            System.err.print(" [-d duration]");
            System.err.print(" -f(orwardLC0Hits)");
            System.err.print(" [-n numToDump]");
            System.err.print(" -r runNumber");
            System.err.print(" [-s startingTick]");
            System.err.print(" [-v(erbose)]");
            System.err.print(" [payloadFile ...]");
            System.err.println();
            System.exit(1);
        }

        if (verbose) {
            dumpSettings();
        }
    }

    public void run()
    {
        try {
            initialize();
        } catch (IllegalArgumentException iae) {
            iae.printStackTrace();
            return;
        }

        if (printIntervals) {
            printIntervals();
        } else {
            String filename = String.format("%s_simplehits_%06d_0_999999.dat",
                                            hubName, runNumber);
            File path = new File(destDir, filename);
            BufferWriter out;
            try {
                out = new BufferWriter(path, false);
            } catch (IOException ioe) {
                System.err.println("Cannot open output file \"" + path +
                                   "\":");
                ioe.printStackTrace();
                return;
            }

            try {
                simplifyHits(out);
            } catch (IOException ioe) {
                System.err.println("Cannot write all hits:");
                ioe.printStackTrace();
            } finally {
                try {
                    out.close();
                } catch (Throwable thr) {
                    thr.printStackTrace();
                }
            }
        }
    }

    private void simplifyHits(BufferWriter out)
        throws IOException
    {
        long firstWrittenUTC = Long.MIN_VALUE;
        long lastWrittenUTC = Long.MIN_VALUE;
        long numWritten = 0;

        File lastFile = null;
        long lastUTC = Long.MIN_VALUE;

        int numFiles = 0;
        for (File file : files) {
            if (verbose) {
                System.out.printf("File %d of %d: %s\n", ++numFiles,
                                  files.length, file.getName());
                System.out.flush();
            }

            long firstUTC = Long.MIN_VALUE;
            long numPayloads = 0;
            PayloadByteReader rdr = new PayloadByteReader(file);
            for (ByteBuffer buf : rdr) {
                if (numPayloads++ >= maxPayloads) {
                    break;
                }

                ILoadablePayload pay;
                try {
                    pay = (ILoadablePayload) factory.getPayload(buf, 0);
                    if (!(pay instanceof DOMHit)) {
                        System.err.println("Payload #" + numPayloads +
                                           " is not a hit: " +
                                           pay.getClass().getName());
                        continue;
                    }

                    pay.loadPayload();
                } catch (PayloadException pex) {
                    System.err.println("For payload #" + numPayloads + ":");
                    pex.printStackTrace();
                    continue;
                }

                DOMHit hit = (DOMHit) pay;

                // check for gaps between files
                if (firstUTC == Long.MIN_VALUE) {
                    firstUTC = hit.getUTCTime();

                    // if we have the last time from a previous file...
                    if (firstUTC >= startUTC && lastUTC != Long.MIN_VALUE) {
                        final long diff = firstUTC - lastUTC;
                        if (diff > MAX_UTC_DIFFERENCE) {
                            final String msg = "WARNING: Significant gap (" +
                                diff + " ticks) between the end of " +
                                lastFile.getName() + " and the start of " +
                                file.getName();
                            System.err.println(msg);
                        }
                    }
                }

                // save this hit's UTC time in case it's the last payload
                lastUTC = hit.getUTCTime();

                // reject hits outside the start/duration range
                if (lastUTC < startUTC) {
                    // too soon
                    continue;
                } else if (endUTC != Long.MIN_VALUE && lastUTC >= endUTC) {
                    // too late
                    break;
                }

                // reject beacon hits
                if (!includeLC0Hits && hit.getLocalCoincidenceMode() == 0 &&
                    hit.getTriggerMode() != 4)
                {
                    continue;
                }

                ByteBuffer simple;
                try {
                    simple = hit.getHitBuffer(null, registry);
                } catch (PayloadException pex) {
                    System.err.println("For payload #" + numPayloads + ":");
                    pex.printStackTrace();
                    continue;
                }

                try {
                    out.write(simple);
                } catch (IOException ioe) {
                    System.err.println("For payload #" + numPayloads + ":");
                    ioe.printStackTrace();
                    continue;
                }

                // update times and count for final summary
                if (firstWrittenUTC == Long.MIN_VALUE) {
                    firstWrittenUTC = hit.getUTCTime();
                }
                lastWrittenUTC = hit.getUTCTime();
                numWritten++;
            }

            // give up if last payload was past the end time
            if (endUTC != Long.MIN_VALUE && lastUTC >= endUTC) {
                break;
            }

            // remember this file for the next iteration
            lastFile = file;
        }

        if (numWritten == 0) {
            System.err.println("ERROR: No hits written!");
        } else {
            final String durStr =
                MiscUtil.formatDurationTicks(lastWrittenUTC - firstWrittenUTC);
            System.out.println("Wrote " + numWritten + " hits covering " +
                               durStr + " [" + firstWrittenUTC + "-" +
                               lastWrittenUTC + "]");
        }
    }

    public static final void main(String[] args)
    {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);

        ConvertHitSpool cvtHS = new ConvertHitSpool();
        cvtHS.processArgs(args);

        cvtHS.run();
    }
}
