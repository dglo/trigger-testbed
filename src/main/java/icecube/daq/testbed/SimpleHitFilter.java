package icecube.daq.testbed;

import icecube.daq.payload.SourceIdRegistry;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A complex object which can be used to filter and sort hit file names.
 */
public class SimpleHitFilter
    implements Comparator, FilenameFilter
{
    private static final Pattern simplePat =
        Pattern.compile("^(\\S*)hub(\\d+)_simplehits_(\\d+)_(\\d+)_(\\d+)" +
                        "_(\\d+)\\.(\\S+)$");

    private int runNum;
    private String hubBase;

    /**
     * Create a hit file filter.
     *
     * @param hubId (1-86 or 201-211)
     * @param runNum run number
     */
    SimpleHitFilter(int hubId, int runNum)
        throws IllegalArgumentException
    {
        this(getHubName(hubId), runNum);
    }

    /**
     * Create a hit file filter.
     *
     * @param hubName hub name ("ichub##" or "ithub##")
     * @param runNum run number
     */
    SimpleHitFilter(String hubName, int runNum)
    {
        this.runNum = runNum;
        this.hubBase = hubName;
    }

    /**
     * Does this file name start with the expected pattern?
     *
     * @return <tt>true</tt> if the file name starts with the expected pattern
     */
    public boolean accept(File dir, String name)
    {
        if (name.indexOf("_simplehits_") > 0) {
            if (name.startsWith(hubBase)) {
                return true;
            } else if (hubBase.startsWith("ic") &&
                       name.startsWith(hubBase.substring(2)))
            {
                return true;
            }
        }

        if (name.startsWith("i3live") && name.endsWith(hubBase)) {
            return true;
        }

        return false;
    }

    /**
     * Base filename
     *
     * @return filename
     */
    public String basename()
    {
        return hubBase;
    }

    /**
     * Compare two objects.
     *
     * @param o1 first object
     * @param o2 second object
     *
     * @return the usual comparison values
     */
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

    private static final int NO_NUMBER = Integer.MIN_VALUE;

    private int extractFileNumber(String name)
    {
        final String hsBase = "HitSpool";

        // find start of numeric string
        final int idx;
        if (name.startsWith(hubBase)) {
            idx = hubBase.length();
        } else if (name.startsWith(hsBase)) {
            idx = hsBase.length();
        } else {
            idx = -1;
        }

        String numStr;
        if (idx > 0) {
            // find end of numeric string
            int end = name.indexOf("_", idx + 1);
            if (end < 0) {
                end = name.indexOf(".", idx + 1);
                if (end < 0) {
                    throw new Error("Cannot find end of file number in \"" +
                                    name + "\" (hub \"" + hubBase +
                                    "\" hs \"" + hsBase + "\")");
                }
            }

            // extract numeric string
            numStr = name.substring(idx + 1, end);
        } else {
            Matcher mtch = simplePat.matcher(name);
            if (mtch.matches()) {
                numStr = mtch.group(4);
            } else {
                numStr = null;
            }
        }

        if (numStr == null) {
            return NO_NUMBER;
        }

        // convert string to integer value
        try {
            return Integer.parseInt(numStr);
        } catch (NumberFormatException nfe) {
            throw new Error("Cannot parse file number \"" + numStr +
                            "\" from \"" + name +
                            "\" (hub \"" + hubBase +
                            "\" hs \"" + hsBase + "\")");
        }
    }

    /**
     * Do the objects implement the same class?
     *
     * @return <tt>true</tt> if they are the same class
     */
    public boolean equals(Object obj)
    {
        if (obj == null) {
            return false;
        }

        return obj.getClass().getName().equals(getClass().getName());
    }

    /*
     * Return the hub name associated with this ID.
     *
     * @param hubId (1-86 or 201-211)
     *
     * @return hub name
     *
     * @throws IOException if the hubId is invalid
     */
    public static String getHubName(int hubId)
        throws IllegalArgumentException
    {
        if (hubId > 0 && hubId < SourceIdRegistry.ICETOP_ID_OFFSET) {
            return String.format("ichub%02d", hubId);
        } else if (hubId > SourceIdRegistry.ICETOP_ID_OFFSET) {
            return String.format("ithub%02d", hubId -
                                 SourceIdRegistry.ICETOP_ID_OFFSET);
        }

        throw new IllegalArgumentException("Bad hub ID " + hubId);
    }

    public static File[] listFiles(File srcDir, int hubId, int runNum)
        throws IOException
    {
        SimpleHitFilter filter = new SimpleHitFilter(hubId, runNum);

        File runDir = new File(srcDir, String.format("run%06d", runNum));

        File[] hitfiles;
        if (runDir.exists()) {
            hitfiles = runDir.listFiles(filter);
        } else {
            hitfiles = srcDir.listFiles(filter);
        }
        if (hitfiles.length == 0) {
            String runStr;
            if (runNum <= 0) {
                runStr = "";
            } else {
                runStr = " run " + runNum;
            }

            String msg =
                String.format("Cannot find hit files for%s %s",
                              runStr, filter.basename());
            throw new IOException(msg);
        }

        // make sure files are in the correct order
        filter.sort(hitfiles);

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

    /**
     * Sort the list of files.
     *
     * @param files list of files
     */
    public void sort(List<File> files)
    {
        Collections.sort(files, this);
    }
}
