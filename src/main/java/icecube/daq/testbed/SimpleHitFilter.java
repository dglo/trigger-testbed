package icecube.daq.testbed;

import icecube.daq.payload.MiscUtil;
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
 * Identify all files for a single in-ice or icetop hub
 */
public class SimpleHitFilter
    implements Comparator, FilenameFilter
{
    private static final int NO_NUMBER = Integer.MIN_VALUE;

    private int hubNumber;
    private String hubName;

    SimpleHitFilter(int hubNumber)
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
        int idx;
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

        final String simpleStr = "_simplehits_";
        if (name.indexOf(simpleStr) == idx) {
            idx += simpleStr.length() - 1;
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

    public static File[] listFiles(File topDir, int hubId, int runNumber)
        throws IOException
    {
        File srcDir = topDir;

        // if there's a run subdirectory under the source directory, add that
        final String runStr = Integer.toString(runNumber);
        File subDir = new File(topDir, runStr);
        if (subDir.isDirectory()) {
            srcDir = subDir;
        } else {
            subDir = new File(topDir, "run" + runStr);
            if (subDir.isDirectory()) {
                srcDir = subDir;
            }
        }

        return listFiles(srcDir, hubId);
    }

    public static File[] listFiles(File topDir, int hubId)
        throws IOException
    {
        SimpleHitFilter filter = new SimpleHitFilter(hubId);

        File srcDir = topDir;

        // if there's a hub subdirectory under the source directory, add that
        final String hubName = MiscUtil.formatHubID(hubId);
        File subDir = new File(topDir, hubName);
        if (subDir.isDirectory()) {
            srcDir = subDir;
        } else if (hubName.startsWith("ic")) {
            subDir = new File(srcDir, hubName.substring(2));
            if (subDir.isDirectory()) {
                srcDir = subDir;
            }
        }

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
