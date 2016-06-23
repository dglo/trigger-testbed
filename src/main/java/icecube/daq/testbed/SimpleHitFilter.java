package icecube.daq.testbed;

import icecube.daq.payload.SourceIdRegistry;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A complex object which can be used to filter and sort hit file names.
 */
public class SimpleHitFilter
    implements Comparator, FilenameFilter
{
    private int runNum;
    private String baseName;
    private String simpleBase;
    private String oldBase;

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
        this.baseName = hubName;

        final String simpleFront;
        if (!hubName.startsWith("ichub")) {
            simpleFront = hubName;
        } else {
            simpleFront = hubName.substring(2);
        }

        simpleBase = String.format("%s_simplehits_", simpleFront);
    }

    /**
     * Does this file name start with the expected pattern?
     *
     * @return <tt>true</tt> if the file name starts with the expected pattern
     */
    public boolean accept(File dir, String name)
    {
        if (!name.startsWith(baseName) && !name.startsWith(simpleBase)) {
            return false;
        }

        if (runNum > 0 && name.startsWith(simpleBase)) {
            String tmpBase;
            if (oldBase != null) {
                tmpBase = oldBase;
            } else {
                tmpBase = String.format("%s%d", simpleBase, runNum);
            }

            if (name.startsWith(tmpBase)) {
                oldBase = tmpBase;
                return true;
            }
        }

        return true;
    }

    /**
     * Base filename
     *
     * @return filename
     */
    public String basename()
    {
        if (oldBase != null) {
            return oldBase;
        }

        return baseName;
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

        if (!s1.startsWith(baseName)) {
            if (!s2.startsWith(baseName)) {
                return s1.compareTo(s2);
            }

            return 1;
        } else if (!s2.startsWith(baseName)) {
            return -1;
        }

        int end1 = s1.indexOf("_", baseName.length() + 1);
        if (end1 < 0) {
            return s1.compareTo(s2);
        }

        String sub1 = s1.substring(baseName.length(), end1);
        int num1;
        try {
            num1 = Integer.parseInt(sub1);
        } catch (NumberFormatException nfe) {
            throw new Error(String.format("Cannot parse \"%s\" from \"%s\"",
                                          sub1, s1));
        }

        int end2 = s2.indexOf("_", baseName.length() + 1);
        String sub2 = s2.substring(baseName.length(), end2);
        int num2;
        try {
            num2 = Integer.parseInt(sub2);
        } catch (NumberFormatException nfe) {
            throw new Error(String.format("Cannot parse \"%s\" from \"%s\"",
                                          sub2, s2));
        }

        return num1 - num2;
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

        File[] hitfiles = srcDir.listFiles(filter);
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
