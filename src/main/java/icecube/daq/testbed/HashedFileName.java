package icecube.daq.testbed;

import icecube.daq.payload.SourceIdRegistry;

import java.math.BigInteger;
import java.security.MessageDigest;

/**
 * Build a compact filename representation of the relevant run quantities.
 */
public abstract class HashedFileName
{
    /**
     * Get the abbreviation for this source ID.
     *
     * @param srcId source ID
     *
     * @return short string
     */
    private static String getShortComponent(int srcId)
    {
        switch (srcId) {
        case SourceIdRegistry.INICE_TRIGGER_SOURCE_ID:
            return "iit";
        case SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID:
            return "itt";
        case SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID:
            return "glbl";
        default:
            break;
        }

        throw new Error("Unknown source ID " + srcId);
    }

    /**
     * Build a hash code from the run configuration file name.
     *
     * @param runCfgName string to hash
     *
     * @return hash string
     */
    public static String hashName(String runCfgName)
    {
        String baseName;
        if (runCfgName.endsWith(".xml")) {
            baseName = runCfgName.substring(0, runCfgName.length() - 4);
        } else {
            baseName = runCfgName;
        }

        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (Exception ex) {
            return "OLD" + oldHashName(runCfgName);
        }

        md.update(baseName.getBytes(), 0, baseName.length());

        BigInteger bigInt = new BigInteger(1, md.digest());
        return bigInt.toString(16);
    }

    /**
     * Build a hashed filename.
     *
     * @param runCfgName run configuration filename
     * @param srcId component source ID
     * @param runNumber run number
     * @param numSrcs number of sources used in the run
     * @param numToSkip number of initial payloads skipped in the run
     * @param numToProcess number of payloads processed in the run
     *
     * @return hashed filename
     */
    public static final String getName(String runCfgName, int srcId,
                                       int runNumber, int numSrcs,
                                       int numToSkip, int numToProcess)
    {
        return getName(runCfgName, srcId, runNumber, -1, numSrcs, numToSkip,
                       numToProcess, false);
    }

    /**
     * Build a hashed filename.
     *
     * @param runCfgName run configuration filename
     * @param srcId component source ID
     * @param runNumber run number
     * @param numSrcs number of sources used in the run
     * @param numToSkip number of initial payloads skipped in the run
     * @param numToProcess number of payloads processed in the run
     *
     * @return hashed filename
     */
    public static final String getName(String runCfgName, int srcId,
                                       int runNumber, int trigId, int numSrcs,
                                       int numToSkip, int numToProcess)
    {
        return getName(runCfgName, srcId, runNumber, trigId, numSrcs,
                       numToSkip, numToProcess, false);
    }

    /**
     * Build a hashed filename.
     *
     * @param runCfgName run configuration filename
     * @param srcId component source ID
     * @param runNumber run number
     * @param numSrcs number of sources used in the run
     * @param numToSkip number of initial payloads skipped in the run
     * @param numToProcess number of payloads processed in the run
     * @param useOldHash if <tt>true</tt> use old hash algorithm
     *
     * @return hashed filename
     */
    public static final String getName(String runCfgName, int srcId,
                                       int runNumber, int numSrcs,
                                       int numToSkip, int numToProcess,
                                       boolean useOldHash)
    {
        return getName(runCfgName, srcId, runNumber, -1, numSrcs, numToSkip,
                      numToProcess, useOldHash);
    }

    /**
     * Build a hashed filename.
     *
     * @param runCfgName run configuration filename
     * @param srcId component source ID
     * @param runNumber run number
     * @param trigId if greater than 0, ID of single trigger algorithm
     * @param numSrcs number of sources used in the run
     * @param numToSkip number of initial payloads skipped in the run
     * @param numToProcess number of payloads processed in the run
     * @param useOldHash if <tt>true</tt> use old hash algorithm
     *
     * @return hashed filename
     */
    public static final String getName(String runCfgName, int srcId,
                                       int runNumber, int trigId, int numSrcs,
                                       int numToSkip, int numToProcess,
                                       boolean useOldHash)
    {
        String cfgHash;
        if (useOldHash) {
            cfgHash = oldHashName(runCfgName);
        } else {
            cfgHash = hashName(runCfgName);
        }

        String compType = getShortComponent(srcId);

        String trigStr;
        if (trigId <= 0) {
            trigStr = "";
        } else {
            trigStr = "-t" + trigId;
        }

        String skipStr;
        if (numToSkip == 0) {
            skipStr = "";
        } else {
            skipStr = "-s" + numToSkip;
        }

        return "rc" + cfgHash + "-" + compType + "-r" + runNumber + trigStr +
            "-h" + numSrcs + skipStr + "-p" + numToProcess +
            ".dat";
    }

    /**
     * Build a less unique hash code from the run configuration file name.
     *
     * @param runCfgName string to hash
     *
     * @return hash string
     */
    public static String oldHashName(String runCfgName)
    {
        String baseName;
        if (runCfgName.endsWith(".xml")) {
            baseName = runCfgName.substring(0, runCfgName.length() - 4);
        } else {
            baseName = runCfgName;
        }

        int hashAdd = 0;
        int hashOr = 0;
        for (int i = 0; i < baseName.length(); i++) {
            int val = ((int) baseName.charAt(i)) % 0xff;
            hashAdd += val;
            hashOr |= val;
        }

        return String.format("%02x%x", hashOr, hashAdd);
    }
}
