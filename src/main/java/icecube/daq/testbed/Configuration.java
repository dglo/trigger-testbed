package icecube.daq.testbed;

import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.payload.impl.TriggerRequest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Branch;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

class ConfigException
    extends Exception
{
    ConfigException(String msg)
    {
        super(msg);
    }

    ConfigException(Exception ex)
    {
        super(ex);
    }
    ConfigException(String msg, Exception ex)
    {
        super(msg, ex);
    }
}

class AlgorithmData
{
    private String name;
    private int type;
    private int cfgId;
    private int srcId;

    AlgorithmData(String name, int type, int cfgId, int srcId)
    {
        this.name = name;
        this.type = type;
        this.cfgId = cfgId;
        this.srcId = srcId;
    }

    public String getName()
    {
        return name;
    }

    public int getSourceId()
    {
        return srcId;
    }

    public int getType()
    {
        return type;
    }
}

public class Configuration
{
    private File file;
    private String trigCfgName;
    private List<Integer> stringHubs;
    private List<Integer> icetopHubs;
    private List<AlgorithmData> algorithmData;

    public Configuration(File configDir, String runCfgName)
        throws ConfigException
    {
        this(buildFileName(configDir, runCfgName));
    }

    public Configuration(File file)
        throws ConfigException
    {
        this.file = file;

        loadRunConfig();

        File trigDir = new File(file.getParentFile(), "trigger");
        if (!trigDir.exists() || !trigDir.isDirectory()) {
            throw new ConfigException("Cannot find trigger configuration" +
                                      " directory in " + file.getParentFile());
        }

        File tcFile = new File(trigDir, trigCfgName);
        if (!tcFile.exists()) {
            tcFile = new File(trigDir, trigCfgName + ".xml");
            if (!tcFile.exists()) {
                throw new ConfigException("Cannot find trigger" +
                                          " configuration \"" + trigCfgName +
                                          "\" in " + trigDir);
            }
        }

        loadTrigConfig(tcFile);
    }

    private static File buildFileName(File configDir, String runCfgName)
        throws ConfigException
    {
        File tmp = new File(configDir, runCfgName);
        if (!tmp.exists()) {
            tmp = new File(configDir, runCfgName + ".xml");
            if (!tmp.exists()) {
                throw new ConfigException("Cannot find run configuration \"" +
                                          runCfgName + "\" in " + configDir);
            }
        }

        return tmp;
    }

    /**
     * Get the number of hubs for the specified trigger handler.
     *
     * @param srcId trigger handler source ID
     *
     * @return number of hubs
     */
    public int getNumberOfSources(int srcId)
        throws ConfigException
    {
        switch (srcId) {
        case SourceIdRegistry.INICE_TRIGGER_SOURCE_ID:
            return stringHubs.size();
        case SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID:
            return icetopHubs.size();
        case SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID:
            if (stringHubs.size() > 0) {
                if (icetopHubs.size() > 0) {
                    return 2;
                }

                return 1;
            } else if (icetopHubs.size() > 0) {
                return 1;
            }
            return 0;
        default:
            break;
        }

        String name = SourceIdRegistry.getDAQNameFromSourceID(srcId);
        throw new ConfigException("Cannot return number of sources for " +
                                  name);
    }

    /**
     * Get the list of hub IDs for the specified trigger handler.
     *
     * @param srcId trigger handler source ID
     *
     * @return list of hub IDs
     */
    public List<Integer> getHubs(int srcId)
        throws ConfigException
    {
        switch (srcId) {
        case SourceIdRegistry.INICE_TRIGGER_SOURCE_ID:
            return stringHubs;
        case SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID:
            return icetopHubs;
        default:
            break;
        }

        String name = SourceIdRegistry.getDAQNameFromSourceID(srcId);
        throw new ConfigException("Cannot return hubs for " + name);
    }

    /**
     * Get run configuration file name.
     *
     * @return file name (with directory path removed)
     */
    public String getName()
    {
        String name = file.getName();
        if (name.endsWith(".xml")) {
            return name.substring(0, name.length() - 4);
        }

        return name;
    }

    /**
     * Extract an integer value from all the text nodes in <tt>branch</tt>.
     *
     * @param branch XML document branch containing an integer string
     *
     * @return integer value
     */
    public static int getNodeInteger(Branch branch)
        throws ConfigException
    {
        String intStr = getNodeText(branch);
        try {
            return Integer.parseInt(intStr);
        } catch (NumberFormatException nfe) {
            throw new ConfigException("Bad integer value \"" + intStr + "\"");
        }
    }

    /**
     * Build a text string from all the text nodes in <tt>branch</tt>.
     *
     * @param branch XML document branch containing a text string
     *
     * @return trimmed text string
     */
    public static String getNodeText(Branch branch)
    {
        StringBuilder str = new StringBuilder();

        for (Iterator iter = branch.nodeIterator(); iter.hasNext(); ) {
            Node node = (Node) iter.next();

            if (node.getNodeType() != Node.TEXT_NODE) {
                continue;
            }

            str.append(node.getText());
        }

        return str.toString().trim();
    }

    /**
     * Get run configuration directory name.
     *
     * @return directory path
     */
    public String getParent()
    {
        return file.getParent();
    }

    public String getTriggerConfigName()
    {
        return trigCfgName;
    }

    public boolean hasTriggersFor(int srcId)
    {
        for (AlgorithmData ad : algorithmData) {
            if (ad.getSourceId() == srcId) {
                return true;
            }
        }

        return false;
    }

    private void loadRunConfig()
        throws ConfigException
    {
        FileInputStream in;
        try {
            in = new FileInputStream(file);
        } catch (IOException ioe) {
            throw new ConfigException("Cannot open run configuration" +
                                      " file \"" + file + "\"", ioe);
        }

        try {
            SAXReader rdr = new SAXReader();
            Document doc;
            try {
                doc = rdr.read(in);
            } catch (DocumentException de) {
                throw new ConfigException("Cannot read run configuration" +
                                          " file \"" + file + "\"", de);
            }

            parseRunConfig(doc);
        } finally {
            try {
                in.close();
            } catch (IOException ioe) {
                // ignore errors on close
            }
        }
    }

    private void loadTrigConfig(File file)
        throws ConfigException
    {
        FileInputStream in;
        try {
            in = new FileInputStream(file);
        } catch (IOException ioe) {
            throw new ConfigException("Cannot open run configuration" +
                                      " file \"" + file + "\"", ioe);
        }

        try {
            SAXReader rdr = new SAXReader();
            Document doc;
            try {
                doc = rdr.read(in);
            } catch (DocumentException de) {
                throw new ConfigException("Cannot read run configuration" +
                                          " file \"" + file + "\"", de);
            }

            parseTriggerConfig(doc);
        } finally {
            try {
                in.close();
            } catch (IOException ioe) {
                // ignore errors on close
            }
        }
    }

    private void parseRunConfig(Document doc)
        throws ConfigException
    {
        Node tcNode = doc.selectSingleNode("runConfig/triggerConfig");
        if (tcNode == null) {
            throw new ConfigException("Run configuration file \"" +
                                      file + " does not contain" +
                                      " <triggerConfig>");
        }

        trigCfgName = getNodeText((Branch) tcNode);

        stringHubs = new ArrayList<Integer>();
        icetopHubs = new ArrayList<Integer>();

        List<Node> configNodes = doc.selectNodes("runConfig/domConfigList");
        for (Node n : configNodes) {
            String hubStr = ((Element) n).attributeValue("hub");
            if (hubStr == null || hubStr.trim().length() == 0) {
                throw new ConfigException("No hub specified for" +
                                          " domConfigList entry " +
                                          getNodeText((Branch) n) +
                                          " in run configuration file " +
                                          file);
            }

            try {
                int hub = Integer.parseInt(hubStr);

                int loHub = hub % 1000;
                if (loHub > 0 && loHub < 200) {
                    stringHubs.add(hub);
                } else if (loHub >= 200 && loHub < 300) {
                    icetopHubs.add(hub);
                } else {
                    throw new ConfigException("Bad hub " + hub +
                                              " for domConfigList entry " +
                                              getNodeText((Branch) n) +
                                              " in run configuration file " +
                                              file);
                }
            } catch (NumberFormatException nfe) {
                throw new ConfigException("Bad hub \"" + hubStr +
                                          "\" for domConfigList entry " +
                                          getNodeText((Branch) n) +
                                          " in run configuration file " +
                                          file);
            }
        }

        Collections.sort(stringHubs);
        Collections.sort(icetopHubs);
    }

    private void parseTriggerConfig(Document doc)
        throws ConfigException
    {
        algorithmData = new ArrayList<AlgorithmData>();

        List<Node> tcNodes = doc.selectNodes("activeTriggers/triggerConfig");
        for (Node n : tcNodes) {
            String name =
                getNodeText((Branch) n.selectSingleNode("triggerName"));
            if (name == null || name.length() == 0) {
                throw new ConfigException("Trigger configuration does not" +
                                          " specify a name in " + trigCfgName +
                                          " from run configuration " + file);
            }

            int type;

            String typeStr =
                getNodeText((Branch) n.selectSingleNode("triggerType"));
            try {
                type = Integer.parseInt(typeStr);
            } catch (NumberFormatException nfe) {
                throw new ConfigException("Bad trigger type \"" + typeStr +
                                          " for " + name + " in " +
                                          trigCfgName +
                                          " from run configuration " + file);
            }

            int cfgId;

            String cfgStr =
                getNodeText((Branch) n.selectSingleNode("triggerConfigId"));
            try {
                cfgId = Integer.parseInt(cfgStr);
            } catch (NumberFormatException nfe) {
                throw new ConfigException("Bad config ID \"" + cfgStr +
                                          " for " + name + " in " +
                                          trigCfgName +
                                          " from run configuration " + file);
            }

            int srcId;

            String srcStr =
                getNodeText((Branch) n.selectSingleNode("sourceId"));
            try {
                srcId = Integer.parseInt(srcStr);
            } catch (NumberFormatException nfe) {
                throw new ConfigException("Bad source ID \"" + srcStr +
                                          " for " + name + " in " +
                                          trigCfgName +
                                          " from run configuration " + file);
            }

            if (false && name.startsWith("SimpleMajority")) {
                int threshold = Integer.MIN_VALUE;

                List<Node> pNodes = n.selectNodes("parameterConfig");
                for (Node p : pNodes) {
                    Branch nb = (Branch) p.selectSingleNode("parameterName");
                    String pName = getNodeText(nb);
                    if (pName == null || pName.length() == 0 ||
                        !pName.equals("threshold"))
                    {
                        continue;
                    }

                    Branch tb = (Branch) p.selectSingleNode("parameterValue");
                    threshold = getNodeInteger(tb);
                    break;
                }

                if (threshold != Integer.MIN_VALUE) {
                    name = "SMT" + threshold;
                }
            }

            algorithmData.add(new AlgorithmData(name, type, cfgId, srcId));
        }
    }

    public void setTriggerNames()
    {
        int max = Integer.MIN_VALUE;
        for (AlgorithmData ad : algorithmData) {
            if (ad.getType() >= max) {
                max = ad.getType();
            }
        }

        String[] typeNames = new String[max + 1];
        for (AlgorithmData ad : algorithmData) {
            if (ad.getType() >= 0) {
                String name = ad.getName();
                if (name.endsWith("Trigger")) {
                    name = name.substring(0, name.length() - 7);
                }

                typeNames[ad.getType()] = name;
            }
        }

        TriggerRequest.setTypeNames(typeNames);
    }

    public String toString()
    {
        return file.getName() + "[" + trigCfgName + "::hubs*" +
            stringHubs.size() + " icetop*" + icetopHubs.size() +
            (hasTriggersFor(SourceIdRegistry.INICE_TRIGGER_SOURCE_ID) ?
             " iniceTrig" : "") +
            (hasTriggersFor(SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID) ?
             " icetopTrig" : "") +
            (hasTriggersFor(SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID) ?
             " globalTrig" : "") +
            "]";
    }

    public static final void main(String[] args)
        throws Exception
    {
        File configDir = new File("/Users/dglo/config");

        for (int i = 0; i < args.length; i++) {
            Configuration cfg = new Configuration(configDir, args[i]);
            System.out.println("\"" + args[i] + "\" -> " + cfg);
        }
    }
}
