package icecube.daq.testbed;

import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.payload.impl.TriggerRequest;
import icecube.daq.util.JAXPUtil;
import icecube.daq.util.JAXPUtilException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
     * Extract an integer value from all the text nodes in <tt>topNode</tt>.
     *
     * @param topNode XML document branch containing an integer string
     * @param pathExpr XPath expression
     *
     * @return integer value
     */
    public static int extractInteger(Node topNode, String pathExpr)
        throws ConfigException
    {
        String intStr;
        try {
            intStr = JAXPUtil.extractText(topNode, pathExpr);
        } catch (JAXPUtilException jex) {
            throw new ConfigException(jex);
        }

        if (intStr != null && intStr.length() > 0) {
            try {
                return Integer.parseInt(intStr);
            } catch (NumberFormatException nfe) {
                // throw exception below
            }
        }

        throw new ConfigException("Bad integer value \"" + intStr + "\"");
    }

    /**
     * Build a text string from all the text nodes in <tt>branch</tt>.
     *
     * @param branch XML document branch containing a text string
     *
     * @return trimmed text string
     */
    public static String getNodeText(Node branch)
    {
        if (branch.getNodeType() != Node.ELEMENT_NODE) {
            return "";
        }

        NodeList kids = branch.getChildNodes();
        if (kids == null || kids.getLength() == 0) {
            return "";
        }

        StringBuilder str = new StringBuilder();

        for (int i = 0; i < kids.getLength(); i++) {
            Node node = (Node) kids.item(i);

            if (node.getNodeType() != Node.TEXT_NODE) {
                continue;
            }

            str.append(node.getTextContent());
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
        Document doc;
        try {
            doc = JAXPUtil.loadXMLDocument(file);
        } catch (JAXPUtilException de) {
            throw new ConfigException("Cannot read run configuration" +
                                      " file \"" + file + "\"", de);
        }

        parseRunConfig(doc);
    }

    private void loadTrigConfig(File file)
        throws ConfigException
    {
        Document doc;
        try {
            doc = JAXPUtil.loadXMLDocument(file);
        } catch (JAXPUtilException de) {
            throw new ConfigException("Cannot read run configuration" +
                                      " file \"" + file + "\"", de);
        }

        parseTriggerConfig(doc);
    }

    private void addHubs(Document doc, String listName, String name,
                         List<Integer> stringHubs, List<Integer> icetopHubs)
        throws ConfigException
    {
        NodeList nodes;
        try {
            nodes = JAXPUtil.extractNodeList(doc, "runConfig/" + listName);
        } catch (JAXPUtilException jex) {
            throw new ConfigException(jex);
        }

        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            String hubStr = ((Element) n).getAttribute(name);
            if (hubStr == null || hubStr.trim().length() == 0) {
                throw new ConfigException("No " + name + " specified for " +
                                          listName + " entry " +
                                          n.getTextContent() +
                                          " in run configuration file " +
                                          file);
            }

            try {
                int hub = Integer.parseInt(hubStr);

                int srcHub;
                if (hub > 1000) {
                    srcHub = hub;
                } else {
                    srcHub = hub + SourceIdRegistry.STRING_HUB_SOURCE_ID;
                }

                if (SourceIdRegistry.isIniceHubSourceID(srcHub)) {
                    stringHubs.add(hub);
                } else if (SourceIdRegistry.isIcetopHubSourceID(srcHub)) {
                    icetopHubs.add(hub);
                } else {
                    throw new ConfigException("Bad " + name + " " + hub +
                                              " for " + listName + " entry " +
                                              n.getTextContent() +
                                              " in run configuration file " +
                                              file);
                }
            } catch (NumberFormatException nfe) {
                throw new ConfigException("Bad hub \"" + hubStr +
                                          "\" for domConfigList entry " +
                                          n.getTextContent() +
                                          " in run configuration file " +
                                          file);
            }
        }
    }

    private void parseRunConfig(Document doc)
        throws ConfigException
    {
        String tmpName;
        try {
            tmpName = JAXPUtil.extractText(doc, "runConfig/triggerConfig");
        } catch (JAXPUtilException jex) {
            tmpName = null;
        }
        if (tmpName == null) {
            throw new ConfigException("Run configuration file \"" +
                                      file + " does not contain" +
                                      " <triggerConfig>");
        }

        trigCfgName = tmpName;

        stringHubs = new ArrayList<Integer>();
        icetopHubs = new ArrayList<Integer>();

        addHubs(doc, "domConfigList", "hub", stringHubs, icetopHubs);
        addHubs(doc, "stringHub", "hubId", stringHubs, icetopHubs);

        Collections.sort(stringHubs);
        Collections.sort(icetopHubs);
    }

    private void parseTriggerConfig(Document doc)
        throws ConfigException
    {
        algorithmData = new ArrayList<AlgorithmData>();

        NodeList tcNodes;
        try {
            tcNodes =
                JAXPUtil.extractNodeList(doc, "activeTriggers/triggerConfig");
        } catch (JAXPUtilException jex) {
            throw new ConfigException(jex);
        }

        for (int i = 0; i < tcNodes.getLength(); i++) {
            Node n = tcNodes.item(i);

            String name;
            try {
                name = JAXPUtil.extractText(n, "triggerName");
            } catch (JAXPUtilException jex) {
                name = null;
            }

            if (name == null || name.length() == 0) {
                throw new ConfigException("Trigger configuration does not" +
                                          " specify a name in " + trigCfgName +
                                          " from run configuration " + file);
            }

            int type;
            try {
                type = extractInteger(n, "triggerType");
            } catch (ConfigException ce) {
                throw new ConfigException("Bad trigger type for " + name +
                                          " in " + trigCfgName +
                                          " from run configuration " + file);
            }

            int cfgId;
            try {
                cfgId = extractInteger(n, "triggerConfigId");
            } catch (ConfigException ce) {
                throw new ConfigException("Bad config ID for " + name +
                                          " in " + trigCfgName +
                                          " from run configuration " + file);
            }

            int srcId;
            try {
                srcId = extractInteger(n, "sourceId");
            } catch (ConfigException ce) {
                throw new ConfigException("Bad source ID for " + name +
                                          " in " + trigCfgName +
                                          " from run configuration " + file);
            }

            if (false && name.startsWith("SimpleMajority")) {
                int threshold = Integer.MIN_VALUE;

                NodeList pNodes;
                try {
                    pNodes = JAXPUtil.extractNodeList(n, "parameterConfig");
                } catch (JAXPUtilException jex) {
                    throw new ConfigException(jex);
                }

                for (int j = 0; j < pNodes.getLength(); j++) {
                    Node p = pNodes.item(j);

                    String pName;
                    try {
                        pName = JAXPUtil.extractText(p, "parameterName");
                    } catch (JAXPUtilException jex) {
                        continue;
                    }
                    if (pName == null || pName.length() == 0 ||
                        !pName.equals("threshold"))
                    {
                        continue;
                    }

                    threshold = extractInteger(p, "parameterValue");
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
