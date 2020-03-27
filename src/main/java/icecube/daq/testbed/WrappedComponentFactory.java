package icecube.daq.testbed;

import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.trigger.component.TriggerComponent;

/**
 * Wrap a common interface around a DAQ component.
 */
abstract class WrappedComponentFactory
    extends ObjectCreator
{
    private static final String standardPkg = "icecube.daq.trigger.component.";
    private static final String oldPkg = "icecube.daq.oldtrigger.component.";

    /**
     * Wrap a DAQ component.
     *
     * @param baseName base component class name
     */
    public static WrappedComponent create(String baseName)
    {
        String className = null;

        final String lowName = baseName.toLowerCase();
        if (lowName.equals("inice")) {
            baseName = standardPkg + "IniceTriggerComponent";
        } else if (lowName.equals("icetop")) {
            baseName = standardPkg + "IcetopTriggerComponent";
        } else if (lowName.equals("global")) {
            baseName = standardPkg + "GlobalTriggerComponent";
        }

        Class classObj = null;
        for (int c = 0; classObj == null; c++) {
            if (c == 0) {
                className = baseName;
            } else if (c == 1) {
                className = standardPkg + baseName;
            } else if (c == 2) {
                className = oldPkg + baseName;
            } else if (c == 3) {
                className = standardPkg + baseName + "Component";
            } else if (c == 4) {
                className = oldPkg + baseName +
                    "Component";
            } else {
                throw new Error("Bad class name \"" + baseName + "\"");
            }

            try {
                classObj = Class.forName(className);
            } catch (ClassNotFoundException cnfe) {
                // nope, that wasn't it
                classObj = null;
            }
        }

        Object obj = createObject(classObj);
        if (obj instanceof TriggerComponent) {
            TriggerComponent comp = (TriggerComponent) obj;
            try {
                comp.initialize();
            } catch (DAQCompException dcex) {
                throw new Error("Cannot initialize <" +
                                comp.getClass().getName() +
                                ">" + comp);
            }

            return new NewComponent(comp);
        }

        throw new Error("Unknown class " + obj);
    }
}
