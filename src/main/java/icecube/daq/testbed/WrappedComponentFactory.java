package icecube.daq.testbed;

import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.trigger.component.TriggerComponent;

/**
 * Wrap a common interface around a DAQ component.
 */
abstract class WrappedComponentFactory
    extends ObjectCreator
{
    /**
     * Wrap a DAQ component.
     *
     * @param baseName base component class name
     */
    public static WrappedComponent create(String baseName)
    {
        String className = null;

        Class classObj = null;
        for (int c = 0; classObj == null; c++) {
            if (c == 0) {
                className = baseName;
            } else if (c == 1) {
                className = "icecube.daq.trigger.component." + baseName;
            } else if (c == 2) {
                className = "icecube.daq.oldtrigger.component." + baseName;
            } else if (c == 3) {
                className = "icecube.daq.trigger.component." + baseName +
                    "Component";
            } else if (c == 4) {
                className = "icecube.daq.oldtrigger.component." + baseName +
                    "Component";
            } else {
                throw new Error("Bad class name \"" + baseName + "\"");
            }

            try {
                classObj = Class.forName(className);
            } catch (ClassNotFoundException cnfe) {
                // nope, that wasn't it
System.err.println("Couldn't load " + className);
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
