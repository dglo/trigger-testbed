package icecube.daq.testbed;

import icecube.daq.trigger.component.TriggerComponent;

import java.lang.reflect.Constructor;

/**
 * Wrap a common interface around a DAQ component.
 */
abstract class WrappedComponentFactory
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
            return new NewComponent((TriggerComponent) obj);
        }

        throw new Error("Unknown class " + obj);
    }

    private static Object createObject(Class classObj)
    {
        Constructor[] ctors = classObj.getDeclaredConstructors();
        Constructor ctor = null;
        for (int i = 0; i < ctors.length; i++) {
            ctor = ctors[i];
            if (ctor.getGenericParameterTypes().length == 0) {
                break;
            }
        }

        if (ctor == null) {
            throw new Error("Class " + classObj.getName() +
                            " has no default constructors");
        }

        try {
            ctor.setAccessible(true);
            return ctor.newInstance();
        } catch (Exception ex) {
            throw new Error("Cannot create class \"" + classObj.getName() +
                            "\"", ex);
        }

    }
}
