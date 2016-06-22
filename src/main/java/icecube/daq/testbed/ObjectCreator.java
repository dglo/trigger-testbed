package icecube.daq.testbed;

import java.lang.reflect.Constructor;

/**
 * Wrap a common interface around a DAQ component.
 */
abstract class ObjectCreator
{
    public static Object createObject(Class classObj)
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
