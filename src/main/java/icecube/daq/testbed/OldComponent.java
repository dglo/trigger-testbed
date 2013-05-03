package icecube.daq.testbed;

import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.oldtrigger.component.OldTriggerComponent;
import icecube.daq.payload.SourceIdRegistry;

import java.io.File;
import java.io.PrintStream;
import java.nio.channels.Pipe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Old trigger component.
 */
class OldComponent
    extends WrappedComponent
{
    OldComponent(OldTriggerComponent comp)
    {
        super(comp, "Old");
    }
}
