package icecube.daq.testbed;

import icecube.daq.juggler.component.DAQCompException;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.trigger.component.TriggerComponent;

import java.io.File;
import java.io.PrintStream;
import java.nio.channels.Pipe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * New trigger component.
 */
class NewComponent
    extends WrappedComponent
{
    private static final Log LOG = LogFactory.getLog(NewComponent.class);

    private TriggerComponent comp;
    private Pipe[] tails;

    NewComponent(TriggerComponent comp)
    {
        super(comp, "New");
    }
}
