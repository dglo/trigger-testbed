package icecube.daq.testbed;

import icecube.daq.trigger.component.TriggerComponent;

/**
 * New trigger component.
 */
class NewComponent
    extends WrappedComponent
{
    private TriggerComponent comp;

    NewComponent(TriggerComponent comp)
    {
        super(comp, "New");
    }
}
