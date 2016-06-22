package icecube.daq.testbed;

import icecube.daq.trigger.algorithm.ITriggerAlgorithm;

import java.util.List;

public interface Consumer
{
    /**
     * Configure consumer using list of active trigger algorithms.
     *
     * @param algorithms list of active trigger algorithms
     */
    //void configure(List<ITriggerAlgorithm> algorithms);

    /**
     * Was the handler forced to stop?
     * @return <tt>true</tt> if handler was forced to stop
     */
    boolean forcedStop();

    /**
     * Get the number of bad payloads.
     *
     * @return number of bad payloads
     */
    int getNumberFailed();

    /**
     * Get the number of successfully consumed payloads.
     *
     * @return number of payloads
     */
    int getNumberWritten();

    /**
     * Write a consumer report to the standard output.
     *
     * @param clockSecs time required to process all the data
     *
     * @return <tt>true</tt> if all payloads were found
     */
    boolean report(double clockSecs);

    /**
     * Handler should forceably stop processing inputs.
     */
    void setForcedStop();
}
