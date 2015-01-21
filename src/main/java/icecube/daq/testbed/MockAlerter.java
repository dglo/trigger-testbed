package icecube.daq.testbed;

import icecube.daq.juggler.alert.AlertException;
import icecube.daq.juggler.alert.Alerter;
import icecube.daq.payload.IUTCTime;
import java.util.Calendar;
import java.util.Map;

public class MockAlerter
    implements Alerter
{
    public void close()
    {
    }

    public String getService()
    {
        return DEFAULT_SERVICE;
    }

    public boolean isActive()
    {
        return true;
    }

    public void send(String s0, Alerter.Priority x1, Calendar x2, Map x3)
        throws AlertException
    {
    }

    public void send(String s0, Alerter.Priority x1, IUTCTime x2, Map x3)
        throws AlertException
    {
    }

    public void send(String s0, Alerter.Priority x1, Map x2)
        throws AlertException
    {
    }

    public void sendAlert(Alerter.Priority x0, String s1, Map x2)
        throws AlertException
    {
    }

    public void sendAlert(Alerter.Priority x0, String s1, String s2, Map x3)
        throws AlertException
    {
    }

    public void sendAlert(Calendar x0, Alerter.Priority x1, String s2,
                          String s3, Map x4)
        throws AlertException
    {
    }

    public void sendAlert(IUTCTime x0, Alerter.Priority x1, String s2,
                          String s3, Map x4)
        throws AlertException
    {
    }

    public void sendObject(Object x0)
        throws AlertException
    {
    }

    public void setAddress(String s0, int i1)
        throws AlertException
    {
    }
}
