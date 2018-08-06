package icecube.daq.testbed;

import icecube.daq.payload.IPayload;

import java.util.Comparator;

class PayloadComparator
    implements Comparator<IPayload>
{
    public int compare(IPayload p1, IPayload p2)
    {
        if (p1 == null) {
            if (p2 == null) {
                return 0;
            }

            return 1;
        } else if (p2 == null) {
            return -1;
        }

        final long diff = p1.getUTCTime() - p2.getUTCTime();
        if (diff < 0) {
            return -1;
        } else if (diff > 0) {
            return 1;
        } else {
            return 0;
        }
    }

    public boolean equals(Object obj)
    {
        return obj == this;
    }
}
