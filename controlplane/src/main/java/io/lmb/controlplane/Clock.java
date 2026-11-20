package io.lmb.controlplane;

import java.time.Instant;

/**
 * Time source for the control plane. {@link #epochNs()} is wall time (used for calendar instants,
 * which become cluster timer deadlines); {@link #monoNs()} is monotonic (timeouts, lease).
 * Tests inject a clock driven by the in-memory venue.
 */
public interface Clock
{
    long epochNs();

    long monoNs();

    default long epochMs()
    {
        return epochNs() / 1_000_000L;
    }

    static Clock system()
    {
        return new Clock()
        {
            public long epochNs()
            {
                final Instant now = Instant.now();
                return now.getEpochSecond() * 1_000_000_000L + now.getNano();
            }

            public long monoNs()
            {
                return System.nanoTime();
            }
        };
    }
}
