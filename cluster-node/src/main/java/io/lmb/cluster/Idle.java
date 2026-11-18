package io.lmb.cluster;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Idle strategies by configuration name: backoff (default), sleep, yield, busy. */
final class Idle
{
    private Idle()
    {
    }

    static IdleStrategy of(final String name)
    {
        return switch (name.trim().toLowerCase())
        {
            case "busy" -> new BusySpinIdleStrategy();
            case "yield", "yielding" -> new YieldingIdleStrategy();
            case "sleep", "sleeping" -> new SleepingMillisIdleStrategy(1);
            case "backoff" -> new BackoffIdleStrategy(
                10, 20, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MILLISECONDS.toNanos(1));
            default -> throw new IllegalArgumentException("unknown idle strategy: " + name);
        };
    }

    static Supplier<IdleStrategy> supplier(final String name)
    {
        of(name);
        return () -> of(name);
    }
}
