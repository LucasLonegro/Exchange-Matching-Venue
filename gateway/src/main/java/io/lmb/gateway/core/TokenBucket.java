package io.lmb.gateway.core;

/** Per-session token bucket, refilled lazily from the caller's clock. Single-threaded. */
public final class TokenBucket
{
    private final double ratePerNs;
    private final double capacity;
    private double tokens;
    private long lastNs;

    public TokenBucket(final double ratePerSec, final int burst, final long nowNs)
    {
        this.ratePerNs = ratePerSec / 1e9;
        this.capacity = Math.max(1, burst);
        this.tokens = capacity;
        this.lastNs = nowNs;
    }

    public boolean tryAcquire(final long nowNs)
    {
        if (nowNs > lastNs)
        {
            tokens = Math.min(capacity, tokens + (nowNs - lastNs) * ratePerNs);
            lastNs = nowNs;
        }
        if (tokens >= 1.0)
        {
            tokens -= 1.0;
            return true;
        }
        return false;
    }
}
