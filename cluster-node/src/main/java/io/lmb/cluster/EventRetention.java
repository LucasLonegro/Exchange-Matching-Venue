package io.lmb.cluster;

/**
 * Retention policy for closed event segments (the live segment is never purged). Only a prefix of the
 * segments (the oldest) is ever purged, so the stream a member holds stays one contiguous run.
 *
 * @param maxSegments keep at most this many closed segments; 0 = no limit
 * @param maxAgeMs    purge closed segments whose recording stopped longer ago than this; 0 = no limit
 * @param intervalNs  how often the policy is applied
 */
public record EventRetention(int maxSegments, long maxAgeMs, long intervalNs)
{
    public static final EventRetention NONE = new EventRetention(0, 0, 60_000_000_000L);

    public boolean enabled()
    {
        return maxSegments > 0 || maxAgeMs > 0;
    }

    /**
     * Number of oldest closed segments to purge.
     *
     * @param stopTimestampsMs stop time (epoch ms) of the closed segments, oldest first
     * @param count            number of closed segments in the array
     * @param nowMs            current epoch ms
     */
    public int purgeCount(final long[] stopTimestampsMs, final int count, final long nowMs)
    {
        int n = 0;
        if (maxSegments > 0 && count > maxSegments)
        {
            n = count - maxSegments;
        }
        if (maxAgeMs > 0)
        {
            int aged = 0;
            while (aged < count && nowMs - stopTimestampsMs[aged] > maxAgeMs)
            {
                aged++;
            }
            n = Math.max(n, aged);
        }
        return n;
    }
}
