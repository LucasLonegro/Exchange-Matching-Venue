package io.lmb.matcher;

import io.lmb.common.Config;

/**
 * Sizing only. None of these values changes any event or snapshot byte: they set initial pool and
 * table capacities (everything grows on demand) and the width of the tick-indexed level window.
 */
public record MatcherConfig(int orderPoolSize, int levelPoolSize, int levelWindowTicks, int dedupeCapacity)
{
    public static final MatcherConfig DEFAULT = new MatcherConfig(1 << 16, 1 << 14, 4096, 1 << 18);

    public MatcherConfig
    {
        if (orderPoolSize < 1 || levelPoolSize < 1 || dedupeCapacity < 16)
        {
            throw new IllegalArgumentException("invalid sizes");
        }
        if (levelWindowTicks < 64 || Integer.bitCount(levelWindowTicks) != 1)
        {
            throw new IllegalArgumentException("levelWindowTicks must be a power of two >= 64");
        }
    }

    /** From system properties / environment: lmb.matcher.orderPoolSize, levelPoolSize, levelWindowTicks, dedupeCapacity. */
    public static MatcherConfig fromConfig()
    {
        return new MatcherConfig(
            Config.getInt("lmb.matcher.orderPoolSize", DEFAULT.orderPoolSize),
            Config.getInt("lmb.matcher.levelPoolSize", DEFAULT.levelPoolSize),
            Config.getInt("lmb.matcher.levelWindowTicks", DEFAULT.levelWindowTicks),
            Config.getInt("lmb.matcher.dedupeCapacity", DEFAULT.dedupeCapacity));
    }
}
