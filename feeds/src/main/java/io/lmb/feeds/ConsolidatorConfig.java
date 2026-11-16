package io.lmb.feeds;

import io.lmb.common.Topology;

/**
 * NBBO consolidator parameters. Code defaults are the local profile; production values are listed in
 * docs/modules/feeds.md and must be set in the production topology.
 *
 * @param staleNs        a venue whose data is older than this (silent, or arriving late) is excluded
 * @param minVenues      fewer contributing venues than this: no reference price is published
 * @param minIntervalNs  per symbol, at most one ReferencePrice per interval (later changes wait, latest wins)
 * @param minChangeTicks per symbol, a new reference within this many ticks of the last one submitted is suppressed
 * @param sourceId       consolidator instance id, written into ReferencePrice.sourceId
 * @param venueCount     venues 0 .. venueCount-1
 */
public record ConsolidatorConfig(
    long staleNs, int minVenues, long minIntervalNs, long minChangeTicks, int sourceId, int venueCount)
{
    public static final String STALE_NS = "lmb.nbbo.staleNs";
    public static final long DEFAULT_STALE_NS = 50_000_000L;
    public static final String MIN_VENUES = "lmb.nbbo.minVenues";
    public static final int DEFAULT_MIN_VENUES = 2;
    public static final String MIN_INTERVAL_NS = "lmb.nbbo.minIntervalNs";
    public static final long DEFAULT_MIN_INTERVAL_NS = 10_000_000L;
    public static final String MIN_CHANGE_TICKS = "lmb.nbbo.minChangeTicks";
    public static final long DEFAULT_MIN_CHANGE_TICKS = 1;

    public ConsolidatorConfig
    {
        if (staleNs <= 0 || minVenues < 1 || minIntervalNs < 0 || minChangeTicks < 0)
        {
            throw new IllegalArgumentException("invalid consolidator config");
        }
        if (venueCount < 1 || venueCount > 64)
        {
            throw new IllegalArgumentException("venueCount must be 1..64: " + venueCount);
        }
        if (sourceId < 0 || sourceId > 0xFFFF)
        {
            throw new IllegalArgumentException("sourceId must fit uint16: " + sourceId);
        }
    }

    public static long staleNs(final Topology topology)
    {
        return topology.getLong(STALE_NS, DEFAULT_STALE_NS);
    }

    public static ConsolidatorConfig from(final Topology topology, final int instanceId)
    {
        return new ConsolidatorConfig(
            staleNs(topology),
            topology.getInt(MIN_VENUES, DEFAULT_MIN_VENUES),
            topology.getLong(MIN_INTERVAL_NS, DEFAULT_MIN_INTERVAL_NS),
            topology.getLong(MIN_CHANGE_TICKS, DEFAULT_MIN_CHANGE_TICKS),
            instanceId,
            FeedsConfig.venueCount(topology));
    }
}
