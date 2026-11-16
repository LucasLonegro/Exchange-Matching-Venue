package io.lmb.feeds;

import io.lmb.common.Topology;

/** {@link VenueFeedHandler.Provider} {@code udp-sim}: the simulated venues of {@link FeedSimulator}. */
public final class UdpFeedHandlerProvider implements VenueFeedHandler.Provider
{
    public static final String NAME = "udp-sim";

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public VenueFeedHandler create(final VenueFeedHandler.Context context)
    {
        final Topology t = context.topology();
        final long staleNs = ConsolidatorConfig.staleNs(t);
        return new UdpVenueFeedHandler(
            FeedsConfig.feedHost(t, context.instanceId()),
            FeedsConfig.feedBasePort(t, context.instanceId()),
            context.venueCount(),
            context.clock(),
            t.getLong(FeedsConfig.GAP_TIMEOUT_NS, FeedsConfig.DEFAULT_GAP_TIMEOUT_NS),
            t.getLong(FeedsConfig.SILENCE_TIMEOUT_NS, staleNs));
    }
}
