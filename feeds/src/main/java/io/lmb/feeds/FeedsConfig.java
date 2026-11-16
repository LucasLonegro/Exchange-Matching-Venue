package io.lmb.feeds;

import io.lmb.common.Topology;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration keys of the feeds module that are shared by the simulator and the handler, and the
 * UDP port layout. All keys are read through {@link Topology#get}, so a system property or
 * environment variable overrides the topology file. Defaults are the local profile.
 *
 * <p>Port layout of the simulated venues: consolidator instance {@code i} receives venue {@code v},
 * line {@code L} (0 = A, 1 = B) on {@code feedBasePort(i) + 2*v + L}. The simulator unicasts every
 * packet to each consolidator instance (the stand-in for one multicast group per venue and line).
 */
public final class FeedsConfig
{
    public static final String VENUES = "lmb.feeds.venues";
    public static final int DEFAULT_VENUES = 4;
    public static final int MAX_VENUES = 10;

    public static final String HANDLER = "lmb.feeds.handler";
    public static final String DEFAULT_HANDLER = UdpFeedHandlerProvider.NAME;

    public static final String GAP_TIMEOUT_NS = "lmb.feeds.gapTimeoutNs";
    public static final long DEFAULT_GAP_TIMEOUT_NS = 2_000_000L;

    public static final String SILENCE_TIMEOUT_NS = "lmb.feeds.silenceTimeoutNs";

    public static final int DEFAULT_FEED_BASE_PORT = 9660;
    public static final int FEED_PORT_STRIDE = 20;

    public static final String SIM_TARGETS = "lmb.feedsim.targets";

    private FeedsConfig()
    {
    }

    public static int venueCount(final Topology topology)
    {
        final int n = topology.getInt(VENUES, DEFAULT_VENUES);
        if (n < 1 || n > MAX_VENUES)
        {
            throw new IllegalArgumentException(VENUES + " must be 1.." + MAX_VENUES + ": " + n);
        }
        return n;
    }

    public static String feedHost(final Topology topology, final int consolidatorInstance)
    {
        return topology.get("lmb.consolidator." + consolidatorInstance + ".feedHost", "127.0.0.1");
    }

    public static int feedBasePort(final Topology topology, final int consolidatorInstance)
    {
        return topology.getInt(
            "lmb.consolidator." + consolidatorInstance + ".feedBasePort",
            DEFAULT_FEED_BASE_PORT + FEED_PORT_STRIDE * consolidatorInstance);
    }

    public static int linePort(final int basePort, final int venueId, final int line)
    {
        return basePort + 2 * venueId + line;
    }

    /**
     * Where the simulator sends: {@code lmb.feedsim.targets} as {@code host:basePort,host:basePort},
     * or by default one target per consolidator instance (feedHost, feedBasePort).
     */
    public static List<InetSocketAddress> simulatorTargets(final Topology topology)
    {
        final List<InetSocketAddress> out = new ArrayList<>();
        final String explicit = topology.get(SIM_TARGETS, null);
        if (explicit != null && !explicit.isBlank())
        {
            for (final String part : explicit.split(","))
            {
                final String t = part.trim();
                final int colon = t.lastIndexOf(':');
                if (colon <= 0)
                {
                    throw new IllegalArgumentException(SIM_TARGETS + " entry is not host:port: " + t);
                }
                out.add(new InetSocketAddress(t.substring(0, colon), Integer.parseInt(t.substring(colon + 1))));
            }
            return out;
        }
        for (int i = 0; i < topology.consolidatorInstances(); i++)
        {
            out.add(new InetSocketAddress(feedHost(topology, i), feedBasePort(topology, i)));
        }
        return out;
    }
}
