package io.lmb.feeds;

import io.lmb.common.SymbolDirectory;
import io.lmb.common.Topology;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.agrona.concurrent.EpochNanoClock;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Service provider interface for external market data: delivers normalised top-of-book quotes for
 * several external venues, already A/B arbitrated and gap-checked, with receive timestamps and
 * per-venue status. The consolidator depends only on this interface.
 *
 * <p>Implementations: {@link UdpVenueFeedHandler} (the simulated venues of {@link FeedSimulator},
 * provider name {@code udp-sim}). A ticker-plant adapter (for example over a vendor's C/C++ or Java
 * client API) implements the same interface and registers a {@link Provider} in
 * {@code META-INF/services/io.lmb.feeds.VenueFeedHandler$Provider}; selecting it is a configuration
 * change ({@code lmb.feeds.handler}). See docs/modules/feeds.md.
 *
 * <p>Threading: {@link #start}, {@link #poll} and {@link #close} are called from one thread (the
 * consolidator's duty thread), and every {@link Listener} callback must be made from inside
 * {@link #poll} on that thread. An adapter whose vendor library calls back on its own threads must
 * hand the data over through a bounded, conflating structure (latest top of book per symbol) and
 * replay it from {@link #poll}.
 */
public interface VenueFeedHandler extends AutoCloseable
{
    /** Receives normalised market data. All callbacks happen on the thread calling {@link #poll}. */
    interface Listener
    {
        /** A full top-of-book replacement for (venue, symbol). The quote object is only valid during the call. */
        void onQuote(NormalizedQuote quote);

        /**
         * The venue's stream is complete and in sequence up to venue time {@code venueTsNs}: every quote
         * the venue had published by then has been delivered. Sent for explicit heartbeats and after
         * every in-sequence packet. This is what lets a quiet symbol's last quote count as current.
         */
        void onHeartbeat(int venueId, long venueTsNs, long receiveTsNs);

        /**
         * Status change of a venue's feed. For {@link VenueStatus#GAP}, {@code missedMessages} is the
         * number of sequence numbers lost (0 if unknown, for example a feed session restart).
         */
        void onVenueStatus(int venueId, VenueStatus status, long missedMessages, long tsNs);
    }

    /** What a provider gets to build a handler. */
    record Context(Topology topology, int instanceId, SymbolDirectory symbols, int venueCount, EpochNanoClock clock)
    {
    }

    /** Discovered with {@link ServiceLoader}; selected by {@link #name()} via {@code lmb.feeds.handler}. */
    interface Provider
    {
        String name();

        VenueFeedHandler create(Context context);
    }

    /** Short identifier, the provider name. */
    String name();

    /** Venues this handler delivers, with ids 0 .. venueCount-1. */
    int venueCount();

    /** Opens the feed (binds sockets, logs in to the ticker plant...). Callbacks start with the next {@link #poll}. */
    void start(Listener listener);

    /** Duty cycle: read available data, run arbitration and timeouts, invoke the listener. Non-blocking. */
    int poll();

    /** Optional handler-specific metrics (line counters, vendor statistics). Names must start {@code lmb_feeds_}. */
    default void registerMetrics(final PrometheusRegistry registry)
    {
    }

    @Override
    void close();

    /** Finds the provider called {@code name} on the classpath and creates a handler. */
    static VenueFeedHandler load(final String name, final Context context)
    {
        final List<String> available = new ArrayList<>();
        for (final Provider provider : ServiceLoader.load(Provider.class))
        {
            if (provider.name().equals(name))
            {
                return provider.create(context);
            }
            available.add(provider.name());
        }
        throw new IllegalStateException("no VenueFeedHandler provider named '" + name + "' (available: " + available + ")");
    }
}
