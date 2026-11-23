package io.lmb.feeds;

import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/** Counters of an {@link NbboConsolidator}: written by its duty thread, read by the metrics endpoint and tests. */
public final class ConsolidatorStats
{
    final int venueCount;
    final AtomicLongArray quotes;
    final AtomicLongArray gaps;
    final AtomicLongArray gapMessages;
    final AtomicLongArray staleTransitions;
    final AtomicLongArray venueFresh;
    final AtomicLongArray venueDataAgeNs;

    final AtomicLong submitted = new AtomicLong();
    final AtomicLong suppressedMinChange = new AtomicLong();
    final AtomicLong superseded = new AtomicLong();
    final AtomicLong unavailable = new AtomicLong();
    final AtomicLong backPressure = new AtomicLong();
    final AtomicLong crossed = new AtomicLong();
    final AtomicLong locked = new AtomicLong();
    final AtomicLong unknownSymbol = new AtomicLong();
    final AtomicLong invalidQuotes = new AtomicLong();
    final LatencyGauges quoteToCommand = new LatencyGauges();

    ConsolidatorStats(final int venueCount)
    {
        this.venueCount = venueCount;
        quotes = new AtomicLongArray(venueCount);
        gaps = new AtomicLongArray(venueCount);
        gapMessages = new AtomicLongArray(venueCount);
        staleTransitions = new AtomicLongArray(venueCount);
        venueFresh = new AtomicLongArray(venueCount);
        venueDataAgeNs = new AtomicLongArray(venueCount);
    }

    public long quotes(final int venueId)
    {
        return quotes.get(venueId);
    }

    public long gaps(final int venueId)
    {
        return gaps.get(venueId);
    }

    public long gapMessages(final int venueId)
    {
        return gapMessages.get(venueId);
    }

    public long staleTransitions(final int venueId)
    {
        return staleTransitions.get(venueId);
    }

    public boolean venueFresh(final int venueId)
    {
        return venueFresh.get(venueId) != 0;
    }

    public long submitted()
    {
        return submitted.get();
    }

    public long suppressedMinChange()
    {
        return suppressedMinChange.get();
    }

    public long superseded()
    {
        return superseded.get();
    }

    public long unavailable()
    {
        return unavailable.get();
    }

    public long backPressure()
    {
        return backPressure.get();
    }

    public long crossed()
    {
        return crossed.get();
    }

    public long locked()
    {
        return locked.get();
    }

    public long invalidQuotes()
    {
        return invalidQuotes.get();
    }

    public LatencyGauges quoteToCommand()
    {
        return quoteToCommand;
    }

    public void register(final PrometheusRegistry registry)
    {
        perVenueGauge(registry, "lmb_feeds_venue_stale", "1 if the venue is excluded from the NBBO as stale",
            venueFresh, true, 1);
        perVenueGauge(registry, "lmb_feeds_venue_data_age_seconds",
            "Age of the venue's newest in-sequence data: now - min(venueTs, receiveTs)", venueDataAgeNs, false, 1e-9);
        perVenueCounter(registry, "lmb_feeds_venue_stale_transitions", "Times the venue became stale", staleTransitions);
        perVenueCounter(registry, "lmb_feeds_gaps", "Sequence gaps reported for the venue (all lines lost)", gaps);
        perVenueCounter(registry, "lmb_feeds_venue_gap_messages", "Messages lost in venue gaps", gapMessages);
        perVenueCounter(registry, "lmb_feeds_messages", "Quotes delivered by the feed handler per venue", quotes);

        counter(registry, "lmb_feeds_reference_prices_submitted", "ReferencePrice commands accepted by a shard", submitted);
        CounterWithCallback.builder()
            .name("lmb_feeds_reference_prices_suppressed")
            .help("Reference updates not sent: min_change (moved less than lmb.nbbo.minChangeTicks), " +
                "superseded (replaced by a newer value while throttled or back-pressured), " +
                "unavailable (NBBO lost: too few fresh venues or one-sided)")
            .labelNames("reason")
            .callback(cb ->
            {
                cb.call(suppressedMinChange.get(), "min_change");
                cb.call(superseded.get(), "superseded");
                cb.call(unavailable.get(), "unavailable");
            })
            .register(registry);
        counter(registry, "lmb_feeds_command_back_pressure", "Offers refused by a shard's command client", backPressure);
        counter(registry, "lmb_feeds_nbbo_crossed", "NBBO computations that found a crossed market", crossed);
        counter(registry, "lmb_feeds_nbbo_locked", "NBBO computations that produced a locked market", locked);
        counter(registry, "lmb_feeds_unknown_symbol_quotes", "Quotes for symbols not in the directory", unknownSymbol);
        counter(registry, "lmb_feeds_invalid_quotes", "Venue quotes ignored as self-crossed, self-locked or empty", invalidQuotes);
        quoteToCommand.register(registry, "lmb_feeds_quote_to_command_latency_ns",
            "Receive time of the triggering quote to ReferencePrice offer accepted, ns");
    }

    private void perVenueGauge(
        final PrometheusRegistry registry, final String name, final String help, final AtomicLongArray values,
        final boolean invertBoolean, final double scale)
    {
        GaugeWithCallback.builder()
            .name(name)
            .help(help)
            .labelNames("venue")
            .callback(cb ->
            {
                for (int v = 0; v < venueCount; v++)
                {
                    final long raw = values.get(v);
                    cb.call(invertBoolean ? (raw == 0 ? 1 : 0) : raw * scale, Integer.toString(v));
                }
            })
            .register(registry);
    }

    private void perVenueCounter(
        final PrometheusRegistry registry, final String name, final String help, final AtomicLongArray values)
    {
        CounterWithCallback.builder()
            .name(name)
            .help(help)
            .labelNames("venue")
            .callback(cb ->
            {
                for (int v = 0; v < venueCount; v++)
                {
                    cb.call(values.get(v), Integer.toString(v));
                }
            })
            .register(registry);
    }

    private static void counter(final PrometheusRegistry registry, final String name, final String help, final AtomicLong value)
    {
        CounterWithCallback.builder().name(name).help(help).callback(cb -> cb.call(value.get())).register(registry);
    }
}
