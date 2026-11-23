package io.lmb.feeds;

import io.lmb.api.CommandClient;
import io.lmb.common.SymbolDirectory;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.lmb.feeds.NormalizedQuote.NO_PRICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NbboConsolidatorTest
{
    private static final long MS = 1_000_000L;
    private static final long T0 = 1_700_000_000_000_000_000L;
    private static final int AAA = 1; // shard 0, tick 1
    private static final int BBB = 2; // shard 1, tick 5

    private static final SymbolDirectory SYMBOLS = new SymbolDirectory(List.of(
        new SymbolDirectory.Symbol(BBB, "BBB", 1, 5, 1, 5000, 1000, 100_000),
        new SymbolDirectory.Symbol(AAA, "AAA", 0, 1, 1, 10_000, 1000, 100_000)));

    private final TestFeeds.ManualClock clock = new TestFeeds.ManualClock(T0);
    private final FakeCommandClient shard0 = new FakeCommandClient(0);
    private final FakeCommandClient shard1 = new FakeCommandClient(1);
    private final NormalizedQuote quote = new NormalizedQuote();
    private NbboConsolidator nbbo = create(2);

    private NbboConsolidator create(final int minVenues)
    {
        return new NbboConsolidator(new ConsolidatorConfig(5 * MS, minVenues, MS, 1, 7, 3), SYMBOLS,
            new CommandClient[]{shard0, shard1}, clock);
    }

    private void up(final int... venues)
    {
        for (final int v : venues)
        {
            nbbo.onVenueStatus(v, VenueStatus.UP, 0, clock.now);
        }
    }

    private void quote(final int venue, final int symbol, final long bid, final long ask)
    {
        nbbo.onQuote(quote.set(venue, symbol, bid, 100, ask, 100, clock.now, clock.now, 1));
    }

    private void heartbeat(final int... venues)
    {
        for (final int v : venues)
        {
            nbbo.onHeartbeat(v, clock.now, clock.now);
        }
    }

    private List<Long> refs(final FakeCommandClient c)
    {
        return c.accepted.stream().map(FakeCommandClient.Ref::ref).toList();
    }

    @Test
    void midpointRoundsHalfUpToTheTick()
    {
        assertEquals(101, NbboConsolidator.midToTick(100, 101, 1));
        assertEquals(100, NbboConsolidator.midToTick(100, 100, 1));
        assertEquals(105, NbboConsolidator.midToTick(100, 105, 5));
        assertEquals(105, NbboConsolidator.midToTick(100, 110, 5));
        assertEquals(100, NbboConsolidator.midToTick(101, 103, 5));
    }

    @Test
    void publishesOnceEnoughFreshVenuesQuoteToTheOwningShard()
    {
        up(0, 1);
        quote(0, AAA, 100, 102);
        nbbo.doWork();
        assertTrue(shard0.accepted.isEmpty(), "one venue is below minVenues");

        quote(1, AAA, 99, 101);
        quote(0, BBB, 5000, 5010);
        quote(1, BBB, 4995, 5005);
        nbbo.doWork();

        assertEquals(List.of(new FakeCommandClient.Ref(AAA, 100, 101, 101, T0, 7, 2)), shard0.accepted);
        assertEquals(List.of(new FakeCommandClient.Ref(BBB, 5000, 5005, 5005, T0, 7, 2)), shard1.accepted);
        assertEquals(2, nbbo.stats().submitted());
        assertTrue(shard0.polls > 0 && shard1.polls > 0, "command clients are polled by the duty cycle");
    }

    @Test
    void silentVenueIsExcludedAfterStaleNsAndTheNbboMoves()
    {
        up(0, 1, 2);
        quote(0, AAA, 105, 107); // best bid, venue 0
        quote(1, AAA, 100, 106);
        quote(2, AAA, 101, 106);
        nbbo.doWork();
        assertEquals(List.of(106L), refs(shard0));

        clock.now += 5 * MS;
        heartbeat(1, 2);
        nbbo.doWork();
        assertTrue(nbbo.isVenueFresh(0), "exactly staleNs old is still fresh");

        clock.now += 1;
        nbbo.doWork();
        assertFalse(nbbo.isVenueFresh(0));
        assertEquals(List.of(106L, 104L), refs(shard0)); // (101 + 106) / 2 = 103.5 -> 104
        assertEquals(1, nbbo.stats().staleTransitions(0));
        assertEquals(2, shard0.accepted.get(1).venues());
        assertTrue(shard0.accepted.get(1).sourceTs() > shard0.accepted.get(0).sourceTs());
    }

    @Test
    void venueWhoseClockLagsIsStaleEvenWhileItsPacketsArrive()
    {
        up(0, 1, 2);
        quote(1, AAA, 100, 104);
        quote(2, AAA, 100, 104);
        nbbo.onQuote(quote.set(0, AAA, 103, 100, 104, 100, clock.now - 6 * MS, clock.now, 1));
        nbbo.doWork();
        assertFalse(nbbo.isVenueFresh(0));
        assertEquals(List.of(102L), refs(shard0), "venue 0's better bid is ignored");
    }

    @Test
    void handlerStaleStatusExcludesTheVenueImmediately()
    {
        up(0, 1, 2);
        quote(0, AAA, 100, 110);
        quote(1, AAA, 100, 104);
        quote(2, AAA, 100, 108);
        nbbo.doWork();
        clock.now += MS; // past the per-symbol throttle
        nbbo.onVenueStatus(1, VenueStatus.STALE, 0, clock.now);
        nbbo.doWork();
        assertEquals(List.of(102L, 104L), refs(shard0));
    }

    @Test
    void gapVoidsTheVenuesQuotesUntilRequoted()
    {
        up(0, 1);
        quote(0, AAA, 100, 102);
        quote(1, AAA, 100, 102);
        nbbo.doWork();
        assertEquals(1, shard0.accepted.size());

        nbbo.onVenueStatus(1, VenueStatus.GAP, 3, clock.now);
        quote(0, AAA, 90, 92); // only one venue has a valid quote now
        nbbo.doWork();
        assertEquals(1, shard0.accepted.size());
        assertEquals(1, nbbo.stats().unavailable());
        assertEquals(1, nbbo.stats().gaps(1));
        assertEquals(3, nbbo.stats().gapMessages(1));

        clock.now += MS;
        quote(1, AAA, 90, 92);
        nbbo.doWork();
        assertEquals(List.of(101L, 91L), refs(shard0));
    }

    @Test
    void throttlesPerSymbolAndSendsTheLatestValue()
    {
        up(0, 1);
        quote(0, AAA, 100, 102);
        quote(1, AAA, 100, 102);
        nbbo.doWork();
        clock.now += MS / 10;
        quote(0, AAA, 102, 104);
        quote(1, AAA, 102, 104);
        nbbo.doWork();
        clock.now += MS / 10;
        quote(0, AAA, 104, 106);
        quote(1, AAA, 104, 106);
        nbbo.doWork();
        assertEquals(List.of(101L), refs(shard0), "within minIntervalNs");
        assertEquals(1, nbbo.stats().superseded());

        clock.now = T0 + MS;
        nbbo.doWork();
        assertEquals(List.of(101L, 105L), refs(shard0));
        clock.now += 10 * MS;
        heartbeat(0, 1);
        nbbo.doWork();
        assertEquals(2, shard0.accepted.size(), "nothing pending");
    }

    @Test
    void unchangedReferenceIsSuppressed()
    {
        up(0, 1);
        quote(0, AAA, 100, 102);
        quote(1, AAA, 100, 102);
        nbbo.doWork();
        clock.now += 2 * MS;
        quote(0, AAA, 99, 103); // NBBO moves, midpoint does not
        nbbo.doWork();
        assertEquals(1, shard0.accepted.size());
        assertEquals(1, nbbo.stats().suppressedMinChange());
    }

    @Test
    void backPressureOnOneShardRetriesWithoutBlockingTheOther()
    {
        up(0, 1);
        shard0.refuseNext = 1;
        quote(0, AAA, 100, 102);
        quote(1, AAA, 100, 102);
        quote(0, BBB, 5000, 5010);
        quote(1, BBB, 5000, 5010);
        nbbo.doWork();
        assertEquals(0, shard0.accepted.size());
        assertEquals(1, shard1.accepted.size());
        assertEquals(1, nbbo.stats().backPressure());
        nbbo.doWork();
        assertEquals(List.of(101L), refs(shard0));
    }

    @Test
    void crossedNbboKeepsTheLastReferenceAndLockedIsPublished()
    {
        up(0, 1);
        quote(0, AAA, 100, 102);
        quote(1, AAA, 100, 102);
        nbbo.doWork();
        clock.now += 2 * MS;
        quote(0, AAA, 105, 107);
        quote(1, AAA, 101, 103); // NBB 105 > NBO 103
        nbbo.doWork();
        assertEquals(List.of(101L), refs(shard0));
        assertEquals(1, nbbo.stats().crossed());

        quote(1, AAA, 101, 105); // NBB 105 = NBO 105
        nbbo.doWork();
        assertEquals(List.of(101L, 105L), refs(shard0));
        assertEquals(1, nbbo.stats().locked());
    }

    @Test
    void oneSidedOrInvalidVenueQuotesDoNotFormAnNbbo()
    {
        up(0, 1);
        quote(0, AAA, 100, NO_PRICE);
        quote(1, AAA, 101, NO_PRICE);
        nbbo.doWork();
        assertTrue(shard0.accepted.isEmpty(), "no offer anywhere");

        quote(1, AAA, 101, 103);
        nbbo.doWork();
        assertEquals(List.of(102L), refs(shard0));

        quote(1, AAA, 104, 103); // self-crossed: replaces (withdraws) venue 1's quote
        nbbo.doWork();
        assertEquals(1, nbbo.stats().invalidQuotes());
        assertEquals(1, nbbo.stats().unavailable(), "venue 0 alone is one-sided");
        assertEquals(1, shard0.accepted.size());

        quote(0, 999, 1, 2);
        assertEquals(1, nbbo.stats().unknownSymbol.get());
    }

    @Test
    void sourceTimestampIsStrictlyIncreasingPerSymbol()
    {
        nbbo = create(1);
        up(0, 1);
        quote(0, AAA, 100, 102);
        quote(1, AAA, 90, 110);
        nbbo.doWork();
        nbbo.onVenueStatus(0, VenueStatus.STALE, 0, clock.now); // NBBO changes, no newer venue data
        clock.now += 2 * MS;
        nbbo.doWork();
        assertEquals(List.of(101L, 100L), refs(shard0));
        assertEquals(T0 + 1, shard0.accepted.get(1).sourceTs());
    }

    @Test
    void readinessFollowsTheCommandClients()
    {
        shard1.connected = false;
        nbbo.doWork();
        assertFalse(nbbo.allShardsConnected());
        shard1.connected = true;
        nbbo.doWork();
        assertTrue(nbbo.allShardsConnected());
    }

    @Test
    void rejectsASymbolWhoseShardHasNoClient()
    {
        assertThrows(IllegalArgumentException.class, () -> new NbboConsolidator(
            new ConsolidatorConfig(5 * MS, 2, MS, 1, 7, 3), SYMBOLS, new CommandClient[]{shard0}, clock));
    }

    @Test
    void exposesMetricsUnderTheDeployNames()
    {
        final PrometheusRegistry registry = new PrometheusRegistry();
        nbbo.stats().register(registry);
        up(0, 1);
        quote(0, AAA, 100, 102);
        quote(1, AAA, 100, 102);
        nbbo.onVenueStatus(1, VenueStatus.GAP, 2, clock.now);
        nbbo.doWork();
        final String text = Scrape.text(registry);
        assertEquals(1.0, Scrape.value(text, "lmb_feeds_messages_total{venue=\"0\"}"));
        assertEquals(1.0, Scrape.value(text, "lmb_feeds_gaps_total{venue=\"1\"}"));
        assertEquals(0.0, Scrape.value(text, "lmb_feeds_venue_stale{venue=\"0\"}"));
        assertEquals(1.0, Scrape.value(text, "lmb_feeds_venue_stale{venue=\"2\"}"));
        assertEquals(0.0, Scrape.value(text, "lmb_feeds_reference_prices_submitted_total"));
        assertTrue(text.contains("lmb_feeds_quote_to_command_latency_ns{quantile=\"p99\"}"), text);
    }
}
