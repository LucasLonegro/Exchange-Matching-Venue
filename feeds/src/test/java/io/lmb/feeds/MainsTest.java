package io.lmb.feeds;

import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.common.SymbolDirectory;
import io.lmb.common.Topology;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Both mains end to end over loopback UDP and HTTP, with fake shard command clients: the simulator's
 * quotes become ReferencePrice commands on the owning shard. Ports 35200-35259.
 */
class MainsTest
{
    private static final int CONSOLIDATOR_METRICS = 35200;
    private static final int FEED_BASE = 35210; // 4 venues x 2 lines: 35210-35217
    private static final int SIM_METRICS = 35250;

    /** Two shards, each a {@link FakeCommandClient}; no event streams. */
    static final class FakeVenueClients implements VenueClients
    {
        final FakeCommandClient[] shards = {new FakeCommandClient(0), new FakeCommandClient(1)};

        @Override
        public int shardCount()
        {
            return shards.length;
        }

        @Override
        public CommandClient commandClient(final int shard)
        {
            return shards[shard];
        }

        @Override
        public EventStreamClient eventStream(final int shard, final long fromPosition)
        {
            throw new UnsupportedOperationException("the consolidator does not read events");
        }

        @Override
        public void close()
        {
        }
    }

    @Test
    void simulatorQuotesBecomeReferencePricesOnTheOwningShardAndBothEndpointsServeMetrics() throws Exception
    {
        final SymbolDirectory symbols = new SymbolDirectory(List.of(
            new SymbolDirectory.Symbol(1, "AAA", 0, 1, 1, 19000, 1000, 100000),
            new SymbolDirectory.Symbol(2, "BBB", 1, 1, 1, 42000, 1000, 100000),
            new SymbolDirectory.Symbol(3, "CCC", 0, 5, 1, 5000, 1000, 100000),
            new SymbolDirectory.Symbol(4, "DDD", 1, 1, 1, 2500, 1000, 100000),
            new SymbolDirectory.Symbol(5, "EEE", 1, 10, 1, 100000, 1000, 100000)));
        final Properties p = new Properties();
        p.setProperty("lmb.consolidators", "1");
        p.setProperty("lmb.consolidator.0.metricsPort", Integer.toString(CONSOLIDATOR_METRICS));
        p.setProperty("lmb.consolidator.0.feedBasePort", Integer.toString(FEED_BASE));
        p.setProperty("lmb.feedsim.metricsPort", Integer.toString(SIM_METRICS));
        p.setProperty("lmb.feedsim.quotesPerSecond", "2000");
        p.setProperty("lmb.nbbo.minIntervalNs", "1000000");
        final Topology topology = Topology.of(p);

        final FakeVenueClients venue = new FakeVenueClients();
        venue.shards[1].connected = false;
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        final PrintStream out = new PrintStream(stdout, true, StandardCharsets.UTF_8);

        try (ConsolidatorMain consolidator = ConsolidatorMain.start(topology, 0, symbols, venue, new SystemEpochNanoClock()))
        {
            final CompletableFuture<Boolean> ready = CompletableFuture.supplyAsync(() ->
            {
                try
                {
                    return consolidator.awaitReadyAndAnnounce(out);
                }
                catch (final InterruptedException e)
                {
                    throw new IllegalStateException(e);
                }
            });
            Thread.sleep(200);
            assertFalse(ready.isDone(), "not ready while shard 1 is disconnected");
            assertEquals("", stdout.toString(StandardCharsets.UTF_8));
            venue.shards[1].connected = true;
            assertTrue(ready.get(10, TimeUnit.SECONDS));
            assertEquals("LMB_READY consolidator 0" + System.lineSeparator(), stdout.toString(StandardCharsets.UTF_8));

            try (FeedSimulatorMain sim = FeedSimulatorMain.start(topology, 0, symbols, new SystemEpochNanoClock()))
            {
                final Map<Long, FakeCommandClient.Ref> latest = awaitEverySymbol(venue, symbols, 10_000);
                for (final SymbolDirectory.Symbol s : symbols.all())
                {
                    final FakeCommandClient.Ref r = latest.get((long)s.symbolId());
                    final long tick = s.tickSize();
                    assertEquals(0, r.ref() % tick, s.ticker() + " reference on tick: " + r);
                    assertTrue(r.bid() <= r.ref() && r.ref() <= r.offer(), s.ticker() + " ref inside NBBO: " + r);
                    assertTrue(Math.abs(r.ref() - s.referencePrice()) <= s.referencePrice() / 10,
                        s.ticker() + " near the directory reference " + s.referencePrice() + ": " + r);
                    assertTrue(r.venues() >= 2, "at least minVenues contributed: " + r);
                    assertEquals(0, r.sourceId());
                }
                for (int shard = 0; shard < 2; shard++)
                {
                    long lastTs = Long.MIN_VALUE;
                    final Map<Long, Long> lastTsBySymbol = new HashMap<>();
                    for (final FakeCommandClient.Ref r : venue.shards[shard].snapshot())
                    {
                        assertEquals(shard, symbols.byId((int)r.symbolId()).shard(), "routed to the owner");
                        final Long prev = lastTsBySymbol.put(r.symbolId(), r.sourceTs());
                        assertTrue(prev == null || r.sourceTs() > prev, "sourceTs increases per symbol");
                        lastTs = Math.max(lastTs, r.sourceTs());
                    }
                    assertTrue(lastTs > 0);
                }

                final String consolidatorMetrics = get(CONSOLIDATOR_METRICS);
                assertTrue(Scrape.value(consolidatorMetrics, "lmb_feeds_reference_prices_submitted_total") > 0,
                    consolidatorMetrics);
                for (int v = 0; v < 4; v++)
                {
                    assertTrue(Scrape.value(consolidatorMetrics, "lmb_feeds_messages_total{venue=\"" + v + "\"}") > 0);
                    assertTrue(Scrape.value(consolidatorMetrics,
                        "lmb_feeds_line_packets_total{line=\"B\",venue=\"" + v + "\"}") > 0, consolidatorMetrics);
                }
                assertEquals(0.0, Scrape.value(consolidatorMetrics, "lmb_feeds_malformed_packets_total{venue=\"0\"}"));
                assertTrue(consolidatorMetrics.contains("lmb_feeds_quote_to_command_latency_ns{quantile=\"p99\"}"),
                    consolidatorMetrics);

                final String simMetrics = get(SIM_METRICS);
                assertTrue(Scrape.value(simMetrics, "lmb_feeds_sim_quotes_total{venue=\"3\"}") > 0, simMetrics);
                assertTrue(Scrape.value(simMetrics, "lmb_feeds_sim_packets_total{line=\"A\",venue=\"0\"}") > 0, simMetrics);
                assertEquals(0.0, Scrape.value(simMetrics, "lmb_feeds_sim_send_failures_total"));

                sim.close();
                assertTrue(sim.loop().isStopped());
                assertNull(sim.loop().failure());
            }
            consolidator.close();
            assertTrue(consolidator.loop().isStopped());
            assertNull(consolidator.loop().failure());
        }
    }

    private static Map<Long, FakeCommandClient.Ref> awaitEverySymbol(
        final FakeVenueClients venue, final SymbolDirectory symbols, final long timeoutMs) throws InterruptedException
    {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (true)
        {
            final Map<Long, FakeCommandClient.Ref> latest = new HashMap<>();
            for (final FakeCommandClient shard : venue.shards)
            {
                for (final FakeCommandClient.Ref r : shard.snapshot())
                {
                    latest.put(r.symbolId(), r);
                }
            }
            if (latest.size() == symbols.all().size())
            {
                return latest;
            }
            if (System.currentTimeMillis() > deadline)
            {
                throw new AssertionError("reference prices only for " + latest.keySet());
            }
            Thread.sleep(20);
        }
    }

    private static String get(final int port) throws Exception
    {
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build())
        {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics"))
                    .timeout(Duration.ofSeconds(5)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            return response.body();
        }
    }
}
