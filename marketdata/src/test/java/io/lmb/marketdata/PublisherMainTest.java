package io.lmb.marketdata;

import io.lmb.common.Topology;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The process wiring of {@link MarketDataPublisherMain}: configuration from the topology, the READY line only once
 * every shard's stream is connected and caught up, start-up backlog kept off UDP, metrics, final checkpoint.
 */
class PublisherMainTest
{
    @TempDir
    Path tmp;

    private MarketDataPublisher publisher;
    private Thread announcer;

    @AfterEach
    void tearDown() throws Exception
    {
        if (publisher != null)
        {
            publisher.close();
        }
        if (announcer != null)
        {
            announcer.join(10_000);
        }
    }

    private Topology topology(final int base)
    {
        final Properties p = new Properties();
        p.setProperty("lmb.shards", "2");
        p.setProperty("lmb.data.dir", tmp.toString());
        p.setProperty("lmb.md.0.bindHost", "127.0.0.1");
        p.setProperty("lmb.md.0.retransmitPort", Integer.toString(base));
        p.setProperty("lmb.md.0.grpcPort", Integer.toString(base + 1));
        p.setProperty("lmb.md.0.incremental", "127.0.0.1:" + (base + 2));
        p.setProperty("lmb.md.0.snapshot", "none");
        p.setProperty("lmb.md.0.metricsPort", Integer.toString(base + 4));
        p.setProperty("lmb.md.pollLimit", "16");
        p.setProperty("lmb.md.historyDepth", "512");
        p.setProperty("lmb.md.sessionId", "7");
        return Topology.of(p);
    }

    @Test
    void configurationComesFromTheTopology()
    {
        final int base = 34990;
        final MdConfig c = MdConfig.load(topology(base), 0);
        assertEquals(2, c.shards);
        assertEquals(base, c.retransmitPort);
        assertEquals(base + 1, c.grpcPort);
        assertEquals(base + 4, c.metricsPort);
        assertEquals("127.0.0.1", c.bindHost);
        assertEquals(java.util.List.of(new InetSocketAddress("127.0.0.1", base + 2)), c.incrementalDestinations);
        assertTrue(c.snapshotDestinations.isEmpty());
        assertEquals(16, c.pollLimit);
        assertEquals(512, c.historyDepth);
        assertEquals(7, c.sessionId);
        assertEquals(tmp.resolve("marketdata").resolve("0"), c.dataDir);
    }

    @Test
    void readyLineWaitsForEveryShardAndTheBacklogIsNotMulticast() throws Exception
    {
        final int base = TestSupport.ports();
        final MdConfig config = MdConfig.load(topology(base), 0);
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        CheckpointRestartTest.activity(v, 77, 20); // a backlog of many polls on both shards
        final GatedVenue gated = new GatedVenue(v.venue);
        gated.setConnected(1, false); // shard 1's members are still starting

        publisher = new MarketDataPublisher(config, gated);
        publisher.start();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final AtomicBoolean announced = new AtomicBoolean();
        announcer = Thread.ofPlatform().start(() ->
        {
            try
            {
                announced.set(MarketDataPublisherMain.awaitReadyAndAnnounce(
                    publisher, config, new PrintStream(out, true, StandardCharsets.UTF_8)));
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });

        TestSupport.await("shard 0 consumed", 10_000,
            () -> publisher.engine().builder().lastEventSeq(0) == v.venue.eventCount(0));
        Thread.sleep(200);
        assertFalse(publisher.engine().isCaughtUp());
        assertEquals("", out.toString(StandardCharsets.UTF_8), "READY before shard 1 connected");

        gated.setConnected(1, true);
        TestSupport.await("READY line", 10_000, () -> out.toString(StandardCharsets.UTF_8).contains("\n"));
        assertEquals("LMB_READY marketdata 0" + System.lineSeparator(), out.toString(StandardCharsets.UTF_8));
        announcer.join(5_000);
        assertTrue(announced.get());
        assertEquals(v.venue.eventCount(1), publisher.engine().builder().lastEventSeq(1));

        // The start-up backlog went to history and gRPC only; the first full poll of each shard was multicast.
        final long messages = publisher.metrics().messages.get();
        final long suppressed = publisher.metrics().suppressedMessages.get();
        assertTrue(suppressed > messages / 2, "suppressed " + suppressed + " of " + messages);
        assertTrue(suppressed < messages);

        // Live traffic after READY is multicast.
        CheckpointRestartTest.activity(v, 5_000, 3);
        TestSupport.await("live traffic consumed", 10_000,
            () -> publisher.engine().builder().lastEventSeq(0) == v.venue.eventCount(0));
        assertTrue(publisher.metrics().messages.get() > messages);
        assertEquals(suppressed, publisher.metrics().suppressedMessages.get());

        final HttpResponse<String> metrics = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + (base + 4) + "/metrics")).build(),
            HttpResponse.BodyHandlers.ofString());
        assertEquals(200, metrics.statusCode());
        assertTrue(metrics.body().contains("lmb_marketdata_last_event_seq{shard=\"1\"} " + v.venue.eventCount(1)),
            metrics.body());
        assertTrue(metrics.body().contains("lmb_marketdata_suppressed_messages_total"), metrics.body());

        publisher.close();
        publisher = null;
        assertTrue(Files.exists(config.dataDir.resolve(StateCheckpoint.FILE_NAME)), "final checkpoint on stop");
    }

    @Test
    void failureBeforeReadyIsReportedAndNoReadyLineIsPrinted() throws Exception
    {
        final int base = TestSupport.ports();
        final MdConfig config = MdConfig.load(topology(base), 0);
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        final GatedVenue gated = new GatedVenue(v.venue);
        gated.dropEventSeq = 3; // the stream loses an event: the publisher must stop, not publish
        publisher = new MarketDataPublisher(config, gated);
        publisher.start();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertFalse(MarketDataPublisherMain.awaitReadyAndAnnounce(
            publisher, config, new PrintStream(out, true, StandardCharsets.UTF_8)));
        assertEquals("", out.toString(StandardCharsets.UTF_8));
        assertTrue(publisher.failure() instanceof IllegalStateException, String.valueOf(publisher.failure()));
    }
}
