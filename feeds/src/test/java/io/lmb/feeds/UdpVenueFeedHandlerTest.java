package io.lmb.feeds;

import io.lmb.common.SymbolDirectory;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.List;

import static io.lmb.feeds.SimFeedProtocol.LINE_A;
import static io.lmb.feeds.SimFeedProtocol.LINE_B;
import static io.lmb.feeds.TestFeeds.packet;
import static io.lmb.feeds.TestFeeds.q;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UdpVenueFeedHandlerTest
{
    private static final int BASE = 35_000;

    private static void send(final DatagramChannel out, final int venue, final int line, final UnsafeBuffer packet)
        throws IOException
    {
        out.send(packet.byteBuffer() != null ? packet.byteBuffer() : java.nio.ByteBuffer.wrap(packet.byteArray()),
            new InetSocketAddress("127.0.0.1", FeedsConfig.linePort(BASE, venue, line)));
    }

    private static void pollUntil(final VenueFeedHandler h, final TestFeeds.Recorder rec, final int events)
        throws InterruptedException
    {
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (rec.events.size() < events && System.nanoTime() < deadline)
        {
            if (h.poll() == 0)
            {
                Thread.sleep(1);
            }
        }
    }

    @Test
    void receivesAndArbitratesBothLinesOfEachVenue() throws Exception
    {
        final TestFeeds.ManualClock clock = new TestFeeds.ManualClock(1_000);
        final TestFeeds.Recorder rec = new TestFeeds.Recorder();
        try (UdpVenueFeedHandler h = new UdpVenueFeedHandler("127.0.0.1", BASE, 2, clock, 1_000_000_000L, 60_000_000_000L);
             DatagramChannel out = DatagramChannel.open())
        {
            final PrometheusRegistry registry = new PrometheusRegistry();
            h.registerMetrics(registry);
            h.start(rec);
            send(out, 1, LINE_A, packet(LINE_A, 1, 7, 1, 100, q(3, 10, 11)));
            pollUntil(h, rec, 3); // UP, quote, heartbeat
            send(out, 1, LINE_B, packet(LINE_B, 1, 7, 1, 100, q(3, 10, 11)));
            send(out, 0, LINE_B, packet(LINE_B, 0, 9, 5, 200, q(4, 20, 21)));
            pollUntil(h, rec, 6);

            assertEquals(List.of("ST 1 UP 0", "Q 1 3 1 10 11", "HB 1 100", "ST 0 UP 0", "Q 0 4 5 20 21", "HB 0 200"),
                rec.events);
            assertEquals(1, h.arbitrator(1).duplicates());
            final String text = Scrape.text(registry);
            assertEquals(1.0, Scrape.value(text, "lmb_feeds_line_packets_total{line=\"B\",venue=\"1\"}"));
            assertEquals(1.0, Scrape.value(text, "lmb_feeds_arbitration_duplicates_total{venue=\"1\"}"));
            assertTrue(text.contains("lmb_feeds_line_silence_seconds"));
        }
    }

    @Test
    void portAlreadyBoundFailsFastInsteadOfSharingTheTraffic()
    {
        final TestFeeds.ManualClock clock = new TestFeeds.ManualClock(0);
        try (UdpVenueFeedHandler first = new UdpVenueFeedHandler("127.0.0.1", BASE + 10, 1, clock, 1, 1);
             UdpVenueFeedHandler second = new UdpVenueFeedHandler("127.0.0.1", BASE + 11, 1, clock, 1, 1))
        {
            first.start(new TestFeeds.Recorder());
            assertThrows(UncheckedIOException.class, () -> second.start(new TestFeeds.Recorder()));
        }
    }

    @Test
    void providerIsDiscoveredByName()
    {
        final VenueFeedHandler.Context ctx = new VenueFeedHandler.Context(
            ConfigTest.topology("lmb.consolidator.0.feedBasePort", "35020"), 0, new SymbolDirectory(List.of()), 3,
            new TestFeeds.ManualClock(0));
        try (VenueFeedHandler h = VenueFeedHandler.load(FeedsConfig.DEFAULT_HANDLER, ctx))
        {
            assertInstanceOf(UdpVenueFeedHandler.class, h);
            assertEquals(3, h.venueCount());
        }
        final IllegalStateException e =
            assertThrows(IllegalStateException.class, () -> VenueFeedHandler.load("vendor-x", ctx));
        assertTrue(e.getMessage().contains("udp-sim"), e.getMessage());
    }
}
