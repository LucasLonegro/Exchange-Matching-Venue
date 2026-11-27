package io.lmb.marketdata;

import io.lmb.marketdata.book.OrderBook;
import io.lmb.marketdata.client.FeedClient;
import io.lmb.marketdata.client.RetransmitClient;
import io.lmb.marketdata.sbe.RetransmitStatus;
import io.lmb.marketdata.sbe.SnapshotLevelsDecoder;
import io.lmb.marketdata.wire.MdWire;
import io.lmb.sbe.SymbolState;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeedRecoveryTest
{
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception
    {
        for (int i = closeables.size() - 1; i >= 0; i--)
        {
            closeables.get(i).close();
        }
    }

    /** Checks that every symbol's messages arrive exactly once, in order, without holes (except at snapshots). */
    static final class Recorder implements FeedClient.Listener
    {
        final Map<Integer, Long> last = new HashMap<>();
        final Map<Integer, Integer> trades = new HashMap<>();
        String violation;

        @Override
        public void onMessage(final int symbolId, final long seq, final DirectBuffer buffer, final int offset, final int length)
        {
            final long prev = last.getOrDefault(symbolId, 0L);
            if (seq != prev + 1 && violation == null)
            {
                violation = "symbol " + symbolId + ": seq " + prev + " then " + seq;
            }
            last.put(symbolId, seq);
            if (MdWire.templateId(buffer, offset) == io.lmb.marketdata.sbe.TradePrintDecoder.TEMPLATE_ID)
            {
                trades.merge(symbolId, 1, Integer::sum);
            }
        }

        @Override
        public void onSnapshotApplied(final int symbolId, final long lastSeq)
        {
            last.put(symbolId, lastSeq);
        }
    }

    private MarketDataPublisher publisher(final VenueDriver v, final MdConfig c) throws Exception
    {
        final MarketDataPublisher p = new MarketDataPublisher(c, v.venue);
        closeables.add(p);
        return p;
    }

    private FeedClient client(final Recorder r, final long gapTimeoutMs, final int... bases) throws Exception
    {
        final FeedClient.Config cc = new FeedClient.Config();
        for (final int base : bases)
        {
            cc.incremental.add(new InetSocketAddress("127.0.0.1", base + 2));
            cc.snapshot.add(new InetSocketAddress("127.0.0.1", base + 3));
            cc.retransmit.add(new InetSocketAddress("127.0.0.1", base));
        }
        cc.gapTimeoutMs = gapTimeoutMs;
        final FeedClient c = new FeedClient(cc, r);
        closeables.add(c);
        return c;
    }

    private static void pollUntil(final FeedClient c, final String what, final long timeoutMs, final BooleanSupplier done)
        throws Exception
    {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!done.getAsBoolean())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new AssertionError("timed out: " + what);
            }
            if (c.poll(System.nanoTime()) == 0)
            {
                Thread.sleep(1);
            }
        }
    }

    /** Polls the client while the venue is driven, so its socket buffers never overflow. */
    private static void activity(final VenueDriver v, final FeedClient c, final long seed, final int rounds) throws Exception
    {
        for (int i = 0; i < rounds; i++)
        {
            v.randomSession(0, new int[] {1, 2}, seed + i, 20);
            if (v.venue.shardCount() > 1)
            {
                v.randomSession(1, new int[] {3}, seed + 1000 + i, 10);
            }
            for (int k = 0; k < 5; k++)
            {
                c.poll(System.nanoTime());
                Thread.sleep(1);
            }
        }
    }

    private static void assertConverges(final FeedClient c, final VenueDriver v, final MarketDataPublisher p) throws Exception
    {
        TestSupport.await("publisher consumed", 10_000, () ->
        {
            long n = 0;
            for (int s = 0; s < v.venue.shardCount(); s++)
            {
                n += v.venue.eventCount(s);
            }
            return p.metrics().events.get() == n;
        });
        final Map<Integer, OrderBook> reference = TestSupport.referenceBooks(v.venue);
        for (final int id : reference.keySet())
        {
            final long seq = p.engine().builder().symbol(id).seq();
            pollUntil(c, "symbol " + id + " at seq " + seq, 10_000,
                () -> c.symbol(id) != null && c.symbol(id).lastSeq() == seq);
            assertTrue(c.symbol(id).book.sameLevels(reference.get(id)), "book " + id + ": " + c.symbol(id).book
                + " vs " + reference.get(id));
        }
    }

    @Test
    void feedsAAndBAreArbitratedIntoOneCompleteStream() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        final int a = TestSupport.ports();
        final int b = TestSupport.ports();
        final Recorder r = new Recorder();
        final FeedClient c = client(r, 200, a, b);
        final MarketDataPublisher pa = publisher(v, TestSupport.config(0, 2, a, true, null));
        final MarketDataPublisher pb = publisher(v, TestSupport.config(1, 2, b, true, null));
        pa.start();
        pb.start();
        activity(v, c, 1, 40);
        assertConverges(c, v, pa);
        assertNull(r.violation, r.violation);
        assertTrue(c.duplicates() > c.messagesApplied() / 2, "B duplicates A: " + c.duplicates());
        assertEquals(0, c.snapshotRecoveries());
        assertTrue(r.trades.getOrDefault(1, 0) > 10);
    }

    @Test
    void lossOnFeedAIsCoveredByFeedBWithoutRetransmission() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        final int a = TestSupport.ports();
        final int b = TestSupport.ports();
        final Recorder r = new Recorder();
        final FeedClient c = client(r, 2_000, a, b);
        final MarketDataPublisher pa = publisher(v, TestSupport.config(0, 2, a, true, null));
        final MarketDataPublisher pb = publisher(v, TestSupport.config(1, 2, b, true, null));
        pa.feed().dropFilter(MdWire.CHANNEL_INCREMENTAL, packetSeq -> packetSeq % 3 == 0);
        pa.start();
        pb.start();
        activity(v, c, 50, 40);
        assertConverges(c, v, pa);
        assertNull(r.violation, r.violation);
        assertTrue(c.packetsLost() > 10, "A lost packets: " + c.packetsLost());
        assertEquals(0, c.retransmitRequests());
    }

    @Test
    void lossOnASingleFeedIsRecoveredByTcpRetransmission() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        final int a = TestSupport.ports();
        final Recorder r = new Recorder();
        final FeedClient c = client(r, 5, a);
        final MarketDataPublisher pa = publisher(v, TestSupport.config(0, 2, a, true, null));
        pa.feed().dropFilter(MdWire.CHANNEL_INCREMENTAL, packetSeq -> packetSeq % 5 == 2);
        pa.start();
        activity(v, c, 100, 40);
        assertConverges(c, v, pa);
        assertNull(r.violation, r.violation);
        assertTrue(c.retransmitRequests() > 0);
        assertTrue(c.retransmittedMessages() > 0);
        assertEquals(0, c.snapshotRecoveries());
    }

    @Test
    void gapOlderThanTheHistoryIsRecoveredFromASnapshot() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        final int a = TestSupport.ports();
        final Recorder r = new Recorder();
        final FeedClient c = client(r, 5, a);
        final MdConfig cfg = TestSupport.config(0, 1, a, true, null);
        cfg.historyDepth = 8;
        cfg.snapshotDestinations = MdConfig.parse(""); // force the TCP snapshot path
        final MarketDataPublisher pa = publisher(v, cfg);
        pa.feed().dropFilter(MdWire.CHANNEL_INCREMENTAL, packetSeq -> packetSeq >= 20 && packetSeq < 120);
        pa.start();
        activity(v, c, 200, 40);
        assertConverges(c, v, pa);
        assertNull(r.violation, r.violation);
        assertTrue(c.snapshotRecoveries() > 0);
    }

    @Test
    void lateJoinAndTailLossAreRecoveredThroughTheSnapshotChannel() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        v.randomSession(0, new int[] {1, 2}, 300, 600); // before the publisher starts: replayed, not multicast
        final int a = TestSupport.ports();
        final Recorder r = new Recorder();
        final FeedClient c = client(r, 5, a);
        final AtomicBoolean dropAll = new AtomicBoolean();
        final MarketDataPublisher pa = publisher(v, TestSupport.config(0, 1, a, true, null));
        pa.feed().dropFilter(MdWire.CHANNEL_INCREMENTAL, packetSeq -> dropAll.get());
        pa.start();
        TestSupport.await("caught up", 10_000, () -> pa.metrics().events.get() == v.venue.eventCount(0));
        assertTrue(pa.metrics().suppressedMessages.get() > 0, "catch-up was not multicast");
        activity(v, c, 301, 10);
        dropAll.set(true);
        activity(v, c, 400, 5); // the tail of the stream never arrives over UDP
        assertConverges(c, v, pa);
        assertNull(r.violation, r.violation);
    }

    @Test
    void referenceCloseAndCauseAreRestoredFromASnapshot() throws Exception
    {
        final int vol = DayCycleMdTest.VOL;
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        v.volatilityThenRollovers(0, vol, DayCycleMdTest.DAY1);
        final int a = TestSupport.ports();
        final MdConfig cfg = TestSupport.config(0, 1, a, true, null);
        cfg.historyDepth = 2; // the missed messages cannot be retransmitted: the client must use a snapshot
        final MarketDataPublisher pa = publisher(v, cfg);
        pa.start();
        TestSupport.await("caught up", 10_000, () -> pa.metrics().events.get() == v.venue.eventCount(0));
        final Recorder r = new Recorder();
        final FeedClient c = client(r, 5, a); // joins after the day-cycle messages were multicast
        v.order(0, 1, vol, io.lmb.sbe.Side.BUY, 10_100, 5); // one incremental after the missed state
        v.order(0, 1, 1, io.lmb.sbe.Side.BUY, 10_000, 5);
        TestSupport.await("orders consumed", 10_000, () -> pa.metrics().events.get() == v.venue.eventCount(0));
        final SymbolBook sb = pa.engine().builder().symbol(vol);
        final long seq = sb.seq();
        final long seq1 = pa.engine().builder().symbol(1).seq();
        pollUntil(c, "symbols at seq " + seq + "/" + seq1, 10_000, () -> c.symbol(vol) != null
            && c.symbol(vol).lastSeq() == seq && c.symbol(1) != null && c.symbol(1).lastSeq() == seq1);
        assertNull(r.violation, r.violation);
        assertTrue(c.snapshotRecoveries() > 0);

        final FeedClient.Reference ref = c.symbol(vol).reference();
        assertEquals(VenueDriver.VOL_PRICE, ref.referencePrice());
        assertEquals(sb.bandLow(), ref.bandLow());
        assertEquals(sb.bandHigh(), ref.bandHigh());
        assertTrue(ref.bandLow() < VenueDriver.VOL_PRICE && VenueDriver.VOL_PRICE < ref.bandHigh());
        assertEquals(VenueDriver.VOL_PRICE, ref.closePrice());
        assertEquals(VenueDriver.VOL_PRICE, ref.previousClose());
        assertEquals(DayCycleMdTest.DAY1, ref.closeTradingDate());
        assertEquals(io.lmb.marketdata.sbe.CloseSource.PREVIOUS_CLOSE, ref.closeSource());
        assertEquals(io.lmb.marketdata.sbe.StateCause.DAY_ROLLOVER, ref.stateCause());
        assertEquals(5000, ref.priceBandBps());
        assertEquals(100, ref.dynamicBandBps());
        assertEquals(VenueDriver.VOL_AUCTION_NS, ref.volatilityAuctionNs());
        assertEquals(SymbolState.PRE_OPEN.value(), c.symbol(vol).state());

        // symbol 1 never traded: the listing band around its reference, a NONE close, rolled to PRE_OPEN
        final FeedClient.Reference ref1 = c.symbol(1).reference();
        assertEquals(10_000, ref1.referencePrice());
        final long[] listing1 = DayCycleMdTest.venueReferences(v, 0, 1).get(0);
        assertEquals(listing1[1], ref1.bandLow());
        assertEquals(listing1[2], ref1.bandHigh());
        assertEquals(Long.MIN_VALUE, ref1.closePrice());
        assertEquals(io.lmb.marketdata.sbe.CloseSource.NONE, ref1.closeSource());
        assertEquals(io.lmb.marketdata.sbe.StateCause.DAY_ROLLOVER, ref1.stateCause());
    }

    @Test
    void retransmissionServerAnswersEveryStatusAndServesExactBytes() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        final int a = TestSupport.ports();
        final MdConfig cfg = TestSupport.config(0, 1, a, false, null);
        cfg.historyDepth = 64;
        cfg.retransmitMaxPerRequest = 10;
        final MarketDataPublisher pa = publisher(v, cfg);
        pa.start();
        v.randomSession(0, new int[] {1, 2}, 500, 400);
        TestSupport.await("caught up", 10_000, () -> pa.metrics().events.get() == v.venue.eventCount(0));

        // Independent builder over the same events: the publisher must serve exactly these bytes.
        final BookBuilderTest.Collector expected = new BookBuilderTest.Collector();
        final BookBuilder builder = new BookBuilder(expected);
        for (long seq = 1; seq <= v.venue.eventCount(0); seq++)
        {
            final byte[] e = v.venue.event(0, seq);
            builder.onEvent(new UnsafeBuffer(e), 0, e.length, 0);
        }
        final List<byte[]> msgs = expected.bySymbol.get(1);
        final long lastSeq = msgs.size();
        assertTrue(lastSeq > 100);

        try (RetransmitClient rc = new RetransmitClient(new InetSocketAddress("127.0.0.1", a), 2000))
        {
            assertEquals(RetransmitStatus.UNKNOWN_SYMBOL, rc.retransmit(999, 1, 5).status);
            assertEquals(RetransmitStatus.INVALID_RANGE, rc.retransmit(1, 0, 5).status);
            assertEquals(RetransmitStatus.INVALID_RANGE, rc.retransmit(1, 9, 5).status);
            final RetransmitClient.Response old = rc.retransmit(1, 1, 5);
            assertEquals(RetransmitStatus.TOO_OLD, old.status);
            assertEquals(lastSeq - 63, old.oldestAvailableSeq);
            assertEquals(lastSeq, old.lastSeq);
            assertEquals(0, old.messages.size());

            final RetransmitClient.Response capped = rc.retransmit(1, old.oldestAvailableSeq, lastSeq + 100);
            assertEquals(RetransmitStatus.OK, capped.status);
            assertEquals(10, capped.messages.size());
            assertEquals(old.oldestAvailableSeq + 9, capped.toSeq);
            for (int i = 0; i < 10; i++)
            {
                assertArrayEquals(msgs.get((int)(old.oldestAvailableSeq - 1) + i), capped.messages.get(i));
            }
            final RetransmitClient.Response tail = rc.retransmit(1, lastSeq, lastSeq + 5);
            assertEquals(1, tail.messages.size());
            assertArrayEquals(msgs.get((int)lastSeq - 1), tail.messages.get(0));
            final RetransmitClient.Response future = rc.retransmit(1, lastSeq + 1, lastSeq + 5);
            assertEquals(RetransmitStatus.OK, future.status);
            assertEquals(0, future.messages.size());

            final RetransmitClient.Response snap = rc.snapshot(1);
            assertEquals(RetransmitStatus.OK, snap.status);
            assertEquals(lastSeq, snap.lastSeq);
            final OrderBook book = new OrderBook();
            final SnapshotLevelsDecoder d = new SnapshotLevelsDecoder();
            for (final byte[] m : snap.messages)
            {
                final UnsafeBuffer mb = new UnsafeBuffer(m);
                if (MdWire.templateId(mb, 0) == SnapshotLevelsDecoder.TEMPLATE_ID)
                {
                    d.wrap(mb, MdWire.HEADER_LENGTH, MdWire.blockLength(mb, 0), MdWire.version(mb, 0));
                    for (final SnapshotLevelsDecoder.LevelsDecoder l : d.levels())
                    {
                        book.set(OrderBook.sideIndex(l.sideRaw()), l.price(), l.quantity(), l.orderCount());
                    }
                }
            }
            assertTrue(book.sameLevels(TestSupport.referenceBooks(v.venue).get(1)));
            assertEquals(RetransmitStatus.UNKNOWN_SYMBOL, rc.snapshot(999).status);
        }
    }

    @Test
    void idleFeedSendsHeartbeatsAndMetricsAreExported() throws Exception
    {
        final VenueDriver v = new VenueDriver(1);
        v.symbol(0, 1, "AAA", 10_000, SymbolState.PRE_OPEN);
        final int a = TestSupport.ports();
        final Recorder r = new Recorder();
        final FeedClient c = client(r, 5, a);
        final MdConfig cfg = TestSupport.config(0, 1, a, true, null);
        cfg.metricsPort = a + 4;
        final MarketDataPublisher pa = publisher(v, cfg);
        pa.start();
        pollUntil(c, "heartbeats", 5_000, () -> c.heartbeats() >= 4);
        pollUntil(c, "symbol via snapshot channel", 5_000, () -> c.symbol(1) != null
            && c.symbol(1).lastSeq() == pa.engine().builder().symbol(1).seq());
        assertEquals("AAA", c.symbol(1).ticker());
        final HttpResponse<String> resp = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + (a + 4) + "/metrics")).build(),
            HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("lmb_marketdata_heartbeats_total"), resp.body());
        assertTrue(resp.body().contains("lmb_marketdata_last_event_seq{shard=\"0\"} " + v.venue.eventCount(0)), resp.body());
        assertTrue(resp.body().contains("lmb_marketdata_receive_to_send_ns"), resp.body());
    }
}
