package io.lmb.marketdata;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.lmb.marketdata.book.OrderBook;
import io.lmb.proto.marketdata.MarketDataGrpc;
import io.lmb.proto.marketdata.MarketDataUpdate;
import io.lmb.proto.marketdata.PriceLevel;
import io.lmb.proto.marketdata.SubscribeRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcSubscribeTest
{
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception
    {
        for (final ManagedChannel c : channels)
        {
            c.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        for (int i = closeables.size() - 1; i >= 0; i--)
        {
            closeables.get(i).close();
        }
    }

    /** Applies updates to per-symbol books and checks md_seq ordering. */
    static final class Client implements StreamObserver<MarketDataUpdate>
    {
        final int depth;
        final Map<Integer, OrderBook> books = new HashMap<>();
        final Map<Integer, Long> lastSeq = new HashMap<>();
        final List<MarketDataUpdate> updates = new ArrayList<>();
        volatile Throwable error;
        String violation;

        Client(final int depth)
        {
            this.depth = depth;
        }

        @Override
        public synchronized void onNext(final MarketDataUpdate u)
        {
            updates.add(u);
            final int id = u.getSymbolId();
            final Long prev = lastSeq.get(id);
            if (prev != null && u.getMdSeq() < prev && violation == null)
            {
                violation = "md_seq went back on " + u.getSymbol() + ": " + prev + " -> " + u.getMdSeq();
            }
            if (prev != null && u.getMdSeq() == prev && !u.hasIncremental() && violation == null)
            {
                violation = "non-book update reused md_seq " + prev;
            }
            lastSeq.put(id, u.getMdSeq());
            if (u.hasSnapshot())
            {
                final OrderBook b = new OrderBook();
                u.getSnapshot().getBidsList().forEach(l -> b.set(OrderBook.BID, l.getPriceCents(), l.getQuantity(), l.getOrderCount()));
                u.getSnapshot().getAsksList().forEach(l -> b.set(OrderBook.ASK, l.getPriceCents(), l.getQuantity(), l.getOrderCount()));
                books.put(id, b);
            }
            else if (u.hasIncremental())
            {
                final OrderBook b = books.computeIfAbsent(id, k -> new OrderBook());
                for (final PriceLevel l : u.getIncremental().getBidChangesList())
                {
                    b.set(OrderBook.BID, l.getPriceCents(), l.getQuantity(), l.getOrderCount());
                }
                for (final PriceLevel l : u.getIncremental().getAskChangesList())
                {
                    b.set(OrderBook.ASK, l.getPriceCents(), l.getQuantity(), l.getOrderCount());
                }
            }
            if (depth > 0 && books.containsKey(id) && violation == null
                && (books.get(id).levels(OrderBook.BID) > depth || books.get(id).levels(OrderBook.ASK) > depth))
            {
                violation = "client book deeper than " + depth;
            }
        }

        @Override
        public void onError(final Throwable t)
        {
            error = t;
        }

        @Override
        public void onCompleted()
        {
        }

        synchronized long seq(final int symbolId)
        {
            return lastSeq.getOrDefault(symbolId, 0L);
        }

        synchronized boolean bookEquals(final int symbolId, final OrderBook expected)
        {
            final OrderBook b = books.get(symbolId);
            return b != null && b.sameLevels(expected);
        }

        synchronized long count(final java.util.function.Predicate<MarketDataUpdate> p)
        {
            return updates.stream().filter(p).count();
        }
    }

    private MarketDataPublisher publisher(final VenueDriver v, final int maxQueued) throws Exception
    {
        final MdConfig c = TestSupport.config(0, v.venue.shardCount(), TestSupport.ports(), false, null);
        c.grpcMaxQueued = maxQueued;
        final MarketDataPublisher p = new MarketDataPublisher(c, v.venue);
        closeables.add(p);
        p.start();
        return p;
    }

    private MarketDataGrpc.MarketDataStub stub(final MarketDataPublisher p, final int flowControlWindow)
    {
        final NettyChannelBuilder b = NettyChannelBuilder.forAddress("127.0.0.1", p.grpcPort()).usePlaintext();
        if (flowControlWindow > 0)
        {
            b.flowControlWindow(flowControlWindow);
        }
        final ManagedChannel ch = b.build();
        channels.add(ch);
        return MarketDataGrpc.newStub(ch);
    }

    private static long totalEvents(final VenueDriver v)
    {
        long n = 0;
        for (int s = 0; s < v.venue.shardCount(); s++)
        {
            n += v.venue.eventCount(s);
        }
        return n;
    }

    private static void awaitConsumed(final MarketDataPublisher p, final VenueDriver v)
    {
        TestSupport.await("publisher to consume every event", 10_000, () -> p.metrics().events.get() == totalEvents(v));
    }

    @Test
    void snapshotThenIncrementalsRebuildTheMatcherBook() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        v.randomSession(0, new int[] {1, 2}, 11, 300);
        final MarketDataPublisher p = publisher(v, 100_000);
        awaitConsumed(p, v);

        final Client all = new Client(0);
        stub(p, 0).subscribe(SubscribeRequest.newBuilder().build(), all);
        TestSupport.await("3 snapshots", 5_000, () -> all.count(MarketDataUpdate::hasSnapshot) == 3);
        synchronized (all)
        {
            for (int i = 0; i < 3; i++)
            {
                final MarketDataUpdate u = all.updates.get(i);
                assertTrue(u.hasSnapshot());
                assertEquals(i + 1, u.getSymbolId());
                assertEquals(p.engine().builder().symbol(i + 1).seq(), u.getMdSeq());
                assertEquals("CONTINUOUS", u.getSnapshot().getState());
            }
            assertEquals("AAA", all.updates.get(0).getSymbol());
        }

        v.randomSession(0, new int[] {1, 2}, 12, 800);
        v.randomSession(1, new int[] {3}, 13, 400);
        awaitConsumed(p, v);
        final Map<Integer, OrderBook> reference = TestSupport.referenceBooks(v.venue);
        for (final int id : new int[] {1, 2, 3})
        {
            final long finalSeq = p.engine().builder().symbol(id).seq();
            TestSupport.await("md_seq " + finalSeq + " for " + id, 5_000, () -> all.seq(id) == finalSeq);
            assertTrue(all.bookEquals(id, reference.get(id)), "book " + id);
        }
        assertNull(all.violation, all.violation);
        assertNull(all.error);
        assertTrue(all.count(MarketDataUpdate::hasTrade) > 50);
        assertTrue(all.count(MarketDataUpdate::hasStatus) > 0);
    }

    @Test
    void referenceCloseAndCauseReachLiveAndLateSubscribers() throws Exception
    {
        final int vol = DayCycleMdTest.VOL;
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        final MarketDataPublisher p = publisher(v, 100_000);
        awaitConsumed(p, v);
        final Client live = new Client(0);
        stub(p, 0).subscribe(SubscribeRequest.newBuilder().build(), live);
        TestSupport.await("2 snapshots", 5_000, () -> live.count(MarketDataUpdate::hasSnapshot) == 2);
        synchronized (live)
        {
            final io.lmb.proto.marketdata.BookSnapshot s = live.updates.get(0).getSnapshot();
            assertEquals("", s.getStateCause(), "no SymbolStatus yet");
            assertEquals(10_000, s.getReference().getReferencePriceCents());
            assertTrue(s.getReference().getHasBand(), "the listing applied the band");
            assertFalse(s.hasLastClose());
        }

        v.volatilityThenRollovers(0, vol, DayCycleMdTest.DAY1);
        awaitConsumed(p, v);
        final long finalSeq = p.engine().builder().symbol(vol).seq();
        TestSupport.await("md_seq " + finalSeq, 5_000, () -> live.seq(vol) == finalSeq);
        final List<MarketDataUpdate> u;
        synchronized (live)
        {
            u = live.updates.stream().filter(x -> x.getSymbolId() == vol).toList();
        }
        final List<String> causes = u.stream().filter(MarketDataUpdate::hasStatus).map(x -> x.getStatus().getCause()).toList();
        assertEquals(List.of("VOLATILITY_INTERRUPTION", "VOLATILITY_AUCTION_END", "DAY_ROLLOVER"), causes);
        final List<io.lmb.proto.marketdata.ReferencePrice> refs = u.stream().filter(MarketDataUpdate::hasReference)
            .map(MarketDataUpdate::getReference).toList();
        assertEquals(4, refs.size(), "listing + auction end + two rollovers");
        assertEquals(10_000, refs.get(0).getReferencePriceCents());
        assertTrue(refs.get(0).getHasBand());
        for (final io.lmb.proto.marketdata.ReferencePrice r : refs.subList(1, refs.size()))
        {
            assertTrue(r.getHasReferencePrice() && r.getHasBand());
            assertEquals(VenueDriver.VOL_PRICE, r.getReferencePriceCents());
            assertTrue(r.getBandLowCents() < VenueDriver.VOL_PRICE && VenueDriver.VOL_PRICE < r.getBandHighCents());
        }
        final List<io.lmb.proto.marketdata.ClosingPrice> closes = u.stream().filter(MarketDataUpdate::hasClose)
            .map(MarketDataUpdate::getClose).toList();
        assertEquals(2, closes.size());
        assertEquals("LAST_TRADE", closes.get(0).getSource());
        assertTrue(closes.get(0).getHasPrice() && !closes.get(0).getHasPreviousClose());
        assertEquals(0, closes.get(0).getTradingDate());
        assertEquals("PREVIOUS_CLOSE", closes.get(1).getSource());
        assertEquals(VenueDriver.VOL_PRICE, closes.get(1).getPreviousCloseCents());
        assertEquals(DayCycleMdTest.DAY1, closes.get(1).getTradingDate());
        // the first close comes before the reference price it sets
        int firstClose = -1;
        int lastReference = -1;
        for (int i = 0; i < u.size(); i++)
        {
            firstClose = firstClose < 0 && u.get(i).hasClose() ? i : firstClose;
            lastReference = u.get(i).hasReference() ? i : lastReference;
        }
        assertTrue(firstClose > 0 && lastReference > firstClose, firstClose + " " + lastReference);
        assertNull(live.violation, live.violation);

        final Client late = new Client(0);
        stub(p, 0).subscribe(SubscribeRequest.newBuilder().addSymbols("VOL" + vol).build(), late);
        TestSupport.await("late snapshot", 5_000, () -> late.count(MarketDataUpdate::hasSnapshot) == 1);
        synchronized (late)
        {
            final MarketDataUpdate su = late.updates.get(0);
            assertEquals(finalSeq, su.getMdSeq());
            final io.lmb.proto.marketdata.BookSnapshot s = su.getSnapshot();
            assertEquals("PRE_OPEN", s.getState());
            assertEquals("DAY_ROLLOVER", s.getStateCause());
            assertEquals(refs.get(3), s.getReference());
            assertEquals(closes.get(1), s.getLastClose());
        }
        assertNull(late.error);
    }

    @Test
    void depthLimitedSubscriberHoldsExactlyTheTopLevels() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        v.randomSession(0, new int[] {1, 2}, 21, 200);
        final MarketDataPublisher p = publisher(v, 100_000);
        final Client top3 = new Client(3);
        stub(p, 0).subscribe(SubscribeRequest.newBuilder().addSymbols("AAA").setDepth(3).build(), top3);
        v.randomSession(0, new int[] {1, 2}, 22, 1200);
        awaitConsumed(p, v);
        final OrderBook expected = new OrderBook();
        expected.copyTop(TestSupport.referenceBooks(v.venue).get(1), 3);
        TestSupport.await("top-3 book", 5_000, () -> top3.bookEquals(1, expected));
        assertNull(top3.violation, top3.violation);
        assertEquals(0, top3.count(u -> u.getSymbolId() != 1), "only AAA");
    }

    @Test
    void conflatedSubscriberGetsAtMostOneBookUpdatePerIntervalAndEveryTrade() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        final MarketDataPublisher p = publisher(v, 100_000);
        final Client every = new Client(0);
        final Client conflated = new Client(0);
        stub(p, 0).subscribe(SubscribeRequest.newBuilder().addSymbols("AAA").build(), every);
        stub(p, 0).subscribe(SubscribeRequest.newBuilder().addSymbols("AAA").setConflationMs(200).build(), conflated);
        TestSupport.await("snapshots", 5_000, () -> every.seq(1) > 0 && conflated.seq(1) > 0);

        final long start = System.nanoTime();
        for (int i = 0; i < 40; i++)
        {
            v.randomSession(0, new int[] {1}, 100 + i, 25);
            Thread.sleep(20);
        }
        awaitConsumed(p, v);
        final long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        final OrderBook reference = TestSupport.referenceBooks(v.venue).get(1);
        TestSupport.await("conflated book converges", 5_000, () -> conflated.bookEquals(1, reference));
        TestSupport.await("full book converges", 5_000, () -> every.bookEquals(1, reference));
        Thread.sleep(300);

        final long conflatedBookUpdates = conflated.count(MarketDataUpdate::hasIncremental);
        final long everyBookUpdates = every.count(MarketDataUpdate::hasIncremental);
        assertTrue(conflatedBookUpdates <= elapsedMs / 200 + 3,
            conflatedBookUpdates + " book updates in " + elapsedMs + " ms");
        assertTrue(everyBookUpdates > 4 * conflatedBookUpdates, everyBookUpdates + " vs " + conflatedBookUpdates);
        assertEquals(every.count(MarketDataUpdate::hasTrade), conflated.count(MarketDataUpdate::hasTrade));
        assertTrue(every.count(MarketDataUpdate::hasTrade) > 20);
        assertNull(conflated.violation, conflated.violation);
        assertNull(every.violation, every.violation);
    }

    @Test
    void slowConsumerIsDisconnectedWithoutAffectingOthers() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        final MarketDataPublisher p = publisher(v, 200);
        final Client healthy = new Client(0);
        stub(p, 0).subscribe(SubscribeRequest.newBuilder().build(), healthy);

        final Throwable[] slowError = new Throwable[1];
        stub(p, 16 * 1024).subscribe(SubscribeRequest.newBuilder().build(),
            new ClientResponseObserver<SubscribeRequest, MarketDataUpdate>()
            {
                @Override
                public void beforeStart(final ClientCallStreamObserver<SubscribeRequest> requestStream)
                {
                    requestStream.disableAutoRequestWithInitial(1); // reads one message, then never again
                }

                @Override
                public void onNext(final MarketDataUpdate value)
                {
                }

                @Override
                public void onError(final Throwable t)
                {
                    slowError[0] = t;
                }

                @Override
                public void onCompleted()
                {
                }
            });
        TestSupport.await("both subscribed", 5_000, () -> p.metrics().grpcSubscribers.get() == 2);
        v.randomSession(0, new int[] {1, 2}, 31, 4000);
        awaitConsumed(p, v);
        TestSupport.await("slow consumer cut", 10_000, () -> p.metrics().grpcSlowConsumerDisconnects.get() == 1);
        TestSupport.await("subscriber count", 5_000, () -> p.metrics().grpcSubscribers.get() == 1);
        final Map<Integer, OrderBook> reference = TestSupport.referenceBooks(v.venue);
        TestSupport.await("healthy converges", 10_000,
            () -> healthy.bookEquals(1, reference.get(1)) && healthy.bookEquals(2, reference.get(2)));
        assertNull(healthy.error);
        assertNull(healthy.violation, healthy.violation);
    }

    @Test
    void invalidRequestIsRejected() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        final MarketDataPublisher p = publisher(v, 100);
        final ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", p.grpcPort()).usePlaintext().build();
        channels.add(ch);
        final StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () ->
            MarketDataGrpc.newBlockingStub(ch).subscribe(SubscribeRequest.newBuilder().setDepth(1_000_000).build()).hasNext());
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        assertFalse(p.metrics().grpcSubscribers.get() > 0);
    }
}
