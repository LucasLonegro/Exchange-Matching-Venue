package io.lmb.marketdata;

import io.lmb.api.EventStreamClient;
import io.lmb.common.Events;
import io.lmb.marketdata.book.OrderBook;
import io.lmb.marketdata.sbe.LevelUpdateDecoder;
import io.lmb.marketdata.sbe.TradePrintDecoder;
import io.lmb.marketdata.wire.MdWire;
import io.lmb.sbe.BookDeltaDecoder;
import io.lmb.sbe.Side;
import io.lmb.sbe.SymbolState;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BookBuilderTest
{
    /** Collects the builder output: per symbol, the message bytes in order; plus batch boundaries. */
    static final class Collector implements BookBuilder.Output
    {
        final Map<Integer, List<byte[]>> bySymbol = new TreeMap<>();
        final List<byte[]> current = new ArrayList<>();
        final List<List<byte[]>> batches = new ArrayList<>();

        @Override
        public void onMessage(final int shardId, final SymbolBook symbol, final DirectBuffer buffer, final int offset, final int length)
        {
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            bySymbol.computeIfAbsent(symbol.symbolId, k -> new ArrayList<>()).add(copy);
            current.add(copy);
        }

        @Override
        public void onBatchEnd(final int shardId, final long firstReceiveNanos, final long eventTimestampNs)
        {
            batches.add(new ArrayList<>(current));
            current.clear();
        }
    }

    /** Applies a LevelUpdate (other templates ignored) to the book. */
    static void applyMd(final OrderBook book, final byte[] msg)
    {
        final UnsafeBuffer b = new UnsafeBuffer(msg);
        if (MdWire.templateId(b, 0) == LevelUpdateDecoder.TEMPLATE_ID)
        {
            final LevelUpdateDecoder d = new LevelUpdateDecoder().wrap(b, MdWire.HEADER_LENGTH,
                MdWire.blockLength(b, 0), MdWire.version(b, 0));
            book.set(OrderBook.sideIndex(d.sideRaw()), d.price(), d.quantity(), d.orderCount());
        }
    }

    private static VenueDriver threeSymbolVenue(final long seed, final int steps)
    {
        final VenueDriver v = new VenueDriver(2);
        for (int shard = 0; shard < 2; shard++)
        {
            for (int firm = 1; firm <= 3; firm++)
            {
                v.firm(shard, firm);
            }
        }
        v.symbol(0, 1, "AAA", 10_000, SymbolState.CONTINUOUS);
        v.symbol(0, 2, "BBB", 10_000, SymbolState.PRE_OPEN);
        v.state(0, 2, SymbolState.CONTINUOUS);
        v.symbol(1, 3, "CCC", 10_000, SymbolState.CONTINUOUS);
        v.randomSession(0, new int[] {1, 2}, seed, steps);
        v.randomSession(1, new int[] {3}, seed + 1, steps / 2);
        return v;
    }

    private static void drain(final VenueDriver v, final int shard, final BookBuilder builder, final int chunk)
    {
        final EventStreamClient s = v.venue.eventStream(shard, 0);
        while (s.poll((b, o, l) -> builder.onEvent(b, o, l, 0), chunk) > 0)
        {
        }
    }

    @Test
    void incrementalsRebuildTheMatcherBookAndSeqIsDensePerSymbol()
    {
        final VenueDriver v = threeSymbolVenue(42, 3000);
        final Collector out = new Collector();
        final BookBuilder builder = new BookBuilder(out);
        drain(v, 0, builder, 100);
        drain(v, 1, builder, 7);

        // Independent reference: every BookDelta of the raw event streams applied to a book.
        final Map<Integer, OrderBook> reference = new TreeMap<>();
        final BookDeltaDecoder delta = new BookDeltaDecoder();
        int trades = 0;
        for (int shard = 0; shard < 2; shard++)
        {
            for (long seq = 1; seq <= v.venue.eventCount(shard); seq++)
            {
                final UnsafeBuffer e = new UnsafeBuffer(v.venue.event(shard, seq));
                final int t = Events.templateId(e, 0);
                if (t == BookDeltaDecoder.TEMPLATE_ID)
                {
                    delta.wrap(e, MdWire.HEADER_LENGTH, MdWire.blockLength(e, 0), MdWire.version(e, 0));
                    reference.computeIfAbsent((int)delta.symbolId(), k -> new OrderBook())
                        .set(OrderBook.sideIndex(delta.sideRaw()), delta.price(), delta.quantity(), delta.orderCount());
                }
                else if (t == io.lmb.sbe.TradeDecoder.TEMPLATE_ID)
                {
                    trades++;
                }
            }
            assertEquals(v.venue.eventCount(shard), builder.lastEventSeq(shard));
        }
        assertEquals(0, builder.eventGaps());

        int tradePrints = 0;
        for (final int symbolId : new int[] {1, 2, 3})
        {
            final List<byte[]> msgs = out.bySymbol.get(symbolId);
            final OrderBook rebuilt = new OrderBook();
            for (int i = 0; i < msgs.size(); i++)
            {
                final UnsafeBuffer b = new UnsafeBuffer(msgs.get(i));
                assertEquals(i + 1, MdWire.seq(b, 0), "dense seq for symbol " + symbolId);
                assertEquals(symbolId, MdWire.symbolId(b, 0));
                if (MdWire.templateId(b, 0) == TradePrintDecoder.TEMPLATE_ID)
                {
                    tradePrints++;
                }
                applyMd(rebuilt, msgs.get(i));
            }
            final SymbolBook sb = builder.symbol(symbolId);
            assertEquals(msgs.size(), sb.seq());
            assertFalse(sb.book.isEmpty(), "test should leave resting orders");
            assertTrue(rebuilt.sameLevels(sb.book), "md rebuild == builder book for " + symbolId);
            assertTrue(reference.get(symbolId).sameLevels(sb.book), "builder book == BookDelta book for " + symbolId
                + "\n" + reference.get(symbolId) + "\n" + sb.book);
        }
        assertTrue(trades > 100, "scenario must trade");
        assertEquals(trades, tradePrints);
    }

    @Test
    void endOfBatchFlagMarksLastMessageOfEachSymbolInEachBatch()
    {
        final VenueDriver v = threeSymbolVenue(7, 1500);
        final Collector out = new Collector();
        final BookBuilder builder = new BookBuilder(out);
        drain(v, 0, builder, 1000);
        assertFalse(out.batches.isEmpty());
        for (final List<byte[]> batch : out.batches)
        {
            final Map<Integer, Integer> lastIndex = new TreeMap<>();
            for (int i = 0; i < batch.size(); i++)
            {
                lastIndex.put(MdWire.symbolId(new UnsafeBuffer(batch.get(i)), 0), i);
            }
            for (int i = 0; i < batch.size(); i++)
            {
                final UnsafeBuffer b = new UnsafeBuffer(batch.get(i));
                final boolean last = lastIndex.get(MdWire.symbolId(b, 0)) == i;
                assertEquals(last ? MdWire.FLAG_END_OF_BATCH : 0, MdWire.flags(b, 0));
            }
        }
    }

    @Test
    void snapshotPlusLaterIncrementalsEqualsFinalBook()
    {
        final VenueDriver v = threeSymbolVenue(99, 2000);
        final Collector out = new Collector();
        final BookBuilder builder = new BookBuilder(out);
        final EventStreamClient s = v.venue.eventStream(0, 0);
        final List<long[]> snapSeqs = new ArrayList<>();
        final List<OrderBook> snaps = new ArrayList<>();
        int polls = 0;
        while (s.poll((b, o, l) -> builder.onEvent(b, o, l, 0), 37) > 0)
        {
            if (++polls % 11 == 0 && builder.symbol(1) != null)
            {
                final OrderBook copy = new OrderBook();
                copy.copyFrom(builder.symbol(1).book);
                snaps.add(copy);
                snapSeqs.add(new long[] {builder.symbol(1).seq()});
            }
        }
        assertTrue(snaps.size() > 5);
        final List<byte[]> msgs = out.bySymbol.get(1);
        for (int k = 0; k < snaps.size(); k++)
        {
            final OrderBook book = snaps.get(k);
            for (final byte[] m : msgs)
            {
                if (MdWire.seq(new UnsafeBuffer(m), 0) > snapSeqs.get(k)[0])
                {
                    applyMd(book, m);
                }
            }
            assertTrue(book.sameLevels(builder.symbol(1).book), "snapshot " + k);
        }
    }

    @Test
    void outputIsIndependentOfChunkingInterleavingAndDuplicates()
    {
        final VenueDriver v = threeSymbolVenue(5, 1200);
        final Collector a = new Collector();
        final BookBuilder ba = new BookBuilder(a);
        drain(v, 0, ba, 1_000_000);
        drain(v, 1, ba, 1_000_000);

        final Collector b = new Collector();
        final BookBuilder bb = new BookBuilder(b);
        final long n0 = v.venue.eventCount(0);
        final long n1 = v.venue.eventCount(1);
        long i0 = 1;
        long i1 = 1;
        while (i0 <= n0 || i1 <= n1)
        {
            if (i1 <= n1)
            {
                final byte[] e = v.venue.event(1, i1++);
                bb.onEvent(new UnsafeBuffer(e), 0, e.length, 0);
                bb.onEvent(new UnsafeBuffer(e), 0, e.length, 0); // at-least-once delivery
            }
            for (int k = 0; k < 3 && i0 <= n0; k++)
            {
                final byte[] e = v.venue.event(0, i0++);
                bb.onEvent(new UnsafeBuffer(e), 0, e.length, 0);
                if (i0 > 2)
                {
                    final byte[] old = v.venue.event(0, i0 - 2);
                    bb.onEvent(new UnsafeBuffer(old), 0, old.length, 0);
                }
            }
        }
        assertTrue(bb.duplicateEvents() > 0);
        assertEquals(a.bySymbol.keySet(), b.bySymbol.keySet());
        for (final int symbolId : a.bySymbol.keySet())
        {
            final List<byte[]> x = a.bySymbol.get(symbolId);
            final List<byte[]> y = b.bySymbol.get(symbolId);
            assertEquals(x.size(), y.size());
            for (int i = 0; i < x.size(); i++)
            {
                assertArrayEquals(x.get(i), y.get(i));
            }
        }
    }

    @Test
    void nothingIsPublishedBeforeCommandAppliedAndDayRolloverEmptiesBooks()
    {
        final VenueDriver v = new VenueDriver(1);
        v.firm(0, 1);
        v.symbol(0, 1, "AAA", 10_000, SymbolState.CONTINUOUS);
        v.order(0, 1, 1, Side.BUY, 9_990, 100);
        v.order(0, 1, 1, Side.SELL, 10_010, 100);
        final Collector out = new Collector();
        final BookBuilder builder = new BookBuilder(out);
        final long n = v.venue.eventCount(0);
        for (long seq = 1; seq < n; seq++) // everything but the final CommandApplied
        {
            final byte[] e = v.venue.event(0, seq);
            builder.onEvent(new UnsafeBuffer(e), 0, e.length, 0);
        }
        final int before = out.bySymbol.get(1).size();
        assertEquals(1, builder.symbol(1).book.levels(OrderBook.BID));
        assertEquals(0, builder.symbol(1).book.levels(OrderBook.ASK), "staged ask not applied yet");
        final byte[] last = v.venue.event(0, n);
        builder.onEvent(new UnsafeBuffer(last), 0, last.length, 0);
        assertEquals(before + 1, out.bySymbol.get(1).size());
        assertEquals(1, builder.symbol(1).book.levels(OrderBook.ASK));

        v.dayRollover(0, 20261117);
        final EventStreamClient s = v.venue.eventStream(0, n);
        while (s.poll((b, o, l) -> builder.onEvent(b, o, l, 0), 100) > 0)
        {
        }
        assertTrue(builder.symbol(1).book.isEmpty());
        assertEquals(SymbolState.PRE_OPEN.value(), builder.symbol(1).state());
        final OrderBook rebuilt = new OrderBook();
        out.bySymbol.get(1).forEach(m -> applyMd(rebuilt, m));
        assertTrue(rebuilt.isEmpty());
    }
}
