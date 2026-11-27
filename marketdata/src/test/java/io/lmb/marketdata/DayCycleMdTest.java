package io.lmb.marketdata;

import io.lmb.api.EventStreamClient;
import io.lmb.common.Events;
import io.lmb.marketdata.sbe.CloseSource;
import io.lmb.marketdata.sbe.ClosingPriceDecoder;
import io.lmb.marketdata.sbe.ReferencePriceDecoder;
import io.lmb.marketdata.sbe.SnapshotBeginDecoder;
import io.lmb.marketdata.sbe.StateCause;
import io.lmb.marketdata.sbe.SymbolStatusDecoder;
import io.lmb.marketdata.sbe.TradePrintDecoder;
import io.lmb.marketdata.sbe.TradingState;
import io.lmb.marketdata.wire.MdWire;
import io.lmb.sbe.ReferencePriceAppliedDecoder;
import io.lmb.sbe.SymbolState;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** day-cycle / band-stp: SymbolStatus.cause, ReferencePrice, ClosingPrice, SnapshotBegin v2 and checkpoint v2. */
class DayCycleMdTest
{
    static final int VOL = 9;
    static final int DAY1 = 20261117;
    private static final long NULL_PRICE = Long.MIN_VALUE;

    /** One shard, firms 1 and 2, symbol VOL through a volatility interruption and two rollovers, all drained. */
    private static BookBuilder volatilityBuilder(final VenueDriver v, final BookBuilderTest.Collector out)
    {
        v.firm(0, 1);
        v.firm(0, 2);
        v.volatilityThenRollovers(0, VOL, DAY1);
        final BookBuilder builder = new BookBuilder(out);
        final EventStreamClient s = v.venue.eventStream(0, 0);
        while (s.poll((b, o, l) -> builder.onEvent(b, o, l, 0), 13) > 0)
        {
        }
        return builder;
    }

    private static int indexOf(final List<byte[]> msgs, final int from, final int templateId)
    {
        for (int i = from; i < msgs.size(); i++)
        {
            if (MdWire.templateId(new UnsafeBuffer(msgs.get(i)), 0) == templateId)
            {
                return i;
            }
        }
        return -1;
    }

    private static SymbolStatusDecoder status(final byte[] m)
    {
        final UnsafeBuffer b = new UnsafeBuffer(m);
        return new SymbolStatusDecoder().wrap(b, MdWire.HEADER_LENGTH, MdWire.blockLength(b, 0), MdWire.version(b, 0));
    }

    private static ReferencePriceDecoder reference(final byte[] m)
    {
        final UnsafeBuffer b = new UnsafeBuffer(m);
        return new ReferencePriceDecoder().wrap(b, MdWire.HEADER_LENGTH, MdWire.blockLength(b, 0), MdWire.version(b, 0));
    }

    private static ClosingPriceDecoder close(final byte[] m)
    {
        final UnsafeBuffer b = new UnsafeBuffer(m);
        return new ClosingPriceDecoder().wrap(b, MdWire.HEADER_LENGTH, MdWire.blockLength(b, 0), MdWire.version(b, 0));
    }

    private static SnapshotBeginDecoder snapshotBegin(final SymbolBook s)
    {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        new SnapshotWriter().write(s, buf, 0);
        final int msg = MdWire.FRAME_PREFIX_LENGTH;
        assertEquals(SnapshotBeginDecoder.TEMPLATE_ID, MdWire.templateId(buf, msg));
        return new SnapshotBeginDecoder().wrap(buf, msg + MdWire.HEADER_LENGTH, MdWire.blockLength(buf, msg),
            MdWire.version(buf, msg));
    }

    /** The venue's ReferencePriceApplied events for the symbol, as {referencePrice, bandLow, bandHigh}. */
    static List<long[]> venueReferences(final VenueDriver v, final int shard, final int symbolId)
    {
        final List<long[]> out = new ArrayList<>();
        final ReferencePriceAppliedDecoder d = new ReferencePriceAppliedDecoder();
        for (long seq = 1; seq <= v.venue.eventCount(shard); seq++)
        {
            final UnsafeBuffer e = new UnsafeBuffer(v.venue.event(shard, seq));
            if (Events.templateId(e, 0) == ReferencePriceAppliedDecoder.TEMPLATE_ID)
            {
                d.wrap(e, MdWire.HEADER_LENGTH, MdWire.blockLength(e, 0), MdWire.version(e, 0));
                if (d.symbolId() == symbolId)
                {
                    out.add(new long[] {d.referencePrice(), d.bandLow(), d.bandHigh()});
                }
            }
        }
        return out;
    }

    @Test
    void volatilityInterruptionPublishesTheCauseThenTheReferencePriceAfterTheAuction()
    {
        final VenueDriver v = new VenueDriver(1);
        final BookBuilderTest.Collector out = new BookBuilderTest.Collector();
        volatilityBuilder(v, out);
        final List<byte[]> msgs = out.bySymbol.get(VOL);
        final List<long[]> refs = venueReferences(v, 0, VOL);
        assertEquals(4, refs.size(), "listing + auction end + two rollovers");

        final int interrupted = indexOf(msgs, 0, SymbolStatusDecoder.TEMPLATE_ID);
        assertTrue(interrupted > 0);
        final SymbolStatusDecoder vi = status(msgs.get(interrupted));
        assertEquals(StateCause.VOLATILITY_INTERRUPTION, vi.cause());
        assertEquals(TradingState.CONTINUOUS, vi.previousState());
        assertEquals(TradingState.AUCTION_CALL, vi.state());
        assertEquals(-1, indexOf(msgs.subList(0, interrupted), 0, TradePrintDecoder.TEMPLATE_ID), "collar stops the cross");
        final int listed = indexOf(msgs.subList(0, interrupted), 0, ReferencePriceDecoder.TEMPLATE_ID);
        assertTrue(listed >= 0, "the listing publishes its ReferencePrice");
        assertEquals(10_000, reference(msgs.get(listed)).referencePrice());
        assertEquals(refs.get(0)[1], reference(msgs.get(listed)).bandLow());
        assertEquals(refs.get(0)[2], reference(msgs.get(listed)).bandHigh());

        final int print = indexOf(msgs, interrupted, TradePrintDecoder.TEMPLATE_ID);
        final int ref = indexOf(msgs, interrupted, ReferencePriceDecoder.TEMPLATE_ID);
        final int ended = indexOf(msgs, interrupted + 1, SymbolStatusDecoder.TEMPLATE_ID);
        assertTrue(print > interrupted && ref > print && ended > interrupted, print + " " + ref + " " + ended);
        final ReferencePriceDecoder rp = reference(msgs.get(ref));
        assertEquals(VenueDriver.VOL_PRICE, rp.referencePrice(), "band re-centred on the uncross price");
        assertEquals(refs.get(1)[1], rp.bandLow());
        assertEquals(refs.get(1)[2], rp.bandHigh());
        assertTrue(rp.bandLow() < VenueDriver.VOL_PRICE && VenueDriver.VOL_PRICE < rp.bandHigh());
        final SymbolStatusDecoder end = status(msgs.get(ended));
        assertEquals(StateCause.VOLATILITY_AUCTION_END, end.cause());
        assertEquals(TradingState.CONTINUOUS, end.state());
        assertEquals(TradingState.AUCTION_CALL, end.previousState());
    }

    @Test
    void dayRolloverPublishesTheClosingPriceThenTheReferencePrice()
    {
        final VenueDriver v = new VenueDriver(1);
        final BookBuilderTest.Collector out = new BookBuilderTest.Collector();
        final BookBuilder builder = volatilityBuilder(v, out);
        final List<byte[]> msgs = out.bySymbol.get(VOL);
        final List<long[]> refs = venueReferences(v, 0, VOL);

        // first rollover: the day's last trade is the close
        final int close1 = indexOf(msgs, 0, ClosingPriceDecoder.TEMPLATE_ID);
        assertTrue(close1 > 0);
        final ClosingPriceDecoder c1 = close(msgs.get(close1));
        assertEquals(VenueDriver.VOL_PRICE, c1.price());
        assertEquals(NULL_PRICE, c1.previousClose());
        assertEquals(0, c1.tradingDate(), "no trading date before the first rollover");
        assertEquals(CloseSource.LAST_TRADE, c1.source());
        final int ref1 = indexOf(msgs, close1, ReferencePriceDecoder.TEMPLATE_ID);
        assertTrue(ref1 > close1);
        assertEquals(VenueDriver.VOL_PRICE, reference(msgs.get(ref1)).referencePrice());
        assertEquals(refs.get(2)[1], reference(msgs.get(ref1)).bandLow());
        assertEquals(refs.get(2)[2], reference(msgs.get(ref1)).bandHigh());
        final int rolled = indexOf(msgs, close1, SymbolStatusDecoder.TEMPLATE_ID);
        assertTrue(rolled > close1);
        assertEquals(StateCause.DAY_ROLLOVER, status(msgs.get(rolled)).cause());
        assertEquals(TradingState.PRE_OPEN, status(msgs.get(rolled)).state());

        // second rollover: no trade that day, the previous close is carried
        final int close2 = indexOf(msgs, close1 + 1, ClosingPriceDecoder.TEMPLATE_ID);
        assertTrue(close2 > ref1);
        final ClosingPriceDecoder c2 = close(msgs.get(close2));
        assertEquals(VenueDriver.VOL_PRICE, c2.price());
        assertEquals(VenueDriver.VOL_PRICE, c2.previousClose());
        assertEquals(DAY1, c2.tradingDate());
        assertEquals(CloseSource.PREVIOUS_CLOSE, c2.source());
        final int ref2 = indexOf(msgs, close2, ReferencePriceDecoder.TEMPLATE_ID);
        assertTrue(ref2 > close2);
        assertEquals(refs.get(3)[0], reference(msgs.get(ref2)).referencePrice());
        assertEquals(-1, indexOf(msgs, close2, SymbolStatusDecoder.TEMPLATE_ID), "already PRE_OPEN: no state change");

        final SymbolBook s = builder.symbol(VOL);
        assertEquals(VenueDriver.VOL_PRICE, s.referencePrice());
        assertEquals(refs.get(3)[1], s.bandLow());
        assertEquals(refs.get(3)[2], s.bandHigh());
        assertEquals(VenueDriver.VOL_PRICE, s.closePrice());
        assertEquals(DAY1, s.closeTradingDate());
        assertEquals(StateCause.DAY_ROLLOVER.value(), s.stateCause());
        assertEquals(msgs.size(), s.seq());
    }

    @Test
    void snapshotBeginCarriesReferenceBandCloseAndCause()
    {
        final VenueDriver v = new VenueDriver(1);
        final BookBuilder builder = volatilityBuilder(v, new BookBuilderTest.Collector());
        final List<long[]> refs = venueReferences(v, 0, VOL);
        final SnapshotBeginDecoder d = snapshotBegin(builder.symbol(VOL));
        assertEquals(builder.symbol(VOL).seq(), d.lastSeq());
        assertEquals(TradingState.PRE_OPEN, d.state());
        assertEquals(VenueDriver.VOL_PRICE, d.referencePrice());
        assertEquals(refs.get(3)[1], d.bandLow());
        assertEquals(refs.get(3)[2], d.bandHigh());
        assertEquals(VenueDriver.VOL_PRICE, d.closePrice());
        assertEquals(VenueDriver.VOL_PRICE, d.previousClose());
        assertEquals(DAY1, d.closeTradingDate());
        assertEquals(CloseSource.PREVIOUS_CLOSE, d.closeSource());
        assertEquals(StateCause.DAY_ROLLOVER, d.stateCause());
        assertEquals(5000, d.priceBandBps());
        assertEquals(100, d.dynamicBandBps());
        assertEquals(VenueDriver.VOL_AUCTION_NS, d.volatilityAuctionNs());
    }

    @Test
    void snapshotOfAFreshSymbolHasTheListingBandButNoCloseAndNoCause()
    {
        final VenueDriver v = new VenueDriver(1);
        v.symbol(0, 1, "AAA", 10_000, SymbolState.PRE_OPEN);
        final BookBuilder builder = new BookBuilder(new BookBuilderTest.Collector());
        final EventStreamClient s = v.venue.eventStream(0, 0);
        while (s.poll((b, o, l) -> builder.onEvent(b, o, l, 0), 100) > 0)
        {
        }
        final SnapshotBeginDecoder d = snapshotBegin(builder.symbol(1));
        assertEquals(10_000, d.referencePrice());
        final List<long[]> refs = venueReferences(v, 0, 1);
        assertEquals(1, refs.size(), "the definition applies the reference");
        assertEquals(refs.get(0)[1], d.bandLow());
        assertEquals(refs.get(0)[2], d.bandHigh());
        assertTrue(d.bandLow() < 10_000 && 10_000 < d.bandHigh());
        assertEquals(NULL_PRICE, d.closePrice());
        assertEquals(CloseSource.NULL_VAL, d.closeSource());
        assertEquals(StateCause.NULL_VAL, d.stateCause());
    }

    @Test
    void checkpointRoundTripKeepsTheDayCycleFields(@TempDir final Path dir) throws IOException
    {
        final VenueDriver v = new VenueDriver(1);
        final BookBuilder builder = volatilityBuilder(v, new BookBuilderTest.Collector());
        final byte[] data = StateCheckpoint.serialize(builder, 77);
        StateCheckpoint.write(dir, data);
        final BookBuilder restored = new BookBuilder(new BookBuilderTest.Collector());
        assertTrue(StateCheckpoint.restore(dir, restored, 77));
        assertArrayEquals(data, StateCheckpoint.serialize(restored, 77));
        final SymbolBook a = builder.symbol(VOL);
        final SymbolBook b = restored.symbol(VOL);
        assertEquals(VenueDriver.VOL_PRICE, b.referencePrice());
        assertEquals(a.bandLow(), b.bandLow());
        assertEquals(a.bandHigh(), b.bandHigh());
        assertEquals(VenueDriver.VOL_PRICE, b.closePrice());
        assertEquals(DAY1, b.closeTradingDate());
        assertEquals(StateCause.DAY_ROLLOVER.value(), b.stateCause());
        assertEquals(a.seq(), b.seq());
        final SnapshotBeginDecoder d = snapshotBegin(b);
        assertEquals(CloseSource.PREVIOUS_CLOSE, d.closeSource());
        assertEquals(VenueDriver.VOL_PRICE, d.previousClose());
        assertEquals(100, d.dynamicBandBps());
        assertEquals(VenueDriver.VOL_AUCTION_NS, d.volatilityAuctionNs());
    }
}
