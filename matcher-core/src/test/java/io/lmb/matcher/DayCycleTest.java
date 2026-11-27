package io.lmb.matcher;

import io.lmb.sbe.ClosePriceSource;
import io.lmb.sbe.ClosingPriceDecoder;
import io.lmb.sbe.ReferencePriceAppliedDecoder;
import io.lmb.sbe.RejectReason;
import io.lmb.sbe.Side;
import io.lmb.sbe.SymbolState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.lmb.matcher.Cmd.NULL;
import static io.lmb.matcher.OrderScenarioTest.assertKinds;
import static io.lmb.matcher.OrderScenarioTest.assertReject;
import static io.lmb.matcher.OrderScenarioTest.t;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Day cycle: positions carried across DayRollover, the official close (ClosingPrice) and the next day's
 * reference price taken from it.
 */
class DayCycleTest
{
    static final int SYM = 1;
    static final int SYM2 = 2;
    static final int A = 100;
    static final int B = 200;
    static final long SA = 1001;
    static final long SB = 2001;

    private long cl = 1;

    /** B rests a sell, A lifts it: one continuous trade of q at px (A buys). */
    private void cross(final Harness h, final int sym, final long px, final long q)
    {
        h.send(Cmd.limit(SB, cl++, B, sym, Side.SELL, px, q));
        final List<Ev> evs = h.send(Cmd.limit(SA, cl++, A, sym, Side.BUY, px, q));
        assertTrue(t(evs).stream().anyMatch(s -> s.startsWith("TRADE") && s.contains(" px=" + px + " ")), () -> t(evs).toString());
    }

    private static List<String> closes(final List<Ev> evs)
    {
        final List<String> out = new ArrayList<>();
        for (final Ev e : evs)
        {
            if (e.is(ClosingPriceDecoder.TEMPLATE_ID))
            {
                out.add(e.text());
            }
        }
        return out;
    }

    private static List<String> refs(final List<Ev> evs)
    {
        final List<String> out = new ArrayList<>();
        for (final Ev e : evs)
        {
            if (e.is(ReferencePriceAppliedDecoder.TEMPLATE_ID))
            {
                out.add(e.text());
            }
        }
        return out;
    }

    private static String close(final int sym, final long px, final long prev, final int date, final String src)
    {
        return "CLOSE sym=" + sym + " px=" + px + " prev=" + prev + " date=" + date + " src=" + src;
    }

    private static String rollRef(final int sym, final long ref, final long low, final long high, final long ts)
    {
        return "REF sym=" + sym + " ref=" + ref + " bid=" + NULL + " ofr=" + NULL + " band=" + low + ".." + high + " ts=" + ts;
    }

    // ------------------------------------------------------------------ positions

    @Test
    void positionCarriesAcrossRolloverAndTheLimitAppliesToIt()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.risk(9, A, 1_000_000_000_000L, 100, true));
        cross(h, SYM, 10_000, 80);
        final FirmSymbol fsA = h.m.firmSymbols.get(1); // B's (resting seller) firm-symbol was created first
        assertEquals(A, fsA.firm.firmId);
        assertEquals(80, fsA.netPosition);
        h.send(Cmd.limit(SA, cl++, A, SYM, Side.BUY, 9_000, 10)); // open buy quantity, expires at the roll

        h.send(Cmd.dayRoll(1, 20261117));
        assertEquals(80, fsA.netPosition, "position carried");
        assertEquals(-80, h.m.firmSymbols.get(0).netPosition, "seller's position carried");
        assertEquals(0, fsA.openBuyQty);
        assertEquals(0, fsA.openSellQty);
        assertEquals(0, fsA.firm.tradedNet, "daily traded notional reset");
        assertEquals(0, fsA.firm.creditUsed(), "daily credit usage reset");

        // the carried position counts against maxPositionPerSymbol on the new day
        final Harness r = h.restoreCopy();
        for (final Harness x : new Harness[] {h, r})
        {
            x.send(Cmd.state(2, SYM, SymbolState.CONTINUOUS, 0));
            assertReject(x.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_000, 21)), RejectReason.POSITION_LIMIT);
            assertKinds(x.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 9_000, 20)), "ER:NEW", "DELTA");
            assertReject(x.send(Cmd.limit(SA, 3, A, SYM, Side.BUY, 9_000, 1)), RejectReason.POSITION_LIMIT);
            // short side: 80 - 181 = -101 breaches, 80 - 180 = -100 does not
            assertReject(x.send(Cmd.limit(SA, 4, A, SYM, Side.SELL, 10_500, 181)), RejectReason.POSITION_LIMIT);
            assertKinds(x.send(Cmd.limit(SA, 5, A, SYM, Side.SELL, 10_500, 180)), "ER:NEW", "DELTA");
        }
        assertTrue(Harness.sameBytes(h.events.subList(h.events.size() - 14, h.events.size()),
            r.events.subList(r.events.size() - 14, r.events.size())));
        assertArrayEquals(Harness.snapshot(h.m), Harness.snapshot(r.m));
    }

    // ------------------------------------------------------------------ close sources

    @Test
    void closeFromContinuousIsTheLastTradeAndBecomesTheNextReferenceAndBand()
    {
        final Harness h = new Harness().standard();
        cross(h, SYM, 10_010, 5);
        cross(h, SYM, 10_020, 3);
        assertEquals(10_020, h.m.symbols.get(0).lastTradePx);
        final List<Ev> closeEvs = h.send(Cmd.state(2, SYM, SymbolState.CLOSED, 0));
        assertKinds(closeEvs, "STATE", "CLOSE");
        assertEquals(close(SYM, 10_020, NULL, 0, "LAST_TRADE"), closeEvs.get(1).text());
        final ClosingPriceDecoder d = closeEvs.get(1).closingPrice();
        assertEquals(ClosePriceSource.LAST_TRADE, d.source());

        final List<Ev> roll = h.send(Cmd.dayRoll(3, 20261117));
        final long rollTs = h.now;
        assertKinds(roll, "STATE", "REF", "DAYROLLED"); // already closed: no second ClosingPrice
        assertEquals(rollRef(SYM, 10_020, 9_018, 11_022, rollTs), roll.get(1).text());
        final Symbol s = h.m.symbols.get(0);
        assertEquals(10_020, s.refPrice);
        assertEquals(10_020, s.prevClosePx);
        assertEquals(9_018, s.bandLow);
        assertEquals(11_022, s.bandHigh);

        // the new band is enforced
        h.send(Cmd.state(4, SYM, SymbolState.CONTINUOUS, 0));
        assertReject(h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 11_023, 1)), RejectReason.PRICE_BAND);
        assertReject(h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 9_017, 1)), RejectReason.PRICE_BAND);
        assertKinds(h.send(Cmd.limit(SA, 3, A, SYM, Side.BUY, 11_022, 1)), "ER:NEW", "DELTA");

        // a reference price sourced at or before the roll cannot clobber the close; a newer one applies
        assertEquals(List.of(), t(h.send(Cmd.ref(SYM, 12_000, rollTs - 1))));
        assertEquals(List.of(), t(h.send(Cmd.ref(SYM, 12_000, rollTs))));
        assertEquals(10_020, s.refPrice);
        // a SymbolDefinition update does not override it either (the close counts as an applied reference)
        assertTrue(t(h.send(Cmd.symbol(5, SYM, "AAPL", 1, 1, 10_000, 1_000_000, 1000, SymbolState.CONTINUOUS)))
            .get(0).contains(" ref=10020 "));
        assertKinds(h.send(Cmd.ref(SYM, 12_000, rollTs + 1)), "REF");
        assertEquals(12_000, s.refPrice);
    }

    @Test
    void closingAuctionThatTradesIsTheAuctionClose()
    {
        final Harness h = new Harness().standard();
        cross(h, SYM, 10_050, 1); // an earlier continuous print at another price
        h.send(Cmd.state(2, SYM, SymbolState.AUCTION_CALL, 0));
        h.send(Cmd.limit(SA, cl++, A, SYM, Side.BUY, 10_000, 10));
        h.send(Cmd.limit(SB, cl++, B, SYM, Side.SELL, 9_990, 10));
        final List<Ev> evs = h.send(Cmd.state(3, SYM, SymbolState.CLOSED, 0));
        assertKinds(evs, "TRADE", "ER:FILL", "ER:FILL", "DELTA", "DELTA", "AUCT", "STATE", "CLOSE");
        assertEquals(close(SYM, 10_000, NULL, 0, "AUCTION"), evs.get(7).text());
        final List<Ev> roll = h.send(Cmd.dayRoll(4, 20261117));
        assertEquals(List.of(rollRef(SYM, 10_000, 9_000, 11_000, h.now)), refs(roll));
        assertEquals(10_000, h.m.symbols.get(0).refPrice);
    }

    @Test
    void closingAuctionWithoutATradeFallsBackToTheLastTrade()
    {
        final Harness h = new Harness().standard();
        cross(h, SYM, 10_050, 1);
        h.send(Cmd.state(2, SYM, SymbolState.AUCTION_CALL, 0));
        h.send(Cmd.limit(SA, cl++, A, SYM, Side.BUY, 9_900, 10));
        h.send(Cmd.limit(SB, cl++, B, SYM, Side.SELL, 10_100, 10));
        final List<Ev> evs = h.send(Cmd.state(3, SYM, SymbolState.CLOSED, 0));
        assertKinds(evs, "AUCT", "STATE", "ER:EXPIRED", "ER:EXPIRED", "DELTA", "DELTA", "CLOSE");
        assertEquals(close(SYM, 10_050, NULL, 0, "LAST_TRADE"), evs.get(6).text());
    }

    @Test
    void auctionPrintsCountAsTradesAndAHaltedSymbolClosesOnItsLastTrade()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.state(2, SYM, SymbolState.AUCTION_CALL, 0));
        h.send(Cmd.limit(SA, cl++, A, SYM, Side.BUY, 10_000, 5));
        h.send(Cmd.limit(SB, cl++, B, SYM, Side.SELL, 10_000, 5));
        h.send(Cmd.state(3, SYM, SymbolState.CONTINUOUS, 0)); // opening uncross prints at 10_000
        assertEquals(10_000, h.m.symbols.get(0).lastTradePx);
        h.send(Cmd.state(4, SYM, SymbolState.HALTED, 0));
        final List<Ev> evs = h.send(Cmd.state(5, SYM, SymbolState.CLOSED, 0));
        assertEquals(List.of(close(SYM, 10_000, NULL, 0, "LAST_TRADE")), closes(evs));
    }

    @Test
    void noTradeTodayUsesThePreviousCloseAndNoPriceAtAllIsNone()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.symbol(2, SYM2, "MSFT", 1, 1, 5_000, 1_000_000, 1000, SymbolState.CONTINUOUS));
        cross(h, SYM, 10_000, 2);
        final List<Ev> day0 = h.send(Cmd.state(3, Cmd.ALL_SYMBOLS, SymbolState.CLOSED, 0));
        assertEquals(List.of(close(SYM, 10_000, NULL, 0, "LAST_TRADE"), close(SYM2, NULL, NULL, 0, "NONE")), closes(day0));
        assertEquals(ClosePriceSource.NONE, day0.get(day0.size() - 1).closingPrice().source());

        List<Ev> roll = h.send(Cmd.dayRoll(4, 20261117));
        assertEquals(List.of(rollRef(SYM, 10_000, 9_000, 11_000, h.now)), refs(roll), "no reference from a NONE close");
        assertEquals(5_000, h.m.symbols.get(1).refPrice, "symbol without a close keeps its reference");

        h.send(Cmd.state(5, Cmd.ALL_SYMBOLS, SymbolState.CONTINUOUS, 0));
        final List<Ev> day1 = h.send(Cmd.state(6, Cmd.ALL_SYMBOLS, SymbolState.CLOSED, 0));
        assertEquals(List.of(close(SYM, 10_000, 10_000, 20261117, "PREVIOUS_CLOSE"), close(SYM2, NULL, NULL, 20261117, "NONE")),
            closes(day1));

        roll = h.send(Cmd.dayRoll(7, 20261118));
        assertEquals(List.of(rollRef(SYM, 10_000, 9_000, 11_000, h.now)), refs(roll));
        // day 2: symbol 2 finally trades and closes on it; the previous close of symbol 1 is still carried
        h.send(Cmd.state(8, Cmd.ALL_SYMBOLS, SymbolState.CONTINUOUS, 0));
        cross(h, SYM2, 5_100, 1);
        final List<Ev> day2 = h.send(Cmd.state(9, Cmd.ALL_SYMBOLS, SymbolState.CLOSED, 0));
        assertEquals(List.of(close(SYM, 10_000, 10_000, 20261118, "PREVIOUS_CLOSE"), close(SYM2, 5_100, NULL, 20261118, "LAST_TRADE")),
            closes(day2));
    }

    @Test
    void reclosingWithoutTradesRepeatsTheCloseAndTradingAfterACloseClosesAgainAtTheRoll()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.state(1, SYM, SymbolState.AUCTION_CALL, 0));
        h.send(Cmd.limit(SA, cl++, A, SYM, Side.BUY, 10_000, 5));
        h.send(Cmd.limit(SB, cl++, B, SYM, Side.SELL, 10_000, 5));
        final String auctionClose = close(SYM, 10_000, NULL, 0, "AUCTION");
        assertEquals(List.of(auctionClose), closes(h.send(Cmd.state(2, SYM, SymbolState.CLOSED, 0))));
        // CLOSED -> PRE_OPEN -> CLOSED without a trade: the same close, source kept
        h.send(Cmd.state(3, SYM, SymbolState.PRE_OPEN, 0));
        assertEquals(List.of(auctionClose), closes(h.send(Cmd.state(4, SYM, SymbolState.CLOSED, 0))));
        // reopened without trading, the roll keeps the close (no implicit ClosingPrice)
        h.send(Cmd.state(5, SYM, SymbolState.PRE_OPEN, 0));
        h.send(Cmd.state(6, SYM, SymbolState.CONTINUOUS, 0));
        List<Ev> roll = h.send(Cmd.dayRoll(7, 20261117));
        assertKinds(roll, "STATE", "REF", "DAYROLLED");

        // next day: a close, then trading resumes after it; the roll closes the symbol again on the last trade
        h.send(Cmd.state(8, SYM, SymbolState.CONTINUOUS, 0));
        cross(h, SYM, 10_100, 1);
        h.send(Cmd.state(9, SYM, SymbolState.CLOSED, 0));
        h.send(Cmd.state(10, SYM, SymbolState.PRE_OPEN, 0));
        h.send(Cmd.state(11, SYM, SymbolState.CONTINUOUS, 0));
        cross(h, SYM, 10_200, 1);
        h.send(Cmd.limit(SA, cl++, A, SYM, Side.BUY, 10_000, 1));
        roll = h.send(Cmd.dayRoll(12, 20261118));
        assertKinds(roll, "ER:EXPIRED", "DELTA", "CLOSE", "STATE", "REF", "DAYROLLED");
        assertEquals(close(SYM, 10_200, 10_000, 20261117, "LAST_TRADE"), roll.get(2).text());
        assertEquals(rollRef(SYM, 10_200, 9_180, 11_220, h.now), roll.get(4).text());
    }

    @Test
    void scheduledCloseEmitsTheCloseFromTheTimerEntry()
    {
        final Harness h = new Harness().standard();
        cross(h, SYM, 10_030, 1);
        final long at = h.now + 10_000;
        assertEquals(List.of(), t(h.send(Cmd.state(2, SYM, SymbolState.CLOSED, at))));
        final List<Ev> fired = h.advanceTo(at);
        assertEquals(List.of(close(SYM, 10_030, NULL, 0, "LAST_TRADE")), closes(fired));
        assertEquals(0, h.entries.get(h.entries.size() - 1).command() == null ? 0 : 1, "emitted by the timer entry");
    }

    @Test
    void redefiningARemovedSymbolStartsWithoutHistory()
    {
        final Harness h = new Harness().standard();
        cross(h, SYM, 10_030, 1);
        h.send(Cmd.state(2, SYM, SymbolState.CLOSED, 0));
        h.send(Cmd.dayRoll(3, 20261117));
        h.send(Cmd.state(4, SYM, SymbolState.REMOVED, 0));
        h.send(Cmd.symbol(5, SYM, "AAPL", 1, 1, 9_000, 1_000_000, 1000, SymbolState.CONTINUOUS));
        assertEquals(List.of(close(SYM, NULL, NULL, 20261117, "NONE")), closes(h.send(Cmd.state(6, SYM, SymbolState.CLOSED, 0))));
        assertEquals(List.of(), refs(h.send(Cmd.dayRoll(7, 20261118))));
        assertEquals(9_000, h.m.symbols.get(0).refPrice);
    }

    // ------------------------------------------------------------------ snapshots and determinism

    /** First part of a two-day script: two symbols, a position limit, trades and a resting order. */
    private Harness scriptPart1()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.symbol(2, SYM2, "MSFT", 1, 1, 5_000, 1_000_000, 1000, SymbolState.CONTINUOUS));
        h.send(Cmd.risk(3, A, 1_000_000_000_000L, 500, true));
        cross(h, SYM, 10_010, 40);
        cross(h, SYM, 10_020, 30);
        h.send(Cmd.limit(SB, cl++, B, SYM2, Side.SELL, 5_050, 7)); // resting, expires at the close
        return h;
    }

    private void scriptCloseAndRoll(final Harness h)
    {
        h.send(Cmd.state(10, SYM2, SymbolState.AUCTION_CALL, 0));
        h.send(Cmd.limit(SA, cl++, A, SYM2, Side.BUY, 5_060, 5));
        h.send(Cmd.state(11, Cmd.ALL_SYMBOLS, SymbolState.CLOSED, 0)); // SYM2 closing auction trades 5 at 5_050
        h.send(Cmd.dayRoll(12, 20261117));
    }

    private void scriptDay2(final Harness h)
    {
        h.send(Cmd.state(20, Cmd.ALL_SYMBOLS, SymbolState.CONTINUOUS, 0));
        h.send(Cmd.limit(SA, cl++, A, SYM, Side.BUY, 10_020, 431)); // 70 carried + 431 > 500: POSITION_LIMIT
        h.send(Cmd.limit(SA, cl++, A, SYM, Side.BUY, 10_020, 430));
        h.send(Cmd.ref(SYM, 11_000, 1)); // stale: ignored
        cross(h, SYM2, 5_040, 2);
        h.send(Cmd.state(21, SYM, SymbolState.CLOSED, 0)); // PREVIOUS_CLOSE
        h.send(Cmd.dayRoll(22, 20261118)); // SYM2 implicit close LAST_TRADE
    }

    @Test
    void snapshotRoundTripsMidDayAndAcrossTheRollover()
    {
        final Harness a = scriptPart1();
        final Harness b = a.restoreCopy(); // mid-day: trades printed, no close yet
        assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(b.m));
        final int split1 = a.entries.size();
        scriptCloseAndRoll(a);
        final Harness c = a.restoreCopy(); // right after the roll: carried positions, previous closes, new refs
        assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(c.m));
        final int split2 = a.entries.size();
        scriptDay2(a);

        for (final Harness.Entry e : a.entries.subList(split1, a.entries.size()))
        {
            b.apply(e);
        }
        for (final Harness.Entry e : a.entries.subList(split2, a.entries.size()))
        {
            c.apply(e);
        }
        final List<String> all = Harness.texts(a.events);
        assertTrue(all.contains(close(SYM2, 5_050, NULL, 0, "AUCTION")), all::toString);
        assertTrue(all.contains(close(SYM, 10_020, 10_020, 20261117, "PREVIOUS_CLOSE")), all::toString);
        assertTrue(all.contains(close(SYM2, 5_040, 5_050, 20261117, "LAST_TRADE")), all::toString);
        assertTrue(all.stream().anyMatch(s -> s.contains("rej=POSITION_LIMIT")));
        for (final Harness r : new Harness[] {b, c})
        {
            final List<Ev> tailA = a.events.subList(a.events.size() - (r.events.size() - 1), a.events.size());
            assertTrue(Harness.sameBytes(tailA, r.events.subList(1, r.events.size())), "outputs after restore identical");
            assertArrayEquals(a.m.chainHash(), r.m.chainHash());
            assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(r.m), "next snapshots identical");
        }
        assertEquals(5_040, a.m.symbols.get(1).refPrice);
        assertEquals(10_020, a.m.symbols.get(0).refPrice);
    }

    @Test
    void multiDayRunIsDeterministicAcrossDifferentlySizedInstances()
    {
        final Harness a = scriptPart1();
        scriptCloseAndRoll(a);
        scriptDay2(a);
        final Harness b = new Harness(new MatcherShard(0, new MatcherConfig(1 << 12, 1 << 10, 4096, 1 << 12)));
        for (final Harness.Entry e : a.entries)
        {
            b.apply(e);
        }
        assertTrue(Harness.sameBytes(a.events, b.events));
        assertArrayEquals(a.m.chainHash(), b.m.chainHash());
        assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(b.m));
        new StreamChecker().check(a.events);
    }
}
