package io.lmb.matcher;

import io.lmb.sbe.CancelReason;
import io.lmb.sbe.ExecType;
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
 * Dynamic collar with volatility interruption (and its interaction with the state machine and the day cycle),
 * and self-trade prevention.
 */
class BandStpTest
{
    static final int SYM = 1;
    static final int SYM2 = 2;
    static final int A = 100;
    static final int B = 200;
    static final int C = 300;
    static final long SA = 1001;
    static final long SA2 = 1002; // a second session of firm A
    static final long SB = 2001;
    static final long SC = 3001;
    /** Collar 1% around the last print; volatility auction 1 ms. Static band 10% around ref $100.00. */
    static final long DYN = 100;
    static final long VOL = 1_000_000;

    private long cl = 100;

    static Harness collared(final SymbolState initial)
    {
        final Harness h = new Harness();
        h.send(Cmd.symbol(1, SYM, "AAPL", 1, 1, 10_000, 1_000_000, 1000, initial, DYN, VOL));
        for (final int f : new int[] {A, B, C})
        {
            h.send(Cmd.risk(f, f, 1_000_000_000_000L, 10_000_000, true));
        }
        return h;
    }

    /** B offers 10 @ 100.00, 10 @ 100.50 and 10 @ 101.50 (outside the 1% collar around 100.00). */
    static Harness withLadder()
    {
        final Harness h = collared(SymbolState.CONTINUOUS);
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_000, 10));
        h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 10_050, 10));
        h.send(Cmd.limit(SB, 3, B, SYM, Side.SELL, 10_150, 10));
        return h;
    }

    /** withLadder, then A's DAY buy 30 @ 102.00 trades 20 and trips the collar: a volatility auction, crossed book. */
    static Harness inVolatilityAuction()
    {
        final Harness h = withLadder();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_200, 30));
        assertTrue(h.m.symbols.get(0).inVolatilityAuction());
        return h;
    }

    static Symbol sym(final Harness h, final int idx)
    {
        return h.m.symbols.get(idx);
    }

    /** Delivers a timer the machine has already canceled (a late or replayed firing). */
    static List<Ev> fireStale(final Harness h, final long timerId)
    {
        h.logPosition += 64;
        return h.apply(new Harness.Entry(null, timerId, h.logPosition, h.now));
    }

    static List<String> only(final List<Ev> evs, final String prefix)
    {
        final List<String> out = new ArrayList<>();
        for (final String s : t(evs))
        {
            if (s.startsWith(prefix))
            {
                out.add(s);
            }
        }
        return out;
    }

    static String state(final long corr, final int sym, final int rc, final String from, final String to, final String cause)
    {
        return "STATE corr=" + corr + " sym=" + sym + " rc=" + rc + " " + from + "->" + to + " cause=" + cause;
    }

    static String close(final int sym, final long px, final String src)
    {
        return "CLOSE sym=" + sym + " px=" + px + " prev=" + NULL + " date=0 src=" + src;
    }

    static String ref(final int sym, final long ref, final long low, final long high, final long ts)
    {
        return "REF sym=" + sym + " ref=" + ref + " bid=" + NULL + " ofr=" + NULL + " band=" + low + ".." + high + " ts=" + ts;
    }

    // ------------------------------------------------------------------ collar off

    @Test
    void zeroCollarSweepsEveryLevelAsBefore()
    {
        final Harness h = new Harness();
        h.send(Cmd.symbol(1, SYM, "AAPL", 1, 1, 10_000, 1_000_000, 1000, SymbolState.CONTINUOUS, 0, VOL));
        h.send(Cmd.risk(A, A, 1_000_000_000_000L, 10_000_000, true));
        h.send(Cmd.risk(B, B, 1_000_000_000_000L, 10_000_000, true));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_000, 10));
        h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 10_900, 10));
        assertKinds(h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_900, 20)),
            "ER:NEW", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "TRADE", "ER:FILL", "ER:FILL", "DELTA", "DELTA");
        assertTrue(h.timersById.isEmpty());
    }

    @Test
    void symbolDefinitionValidatesTheCollarFields()
    {
        final Harness h = new Harness();
        for (final long[] bad : new long[][] {{100, 0}, {0, -1}, {100, 86_400_000_000_001L}})
        {
            assertEquals(List.of("CMDREJ corr=5 t=11 r=MALFORMED"),
                t(h.send(Cmd.symbol(5, SYM, "X", 1, 1, 10_000, 100, 1000, SymbolState.CONTINUOUS, bad[0], bad[1]))));
        }
        assertKinds(h.send(Cmd.symbol(6, SYM, "X", 1, 1, 10_000, 100, 1000, SymbolState.CONTINUOUS, 0, 0)), "SYMDEF", "REF");
        assertKinds(h.send(Cmd.symbol(7, SYM2, "Y", 1, 1, 10_000, 100, 1000, SymbolState.CONTINUOUS, 0, 0)), "SYMDEF", "REF");
        assertEquals(0, sym(h, 1).dynamicBandBps);
        assertEquals(0, sym(h, 1).volatilityAuctionNs);
        assertKinds(h.send(Cmd.symbol(8, SYM2, "Y", 1, 1, 10_000, 100, 1000, SymbolState.CONTINUOUS, DYN, VOL)), "SYMDEF"); // band unchanged: no REF
        assertEquals(DYN, sym(h, 1).dynamicBandBps);
        assertEquals(VOL, sym(h, 1).volatilityAuctionNs);
    }

    // ------------------------------------------------------------------ interruption and the timer

    @Test
    void sweepStopsAtTheCollarThenTheTimerUncrossesAndRecentresTheBand()
    {
        final Harness h = withLadder();
        assertReject(h.send(Cmd.limit(SC, 1, C, SYM, Side.SELL, 11_100, 1)), RejectReason.PRICE_BAND);
        final List<Ev> evs = h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_200, 30));
        // fills, then the remainder rests (LIMIT DAY), deltas, the state change, the indicative uncross
        assertKinds(evs, "ER:NEW", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "TRADE", "ER:PARTIAL_FILL", "ER:FILL",
            "DELTA", "DELTA", "DELTA", "STATE", "AUCT");
        assertEquals("DELTA sym=1 side=1 px=10200 q=10 n=1", evs.get(7).text());
        assertEquals(state(0, SYM, 0, "CONTINUOUS", "AUCTION_CALL", "VOLATILITY_INTERRUPTION"), evs.get(10).text());
        assertEquals("AUCT sym=1 px=10150 v=10 imb=0 side=255 final=FALSE", evs.get(11).text());
        final Symbol s = sym(h, 0);
        assertEquals(SymbolState.AUCTION_CALL.value(), s.state);
        assertEquals(10_050, s.lastTradePx);
        final long timerId = s.volTimerId;
        final long end = h.now + VOL;
        assertEquals(end, s.volEndNs);
        assertEquals(end, h.timersById.get(timerId));
        assertEquals(0, h.m.volTimers.get(timerId));

        // during the auction: no market orders, limits join the auction book
        assertReject(h.send(Cmd.market(SC, 2, C, SYM, Side.BUY, 1)), RejectReason.MARKET_ORDER_NOT_ALLOWED);
        assertTrue(h.advanceTo(end - 1).isEmpty());

        final List<Ev> fire = h.advanceTo(end);
        assertKinds(fire, "TRADE", "ER:FILL", "ER:FILL", "DELTA", "DELTA", "AUCT", "REF", "STATE");
        assertEquals("TRADE m=3 px=10150 q=10 b=4 s=3 sym=1 bf=100 sf=200 agg=255 auct=TRUE", fire.get(0).text());
        assertEquals(ref(SYM, 10_150, 9_135, 11_165, end), fire.get(6).text());
        assertEquals(state(0, SYM, 0, "AUCTION_CALL", "CONTINUOUS", "VOLATILITY_AUCTION_END"), fire.get(7).text());
        assertEquals(0, s.volTimerId);
        assertTrue(h.m.volTimers.isEmpty());
        assertEquals(10_150, s.refPrice);
        assertEquals(end, s.lastRefSourceTs);

        // the re-centred band admits 111.00; an external reference no newer than the re-centre is stale
        assertKinds(h.send(Cmd.limit(SC, 3, C, SYM, Side.SELL, 11_100, 1)), "ER:NEW", "DELTA");
        assertTrue(h.send(Cmd.ref(SYM, 10_000, end)).isEmpty());
        assertEquals(10_150, s.refPrice);

        // the collar is now centred on the uncross print: 102.50 trades, 103.00 (> 102.515) interrupts again
        h.send(Cmd.limit(SB, 4, B, SYM, Side.SELL, 10_250, 5));
        h.send(Cmd.limit(SB, 5, B, SYM, Side.SELL, 10_300, 5));
        final List<Ev> again = h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 10_300, 10));
        assertEquals(1, only(again, "TRADE").size());
        assertEquals(List.of(state(0, SYM, 0, "CONTINUOUS", "AUCTION_CALL", "VOLATILITY_INTERRUPTION")), only(again, "STATE"));

        // day cycle: the volatility auction ends by timer; the close is the last (auction) print
        final List<Ev> fire2 = h.advanceTo(h.now + VOL);
        assertEquals("REF", fire2.get(fire2.size() - 2).text().substring(0, 3));
        assertEquals(List.of(close(SYM, 10_300, "LAST_TRADE")), only(h.send(Cmd.state(9, SYM, SymbolState.CLOSED, 0)), "CLOSE"));
    }

    @Test
    void theCollarIsCentredOnTheLastPrintBeforeTheOrderStarts()
    {
        final Harness h = collared(SymbolState.CONTINUOUS);
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_050, 1));
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_050, 1)); // last print 100.50: collar 99.495 .. 101.505
        h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 10_150, 1));
        assertKinds(h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 10_150, 1)), "ER:NEW", "TRADE", "ER:FILL", "ER:FILL", "DELTA");
        // one order cannot walk the price: centred on 101.50 for the whole order, 103.00 > 102.515 stops it
        h.send(Cmd.limit(SB, 3, B, SYM, Side.SELL, 10_200, 1));
        h.send(Cmd.limit(SB, 4, B, SYM, Side.SELL, 10_250, 1));
        h.send(Cmd.limit(SB, 5, B, SYM, Side.SELL, 10_300, 1));
        final List<Ev> evs = h.send(Cmd.limit(SA, 3, A, SYM, Side.BUY, 10_300, 3));
        assertEquals(2, only(evs, "TRADE").size());
        assertEquals(List.of(state(0, SYM, 0, "CONTINUOUS", "AUCTION_CALL", "VOLATILITY_INTERRUPTION")), only(evs, "STATE"));
        assertEquals(10_250, sym(h, 0).lastTradePx);
    }

    @Test
    void sellSideSweepStopsBelowTheCollar()
    {
        final Harness h = collared(SymbolState.CONTINUOUS);
        h.send(Cmd.limit(SB, 1, B, SYM, Side.BUY, 10_000, 10));
        h.send(Cmd.limit(SB, 2, B, SYM, Side.BUY, 9_950, 10));
        h.send(Cmd.limit(SB, 3, B, SYM, Side.BUY, 9_850, 10)); // < 99.00
        final List<Ev> evs = h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 9_800, 30));
        assertEquals(2, only(evs, "TRADE").size());
        assertEquals(List.of("DELTA sym=1 side=1 px=10000 q=0 n=0", "DELTA sym=1 side=1 px=9950 q=0 n=0",
            "DELTA sym=1 side=2 px=9800 q=10 n=1"), only(evs, "DELTA"));
        assertEquals(List.of(state(0, SYM, 0, "CONTINUOUS", "AUCTION_CALL", "VOLATILITY_INTERRUPTION")), only(evs, "STATE"));
    }

    @Test
    void iocAndMarketRemaindersOfTheTriggeringOrderAreCanceled()
    {
        final Harness ioc = withLadder();
        final List<Ev> a = ioc.send(Cmd.ioc(SA, 1, A, SYM, Side.BUY, 10_200, 30));
        assertKinds(a, "ER:NEW", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "TRADE", "ER:PARTIAL_FILL", "ER:FILL",
            "ER:CANCELED", "DELTA", "DELTA", "STATE");
        assertEquals(CancelReason.IOC_REMAINDER, a.get(7).er().cancelReason());
        assertEquals(20, a.get(7).er().cumQty());
        assertEquals(state(0, SYM, 0, "CONTINUOUS", "AUCTION_CALL", "VOLATILITY_INTERRUPTION"), a.get(10).text());

        final Harness mkt = withLadder();
        final List<Ev> b = mkt.send(Cmd.market(SA, 1, A, SYM, Side.BUY, 30));
        assertKinds(b, "ER:NEW", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "TRADE", "ER:PARTIAL_FILL", "ER:FILL",
            "ER:CANCELED", "DELTA", "DELTA", "STATE");
        assertEquals(CancelReason.MARKET_COLLAR, b.get(7).er().cancelReason());
        for (final Harness h : new Harness[] {ioc, mkt})
        {
            assertEquals(0, h.m.firms.get(0).openNotional, "remainder released its risk");
            // no crossed book: the auction ends by timer without a print and without re-centring
            final List<Ev> fire = h.advanceTo(h.now + VOL);
            assertEquals(List.of(state(0, SYM, 0, "AUCTION_CALL", "CONTINUOUS", "VOLATILITY_AUCTION_END")), only(fire, "STATE"));
            assertTrue(only(fire, "REF").isEmpty());
            assertEquals(10_000, sym(h, 0).refPrice);
        }
    }

    // ------------------------------------------------------------------ superseded by the state machine

    @Test
    void closingAuctionTakesOverAVolatilityAuctionAndTheCloseIsTheAuction()
    {
        final Harness h = inVolatilityAuction();
        final long timerId = sym(h, 0).volTimerId;
        final List<Ev> call = h.send(Cmd.state(5, SYM, SymbolState.AUCTION_CALL, 0));
        assertEquals(List.of(state(5, SYM, 7, "AUCTION_CALL", "AUCTION_CALL", "COMMAND"),
            "AUCT sym=1 px=10150 v=10 imb=0 side=255 final=FALSE"), t(call));
        assertEquals(0, sym(h, 0).volTimerId);
        assertTrue(h.timersById.isEmpty(), "volatility timer canceled");
        // the scheduled auction is an ordinary one now: a second AUCTION_CALL is not a transition
        assertEquals(List.of("CMDREJ corr=6 t=12 r=INVALID_STATE_TRANSITION"), t(h.send(Cmd.state(6, SYM, SymbolState.AUCTION_CALL, 0))));
        // the timer does not reopen the symbol, even if it is delivered after the cancel
        assertTrue(h.advanceTo(h.now + 2 * VOL).isEmpty());
        assertTrue(fireStale(h, timerId).isEmpty());
        assertEquals(SymbolState.AUCTION_CALL.value(), sym(h, 0).state);

        final List<Ev> closed = h.send(Cmd.state(7, SYM, SymbolState.CLOSED, 0));
        assertKinds(closed, "TRADE", "ER:FILL", "ER:FILL", "DELTA", "DELTA", "AUCT", "STATE", "CLOSE");
        assertEquals(state(7, SYM, 7, "AUCTION_CALL", "CLOSED", "COMMAND"), closed.get(6).text());
        assertEquals(close(SYM, 10_150, "AUCTION"), closed.get(7).text());
        assertTrue(only(closed, "REF").isEmpty(), "no re-centre on CLOSED");

        final List<Ev> roll = h.send(Cmd.dayRoll(8, 20261128));
        assertEquals(List.of(state(8, SYM, 0, "CLOSED", "PRE_OPEN", "DAY_ROLLOVER"),
            ref(SYM, 10_150, 9_135, 11_165, h.now), "DAYROLLED corr=8 date=20261128 n=0"), t(roll));
    }

    @Test
    void marketWideScheduledClosingAuctionTakesOverAVolatilityAuction()
    {
        final Harness h = inVolatilityAuction();
        h.send(Cmd.symbol(2, SYM2, "MSFT", 1, 1, 5_000, 1_000_000, 1000, SymbolState.CONTINUOUS, DYN, VOL));
        h.send(Cmd.limit(SB, 10, B, SYM2, Side.SELL, 5_000, 5));
        h.send(Cmd.limit(SA, 10, A, SYM2, Side.BUY, 5_000, 5));
        final long volTimer = sym(h, 0).volTimerId;
        final long eff = sym(h, 0).volEndNs - VOL / 2;
        assertTrue(h.send(Cmd.state(3, Cmd.ALL_SYMBOLS, SymbolState.AUCTION_CALL, eff)).isEmpty());
        final List<Ev> call = h.advanceTo(eff);
        assertEquals(List.of(state(3, SYM, 7, "AUCTION_CALL", "AUCTION_CALL", "COMMAND"),
            "AUCT sym=1 px=10150 v=10 imb=0 side=255 final=FALSE",
            state(3, SYM2, 7, "CONTINUOUS", "AUCTION_CALL", "COMMAND")), t(call));
        assertTrue(h.m.volTimers.isEmpty());
        assertTrue(!h.timersById.containsKey(volTimer));
        assertTrue(h.advanceTo(eff + 2 * VOL).isEmpty(), "the volatility timer does not reopen symbol 1");
        assertTrue(fireStale(h, volTimer).isEmpty());

        final List<Ev> closed = h.send(Cmd.state(4, Cmd.ALL_SYMBOLS, SymbolState.CLOSED, 0));
        assertEquals(List.of(state(4, SYM, 7, "AUCTION_CALL", "CLOSED", "COMMAND"),
            state(4, SYM2, 7, "AUCTION_CALL", "CLOSED", "COMMAND")), only(closed, "STATE"));
        assertEquals(List.of(close(SYM, 10_150, "AUCTION"), close(SYM2, 5_000, "LAST_TRADE")), only(closed, "CLOSE"));
    }

    @Test
    void haltEndsAVolatilityAuctionAndTheResumeUncrossesWithoutRecentring()
    {
        final Harness h = inVolatilityAuction();
        final long timerId = sym(h, 0).volTimerId;
        assertEquals(List.of(state(5, SYM, 7, "AUCTION_CALL", "HALTED", "COMMAND")), t(h.send(Cmd.state(5, SYM, SymbolState.HALTED, 0))));
        assertTrue(h.timersById.isEmpty());
        assertTrue(h.advanceTo(h.now + 2 * VOL).isEmpty());
        assertTrue(fireStale(h, timerId).isEmpty());
        assertEquals(SymbolState.HALTED.value(), sym(h, 0).state);

        final List<Ev> resume = h.send(Cmd.state(6, SYM, SymbolState.CONTINUOUS, 0));
        assertKinds(resume, "TRADE", "ER:FILL", "ER:FILL", "DELTA", "DELTA", "AUCT", "STATE");
        assertEquals(state(6, SYM, 7, "HALTED", "CONTINUOUS", "COMMAND"), resume.get(6).text());
        assertEquals(10_000, sym(h, 0).refPrice, "no re-centre after a halt");
        assertEquals(9_000, sym(h, 0).bandLow);
        assertEquals(11_000, sym(h, 0).bandHigh);
    }

    @Test
    void closedDuringAVolatilityAuctionUncrossesAndClosesAtTheAuctionPrice()
    {
        final Harness h = inVolatilityAuction();
        final List<Ev> closed = h.send(Cmd.state(5, SYM, SymbolState.CLOSED, 0));
        assertKinds(closed, "TRADE", "ER:FILL", "ER:FILL", "DELTA", "DELTA", "AUCT", "STATE", "CLOSE");
        assertEquals(close(SYM, 10_150, "AUCTION"), closed.get(7).text());
        assertEquals(10_000, sym(h, 0).refPrice);
        assertTrue(h.timersById.isEmpty());
        assertTrue(h.advanceTo(h.now + 2 * VOL).isEmpty());
    }

    @Test
    void dayRolloverDuringAVolatilityAuctionClosesAtTheLastTradeAndCancelsTheTimer()
    {
        final Harness h = inVolatilityAuction();
        final long timerId = sym(h, 0).volTimerId;
        final List<Ev> roll = h.send(Cmd.dayRoll(5, 20261128));
        assertKinds(roll, "ER:EXPIRED", "ER:EXPIRED", "DELTA", "DELTA", "CLOSE", "STATE", "REF", "DAYROLLED");
        assertEquals(close(SYM, 10_050, "LAST_TRADE"), roll.get(4).text());
        assertEquals(state(5, SYM, 0, "AUCTION_CALL", "PRE_OPEN", "DAY_ROLLOVER"), roll.get(5).text());
        assertEquals(ref(SYM, 10_050, 9_045, 11_055, h.now), roll.get(6).text());
        assertEquals(0, sym(h, 0).volTimerId);
        assertTrue(h.timersById.isEmpty());
        assertTrue(fireStale(h, timerId).isEmpty());
        assertEquals(SymbolState.PRE_OPEN.value(), sym(h, 0).state);
        // the next day opens with the collar centred on the reference, which is the close (here the last print)
        h.send(Cmd.state(6, SYM, SymbolState.CONTINUOUS, 0));
        h.send(Cmd.limit(SB, 20, B, SYM, Side.SELL, 10_150, 1));
        h.send(Cmd.limit(SB, 21, B, SYM, Side.SELL, 10_160, 1)); // > 101.505
        final List<Ev> evs = h.send(Cmd.limit(SA, 20, A, SYM, Side.BUY, 10_160, 2));
        assertEquals(1, only(evs, "TRADE").size());
        assertEquals(List.of(state(0, SYM, 0, "CONTINUOUS", "AUCTION_CALL", "VOLATILITY_INTERRUPTION")), only(evs, "STATE"));
    }

    @Test
    void openingAuctionIsNotCollaredAndItsPriceCentresTheCollar()
    {
        final Harness h = collared(SymbolState.PRE_OPEN);
        h.send(Cmd.state(2, SYM, SymbolState.AUCTION_CALL, 0));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_500, 10)); // 5% from the reference
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_600, 10));
        // an ordinary auction: AUCTION_CALL -> AUCTION_CALL is only for a running volatility auction
        assertEquals(List.of("CMDREJ corr=3 t=12 r=INVALID_STATE_TRANSITION"), t(h.send(Cmd.state(3, SYM, SymbolState.AUCTION_CALL, 0))));
        final List<Ev> open = h.send(Cmd.state(4, SYM, SymbolState.CONTINUOUS, 0));
        assertKinds(open, "TRADE", "ER:FILL", "ER:FILL", "DELTA", "DELTA", "AUCT", "STATE");
        assertEquals("AUCT sym=1 px=10500 v=10 imb=0 side=255 final=TRUE", open.get(5).text());
        assertEquals(state(4, SYM, 7, "AUCTION_CALL", "CONTINUOUS", "COMMAND"), open.get(6).text());
        assertEquals(10_000, sym(h, 0).refPrice, "an opening auction does not re-centre the band");
        // collar around 105.00: 106.00 trades, 106.10 (> 106.05) interrupts
        h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 10_600, 1));
        h.send(Cmd.limit(SB, 3, B, SYM, Side.SELL, 10_610, 1));
        final List<Ev> evs = h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 10_610, 2));
        assertEquals(1, only(evs, "TRADE").size());
        assertEquals(List.of(state(0, SYM, 0, "CONTINUOUS", "AUCTION_CALL", "VOLATILITY_INTERRUPTION")), only(evs, "STATE"));
    }

    @Test
    void explicitContinuousEndsAVolatilityAuctionEarlyAndRecentres()
    {
        final Harness h = inVolatilityAuction();
        final List<Ev> evs = h.send(Cmd.state(5, SYM, SymbolState.CONTINUOUS, 0));
        assertKinds(evs, "TRADE", "ER:FILL", "ER:FILL", "DELTA", "DELTA", "AUCT", "REF", "STATE");
        assertEquals(state(5, SYM, 7, "AUCTION_CALL", "CONTINUOUS", "COMMAND"), evs.get(7).text());
        assertEquals(10_150, sym(h, 0).refPrice);
        assertTrue(h.timersById.isEmpty());
    }

    // ------------------------------------------------------------------ snapshot

    @Test
    void snapshotWithAPendingVolatilityTimerContinuesIdentically()
    {
        final Harness a = inVolatilityAuction();
        a.send(Cmd.limit(SC, 1, C, SYM, Side.SELL, 10_100, 5));
        final Harness b = a.restoreCopy();
        final Symbol rs = sym(b, 0);
        assertEquals(sym(a, 0).volTimerId, rs.volTimerId);
        assertEquals(sym(a, 0).volEndNs, rs.volEndNs);
        assertEquals(0, b.m.volTimers.get(rs.volTimerId), "volTimers rebuilt on load");
        assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(b.m));
        for (final Harness h : new Harness[] {a, b})
        {
            h.send(Cmd.limit(SB, 9, B, SYM, Side.SELL, 10_150, 3));
            h.advanceTo(h.now + VOL);
            h.send(Cmd.limit(SB, 10, B, SYM, Side.SELL, 10_200, 3));
            h.send(Cmd.limit(SA, 10, A, SYM, Side.BUY, 10_300, 10));
        }
        final int n = b.events.size() - 1;
        assertTrue(n > 10);
        assertTrue(Harness.sameBytes(a.events.subList(a.events.size() - n, a.events.size()), b.events.subList(1, b.events.size())));
        assertTrue(only(b.events, "REF").size() == 1, "timer fired on the restored machine");
        assertArrayEquals(a.m.chainHash(), b.m.chainHash());
        assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(b.m));
    }

    @Test
    void snapshotAfterATakeoverStillIgnoresTheOldTimer()
    {
        final Harness a = inVolatilityAuction();
        final long timerId = sym(a, 0).volTimerId;
        a.send(Cmd.state(5, SYM, SymbolState.AUCTION_CALL, 0));
        final Harness b = a.restoreCopy();
        assertTrue(b.m.volTimers.isEmpty());
        assertTrue(fireStale(a, timerId).isEmpty());
        assertTrue(fireStale(b, timerId).isEmpty());
        assertArrayEquals(a.m.chainHash(), b.m.chainHash());
        assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(b.m));
    }

    // ------------------------------------------------------------------ self-trade prevention

    @Test
    void selfTradeAfterAPartialFillCancelsTheAggressorRemainderAcrossSessionsOfOneFirm()
    {
        final Harness h = collared(SymbolState.CONTINUOUS);
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_000, 5));
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_000, 10)); // A's own offer behind B's
        h.send(Cmd.limit(SC, 1, C, SYM, Side.SELL, 10_010, 10)); // never reached: STP stops the whole order
        final List<Ev> evs = h.send(Cmd.limit(SA2, 2, A, SYM, Side.BUY, 10_010, 20));
        assertKinds(evs, "ER:NEW", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "ER:CANCELED", "DELTA");
        final Ev cxl = evs.get(4);
        assertEquals(SA2, cxl.er().sessionId());
        assertEquals(CancelReason.SELF_TRADE, cxl.er().cancelReason());
        assertEquals(5, cxl.er().cumQty());
        assertEquals(0, cxl.er().leavesQty());
        assertEquals("DELTA sym=1 side=2 px=10000 q=10 n=1", evs.get(5).text());
        // the resting order is untouched and only it still holds open notional
        assertEquals(100_000, h.m.firms.get(0).openNotional);
        assertKinds(h.send(Cmd.cancel(SA, 3, 1, A, SYM, false)), "ER:CANCELED", "DELTA");
    }

    @Test
    void selfTradeCancelsEveryOrderTypeWithoutATrade()
    {
        final Harness h = collared(SymbolState.CONTINUOUS);
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_000, 10));
        int i = 0;
        for (final byte[] c : new byte[][] {Cmd.limit(SA, 2, A, SYM, Side.BUY, 10_000, 5),
            Cmd.ioc(SA2, 3, A, SYM, Side.BUY, 10_000, 5), Cmd.market(SA2, 4, A, SYM, Side.BUY, 5)})
        {
            final List<Ev> evs = h.send(c);
            assertKinds(evs, "ER:NEW", "ER:CANCELED");
            assertEquals(CancelReason.SELF_TRADE, evs.get(1).er().cancelReason(), "order " + i++);
        }
        assertEquals(100_000, h.m.firms.get(0).openNotional);
    }

    @Test
    void noSelfTradePreventionAcrossFirms()
    {
        final Harness h = collared(SymbolState.CONTINUOUS);
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_000, 5));
        h.send(Cmd.limit(SC, 1, C, SYM, Side.SELL, 10_000, 5));
        final List<Ev> evs = h.send(Cmd.limit(SB, 1, B, SYM, Side.BUY, 10_000, 10));
        assertKinds(evs, "ER:NEW", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "TRADE", "ER:FILL", "ER:FILL", "DELTA");
        assertTrue(t(evs).stream().noneMatch(s -> s.contains("cxl=SELF_TRADE")));
    }

    @Test
    void selfTradePreventionAppliesToReplaces()
    {
        final Harness h = collared(SymbolState.CONTINUOUS);
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_010, 3));
        h.send(Cmd.limit(SA2, 2, A, SYM, Side.SELL, 10_010, 10));
        // A re-prices its bid through B's offer into its own offer: fill 3, then the replaced order is canceled
        final List<Ev> evs = h.send(Cmd.replace(SA, 3, 1, A, SYM, 10_010, 10, false));
        assertKinds(evs, "ER:REPLACED", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "ER:CANCELED", "DELTA", "DELTA");
        assertEquals(CancelReason.SELF_TRADE, evs.get(4).er().cancelReason());
        assertEquals(3, evs.get(4).er().cumQty());
        assertEquals(List.of("DELTA sym=1 side=1 px=9900 q=0 n=0", "DELTA sym=1 side=2 px=10010 q=10 n=1"), only(evs, "DELTA"));
        assertTrue(h.m.orderById.get(evs.get(0).er().orderId()) < 0, "replaced order is gone");
        // the canceled order is unknown afterwards
        assertEquals(ExecType.CANCEL_REJECTED, h.send(Cmd.cancel(SA, 4, 3, A, SYM, false)).get(0).er().execType());
    }

    @Test
    void auctionUncrossDoesNotApplySelfTradePrevention()
    {
        final Harness h = collared(SymbolState.AUCTION_CALL);
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_000, 5));
        h.send(Cmd.limit(SA2, 2, A, SYM, Side.SELL, 10_000, 5));
        final List<Ev> evs = h.send(Cmd.state(2, SYM, SymbolState.CONTINUOUS, 0));
        assertEquals(List.of("TRADE m=1 px=10000 q=5 b=1 s=2 sym=1 bf=100 sf=100 agg=255 auct=TRUE"), only(evs, "TRADE"));
    }

    // ------------------------------------------------------------------ collar centre after a roll

    @Test
    void afterARollTheFirstTradeIsCollaredAroundTheReferenceNotYesterdaysLastPrint()
    {
        final Harness h = collared(SymbolState.CONTINUOUS);
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_000, 1));
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_000, 1));
        final Symbol s = sym(h, 0);
        assertEquals(10_000, s.collarCentre());
        h.send(Cmd.dayRoll(2, 20261128));
        assertEquals(10_000, s.lastTradePx);
        // a reference newer than the roll moves the reference; no trade yet today, so it is the collar centre
        final long ts = h.now + 1;
        assertEquals(1, only(h.send(Cmd.ref(SYM, 10_500, ts)), "REF").size());
        assertEquals(10_500, s.collarCentre());
        h.send(Cmd.state(3, SYM, SymbolState.CONTINUOUS, 0));
        h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 10_550, 1)); // 5.5% from yesterday's print, 0.48% from the ref
        h.send(Cmd.limit(SB, 3, B, SYM, Side.SELL, 10_610, 1)); // > 106.05
        final List<Ev> evs = h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 10_610, 2));
        assertEquals(1, only(evs, "TRADE").size());
        assertTrue(only(evs, "TRADE").get(0).contains(" px=10550 "));
        assertEquals(List.of(state(0, SYM, 0, "CONTINUOUS", "AUCTION_CALL", "VOLATILITY_INTERRUPTION")), only(evs, "STATE"));
        // from the first print of the day on, the collar is centred on the last print
        assertEquals(10_550, s.collarCentre());
    }

    // ------------------------------------------------------------------ SymbolDefinition publishes the static band

    @Test
    void symbolDefinitionThatSetsOrMovesTheBandEmitsReferencePriceApplied()
    {
        final Harness h = new Harness();
        final long none = Long.MIN_VALUE; // no ReferencePrice applied yet
        // created with a reference: SYMDEF then REF (nbb null, sourceTs = the symbol's reference timestamp)
        List<Ev> evs = h.send(Cmd.symbol(1, SYM, "AAPL", 1, 1, 10_000, 1_000, 1000, SymbolState.PRE_OPEN));
        assertKinds(evs, "SYMDEF", "REF");
        assertEquals(ref(SYM, 10_000, 9_000, 11_000, none), evs.get(1).text());
        // created without a reference: no band, no REF
        assertKinds(h.send(Cmd.symbol(2, SYM2, "MSFT", 1, 1, NULL, 1_000, 1000, SymbolState.PRE_OPEN)), "SYMDEF");
        // the same definition again: nothing moved, no REF
        assertKinds(h.send(Cmd.symbol(3, SYM, "AAPL", 1, 1, 10_000, 1_000, 1000, SymbolState.PRE_OPEN)), "SYMDEF");
        // a new reference (taken: no ReferencePrice applied yet), then a new band width
        assertEquals(ref(SYM, 10_200, 9_180, 11_220, none),
            h.send(Cmd.symbol(4, SYM, "AAPL", 1, 1, 10_200, 1_000, 1000, SymbolState.PRE_OPEN)).get(1).text());
        assertEquals(ref(SYM, 10_200, 9_690, 10_710, none),
            h.send(Cmd.symbol(5, SYM, "AAPL", 1, 1, 10_200, 1_000, 500, SymbolState.PRE_OPEN)).get(1).text());
        // the second symbol gets its first reference from a redefinition
        assertEquals(ref(SYM2, 5_000, 4_500, 5_500, none),
            h.send(Cmd.symbol(6, SYM2, "MSFT", 1, 1, 5_000, 1_000, 1000, SymbolState.PRE_OPEN)).get(1).text());

        // after a ReferencePrice, a definition's reference is no longer taken: no change, no REF
        assertEquals(1, only(h.send(Cmd.ref(SYM, 10_500, 5)), "REF").size());
        assertKinds(h.send(Cmd.symbol(7, SYM, "AAPL", 1, 1, 9_000, 1_000, 500, SymbolState.PRE_OPEN)), "SYMDEF");
        assertEquals(10_500, sym(h, 0).refPrice);
        // but a band width change is published around the applied reference, with its timestamp
        evs = h.send(Cmd.symbol(8, SYM, "AAPL", 1, 1, 9_000, 1_000, 200, SymbolState.PRE_OPEN));
        assertKinds(evs, "SYMDEF", "REF");
        assertEquals(ref(SYM, 10_500, 10_290, 10_710, 5), evs.get(1).text());

        // removed, then re-created fresh: REF again, timestamp reset
        h.send(Cmd.state(9, SYM, SymbolState.REMOVED, 0));
        evs = h.send(Cmd.symbol(10, SYM, "AAPL", 1, 1, 10_000, 1_000, 1000, SymbolState.PRE_OPEN));
        assertKinds(evs, "SYMDEF", "REF");
        assertEquals(ref(SYM, 10_000, 9_000, 11_000, none), evs.get(1).text());
    }
}
