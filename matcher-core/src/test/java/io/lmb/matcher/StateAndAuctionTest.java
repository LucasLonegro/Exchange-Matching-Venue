package io.lmb.matcher;

import io.lmb.api.ShardStateMachineFactory;
import io.lmb.sbe.CancelReason;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.LiquidityFlag;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.RejectReason;
import io.lmb.sbe.Side;
import io.lmb.sbe.SymbolState;
import io.lmb.testkit.InMemoryVenue;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.lmb.matcher.Cmd.NULL;
import static io.lmb.matcher.OrderScenarioTest.A;
import static io.lmb.matcher.OrderScenarioTest.B;
import static io.lmb.matcher.OrderScenarioTest.SA;
import static io.lmb.matcher.OrderScenarioTest.SB;
import static io.lmb.matcher.OrderScenarioTest.SYM;
import static io.lmb.matcher.OrderScenarioTest.assertKinds;
import static io.lmb.matcher.OrderScenarioTest.assertReject;
import static io.lmb.matcher.OrderScenarioTest.t;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateAndAuctionTest
{
    static Harness auction()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.state(1, SYM, SymbolState.AUCTION_CALL, 0));
        return h;
    }

    @Test
    void allowedAndForbiddenTransitions()
    {
        final byte[] all = {1, 2, 3, 4, 5, 6};
        for (final byte from : all)
        {
            for (final byte to : all)
            {
                final boolean expected = switch (from)
                {
                    case 1 -> to == 2 || to == 3 || to == 5 || to == 6;
                    case 2 -> to == 3 || to == 4 || to == 5 || to == 6;
                    case 3 -> to == 4 || to == 2 || to == 5 || to == 6;
                    case 4 -> to == 2 || to == 3 || to == 5 || to == 6;
                    case 5 -> to == 1 || to == 6;
                    default -> false;
                };
                assertEquals(expected, Codes.allowed(from, to), from + "->" + to);
            }
        }
        final Harness h = new Harness().standard();
        assertEquals(List.of("CMDREJ corr=5 t=12 r=INVALID_STATE_TRANSITION"), t(h.send(Cmd.state(5, SYM, SymbolState.PRE_OPEN, 0))));
        assertEquals(List.of("CMDREJ corr=6 t=12 r=INVALID_STATE_TRANSITION"), t(h.send(Cmd.state(6, SYM, SymbolState.CONTINUOUS, 0))));
        assertEquals(List.of("CMDREJ corr=7 t=12 r=UNKNOWN_SYMBOL"), t(h.send(Cmd.state(7, 42, SymbolState.HALTED, 0))));
        assertEquals(List.of("STATE corr=8 sym=1 rc=7 CONTINUOUS->HALTED cause=COMMAND"), t(h.send(Cmd.state(8, SYM, SymbolState.HALTED, 0))));
        assertEquals(List.of("CMDREJ corr=9 t=12 r=MALFORMED"), t(h.send(Cmd.poke(Cmd.state(9, SYM, SymbolState.HALTED, 0), 22, 9))));
    }

    @Test
    void haltRetainsBookAllowsCancelsAndResumes()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10));
        h.send(Cmd.state(1, SYM, SymbolState.HALTED, 0));
        assertReject(h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 10)), RejectReason.SYMBOL_HALTED);
        h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 9_800, 10)); // rejected
        h.send(Cmd.state(2, SYM, SymbolState.CONTINUOUS, 0));
        assertKinds(h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 9_900, 10)), "ER:NEW", "TRADE", "ER:FILL", "ER:FILL", "DELTA");
        h.send(Cmd.limit(SA, 3, A, SYM, Side.BUY, 9_800, 10));
        h.send(Cmd.state(3, SYM, SymbolState.HALTED, 0));
        assertKinds(h.send(Cmd.cancel(SA, 4, 3, A, SYM, false)), "ER:CANCELED", "DELTA");
    }

    @Test
    void auctionCallAccumulatesWithoutMatchingAndPublishesIndicative()
    {
        final Harness h = auction();
        assertKinds(h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_100, 100)), "ER:NEW", "DELTA");
        final List<Ev> evs = h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 60));
        assertKinds(evs, "ER:NEW", "DELTA", "AUCT");
        // candidates 10100 (v=60, imb 40) and 9900 (v=60, imb 40) and ref 10000 (v=60 imb 40): ref closest
        assertEquals("AUCT sym=1 px=10000 v=60 imb=40 side=1 final=FALSE", evs.get(2).text());
        // unchanged indicative -> no AuctionUpdate
        assertKinds(h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 9_000, 5)), "ER:NEW", "DELTA");
        assertReject(h.send(Cmd.market(SA, 3, A, SYM, Side.BUY, 5)), RejectReason.MARKET_ORDER_NOT_ALLOWED);
        // IOC in the call: accepted then canceled immediately
        assertKinds(h.send(Cmd.ioc(SA, 4, A, SYM, Side.BUY, 10_100, 5)), "ER:NEW", "ER:CANCELED");
        // cancel changes the indicative
        assertKinds(h.send(Cmd.cancel(SB, 2, 1, B, SYM, false)), "ER:CANCELED", "DELTA", "AUCT");
        assertEquals("AUCT sym=1 px=-9223372036854775808 v=0 imb=0 side=255 final=FALSE",
            h.events.get(h.events.size() - 2).text());
    }

    @Test
    void uncrossMaximisesVolumeThenMinimisesImbalanceAndAllocatesByPriceTime()
    {
        final Harness h = auction();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_200, 30));
        h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 10_100, 50));
        h.send(Cmd.limit(SA, 3, A, SYM, Side.BUY, 10_100, 20));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 40));
        h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 10_100, 50));
        h.send(Cmd.limit(SB, 3, B, SYM, Side.SELL, 10_300, 10));
        // p=10200: buy 30 sell 90 -> 30; p=10100: buy 100 sell 90 -> 90 imb 10; p=9900: buy 100 sell 40 -> 40
        final List<Ev> evs = h.send(Cmd.state(2, SYM, SymbolState.CONTINUOUS, 0));
        final List<String> trades = new ArrayList<>();
        for (final Ev e : evs)
        {
            if (e.is(102))
            {
                assertEquals(10_100, e.trade().price());
                assertEquals("TRUE", e.trade().isAuction().name());
                assertEquals(255, e.trade().aggressorSideRaw());
                trades.add(e.trade().buyOrderId() + "x" + e.trade().sellOrderId() + "=" + e.trade().quantity());
            }
            if (e.is(101))
            {
                assertEquals(LiquidityFlag.AUCTION, e.er().liquidity());
            }
        }
        // bids by price then time: 1(30), 2(50), 3(20); asks: 4(40 @9900), 5(50 @10100)
        assertEquals(List.of("1x4=30", "2x4=10", "2x5=40", "3x5=10"), trades);
        final Ev fin = evs.get(evs.size() - 2);
        assertEquals("AUCT sym=1 px=10100 v=90 imb=10 side=1 final=TRUE", fin.text());
        assertEquals("STATE corr=2 sym=1 rc=7 AUCTION_CALL->CONTINUOUS cause=COMMAND", evs.get(evs.size() - 1).text());
        // remaining book: bid 3 (10 left) @10100, ask 6 @10300: uncrossed
        assertEquals(10, h.m.symbols.get(0).bids.best().totalQty);
        assertEquals(10_300, h.m.symbols.get(0).asks.best().price);
    }

    @Test
    void uncrossTieBreaksClosestToReferenceThenLowest()
    {
        // equal volume and imbalance at 9900 and 10100 -> ref 10000 lies between: ref itself wins
        Harness h = auction();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_100, 10));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 10));
        List<Ev> evs = h.send(Cmd.state(2, SYM, SymbolState.CONTINUOUS, 0));
        assertEquals(10_000, evs.get(0).trade().price());
        // reference outside the range: closest candidate
        h = auction();
        h.send(Cmd.ref(SYM, 10_500, 3));
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_100, 10));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 10));
        evs = h.send(Cmd.state(2, SYM, SymbolState.CONTINUOUS, 0));
        assertEquals(10_100, evs.get(0).trade().price());
        // no reference price: lowest
        h = new Harness().standard();
        h.send(Cmd.symbol(2, 2, "NOREF", 1, 1, NULL, 1000, 500, SymbolState.AUCTION_CALL));
        h.send(Cmd.limit(SA, 1, A, 2, Side.BUY, 10_100, 10));
        h.send(Cmd.limit(SB, 1, B, 2, Side.SELL, 9_900, 10));
        evs = h.send(Cmd.state(2, 2, SymbolState.CONTINUOUS, 0));
        assertEquals(9_900, evs.get(0).trade().price());
    }

    @Test
    void uncrossWithoutCrossStillPublishesFinal()
    {
        final Harness h = auction();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10));
        final List<Ev> evs = h.send(Cmd.state(2, SYM, SymbolState.CONTINUOUS, 0));
        assertEquals(List.of(
            "AUCT sym=1 px=-9223372036854775808 v=0 imb=0 side=255 final=TRUE",
            "STATE corr=2 sym=1 rc=7 AUCTION_CALL->CONTINUOUS cause=COMMAND"), t(evs));
    }

    @Test
    void closingAuctionUncrossesThenExpiresTheRest()
    {
        final Harness h = auction();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_000, 30));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_000, 10));
        h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 10_500, 10));
        final List<Ev> evs = h.send(Cmd.state(3, SYM, SymbolState.CLOSED, 0));
        assertKinds(evs, "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "DELTA", "DELTA", "AUCT", "STATE",
            "ER:EXPIRED", "ER:EXPIRED", "DELTA", "DELTA", "CLOSE");
        assertEquals("CLOSE sym=1 px=10000 prev=" + Cmd.NULL + " date=0 src=AUCTION", evs.get(11).text());
        assertEquals(CancelReason.END_OF_DAY, evs.get(7).er().cancelReason());
        assertEquals(1, evs.get(7).er().orderId());
        assertEquals(10, h.m.firmSymbols.get(0).netPosition); // A bought 10
        assertEquals(0, h.m.allOrders.size);
        assertReject(h.send(Cmd.limit(SA, 9, A, SYM, Side.BUY, 10_000, 1)), RejectReason.SYMBOL_CLOSED);
        assertEquals(List.of("STATE corr=4 sym=1 rc=7 CLOSED->PRE_OPEN cause=COMMAND"), t(h.send(Cmd.state(4, SYM, SymbolState.PRE_OPEN, 0))));
    }

    @Test
    void haltFromAuctionThenResumeUncrossesACrossedBook()
    {
        final Harness h = auction();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_100, 10));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 10));
        assertEquals(List.of("STATE corr=2 sym=1 rc=7 AUCTION_CALL->HALTED cause=COMMAND"), t(h.send(Cmd.state(2, SYM, SymbolState.HALTED, 0))));
        final List<Ev> evs = h.send(Cmd.state(3, SYM, SymbolState.CONTINUOUS, 0));
        assertKinds(evs, "TRADE", "ER:FILL", "ER:FILL", "DELTA", "DELTA", "AUCT", "STATE");
    }

    @Test
    void removedCancelsOrdersAndSymbolCanBeRedefined()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10));
        final List<Ev> evs = h.send(Cmd.state(2, SYM, SymbolState.REMOVED, 0));
        assertKinds(evs, "STATE", "ER:CANCELED", "DELTA");
        assertEquals(CancelReason.SYMBOL_REMOVED, evs.get(1).er().cancelReason());
        assertEquals(List.of("CMDREJ corr=3 t=12 r=UNKNOWN_SYMBOL"), t(h.send(Cmd.state(3, SYM, SymbolState.HALTED, 0))));
        assertEquals(List.of("CMDREJ corr=9 t=10 r=UNKNOWN_SYMBOL"), t(h.send(Cmd.ref(SYM, 10_000, 9))));
        final List<Ev> def = h.send(Cmd.symbol(4, SYM, "AAPL", 1, 1, 20_000, 1000, 100, SymbolState.PRE_OPEN));
        assertEquals("SYMDEF corr=4 sym=1 tick=1 lot=1 ref=20000 max=1000 bps=100 ticker=AAPL state=PRE_OPEN", def.get(0).text());
    }

    @Test
    void symbolDefinitionValidationAndUpdates()
    {
        final Harness h = new Harness().standard();
        assertEquals("CMDREJ corr=1 t=11 r=INVALID_PRICE", t(h.send(Cmd.symbol(1, 5, "X", 0, 1, 100, 10, 10, SymbolState.CONTINUOUS))).get(0));
        assertEquals("CMDREJ corr=2 t=11 r=INVALID_QTY", t(h.send(Cmd.symbol(2, 5, "X", 1, 0, 100, 10, 10, SymbolState.CONTINUOUS))).get(0));
        assertEquals("CMDREJ corr=3 t=11 r=INVALID_PRICE", t(h.send(Cmd.symbol(3, 5, "X", 1, 1, -100, 10, 10, SymbolState.CONTINUOUS))).get(0));
        assertEquals("CMDREJ corr=4 t=11 r=INVALID_STATE_TRANSITION", t(h.send(Cmd.symbol(4, 5, "X", 1, 1, 100, 10, 10, SymbolState.REMOVED))).get(0));
        // update keeps state, ignores initialState; reference price from the definition does not override the feed
        h.send(Cmd.ref(SYM, 10_200, 1));
        final List<Ev> up = h.send(Cmd.symbol(5, SYM, "AAPL", 1, 1, 15_000, 500, 200, SymbolState.PRE_OPEN));
        assertEquals("SYMDEF corr=5 sym=1 tick=1 lot=1 ref=10200 max=500 bps=200 ticker=AAPL state=CONTINUOUS", up.get(0).text());
        assertReject(h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10)), RejectReason.PRICE_BAND); // band now 2%
        // tick size change with resting orders keeps them reachable
        h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 10_001, 10));
        h.send(Cmd.symbol(6, SYM, "AAPL", 5, 1, NULL, 500, 200, SymbolState.PRE_OPEN));
        assertReject(h.send(Cmd.limit(SA, 3, A, SYM, Side.BUY, 10_002, 10)), RejectReason.INVALID_PRICE);
        assertKinds(h.send(Cmd.limit(SA, 4, A, SYM, Side.BUY, 10_005, 10)), "ER:NEW", "DELTA");
        assertKinds(h.send(Cmd.cancel(SA, 5, 2, A, SYM, false)), "ER:CANCELED", "DELTA");
        assertKinds(h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_005, 10)), "ER:NEW", "TRADE", "ER:FILL", "ER:FILL", "DELTA");
    }

    @Test
    void referencePriceAppliedOnlyWhenNewerAndRecentresFarMoves()
    {
        final Harness h = new Harness().standard();
        assertEquals(List.of("REF sym=1 ref=10000 bid=9999 ofr=10001 band=9000..11000 ts=100"), t(h.send(Cmd.ref(SYM, 10_000, 100))));
        assertEquals(List.of(), t(h.send(Cmd.ref(SYM, 10_500, 100)))); // same ts: duplicate from the second consolidator
        assertEquals(List.of(), t(h.send(Cmd.ref(SYM, 10_500, 99))));
        assertEquals(List.of("CMDREJ corr=101 t=10 r=INVALID_PRICE"), t(h.send(Cmd.ref(SYM, NULL, 101))));
        // rounding inward: ref 10001, 10% -> 9000.9 .. 11001.1 -> 9001..11001
        assertEquals("REF sym=1 ref=10001 bid=10000 ofr=10002 band=9001..11001 ts=102", t(h.send(Cmd.ref(SYM, 10_001, 102))).get(0));
        // resting orders survive a move far outside the level window (window 256 ticks in the harness)
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_500, 10));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_500, 10));
        long ts = 200;
        for (long ref = 10_000; ref <= 100_000; ref += 5_000)
        {
            h.send(Cmd.ref(SYM, ref, ts++));
            h.send(Cmd.limit(SB, ts, B, SYM, Side.SELL, ref + 600, 1));
        }
        for (long ref = 100_000; ref >= 10_000; ref -= 7_000)
        {
            h.send(Cmd.ref(SYM, ref, ts++));
        }
        h.send(Cmd.ref(SYM, 10_000, ts++));
        assertKinds(h.send(Cmd.cancel(SA, 9_999, 1, A, SYM, false)), "ER:CANCELED", "DELTA");
        final List<Ev> f = h.send(Cmd.limit(SA, 10_000, A, SYM, Side.BUY, 10_500, 10));
        assertKinds(f, "ER:NEW", "TRADE", "ER:FILL", "ER:FILL", "DELTA");
    }

    @Test
    void massCancelScopes()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.symbol(2, 2, "MSFT", 1, 1, 10_000, 1_000_000, 1000, SymbolState.CONTINUOUS));
        // A: session 1001 on sym1 via gw1, session 1002 on sym2 via gw2; B: sym1
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10));
        h.send(Cmd.newOrder(1002, 2, A, 7, 2, 2, Side.BUY, io.lmb.sbe.OrdType.LIMIT, io.lmb.sbe.TimeInForce.DAY, 9_900, 10, false, 5));
        h.send(Cmd.limit(SA, 3, A, 2, Side.SELL, 10_100, 10));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_100, 10));
        h.send(Cmd.newOrder(SB, 2, B, 7, 2, 2, Side.SELL, io.lmb.sbe.OrdType.LIMIT, io.lmb.sbe.TimeInForce.DAY, 10_200, 10, false, 5));

        List<Ev> evs = h.send(Cmd.massCancel(1, SA, A, 2, 1, 0, MassCancelScope.FIRM_SYMBOL, CancelReason.MASS_CANCEL));
        assertKinds(evs, "ER:CANCELED", "ER:CANCELED", "DELTA", "DELTA", "MASSDONE");
        assertEquals("MASSDONE req=1 sess=1001 f=100 n=2 g=1 scope=FIRM_SYMBOL", evs.get(4).text());
        assertEquals(CancelReason.MASS_CANCEL, evs.get(0).er().cancelReason());
        assertEquals(4001, evs.get(0).er().gatewayInTsNs());

        evs = h.send(Cmd.massCancel(2, 0, 999, 0, 9, 2, MassCancelScope.GATEWAY, CancelReason.ADMIN));
        assertKinds(evs, "ER:CANCELED", "DELTA", "MASSDONE");
        assertEquals(5, evs.get(0).er().orderId());
        assertEquals(0, evs.get(0).er().gatewayInTsNs()); // not the requesting firm's order

        evs = h.send(Cmd.massCancel(3, 0, 0, SYM, 9, 0, MassCancelScope.SYMBOL, CancelReason.ADMIN));
        assertKinds(evs, "ER:CANCELED", "ER:CANCELED", "DELTA", "DELTA", "MASSDONE");
        assertEquals(0, h.m.allOrders.size);

        assertEquals(List.of("MASSDONE req=4 sess=5 f=777 n=0 g=1 scope=FIRM"),
            t(h.send(Cmd.massCancel(4, 5, 777, 0, 1, 0, MassCancelScope.FIRM, CancelReason.MASS_CANCEL))));
        h.send(Cmd.limit(SA, 10, A, SYM, Side.BUY, 9_900, 10));
        h.send(Cmd.limit(SA, 11, A, 2, Side.BUY, 9_900, 10));
        evs = h.send(Cmd.massCancel(5, SA, A, 0, 1, 0, MassCancelScope.FIRM, CancelReason.DISCONNECT));
        assertKinds(evs, "ER:CANCELED", "ER:CANCELED", "DELTA", "DELTA", "MASSDONE");
        assertEquals(CancelReason.DISCONNECT, evs.get(0).er().cancelReason());
        // only MASS_CANCEL / DISCONNECT / ADMIN are valid reasons
        assertEquals(List.of("CMDREJ corr=6 t=4 r=MALFORMED"),
            t(h.send(Cmd.massCancel(6, SA, A, 0, 1, 0, MassCancelScope.FIRM, CancelReason.CLIENT))));
    }

    @Test
    void massCancelInAuctionCallUpdatesIndicative()
    {
        final Harness h = auction();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_100, 10));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 10));
        final List<Ev> evs = h.send(Cmd.massCancel(1, SA, A, 0, 1, 0, MassCancelScope.FIRM, CancelReason.MASS_CANCEL));
        assertKinds(evs, "ER:CANCELED", "DELTA", "AUCT", "MASSDONE");
    }

    @Test
    void dayRolloverExpiresClearsDedupeAndRiskAndResetsStates()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.symbol(2, 2, "MSFT", 1, 1, 10_000, 1_000_000, 1000, SymbolState.AUCTION_CALL));
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 4));
        h.send(Cmd.limit(SA, 2, A, 2, Side.BUY, 9_900, 10));
        final List<Ev> evs = h.send(Cmd.dayRoll(77, 20261117));
        // per symbol: implicit close (neither symbol entered CLOSED), state reset, reference from the close
        assertKinds(evs, "ER:EXPIRED", "ER:EXPIRED", "DELTA", "DELTA", "CLOSE", "STATE", "REF", "CLOSE", "STATE", "DAYROLLED");
        assertEquals(4, evs.get(0).er().cumQty()); // cum kept on the report
        assertEquals("CLOSE sym=1 px=9900 prev=" + Cmd.NULL + " date=0 src=LAST_TRADE", evs.get(4).text());
        assertEquals("STATE corr=77 sym=1 rc=0 CONTINUOUS->PRE_OPEN cause=DAY_ROLLOVER", evs.get(5).text());
        assertEquals("REF sym=1 ref=9900 bid=" + Cmd.NULL + " ofr=" + Cmd.NULL + " band=8910..10890 ts=" + h.now, evs.get(6).text());
        assertEquals("CLOSE sym=2 px=" + Cmd.NULL + " prev=" + Cmd.NULL + " date=0 src=NONE", evs.get(7).text());
        assertEquals("STATE corr=77 sym=2 rc=0 AUCTION_CALL->PRE_OPEN cause=DAY_ROLLOVER", evs.get(8).text());
        assertEquals("DAYROLLED corr=77 date=20261117 n=2", evs.get(9).text());
        assertEquals(0, h.m.firms.get(0).creditUsed()); // daily credit usage reset ...
        assertEquals(4, h.m.firmSymbols.get(0).netPosition); // ... but the position carries over
        assertEquals(-4, h.m.firmSymbols.get(1).netPosition);
        // clOrdIds can be reused after the roll
        h.send(Cmd.state(1, SYM, SymbolState.CONTINUOUS, 0));
        assertKinds(h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10)), "ER:NEW", "DELTA");
        final List<String> roll = t(h.send(Cmd.dayRoll(78, 20261118)));
        assertEquals("DAYROLLED corr=78 date=20261118 n=1", roll.get(roll.size() - 1));
    }

    @Test
    void dayRolloverIsIdempotentPerTradingDate()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.dayRoll(77, 20261117));
        h.send(Cmd.state(1, SYM, SymbolState.CONTINUOUS, 0));
        assertKinds(h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10)), "ER:NEW", "DELTA");
        // a resend of the same roll, or a stale one, changes nothing: the order stays, dedupe is kept
        assertEquals(List.of("CMDREJ corr=77 t=14 r=INVALID_STATE_TRANSITION"), t(h.send(Cmd.dayRoll(77, 20261117))));
        assertEquals(List.of("CMDREJ corr=79 t=14 r=INVALID_STATE_TRANSITION"), t(h.send(Cmd.dayRoll(79, 20261116))));
        assertReject(h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10)), RejectReason.DUPLICATE_CLORDID);
        assertEquals(20261117, h.m.tradingDate);
        // the trading date survives a snapshot, and the next date rolls normally
        final Harness r = h.restoreCopy();
        assertEquals(20261117, r.m.tradingDate);
        final List<String> roll = t(h.send(Cmd.dayRoll(80, 20261118)));
        assertEquals("DAYROLLED corr=80 date=20261118 n=1", roll.get(roll.size() - 1));
    }

    @Test
    void scheduledTransitionsFireThroughTimers()
    {
        final Harness h = new Harness().standard();
        final long t0 = h.now;
        assertEquals(List.of(), t(h.send(Cmd.state(10, SYM, SymbolState.HALTED, t0 + 1_000_000))));
        assertEquals(List.of(), t(h.send(Cmd.state(11, Cmd.ALL_SYMBOLS, SymbolState.CONTINUOUS, t0 + 2_000_000))));
        assertEquals(List.of(), t(h.send(Cmd.state(12, SYM, SymbolState.PRE_OPEN, t0 + 3_000_000)))); // invalid when it fires
        assertEquals(3, h.timersById.size());
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10));
        assertEquals(List.of("STATE corr=10 sym=1 rc=7 CONTINUOUS->HALTED cause=COMMAND"), t(h.advanceTo(t0 + 1_500_000)));
        assertReject(h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 9_900, 10)), RejectReason.SYMBOL_HALTED);
        assertEquals(List.of("STATE corr=11 sym=1 rc=7 HALTED->CONTINUOUS cause=COMMAND",
            "CMDREJ corr=12 t=12 r=INVALID_STATE_TRANSITION"), t(h.advanceTo(t0 + 5_000_000)));
        assertEquals(0, h.m.pending.size());
        // a timer we do not know (or already consumed) is just an empty entry
        assertEquals(List.of(), t(h.apply(new Harness.Entry(null, 999, h.logPosition += 64, h.now))));
        // unknown symbol is rejected at scheduling time
        assertEquals(List.of("CMDREJ corr=13 t=12 r=UNKNOWN_SYMBOL"), t(h.send(Cmd.state(13, 77, SymbolState.HALTED, t0 + 9_000_000))));
    }

    @Test
    void marketWideTransitionSkipsIneligibleSymbols()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.symbol(2, 2, "MSFT", 1, 1, 10_000, 1_000_000, 1000, SymbolState.HALTED));
        h.send(Cmd.symbol(3, 3, "AMZN", 1, 1, 10_000, 1_000_000, 1000, SymbolState.PRE_OPEN));
        assertEquals(List.of("STATE corr=5 sym=1 rc=7 CONTINUOUS->HALTED cause=COMMAND"), t(h.send(Cmd.state(5, Cmd.ALL_SYMBOLS, SymbolState.HALTED, 0))));
        assertEquals(List.of("STATE corr=6 sym=1 rc=7 HALTED->CONTINUOUS cause=COMMAND", "STATE corr=6 sym=2 rc=7 HALTED->CONTINUOUS cause=COMMAND",
            "STATE corr=6 sym=3 rc=7 PRE_OPEN->CONTINUOUS cause=COMMAND"), t(h.send(Cmd.state(6, Cmd.ALL_SYMBOLS, SymbolState.CONTINUOUS, 0))));
    }

    @Test
    void riskLimitUpdateValidation()
    {
        final Harness h = new Harness();
        assertEquals(List.of("CMDREJ corr=1 t=13 r=CREDIT_LIMIT"), t(h.send(Cmd.risk(1, 5, -1, 10, true))));
        assertEquals(List.of("CMDREJ corr=2 t=13 r=POSITION_LIMIT"), t(h.send(Cmd.risk(2, 5, 1, -10, true))));
        assertEquals(List.of("CMDREJ corr=3 t=13 r=MALFORMED"), t(h.send(Cmd.poke(Cmd.risk(3, 5, 1, 10, true), 28, 4))));
        assertEquals(List.of("RISK corr=4 f=5 budget=1 used=0 maxPos=10 en=TRUE"), t(h.send(Cmd.risk(4, 5, 1, 10, true))));
    }

    @Test
    void serviceLoaderFindsTheFactory()
    {
        final var factory = ShardStateMachineFactory.load();
        assertInstanceOf(MatcherShardFactory.class, factory);
        assertEquals(3, factory.create(3).shardId());
    }

    @Test
    void drivesThroughTheTestkitInMemoryVenueWithClusterTimers()
    {
        final InMemoryVenue venue = new InMemoryVenue(1_000_000_000L, new MatcherShard(0), new MatcherShard(1));
        final var c0 = venue.commandClient(0);
        final List<byte[]> cmds = List.of(
            Cmd.symbol(1, 1, "AAPL", 1, 1, 10_000, 1_000_000, 1000, SymbolState.PRE_OPEN),
            Cmd.risk(1, A, 1_000_000_000_000L, 1_000_000, true),
            Cmd.risk(2, B, 1_000_000_000_000L, 1_000_000, true),
            Cmd.state(1, SYM, SymbolState.AUCTION_CALL, 1_000_000_100L),
            Cmd.state(2, SYM, SymbolState.CONTINUOUS, 1_000_000_200L));
        for (final byte[] c : cmds)
        {
            assertTrue(c0.offer(new UnsafeBuffer(c), 0, c.length));
        }
        venue.advanceTimeTo(1_000_000_150L);
        c0.offer(new UnsafeBuffer(Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_000, 10)), 0, Cmd.limit(SA, 1, A, SYM, Side.BUY, 10_000, 10).length);
        final byte[] sell = Cmd.limit(SB, 1, B, SYM, Side.SELL, 10_000, 10);
        c0.offer(new UnsafeBuffer(sell), 0, sell.length);
        venue.advanceTimeTo(1_000_000_300L);
        final var stream = venue.eventStream(0, 0);
        final List<String> texts = new ArrayList<>();
        stream.poll((b, o, l) ->
        {
            final byte[] copy = new byte[l];
            b.getBytes(o, copy);
            texts.add(new Ev(copy).text());
        }, 1000);
        assertTrue(texts.contains("STATE corr=1 sym=1 rc=7 PRE_OPEN->AUCTION_CALL cause=COMMAND"), texts::toString);
        assertTrue(texts.contains("AUCT sym=1 px=10000 v=10 imb=0 side=255 final=TRUE"), texts::toString);
        assertTrue(texts.stream().anyMatch(s -> s.startsWith("TRADE") && s.contains("auct=TRUE")));
        assertEquals(0, venue.eventCount(1));
        assertEquals(ExecType.FILL.name(), texts.stream().filter(s -> s.startsWith("ER FILL")).findFirst().orElseThrow().split(" ")[1]);
    }
}
