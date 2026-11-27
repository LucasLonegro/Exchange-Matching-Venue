package io.lmb.matcher;

import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.LiquidityFlag;
import io.lmb.sbe.OrdStatus;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.RejectReason;
import io.lmb.sbe.Side;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.TimeInForce;
import io.lmb.sbe.CancelReason;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.lmb.matcher.Cmd.NULL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderScenarioTest
{
    static final int SYM = 1;
    static final int A = 100;
    static final int B = 200;
    static final long SA = 1001;
    static final long SB = 2001;

    static List<String> t(final List<Ev> evs)
    {
        return Harness.texts(evs);
    }

    static void assertKinds(final List<Ev> evs, final String... kinds)
    {
        final StringBuilder actual = new StringBuilder();
        for (final Ev e : evs)
        {
            actual.append(e.text(), 0, e.text().indexOf(' ') < 0 ? e.text().length() : e.text().indexOf(' '));
            if (e.is(ExecutionReportDecoder.TEMPLATE_ID))
            {
                actual.append(':').append(e.er().execType());
            }
            actual.append(' ');
        }
        assertEquals(String.join(" ", kinds), actual.toString().trim(), () -> String.join("\n", t(evs)));
    }

    static void assertReject(final List<Ev> evs, final RejectReason reason)
    {
        assertEquals(1, evs.size(), () -> t(evs).toString());
        final ExecutionReportDecoder er = evs.get(0).er();
        assertEquals(ExecType.REJECTED, er.execType());
        assertEquals(OrdStatus.REJECTED, er.ordStatus());
        assertEquals(0, er.orderId());
        assertEquals(reason, er.rejectReason());
    }

    static void assertCancelReject(final List<Ev> evs, final RejectReason reason)
    {
        assertEquals(1, evs.size(), () -> t(evs).toString());
        final ExecutionReportDecoder er = evs.get(0).er();
        assertEquals(ExecType.CANCEL_REJECTED, er.execType());
        assertEquals(reason, er.rejectReason());
    }

    @Test
    void newOrderRestsWithNewAndBookDelta()
    {
        final Harness h = new Harness().standard();
        final List<Ev> evs = h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_990, 100));
        assertKinds(evs, "ER:NEW", "DELTA");
        final ExecutionReportDecoder er = evs.get(0).er();
        assertEquals(1L, er.orderId()); // shard 0, n = 1
        assertEquals(100, er.leavesQty());
        assertEquals(1001, er.gatewayInTsNs());
        assertEquals("DELTA sym=1 side=1 px=9990 q=100 n=1", evs.get(1).text());
    }

    @Test
    void orderIdsEncodeShard()
    {
        final Harness h = new Harness(new MatcherShard(5, new MatcherConfig(64, 16, 256, 16))).standard();
        final List<Ev> evs = h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_990, 100));
        assertEquals((5L << 48) | 1, evs.get(0).er().orderId());
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_990, 100));
        final Ev trade = h.events.stream().filter(e -> e.is(102)).findFirst().orElseThrow();
        assertEquals((5L << 48) | 1, trade.trade().matchId());
    }

    @Test
    void fullMatchPrintsAtRestingPriceWithTakerAndMaker()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_010, 100));
        final List<Ev> evs = h.send(Cmd.limit(SB, 1, B, SYM, Side.BUY, 10_050, 100));
        assertKinds(evs, "ER:NEW", "TRADE", "ER:FILL", "ER:FILL", "DELTA");
        assertEquals("TRADE m=1 px=10010 q=100 b=2 s=1 sym=1 bf=200 sf=100 agg=1 auct=FALSE", evs.get(1).text());
        final ExecutionReportDecoder taker = evs.get(2).er();
        assertEquals(2, taker.orderId());
        assertEquals(LiquidityFlag.TAKER, taker.liquidity());
        assertEquals(10_010, taker.lastPx());
        assertEquals(10_050, taker.price());
        assertEquals(1001, taker.gatewayInTsNs());
        final ExecutionReportDecoder maker = evs.get(3).er();
        assertEquals(1, maker.orderId());
        assertEquals(LiquidityFlag.MAKER, maker.liquidity());
        assertEquals(0, maker.gatewayInTsNs());
        assertEquals(1, maker.matchId());
        assertEquals("DELTA sym=1 side=2 px=10010 q=0 n=0", evs.get(4).text());
    }

    @Test
    void partialFillsSweepLevelsInPriceTimeOrderAndRemainderRests()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_010, 30));
        h.send(Cmd.limit(SA, 2, A, SYM, Side.SELL, 10_000, 20));
        h.send(Cmd.limit(SA, 3, A, SYM, Side.SELL, 10_000, 25));
        final List<Ev> evs = h.send(Cmd.limit(SB, 1, B, SYM, Side.BUY, 10_010, 100));
        assertKinds(evs, "ER:NEW", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "TRADE", "ER:PARTIAL_FILL", "ER:FILL",
            "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "DELTA", "DELTA", "DELTA");
        assertEquals(2, evs.get(1).trade().sellOrderId()); // 10000 first, time priority: order 2 then 3
        assertEquals(3, evs.get(4).trade().sellOrderId());
        assertEquals(1, evs.get(7).trade().sellOrderId());
        final ExecutionReportDecoder lastTaker = evs.get(8).er();
        assertEquals(75, lastTaker.cumQty());
        assertEquals(25, lastTaker.leavesQty());
        // deltas: BUY side first, then SELL best first
        assertEquals("DELTA sym=1 side=1 px=10010 q=25 n=1", evs.get(10).text());
        assertEquals("DELTA sym=1 side=2 px=10000 q=0 n=0", evs.get(11).text());
        assertEquals("DELTA sym=1 side=2 px=10010 q=0 n=0", evs.get(12).text());
    }

    @Test
    void makerPartialFillKeepsPriority()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_000, 100));
        h.send(Cmd.limit(SA, 2, A, SYM, Side.SELL, 10_000, 100));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.BUY, 10_000, 40));
        final List<Ev> evs = h.send(Cmd.limit(SB, 2, B, SYM, Side.BUY, 10_000, 70));
        assertEquals(1, evs.get(1).trade().sellOrderId());
        assertEquals(60, evs.get(1).trade().quantity());
        assertEquals(2, evs.get(4).trade().sellOrderId());
        assertEquals(10, evs.get(4).trade().quantity());
    }

    @Test
    void iocRemainderIsCanceled()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_000, 30));
        final List<Ev> evs = h.send(Cmd.ioc(SB, 1, B, SYM, Side.BUY, 10_000, 100));
        assertKinds(evs, "ER:NEW", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "ER:CANCELED", "DELTA");
        assertEquals(CancelReason.IOC_REMAINDER, evs.get(4).er().cancelReason());
        assertEquals(0, evs.get(4).er().leavesQty());
        assertEquals(30, evs.get(4).er().cumQty());
        // IOC that does not cross: NEW + CANCELED, no book change
        assertKinds(h.send(Cmd.ioc(SB, 2, B, SYM, Side.BUY, 9_000, 10)), "ER:NEW", "ER:CANCELED");
    }

    @Test
    void marketOrderSweepsOnlyInsideBandThenCollar()
    {
        final Harness h = new Harness().standard(); // ref 10000, band 9000..11000
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_500, 10));
        h.send(Cmd.limit(SA, 2, A, SYM, Side.SELL, 11_000, 10));
        h.send(Cmd.ref(SYM, 10_200, 5)); // band 9180..11220: no change to which asks are in band
        h.send(Cmd.ref(SYM, 9_900, 6)); // band 8910..10890: 11000 now outside
        final List<Ev> evs = h.send(Cmd.market(SB, 1, B, SYM, Side.BUY, 50));
        assertKinds(evs, "ER:NEW", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "ER:CANCELED", "DELTA");
        assertEquals(CancelReason.MARKET_COLLAR, evs.get(4).er().cancelReason());
        assertEquals(NULL, evs.get(0).er().price());
        assertEquals(OrdType.MARKET, evs.get(0).er().ordType());
    }

    @Test
    void marketOrderRejections()
    {
        final Harness h = new Harness().standard();
        assertReject(h.send(Cmd.market(SB, 1, B, SYM, Side.BUY, 50)), RejectReason.NO_LIQUIDITY);
        h.send(Cmd.symbol(2, 2, "NOREF", 1, 1, NULL, 1000, 500, SymbolState.CONTINUOUS));
        assertReject(h.send(Cmd.market(SB, 2, B, 2, Side.BUY, 50)), RejectReason.NO_REFERENCE_PRICE);
        // a limit order without a reference price has no band check
        assertKinds(h.send(Cmd.limit(SB, 3, B, 2, Side.BUY, 1, 5)), "ER:NEW", "DELTA");
        // opposite side non-empty but entirely outside the band: accepted, then collar with nothing filled
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_900, 10));
        h.send(Cmd.ref(SYM, 9_000, 7)); // band 8100..9900
        final List<Ev> evs = h.send(Cmd.market(SB, 4, B, SYM, Side.BUY, 5));
        assertKinds(evs, "ER:NEW", "ER:CANCELED");
        assertEquals(CancelReason.MARKET_COLLAR, evs.get(1).er().cancelReason());
    }

    @Test
    void validationOrderAndEveryNewOrderRejectReason()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.symbol(2, 2, "LOT", 5, 100, 10_000, 1_000, 1000, SymbolState.CONTINUOUS));
        h.send(Cmd.risk(9, 400, 1_000_000_000L, 1_000, false));
        assertReject(h.send(Cmd.limit(1, 1, 999, SYM, Side.BUY, 10_000, 1)), RejectReason.UNKNOWN_FIRM);
        assertReject(h.send(Cmd.limit(1, 1, 400, SYM, Side.BUY, 10_000, 1)), RejectReason.FIRM_DISABLED);
        assertReject(h.send(Cmd.limit(SA, 2, A, 77, Side.BUY, 10_000, 1)), RejectReason.UNKNOWN_SYMBOL);
        assertReject(h.send(Cmd.limit(SA, 3, A, 2, Side.BUY, 10_000, 150)), RejectReason.INVALID_QTY); // not a lot multiple
        assertReject(h.send(Cmd.limit(SA, 4, A, 2, Side.BUY, 10_000, 1_100)), RejectReason.INVALID_QTY); // > max
        assertReject(h.send(Cmd.limit(SA, 5, A, 2, Side.BUY, 10_000, 0)), RejectReason.INVALID_QTY);
        assertReject(h.send(Cmd.limit(SA, 6, A, 2, Side.BUY, 10_002, 100)), RejectReason.INVALID_PRICE); // off tick
        assertReject(h.send(Cmd.limit(SA, 7, A, 2, Side.BUY, -5, 100)), RejectReason.INVALID_PRICE);
        assertReject(h.send(Cmd.limit(SA, 8, A, 2, Side.BUY, NULL, 100)), RejectReason.INVALID_PRICE);
        assertReject(h.send(Cmd.limit(SA, 9, A, 2, Side.BUY, 11_005, 100)), RejectReason.PRICE_BAND);
        assertReject(h.send(Cmd.limit(SA, 10, A, 2, Side.BUY, 8_995, 100)), RejectReason.PRICE_BAND);
        assertKinds(h.send(Cmd.limit(SA, 11, A, 2, Side.BUY, 11_000, 100)), "ER:NEW", "DELTA"); // band edge inclusive
        assertKinds(h.send(Cmd.limit(SA, 12, A, 2, Side.BUY, 9_000, 100)), "ER:NEW", "DELTA");
        // quantity is checked before price: both wrong -> INVALID_QTY
        assertReject(h.send(Cmd.limit(SA, 13, A, 2, Side.BUY, 10_002, 150)), RejectReason.INVALID_QTY);
        // price before dedupe: duplicate id with a bad price -> PRICE_BAND
        assertReject(h.send(Cmd.limit(SA, 11, A, 2, Side.BUY, 20_000, 100)), RejectReason.PRICE_BAND);
        assertReject(h.send(Cmd.limit(SA, 11, A, 2, Side.BUY, 10_000, 100)), RejectReason.DUPLICATE_CLORDID);
        // the same clOrdId under another firm is a different key
        assertKinds(h.send(Cmd.limit(SB, 11, B, 2, Side.BUY, 9_000, 100)), "ER:NEW", "DELTA");
    }

    @Test
    void creditLimitCountsOpenOrdersBothSidesAndNetTradedNotional()
    {
        final Harness h = new Harness().standard();
        final Firm a = h.m.firms.get(0);
        h.send(Cmd.risk(1, A, 1_000_000, 1_000_000, true)); // $10,000.00
        assertKinds(h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_000, 60)), "ER:NEW", "DELTA"); // 540,000
        assertReject(h.send(Cmd.limit(SA, 2, A, SYM, Side.SELL, 10_000, 47)), RejectReason.CREDIT_LIMIT); // 470,000
        assertKinds(h.send(Cmd.limit(SA, 3, A, SYM, Side.SELL, 10_000, 46)), "ER:NEW", "DELTA"); // exactly 1,000,000
        assertEquals("RISK corr=2 f=100 budget=1000000 used=1000000 maxPos=1000000 en=TRUE",
            h.send(Cmd.risk(2, A, 1_000_000, 1_000_000, true)).get(0).text());
        // B lifts A's sell: open 460,000 becomes traded -460,000, usage unchanged
        assertKinds(h.send(Cmd.limit(SB, 1, B, SYM, Side.BUY, 10_000, 46)), "ER:NEW", "TRADE", "ER:FILL", "ER:FILL", "DELTA");
        assertEquals(540_000, a.openNotional);
        assertEquals(-460_000, a.tradedNet);
        assertEquals(1_000_000, a.creditUsed());
        // B hits A's buy: the buys net against the sells, usage is |net traded| = 80,000
        assertKinds(h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 9_000, 60)), "ER:NEW", "TRADE", "ER:FILL", "ER:FILL", "DELTA");
        assertEquals(0, a.openNotional);
        assertEquals(80_000, a.tradedNet);
        assertEquals(80_000, a.creditUsed());
        assertKinds(h.send(Cmd.limit(SA, 4, A, SYM, Side.BUY, 9_500, 10)), "ER:NEW", "DELTA"); // +95,000
        // budget below usage is accepted and blocks new orders, cancels still work and release the open notional
        assertEquals("RISK corr=3 f=100 budget=0 used=175000 maxPos=1000000 en=TRUE",
            h.send(Cmd.risk(3, A, 0, 1_000_000, true)).get(0).text());
        assertReject(h.send(Cmd.limit(SA, 5, A, SYM, Side.BUY, 9_500, 1)), RejectReason.CREDIT_LIMIT);
        assertKinds(h.send(Cmd.cancel(SA, 6, 4, A, SYM, false)), "ER:CANCELED", "DELTA");
        assertEquals(80_000, a.creditUsed());
    }

    @Test
    void marketOrdersAreValuedAtTheBandEdge()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_500, 10));
        h.send(Cmd.risk(1, A, 109_999, 1_000_000, true)); // band high 11000 * 10 = 110,000
        assertReject(h.send(Cmd.market(SA, 1, A, SYM, Side.BUY, 10)), RejectReason.CREDIT_LIMIT);
        h.send(Cmd.risk(2, A, 110_000, 1_000_000, true));
        assertKinds(h.send(Cmd.market(SA, 2, A, SYM, Side.BUY, 10)), "ER:NEW", "TRADE", "ER:FILL", "ER:FILL", "DELTA");
    }

    @Test
    void positionLimitIsWorstCase()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.risk(1, A, 1_000_000_000_000L, 100, true));
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 60));
        assertReject(h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 9_900, 41)), RejectReason.POSITION_LIMIT);
        assertKinds(h.send(Cmd.limit(SA, 3, A, SYM, Side.BUY, 9_900, 40)), "ER:NEW", "DELTA");
        // sells are checked against the short side independently
        assertKinds(h.send(Cmd.limit(SA, 4, A, SYM, Side.SELL, 10_100, 100)), "ER:NEW", "DELTA");
        assertReject(h.send(Cmd.limit(SA, 5, A, SYM, Side.SELL, 10_100, 1)), RejectReason.POSITION_LIMIT);
        // after buying 100 (net +100), the short side allows selling 200 in total
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 100));
        assertEquals(100, h.m.firmSymbols.get(0).netPosition);
        assertKinds(h.send(Cmd.limit(SA, 6, A, SYM, Side.SELL, 10_100, 100)), "ER:NEW", "DELTA");
        assertReject(h.send(Cmd.limit(SA, 7, A, SYM, Side.SELL, 10_100, 1)), RejectReason.POSITION_LIMIT);
    }

    @Test
    void killSwitchBlocksNewOrdersAndReplacesButNotCancels()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10));
        final List<Ev> r = h.send(Cmd.risk(5, A, 1_000_000_000_000L, 10_000_000, false));
        assertTrue(r.get(0).text().endsWith("en=FALSE"));
        assertReject(h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 9_900, 10)), RejectReason.FIRM_DISABLED);
        assertCancelReject(h.send(Cmd.replace(SA, 3, 1, A, SYM, 9_900, 5, false)), RejectReason.FIRM_DISABLED);
        assertKinds(h.send(Cmd.cancel(SA, 4, 1, A, SYM, false)), "ER:CANCELED", "DELTA");
    }

    @Test
    void cancelSemantics()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 10));
        final List<Ev> c = h.send(Cmd.cancel(SA, 2, 1, A, SYM, false));
        assertKinds(c, "ER:CANCELED", "DELTA");
        final ExecutionReportDecoder er = c.get(0).er();
        assertEquals(2, er.clOrdId());
        assertEquals(1, er.origClOrdId());
        assertEquals(CancelReason.CLIENT, er.cancelReason());
        assertEquals(OrdStatus.CANCELED, er.ordStatus());
        assertEquals(2002, er.gatewayInTsNs());
        // already canceled (by original id and by the cancel's id)
        assertCancelReject(h.send(Cmd.cancel(SA, 3, 1, A, SYM, false)), RejectReason.TOO_LATE_TO_CANCEL);
        assertCancelReject(h.send(Cmd.cancel(SA, 4, 2, A, SYM, false)), RejectReason.TOO_LATE_TO_CANCEL);
        // unknown
        assertCancelReject(h.send(Cmd.cancel(SA, 5, 999, A, SYM, false)), RejectReason.UNKNOWN_ORDER);
        // rejected order ids are not orders
        h.send(Cmd.limit(SA, 6, A, SYM, Side.BUY, 1, 10)); // PRICE_BAND
        assertCancelReject(h.send(Cmd.cancel(SA, 7, 6, A, SYM, false)), RejectReason.UNKNOWN_ORDER);
        // another firm cannot cancel
        h.send(Cmd.limit(SA, 8, A, SYM, Side.BUY, 9_900, 10));
        assertCancelReject(h.send(Cmd.cancel(SB, 9, 8, B, SYM, false)), RejectReason.UNKNOWN_ORDER);
        // wrong symbol
        assertCancelReject(h.send(Cmd.cancel(SA, 10, 8, A, 2, false)), RejectReason.UNKNOWN_ORDER);
        // unknown firm
        assertCancelReject(h.send(Cmd.cancel(1, 11, 8, 999, SYM, false)), RejectReason.UNKNOWN_FIRM);
        // duplicate cancel id
        assertCancelReject(h.send(Cmd.cancel(SA, 10, 8, A, SYM, false)), RejectReason.DUPLICATE_CLORDID);
        // filled order
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 10));
        final List<Ev> late = h.send(Cmd.cancel(SA, 12, 8, A, SYM, false));
        assertCancelReject(late, RejectReason.TOO_LATE_TO_CANCEL);
        assertEquals(OrdStatus.FILLED, late.get(0).er().ordStatus());
    }

    @Test
    void cancelVersusFillOrderingIsLogOrder()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_000, 10));
        // fill first, then cancel: too late
        h.send(Cmd.limit(SB, 1, B, SYM, Side.BUY, 10_000, 10));
        assertCancelReject(h.send(Cmd.cancel(SA, 2, 1, A, SYM, false)), RejectReason.TOO_LATE_TO_CANCEL);
        // cancel first, then the crossing order rests
        h.send(Cmd.limit(SA, 3, A, SYM, Side.SELL, 10_000, 10));
        assertKinds(h.send(Cmd.cancel(SA, 4, 3, A, SYM, false)), "ER:CANCELED", "DELTA");
        assertKinds(h.send(Cmd.limit(SB, 2, B, SYM, Side.BUY, 10_000, 10)), "ER:NEW", "DELTA");
        // partial fill then cancel: canceled with cum preserved
        h.send(Cmd.limit(SA, 5, A, SYM, Side.SELL, 10_010, 10));
        h.send(Cmd.limit(SB, 3, B, SYM, Side.BUY, 10_010, 4));
        final List<Ev> c = h.send(Cmd.cancel(SA, 6, 5, A, SYM, false));
        assertEquals(4, c.get(0).er().cumQty());
        assertEquals(10, c.get(0).er().orderQty());
        assertEquals(0, c.get(0).er().leavesQty());
    }

    @Test
    void replaceDecreaseKeepsPriorityIncreaseOrPriceChangeLosesIt()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_000, 100));
        h.send(Cmd.limit(SA, 2, A, SYM, Side.SELL, 10_000, 100));
        final List<Ev> r = h.send(Cmd.replace(SA, 3, 1, A, SYM, 10_000, 50, false));
        assertKinds(r, "ER:REPLACED", "DELTA");
        assertEquals(3, r.get(0).er().clOrdId());
        assertEquals(1, r.get(0).er().origClOrdId());
        assertEquals(50, r.get(0).er().orderQty());
        assertEquals("DELTA sym=1 side=2 px=10000 q=150 n=2", r.get(1).text());
        // order 1 (now clOrdId 3) still first
        List<Ev> f = h.send(Cmd.limit(SB, 1, B, SYM, Side.BUY, 10_000, 10));
        assertEquals(1, f.get(1).trade().sellOrderId());
        // increase loses priority
        h.send(Cmd.replace(SA, 4, 3, A, SYM, 10_000, 60, false));
        f = h.send(Cmd.limit(SB, 2, B, SYM, Side.BUY, 10_000, 10));
        assertEquals(2, f.get(1).trade().sellOrderId());
        // the old clOrdId is no longer the order's id
        assertCancelReject(h.send(Cmd.cancel(SA, 5, 3, A, SYM, false)), RejectReason.UNKNOWN_ORDER);
        // price change moves to the new level at the back
        h.send(Cmd.limit(SA, 6, A, SYM, Side.SELL, 10_010, 5));
        h.send(Cmd.replace(SA, 7, 4, A, SYM, 10_010, 60, false));
        assertEquals(List.of(
            "DELTA sym=1 side=2 px=10000 q=0 n=0"),
            List.of(h.send(Cmd.replace(SA, 8, 2, A, SYM, 10_010, 90, false)).get(1).text()));
        f = h.send(Cmd.limit(SB, 3, B, SYM, Side.BUY, 10_010, 200));
        assertEquals(5, f.get(1).trade().sellOrderId()); // orderId 5 (clOrdId 6) was first at 10010
        assertEquals(1, f.get(4).trade().sellOrderId());
        assertEquals(2, f.get(7).trade().sellOrderId());
    }

    @Test
    void replaceThatCrossesTradesAsTaker()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.SELL, 10_010, 30));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.BUY, 9_990, 50));
        final List<Ev> r = h.send(Cmd.replace(SB, 2, 1, B, SYM, 10_010, 50, false));
        assertKinds(r, "ER:REPLACED", "TRADE", "ER:PARTIAL_FILL", "ER:FILL", "DELTA", "DELTA", "DELTA");
        assertEquals(LiquidityFlag.TAKER, r.get(2).er().liquidity());
        assertEquals(2, r.get(2).er().clOrdId());
        assertEquals(3002, r.get(2).er().gatewayInTsNs());
        assertEquals("DELTA sym=1 side=1 px=10010 q=20 n=1", r.get(4).text());
        assertEquals("DELTA sym=1 side=1 px=9990 q=0 n=0", r.get(5).text());
        assertEquals("DELTA sym=1 side=2 px=10010 q=0 n=0", r.get(6).text());
    }

    @Test
    void replaceRejections()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 100));
        h.send(Cmd.limit(SB, 1, B, SYM, Side.SELL, 9_900, 40));
        assertCancelReject(h.send(Cmd.replace(SA, 2, 1, A, SYM, 9_900, 40, false)), RejectReason.INVALID_REPLACE);
        assertCancelReject(h.send(Cmd.replace(SA, 3, 1, A, SYM, 9_900, 0, false)), RejectReason.INVALID_QTY);
        assertCancelReject(h.send(Cmd.replace(SA, 4, 1, A, SYM, 20_000, 100, false)), RejectReason.PRICE_BAND);
        assertCancelReject(h.send(Cmd.replace(SA, 5, 1, A, SYM, NULL, 100, false)), RejectReason.INVALID_PRICE);
        assertCancelReject(h.send(Cmd.replace(SA, 6, 99, A, SYM, 9_900, 100, false)), RejectReason.UNKNOWN_ORDER);
        assertCancelReject(h.send(Cmd.replace(SA, 6, 1, A, SYM, 9_900, 100, false)), RejectReason.DUPLICATE_CLORDID);
        final List<Ev> ok = h.send(Cmd.replace(SA, 7, 1, A, SYM, 9_900, 41, false));
        assertEquals(1, ok.get(0).er().leavesQty());
        assertEquals(40, ok.get(0).er().cumQty());
        assertEquals(OrdStatus.PARTIALLY_FILLED, ok.get(0).er().ordStatus());
        h.send(Cmd.state(1, SYM, SymbolState.HALTED, 0));
        assertCancelReject(h.send(Cmd.replace(SA, 8, 7, A, SYM, 9_900, 50, false)), RejectReason.SYMBOL_HALTED);
        h.send(Cmd.limit(SB, 2, B, SYM, Side.SELL, 9_900, 1)); // halted: rejected
        h.send(Cmd.state(2, SYM, SymbolState.CONTINUOUS, 0));
        h.send(Cmd.limit(SB, 3, B, SYM, Side.SELL, 9_900, 1));
        assertCancelReject(h.send(Cmd.replace(SA, 9, 7, A, SYM, 9_900, 50, false)), RejectReason.TOO_LATE_TO_CANCEL);
    }

    @Test
    void replaceTransfersOrderToRequestingSession()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 100));
        h.send(Cmd.limit(SA, 2, A, SYM, Side.BUY, 9_800, 100));
        final List<Ev> r = h.send(Cmd.replace(1002, 3, 1, A, SYM, 9_900, 90, false));
        assertEquals(1002, r.get(0).er().sessionId());
        h.send(Cmd.limit(SA, 4, A, SYM, Side.BUY, 9_700, 100));
        final List<Ev> mc = h.send(Cmd.massCancel(1, SA, A, 0, 1, 0, io.lmb.sbe.MassCancelScope.SESSION, CancelReason.DISCONNECT));
        assertKinds(mc, "ER:CANCELED", "ER:CANCELED", "DELTA", "DELTA", "MASSDONE");
        assertEquals(2, mc.get(0).er().orderId());
        assertEquals(3, mc.get(1).er().orderId());
    }

    @Test
    void possResendDuplicatesAreDroppedSilentlyForEveryRequestType()
    {
        final Harness h = new Harness().standard();
        final byte[] nos = Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 100);
        h.send(nos);
        final byte[] resend = Cmd.newOrder(SA, 1, A, SYM, Side.BUY, OrdType.LIMIT, TimeInForce.DAY, 9_900, 100, true);
        assertEquals(0, h.send(resend).size());
        // even when the resend would now fail validation (band moved) it is dropped, not rejected
        h.send(Cmd.ref(SYM, 20_000, 9));
        assertEquals(0, h.send(resend).size());
        // a non-resend duplicate is rejected (after price validation passes)
        h.send(Cmd.ref(SYM, 10_000, 10));
        assertReject(h.send(nos), RejectReason.DUPLICATE_CLORDID);
        // a resend of a request id never seen is processed normally
        assertKinds(h.send(Cmd.newOrder(SA, 2, A, SYM, Side.BUY, OrdType.LIMIT, TimeInForce.DAY, 9_900, 1, true)), "ER:NEW", "DELTA");
        h.send(Cmd.replace(SA, 3, 1, A, SYM, 9_900, 50, false));
        assertEquals(0, h.send(Cmd.replace(SA, 3, 1, A, SYM, 9_900, 50, true)).size());
        h.send(Cmd.cancel(SA, 4, 3, A, SYM, false));
        assertEquals(0, h.send(Cmd.cancel(SA, 4, 3, A, SYM, true)).size());
        // the first rejected request id is also consumed: resend dropped, plain duplicate rejected
        assertReject(h.send(Cmd.limit(SA, 5, A, SYM, Side.BUY, 1, 1)), RejectReason.PRICE_BAND);
        assertEquals(0, h.send(Cmd.newOrder(SA, 5, A, SYM, Side.BUY, OrdType.LIMIT, TimeInForce.DAY, 9_900, 1, true)).size());
        assertReject(h.send(Cmd.limit(SA, 5, A, SYM, Side.BUY, 9_900, 1)), RejectReason.DUPLICATE_CLORDID);
    }

    @Test
    void malformedCommandsAreRejectedAndStillEndWithCommandApplied()
    {
        final Harness h = new Harness().standard();
        final byte[] nos = Cmd.limit(SA, 1, A, SYM, Side.BUY, 9_900, 100);
        // unknown template
        final byte[] unknown = nos.clone();
        unknown[2] = 55;
        assertEquals(List.of("CMDREJ corr=0 t=55 r=MALFORMED"), t(h.send(unknown)));
        // truncated
        assertEquals(List.of("CMDREJ corr=0 t=1 r=MALFORMED"), t(h.send(java.util.Arrays.copyOf(nos, 20))));
        // shorter than a header
        assertEquals(List.of("CMDREJ corr=0 t=0 r=MALFORMED"), t(h.send(new byte[] {1, 2, 3})));
        // wrong schema id
        final byte[] schema = nos.clone();
        schema[4] = 9;
        assertEquals(List.of("CMDREJ corr=0 t=1 r=MALFORMED"), t(h.send(schema)));
        // invalid side enum: correlation id is the clOrdId
        assertEquals(List.of("CMDREJ corr=1 t=1 r=MALFORMED"), t(h.send(Cmd.poke(nos, 54, 7))));
        // the malformed order did not consume the clOrdId
        assertKinds(h.send(nos), "ER:NEW", "DELTA");
        // empty input
        assertEquals(List.of("CMDREJ corr=0 t=0 r=MALFORMED"), t(h.send(new byte[0])));
    }

    @Test
    void ordersAreRejectedOutsideTradingStates()
    {
        final Harness h = new Harness().standard();
        h.send(Cmd.symbol(2, 2, "PRE", 1, 1, 10_000, 1_000, 1000, SymbolState.PRE_OPEN));
        assertReject(h.send(Cmd.limit(SA, 1, A, 2, Side.BUY, 10_000, 1)), RejectReason.SYMBOL_CLOSED);
        h.send(Cmd.state(1, 2, SymbolState.CLOSED, 0));
        assertReject(h.send(Cmd.limit(SA, 2, A, 2, Side.BUY, 10_000, 1)), RejectReason.SYMBOL_CLOSED);
        h.send(Cmd.state(2, SYM, SymbolState.HALTED, 0));
        assertReject(h.send(Cmd.limit(SA, 3, A, SYM, Side.BUY, 10_000, 1)), RejectReason.SYMBOL_HALTED);
        h.send(Cmd.state(3, SYM, SymbolState.REMOVED, 0));
        assertReject(h.send(Cmd.limit(SA, 4, A, SYM, Side.BUY, 10_000, 1)), RejectReason.UNKNOWN_SYMBOL);
    }
}
