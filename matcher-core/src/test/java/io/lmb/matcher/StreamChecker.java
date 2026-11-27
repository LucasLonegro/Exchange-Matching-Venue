package io.lmb.matcher;

import io.lmb.sbe.ClosingPriceDecoder;
import io.lmb.sbe.DayRolledDecoder;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateChangedDecoder;
import io.lmb.sbe.TradeDecoder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Event-stream invariants, independent of the machine's internals: per order, cum = sum of fills and
 * leaves + cum = orderQty while live; nothing after a terminal report; every Trade is followed by
 * exactly the two fills it describes with the same matchId, quantity and price; books rebuilt from
 * BookDeltas never cross in CONTINUOUS (checked by the caller through Invariants). Day cycle: every
 * ClosingPrice agrees with the trades of the trading day and the previous close, and no trading day ends
 * (DayRolled) with a trade printed after the symbol's last ClosingPrice.
 */
final class StreamChecker
{
    private final Map<Long, long[]> orders = new HashMap<>(); // orderId -> {cum, qty}
    private final Set<Long> terminated = new HashSet<>();
    long trades;
    long tradedQty;
    long closes;
    // day-cycle, per symbol: last trade of the trading day {price, isAuction}, latest close, previous close
    private final Map<Long, long[]> lastTrade = new HashMap<>();
    private final Map<Long, Long> todayClose = new HashMap<>();
    private final Map<Long, Long> prevClose = new HashMap<>();
    private final Set<Long> tradedSinceClose = new HashSet<>();

    void check(final List<Ev> events)
    {
        for (int i = 0; i < events.size(); i++)
        {
            final Ev e = events.get(i);
            if (e.is(TradeDecoder.TEMPLATE_ID))
            {
                final TradeDecoder t = e.trade();
                final ExecutionReportDecoder first = events.get(i + 1).er();
                final ExecutionReportDecoder second = events.get(i + 2).er();
                require(first.matchId() == t.matchId() && second.matchId() == t.matchId(), "matchId on both fills");
                require(first.lastQty() == t.quantity() && second.lastQty() == t.quantity(), "fill qty");
                require(first.lastPx() == t.price() && second.lastPx() == t.price(), "fill px");
                final boolean firstIsBuy = first.orderId() == t.buyOrderId();
                require(firstIsBuy ? second.orderId() == t.sellOrderId() : first.orderId() == t.sellOrderId() && second.orderId() == t.buyOrderId(),
                    "fills are the trade's two orders");
                trades++;
                tradedQty += t.quantity();
                lastTrade.put(t.symbolId(), new long[] {t.price(), t.isAuctionRaw()});
                tradedSinceClose.add(t.symbolId());
            }
            if (e.is(ClosingPriceDecoder.TEMPLATE_ID))
            {
                final ClosingPriceDecoder c = e.closingPrice();
                final long sym = c.symbolId();
                final long[] last = lastTrade.get(sym);
                final Long prev = prevClose.get(sym);
                final long prevPx = prev == null ? Cmd.NULL : prev;
                require(c.previousClose() == prevPx, "previousClose is the last official close: " + e);
                switch (c.source())
                {
                    case AUCTION -> require(last != null && last[1] == 1 && c.price() == last[0], "auction close = auction print: " + e);
                    case LAST_TRADE -> require(last != null && c.price() == last[0], "last-trade close = last print: " + e);
                    case PREVIOUS_CLOSE -> require(last == null && prev != null && c.price() == prevPx, "previous close: " + e);
                    case NONE -> require(last == null && prev == null && c.price() == Cmd.NULL, "no price: " + e);
                    default -> require(false, "bad close source: " + e);
                }
                todayClose.put(sym, c.price());
                tradedSinceClose.remove(sym);
                closes++;
            }
            if (e.is(DayRolledDecoder.TEMPLATE_ID))
            {
                require(tradedSinceClose.isEmpty(), "trading day ended with a stale close for " + tradedSinceClose);
                for (final Map.Entry<Long, Long> c : todayClose.entrySet())
                {
                    prevClose.put(c.getKey(), c.getValue() == Cmd.NULL ? null : c.getValue());
                }
                todayClose.clear();
                lastTrade.clear();
            }
            if (e.is(SymbolStateChangedDecoder.TEMPLATE_ID) && e.stateChanged().newState() == SymbolState.REMOVED)
            {
                final long sym = e.stateChanged().symbolId();
                lastTrade.remove(sym);
                todayClose.remove(sym);
                prevClose.remove(sym);
                tradedSinceClose.remove(sym);
            }
            if (e.is(ExecutionReportDecoder.TEMPLATE_ID))
            {
                final ExecutionReportDecoder er = e.er();
                final long id = er.orderId();
                if (id == 0)
                {
                    require(er.execType() == ExecType.REJECTED || er.execType() == ExecType.CANCEL_REJECTED, "orderId 0 only for rejects");
                    continue;
                }
                if (er.execType() == ExecType.CANCEL_REJECTED)
                {
                    continue;
                }
                require(!terminated.contains(id), "report after terminal state for " + id + ": " + e);
                long[] st = orders.get(id);
                if (er.execType() == ExecType.NEW)
                {
                    require(st == null, "NEW once");
                    st = new long[] {0, er.orderQty()};
                    orders.put(id, st);
                }
                require(st != null, "report for an order never acknowledged: " + e);
                if (er.execType() == ExecType.PARTIAL_FILL || er.execType() == ExecType.FILL)
                {
                    st[0] += er.lastQty();
                }
                st[1] = er.orderQty();
                require(er.cumQty() == st[0], "cum = sum of fills: " + e);
                final boolean terminal = er.execType() == ExecType.FILL || er.execType() == ExecType.CANCELED ||
                    er.execType() == ExecType.EXPIRED;
                if (terminal)
                {
                    require(er.leavesQty() == 0, "terminal leaves 0");
                    if (er.execType() == ExecType.FILL)
                    {
                        require(er.cumQty() == er.orderQty(), "filled = qty");
                    }
                    terminated.add(id);
                    orders.remove(id);
                }
                else
                {
                    require(er.leavesQty() + er.cumQty() == er.orderQty(), "leaves + cum = qty: " + e);
                    require(er.leavesQty() > 0, "live order has leaves");
                }
            }
        }
    }

    private static void require(final boolean c, final String msg)
    {
        if (!c)
        {
            throw new AssertionError(msg);
        }
    }
}
