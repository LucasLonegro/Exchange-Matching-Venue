package io.lmb.matcher;

import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportDecoder;
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
 * BookDeltas never cross in CONTINUOUS (checked by the caller through Invariants).
 */
final class StreamChecker
{
    private final Map<Long, long[]> orders = new HashMap<>(); // orderId -> {cum, qty}
    private final Set<Long> terminated = new HashSet<>();
    long trades;
    long tradedQty;

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
