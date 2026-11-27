package io.lmb.matcher;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Structural invariants of a MatcherShard, recomputed from scratch. Throws AssertionError on violation. */
final class Invariants
{
    private Invariants()
    {
    }

    private static void require(final boolean cond, final String msg)
    {
        if (!cond)
        {
            throw new AssertionError("invariant violated: " + msg);
        }
    }

    static void check(final MatcherShard m)
    {
        final Set<Order> inBooks = new HashSet<>();
        final Map<Firm, Long> openNotional = new HashMap<>();
        final Map<FirmSymbol, long[]> openQty = new HashMap<>();

        for (final Symbol s : m.symbols)
        {
            for (final BookSide side : new BookSide[] {s.bids, s.asks})
            {
                for (int i = 0; i < side.size; i++)
                {
                    final PriceLevel l = side.sorted[i];
                    require(l.attached && !l.dirty, "level attached and clean");
                    require(l.side == side, "level side");
                    require(side.find(l.price) == l, "index finds level " + l.price);
                    if (i > 0)
                    {
                        require(side.better(l.price, side.sorted[i - 1].price), "sorted strictly worst..best");
                    }
                    require(l.orders.size > 0, "attached level non-empty");
                    long total = 0;
                    int count = 0;
                    Order prev = null;
                    for (Order o = l.orders.head; o != null; o = o.next[Order.L_LEVEL])
                    {
                        require(o.prev[Order.L_LEVEL] == prev, "level links");
                        require(o.level == l, "order.level");
                        require(o.price == l.price, "order price = level price");
                        require(o.isBuy() == side.buy, "order side");
                        require(o.symbol == s, "order symbol");
                        require(o.leaves() > 0, "resting order has leaves");
                        require(o.cumQty >= 0 && o.cumQty < o.qty, "0 <= cum < qty");
                        require(o.ordType == Codes.LIMIT && o.tif == Codes.DAY, "only DAY limits rest");
                        require(o.linked, "resting order linked");
                        require(inBooks.add(o), "order in one level only");
                        total += o.leaves();
                        count++;
                        prev = o;
                    }
                    require(l.orders.tail == prev, "level tail");
                    require(total == l.totalQty, "level total " + total + " vs " + l.totalQty);
                    require(count == l.orders.size, "level count");
                }
            }
            if (s.state == Codes.CONTINUOUS)
            {
                require(!s.crossed(), "book crossed in CONTINUOUS for symbol " + s.symbolId);
            }
            if (s.state == Codes.PRE_OPEN || s.state == Codes.CLOSED || s.state == Codes.REMOVED)
            {
                require(s.bids.size == 0 && s.asks.size == 0, "no orders in state " + s.state);
            }
        }

        int globalCount = 0;
        long lastId = Long.MIN_VALUE;
        for (Order o = m.allOrders.head; o != null; o = o.next[Order.L_GLOBAL])
        {
            require(o.orderId > lastId, "global list in orderId order");
            lastId = o.orderId;
            require(inBooks.contains(o), "listed order is in a book");
            require(m.dedupe.value(o.dedupeEntry) == o.poolIndex, "dedupe entry points to order");
            require(m.dedupe.keyA(o.dedupeEntry) == (o.firm.firmId & 0xFFFF_FFFFL) && m.dedupe.keyB(o.dedupeEntry) == o.clOrdId,
                "dedupe key = (firm, current clOrdId)");
            require(m.orderById.get(o.orderId) == o.poolIndex, "orderById");
            openNotional.merge(o.firm, o.leaves() * o.riskPrice, Long::sum);
            final long[] q = openQty.computeIfAbsent(o.fs, k -> new long[2]);
            q[o.isBuy() ? 0 : 1] += o.leaves();
            globalCount++;
        }
        require(globalCount == inBooks.size(), "every booked order is in the global list");
        require(m.orderPool.inUse() == globalCount, "no leaked orders: pool " + m.orderPool.inUse() + " vs " + globalCount);
        require(m.orderById.size() == globalCount, "orderById size");

        for (final Firm f : m.firms)
        {
            require(f.openNotional == openNotional.getOrDefault(f, 0L), "firm open notional " + f.firmId);
            checkList(f.orders, Order.L_FIRM, o -> o.firm == f);
        }
        for (final FirmSymbol fs : m.firmSymbols)
        {
            final long[] q = openQty.getOrDefault(fs, new long[2]);
            require(fs.openBuyQty == q[0] && fs.openSellQty == q[1], "open qty firm-symbol");
            checkList(fs.orders, Order.L_FS, o -> o.fs == fs);
        }
        int running = 0;
        for (final Symbol s : m.symbols)
        {
            checkList(s.orders, Order.L_SYMBOL, o -> o.symbol == s);
            // band-stp: a volatility timer runs only in AUCTION_CALL and is indexed in volTimers
            if (s.volTimerId != 0)
            {
                running++;
                require(s.state == Codes.AUCTION_CALL, "volatility auction outside AUCTION_CALL");
                require(m.volTimers.get(s.volTimerId) == s.idx, "volTimers index");
                require(s.volEndNs > 0, "volatility auction has a deadline");
            }
            else
            {
                require(s.volEndNs == 0, "no deadline without a volatility timer");
            }
        }
        require(m.volTimers.size() == running, "volTimers has no stale entries");
        int sessionTotal = 0;
        for (final Session se : m.sessions)
        {
            checkList(se.orders, Order.L_SESSION, o -> o.session == se);
            sessionTotal += se.orders.size;
        }
        require(sessionTotal == globalCount, "every order in one session list");
        int levels = 0;
        for (final Symbol s : m.symbols)
        {
            levels += s.bids.size + s.asks.size;
        }
        require(m.levelPool.inUse() == levels, "no leaked levels");
    }

    private static void checkList(final OrderList list, final int k, final java.util.function.Predicate<Order> owner)
    {
        int n = 0;
        long lastId = Long.MIN_VALUE;
        Order prev = null;
        for (Order o = list.head; o != null; o = o.next[k])
        {
            require(o.prev[k] == prev, "list links " + k);
            require(owner.test(o), "list owner " + k);
            require(o.orderId > lastId, "list " + k + " sorted by orderId");
            lastId = o.orderId;
            prev = o;
            n++;
        }
        require(list.tail == prev && list.size == n, "list size/tail " + k);
    }
}
