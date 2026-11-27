package io.lmb.marketdata.grpc;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.lmb.marketdata.PublisherMetrics;
import io.lmb.marketdata.book.OrderBook;
import io.lmb.proto.marketdata.BookIncremental;
import io.lmb.proto.marketdata.BookSnapshot;
import io.lmb.proto.marketdata.MarketDataUpdate;
import io.lmb.proto.marketdata.PriceLevel;
import org.agrona.collections.Int2ObjectHashMap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Set;

/**
 * One gRPC subscription. Every method except {@link #cancel()} runs on the fan-out thread, which is
 * therefore the only thread calling the observer.
 *
 * <p>Book updates are diffs between what this subscriber was last sent (its "view", limited to
 * {@code depth} levels per side) and the fan-out book, so depth limiting and conflation are both just a
 * matter of when the diff is taken. A level leaving the top {@code depth} is sent with quantity 0 and a
 * level entering it with its full quantity, so a client applying snapshot + incrementals holds exactly the
 * top {@code depth} levels.
 */
final class Subscriber
{
    /** Per-symbol state of this subscription. */
    static final class View
    {
        final OrderBook sent = new OrderBook();
        boolean dirty;
        boolean inDirtyList;
        long lastFlushNs = Long.MIN_VALUE / 2;
    }

    final long id;
    private final ServerCallStreamObserver<MarketDataUpdate> observer;
    private final Set<String> tickers; // empty = all
    final int depth;
    final long conflationNs;
    private final int maxQueued;
    private final PublisherMetrics metrics;

    final Int2ObjectHashMap<View> views = new Int2ObjectHashMap<>();
    final ArrayList<MarketDataFanout.FanoutSymbol> dirtySymbols = new ArrayList<>();
    private final ArrayDeque<MarketDataUpdate> queue = new ArrayDeque<>();
    private final OrderBook scratch = new OrderBook();
    private volatile boolean cancelled;
    private boolean closed;

    Subscriber(
        final long id, final ServerCallStreamObserver<MarketDataUpdate> observer, final Set<String> tickers, final int depth,
        final long conflationNs, final int maxQueued, final PublisherMetrics metrics)
    {
        this.id = id;
        this.observer = observer;
        this.tickers = tickers;
        this.depth = depth;
        this.conflationNs = conflationNs;
        this.maxQueued = maxQueued;
        this.metrics = metrics;
    }

    /** Any thread. */
    void cancel()
    {
        cancelled = true;
    }

    boolean isDone()
    {
        return closed || cancelled;
    }

    boolean matches(final MarketDataFanout.FanoutSymbol s)
    {
        return tickers.isEmpty() || tickers.contains(s.ticker);
    }

    int queued()
    {
        return queue.size();
    }

    // ---- book updates ----

    void markDirty(final MarketDataFanout.FanoutSymbol s)
    {
        View v = views.get(s.symbolId);
        if (v == null)
        {
            // Symbol appeared after subscribing without an InstrumentDefinition (should not happen): start empty.
            v = new View();
            views.put(s.symbolId, v);
        }
        v.dirty = true;
        if (!v.inDirtyList)
        {
            v.inDirtyList = true;
            dirtySymbols.add(s);
        }
    }

    /** Sends the symbol's pending book changes now, tagged with the symbol's current md_seq. */
    void flush(final MarketDataFanout.FanoutSymbol s, final long nowNs)
    {
        final View v = views.get(s.symbolId);
        if (v == null || !v.dirty)
        {
            return;
        }
        v.dirty = false;
        v.lastFlushNs = nowNs;
        scratch.copyTop(s.book, depth);
        final BookIncremental.Builder inc = BookIncremental.newBuilder();
        final int bidChanges = diff(v.sent, scratch, OrderBook.BID, inc);
        final int askChanges = diff(v.sent, scratch, OrderBook.ASK, inc);
        v.sent.copyFrom(scratch);
        if (bidChanges + askChanges > 0)
        {
            send(s.update().setIncremental(inc).build());
        }
    }

    /** Pending book changes of conflating subscriptions whose interval has elapsed. */
    int flushDue(final long nowNs)
    {
        int flushed = 0;
        for (int i = dirtySymbols.size() - 1; i >= 0; i--)
        {
            final MarketDataFanout.FanoutSymbol s = dirtySymbols.get(i);
            final View v = views.get(s.symbolId);
            if (v == null || !v.dirty)
            {
                removeDirty(i, v);
            }
            else if (nowNs - v.lastFlushNs >= conflationNs)
            {
                flush(s, nowNs);
                removeDirty(i, v);
                flushed++;
            }
        }
        return flushed;
    }

    private void removeDirty(final int i, final View v)
    {
        final int last = dirtySymbols.size() - 1;
        dirtySymbols.set(i, dirtySymbols.get(last));
        dirtySymbols.remove(last);
        if (v != null)
        {
            v.inDirtyList = false;
        }
    }

    /** Full refresh of one symbol (on subscribe and on InstrumentDefinition). */
    void sendSnapshot(final MarketDataFanout.FanoutSymbol s, final long nowNs)
    {
        View v = views.get(s.symbolId);
        if (v == null)
        {
            v = new View();
            views.put(s.symbolId, v);
        }
        v.sent.copyTop(s.book, depth);
        v.dirty = false;
        v.lastFlushNs = nowNs;
        final BookSnapshot.Builder snap = BookSnapshot.newBuilder().setState(s.stateName())
            .setStateCause(s.stateCauseName());
        final io.lmb.proto.marketdata.ReferencePrice reference = s.reference();
        if (reference != null)
        {
            snap.setReference(reference); // day-cycle / band-stp
        }
        final io.lmb.proto.marketdata.ClosingPrice close = s.close();
        if (close != null)
        {
            snap.setLastClose(close);
        }
        for (int i = 0; i < v.sent.levels(OrderBook.BID); i++)
        {
            snap.addBids(level(v.sent, OrderBook.BID, i));
        }
        for (int i = 0; i < v.sent.levels(OrderBook.ASK); i++)
        {
            snap.addAsks(level(v.sent, OrderBook.ASK, i));
        }
        send(s.update().setSnapshot(snap).build());
    }

    private static PriceLevel level(final OrderBook b, final int side, final int i)
    {
        return PriceLevel.newBuilder().setPriceCents(b.price(side, i)).setQuantity(b.quantity(side, i))
            .setOrderCount(b.orderCount(side, i)).build();
    }

    /** Merge walk of two best-first ladders; emits changed, new and removed (quantity 0) levels, best first. */
    private static int diff(final OrderBook old, final OrderBook cur, final int side, final BookIncremental.Builder out)
    {
        final int n = old.levels(side);
        final int m = cur.levels(side);
        int i = 0;
        int j = 0;
        int changes = 0;
        while (i < n || j < m)
        {
            final int cmp;
            if (i >= n)
            {
                cmp = 1;
            }
            else if (j >= m)
            {
                cmp = -1;
            }
            else
            {
                final long po = old.price(side, i);
                final long pc = cur.price(side, j);
                cmp = po == pc ? 0 : ((side == OrderBook.BID ? po > pc : po < pc) ? -1 : 1);
            }
            final PriceLevel level;
            if (cmp < 0)
            {
                level = PriceLevel.newBuilder().setPriceCents(old.price(side, i)).setQuantity(0).setOrderCount(0).build();
                i++;
            }
            else if (cmp > 0)
            {
                level = level(cur, side, j);
                j++;
            }
            else
            {
                level = old.quantity(side, i) != cur.quantity(side, j) || old.orderCount(side, i) != cur.orderCount(side, j)
                    ? level(cur, side, j) : null;
                i++;
                j++;
            }
            if (level != null)
            {
                changes++;
                if (side == OrderBook.BID)
                {
                    out.addBidChanges(level);
                }
                else
                {
                    out.addAskChanges(level);
                }
            }
        }
        return changes;
    }

    // ---- delivery ----

    void send(final MarketDataUpdate update)
    {
        if (isDone())
        {
            return;
        }
        if (queue.isEmpty() && observer.isReady())
        {
            onNext(update);
            return;
        }
        queue.addLast(update);
        if (queue.size() > maxQueued)
        {
            closed = true;
            queue.clear();
            metrics.grpcSlowConsumerDisconnects.incrementAndGet();
            safeError(Status.RESOURCE_EXHAUSTED.withDescription(
                "slow consumer: more than " + maxQueued + " updates queued; resubscribe (consider conflation_ms)"));
        }
    }

    /** Sends queued updates while the transport accepts them. */
    int drain()
    {
        int n = 0;
        while (!isDone() && !queue.isEmpty() && observer.isReady())
        {
            onNext(queue.pollFirst());
            n++;
        }
        return n;
    }

    private void onNext(final MarketDataUpdate update)
    {
        try
        {
            observer.onNext(update);
            metrics.grpcUpdates.incrementAndGet();
        }
        catch (final RuntimeException e)
        {
            closed = true; // call already cancelled or closed
        }
    }

    void close(final Status status)
    {
        if (!closed)
        {
            closed = true;
            queue.clear();
            if (!cancelled)
            {
                safeError(status);
            }
        }
    }

    private void safeError(final Status status)
    {
        try
        {
            observer.onError(status.asRuntimeException());
        }
        catch (final RuntimeException ignore)
        {
            // already closed
        }
    }
}
