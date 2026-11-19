package io.lmb.gateway.core;

import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.RejectReason;
import org.agrona.DirectBuffer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Last execution report per order, keyed by (firmId, clOrdId) for every id the order was known by
 * (original, replace and cancel request ids). Built from the live event streams, so it covers every
 * order the gateway has seen, not only its own sessions'. Serves OrderQuery.GetOrder and lets the
 * core recognise a resent request whose outcome is already known. Bounded: the oldest keys are
 * evicted beyond {@code maxEntries}. Core thread only.
 */
final class OrderCache
{
    record Key(int firmId, long clOrdId)
    {
    }

    static final class Entry
    {
        final long orderId;
        long sessionId;
        byte[] lastReport;

        Entry(final long orderId)
        {
            this.orderId = orderId;
        }
    }

    private final Map<Key, Entry> byKey;
    private final Map<Long, Entry> byOrderId;

    OrderCache(final int maxEntries)
    {
        byKey = new LinkedHashMap<>(1024, 0.75f, false)
        {
            protected boolean removeEldestEntry(final Map.Entry<Key, Entry> eldest)
            {
                return size() > maxEntries;
            }
        };
        byOrderId = new LinkedHashMap<>(1024, 0.75f, false)
        {
            protected boolean removeEldestEntry(final Map.Entry<Long, Entry> eldest)
            {
                return size() > maxEntries;
            }
        };
    }

    void onReport(final ExecutionReportDecoder er, final DirectBuffer buffer, final int offset, final int length)
    {
        final Key key = new Key((int)er.firmId(), er.clOrdId());
        if (er.execType() == ExecType.REJECTED && er.rejectReason() == RejectReason.DUPLICATE_CLORDID &&
            byKey.containsKey(key))
        {
            return; // a refused duplicate must not overwrite the original order's state
        }
        final byte[] copy = new byte[length];
        buffer.getBytes(offset, copy);
        final long orderId = er.orderId();
        Entry entry;
        if (orderId != 0)
        {
            entry = byOrderId.get(orderId);
            if (entry == null)
            {
                entry = new Entry(orderId);
                byOrderId.put(orderId, entry);
            }
        }
        else
        {
            entry = new Entry(0);
        }
        entry.lastReport = copy;
        entry.sessionId = er.sessionId();
        byKey.put(key, entry);
        if (er.origClOrdId() != 0 && orderId != 0)
        {
            byKey.putIfAbsent(new Key((int)er.firmId(), er.origClOrdId()), entry);
        }
    }

    Entry get(final int firmId, final long clOrdId)
    {
        return byKey.get(new Key(firmId, clOrdId));
    }

    int size()
    {
        return byKey.size();
    }
}
