package io.lmb.gateway.core;

import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/**
 * Everything the core keeps per shard: the command client, the live event stream reader, the two
 * submission lanes (urgent = cancels and mass cancels, normal = new orders and replaces) and the
 * in-flight buffer. Core thread only.
 */
final class ShardChannel
{
    record MassKey(long sessionId, long requestId)
    {
    }

    final int shardId;
    CommandClient commands;
    EventStreamClient live;
    boolean connected;
    /** True once the live reader drained what was available at start (bootstrap). */
    boolean drained;

    final ArrayDeque<InFlight> urgent = new ArrayDeque<>();
    final ArrayDeque<InFlight> normal = new ArrayDeque<>();
    /** Sent and not yet observed, in deadline order (completed entries are skipped lazily). */
    final ArrayDeque<InFlight> sent = new ArrayDeque<>();
    int sentCount;

    private final Int2ObjectHashMap<Long2ObjectHashMap<InFlight>> active = new Int2ObjectHashMap<>();
    final Int2IntHashMap pendingNormalByFirm = new Int2IntHashMap(0);
    final Map<MassKey, InFlight> massActive = new HashMap<>();

    ShardChannel(final int shardId)
    {
        this.shardId = shardId;
    }

    int depth()
    {
        return urgent.size() + normal.size() + sentCount;
    }

    InFlight active(final int firmId, final long clOrdId)
    {
        final Long2ObjectHashMap<InFlight> byFirm = active.get(firmId);
        return byFirm == null ? null : byFirm.get(clOrdId);
    }

    void putActive(final InFlight f)
    {
        Long2ObjectHashMap<InFlight> byFirm = active.get(f.firmId);
        if (byFirm == null)
        {
            byFirm = new Long2ObjectHashMap<>();
            active.put(f.firmId, byFirm);
        }
        byFirm.put(f.clOrdId, f);
    }

    void removeActive(final InFlight f)
    {
        if (f.kind == OrderRequest.Kind.MASS_CANCEL)
        {
            massActive.remove(new MassKey(f.sessionId, f.requestId));
            return;
        }
        final Long2ObjectHashMap<InFlight> byFirm = active.get(f.firmId);
        if (byFirm != null && byFirm.get(f.clOrdId) == f)
        {
            byFirm.remove(f.clOrdId);
        }
    }

    void addPendingNormal(final int firmId, final int delta)
    {
        final int n = pendingNormalByFirm.get(firmId) + delta;
        if (n <= 0)
        {
            pendingNormalByFirm.remove(firmId);
        }
        else
        {
            pendingNormalByFirm.put(firmId, n);
        }
    }

    /** Whether a queued-but-unsent normal-lane command for this firm/clOrdId exists. */
    boolean pendingInNormalLane(final int firmId, final long clOrdId)
    {
        final InFlight f = active(firmId, clOrdId);
        return f != null && !f.sent && !f.urgent;
    }
}
