package io.lmb.controlplane;

import io.lmb.controlplane.TrackedCommand.Kind;
import io.lmb.controlplane.db.Store;
import io.lmb.sbe.SymbolState;

import java.util.ArrayList;
import java.util.List;

/**
 * Makes every shard's symbol set match the database: defines missing symbols, re-defines symbols whose
 * parameters differ, removes symbols marked REMOVED. Runs when a shard (re)connects, when reference data
 * changes, and every reconcileIntervalMs, but only against shards that are connected and whose event
 * stream is caught up (so "missing" really means missing). SymbolDefinition is idempotent, so a duplicate
 * caused by a lost-then-resent command only re-applies the same parameters.
 *
 * <p>Risk limits are reconciled by {@link RiskBudgetAllocator}, the single writer of RiskLimitUpdate.
 */
public final class Reconciler implements Engine.Duty
{
    private long lastRunNs = Long.MIN_VALUE / 2;
    private long lastRefVersion = -1;

    @Override
    public void onStepUp(final Engine engine)
    {
        lastRunNs = Long.MIN_VALUE / 2;
        lastRefVersion = -1;
    }

    @Override
    public void tick(final Engine engine)
    {
        final long now = engine.clock().monoNs();
        final boolean periodic = now - lastRunNs >= engine.config().reconcileIntervalMs * 1_000_000L ||
            engine.refVersion() != lastRefVersion;
        boolean ranAll = true;
        for (int s = 0; s < engine.shardCount(); s++)
        {
            final ShardState sh = engine.shard(s);
            if (!(periodic || sh.needsReconcile))
            {
                continue;
            }
            if (!sh.ready())
            {
                ranAll = false;
                continue;
            }
            reconcileShard(engine, sh, null);
            sh.needsReconcile = false;
        }
        if (periodic && ranAll)
        {
            lastRunNs = now;
            lastRefVersion = engine.refVersion();
        }
    }

    /**
     * Issues whatever commands bring one shard's symbols in line with the database.
     *
     * @return the commands issued
     */
    public static List<TrackedCommand> reconcileShard(final Engine engine, final ShardState sh, final Long auditId)
    {
        final List<TrackedCommand> issued = new ArrayList<>();
        for (final Store.SymbolRow r : engine.ref().symbols())
        {
            if (r.shardId() != sh.shardId)
            {
                continue;
            }
            final ShardState.Symbol obs = sh.symbols.get(r.symbolId());
            if (r.active())
            {
                final String key = "def:" + sh.shardId + ":" + r.symbolId();
                if ((obs == null || !matches(obs, r)) && !engine.tracker().hasActive(key))
                {
                    issued.add(define(engine, sh.shardId, r, auditId));
                }
            }
            else if (obs != null && obs.state != SymbolState.REMOVED)
            {
                final String key = "state:" + sh.shardId + ":" + r.symbolId();
                if (!engine.tracker().hasActive(key))
                {
                    issued.add(state(engine, sh.shardId, r.symbolId(), SymbolState.REMOVED, 0, "RECONCILE", auditId));
                }
            }
        }
        return issued;
    }

    public static boolean matches(final ShardState.Symbol obs, final Store.SymbolRow r)
    {
        return obs.state != SymbolState.REMOVED && obs.ticker.equals(r.ticker()) && obs.tickSize == r.tickSize() &&
            obs.lotSize == r.lotSize() && obs.referencePrice == r.referencePrice() && obs.priceBandBps == r.priceBandBps() &&
            obs.maxOrderQty == r.maxOrderQty();
    }

    static TrackedCommand define(final Engine engine, final int shardId, final Store.SymbolRow r, final Long auditId)
    {
        final TrackedCommand c = engine.issue(shardId, Kind.SYMBOL_DEFINITION, "def:" + shardId + ":" + r.symbolId(), "RECONCILE",
            "define " + r.ticker() + " (" + r.symbolId() + ") on shard " + shardId, 0, auditId,
            corr -> Commands.symbolDefinition(corr, r.symbolId(), r.ticker(), r.tickSize(), r.lotSize(), r.referencePrice(),
                r.priceBandBps(), r.maxOrderQty(), SymbolState.PRE_OPEN));
        c.symbolId = r.symbolId();
        return c;
    }

    /** A per-symbol state command (symbolId >= 0) or a shard-wide one (symbolId < 0). */
    public static TrackedCommand state(final Engine engine, final int shardId, final int symbolId, final SymbolState target,
        final long effectiveNs, final String origin, final Long auditId)
    {
        final boolean all = symbolId < 0;
        final String key = "state:" + shardId + ":" + (all ? "*" : Integer.toString(symbolId));
        engine.tracker().supersedeActive(key);
        if (all)
        {
            for (final TrackedCommand other : new ArrayList<>(engine.tracker().unresolved()))
            {
                if (other.shardId == shardId && other.kind == Kind.SYMBOL_STATE && other.effectiveNs == 0 &&
                    other.status.active() && other.key.startsWith("state:" + shardId + ":"))
                {
                    engine.tracker().supersedeActive(other.key);
                }
            }
        }
        final TrackedCommand c = engine.issue(shardId, Kind.SYMBOL_STATE, key, origin,
            target + " " + (all ? "all symbols" : "symbol " + symbolId) + " on shard " + shardId +
                (effectiveNs > 0 ? " at " + java.time.Instant.ofEpochSecond(0, effectiveNs) : ""),
            effectiveNs, auditId,
            corr -> Commands.symbolState(corr, all ? Commands.ALL_SYMBOLS : symbolId, target, effectiveNs, 0));
        c.symbolId = symbolId;
        c.target = target;
        return c;
    }
}
