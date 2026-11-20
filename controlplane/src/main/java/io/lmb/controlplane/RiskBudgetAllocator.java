package io.lmb.controlplane;

import io.lmb.controlplane.TrackedCommand.Kind;
import io.lmb.controlplane.db.Store;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Splits each firm's credit limit L into per-shard budgets and is the only writer of RiskLimitUpdate.
 *
 * <p><b>Safety.</b> For each (firm, shard) the allocator keeps an upper bound
 * {@code U = max(last applied budget, budgets of every unresolved command)} on what the shard can be
 * enforcing now or later. Commands are written to the database before they are offered, so a new leader
 * sees every command an old leader could have sent. It only ever issues a budget v if
 * {@code sum over other shards of U + max(U_s, v) <= L}; a shrink never raises U, and U only falls when a
 * RiskLimitApplied for that key is observed. Hence the sum of effective budgets never exceeds L, also
 * with lost or duplicated commands. A shrink frees budget for others only once it is confirmed.
 * At most one budget command per (firm, shard) is retried at a time; operator changes of maxPos/enabled
 * (kill switch) supersede it immediately, carrying the same budget so U does not grow.
 *
 * <p><b>Targets.</b> With usage u_s from the last RiskLimitApplied and headroom H = L - sum(u):
 * {@code t_s = u_s + floor(H * (alpha/n + (1-alpha) * u_s/sum(u)))}; if sum(u) >= L, t_s = floor(L * u_s/sum(u)).
 * A shard close to its budget therefore always has t_s above its usage while headroom exists. Changes
 * smaller than minStep (a fraction of L) are ignored to avoid churn. Usage is refreshed by re-sending the
 * current limits every refreshMs (the shard echoes creditUsed).
 */
public final class RiskBudgetAllocator implements Engine.Duty
{
    private long lastRunNs = Long.MIN_VALUE / 2;
    private final Map<String, Long> targets = new TreeMap<>();
    private final Map<String, Long> dirtyTargets = new TreeMap<>();

    public static String key(final int shardId, final int firmId)
    {
        return "risk:" + shardId + ":" + firmId;
    }

    @Override
    public void onStepUp(final Engine engine)
    {
        lastRunNs = Long.MIN_VALUE / 2;
        targets.clear();
        dirtyTargets.clear();
    }

    @Override
    public void tick(final Engine engine)
    {
        final long now = engine.clock().monoNs();
        if (now - lastRunNs < engine.config().allocatorIntervalMs * 1_000_000L)
        {
            return;
        }
        lastRunNs = now;
        for (final Store.FirmRow f : engine.ref().firms())
        {
            runFirm(engine, f, null);
        }
    }

    /** Upper bound on the budget shard s may enforce for the firm, now or later. */
    public static long upperBound(final Engine engine, final int shardId, final int firmId)
    {
        final ShardState.Firm obs = engine.shard(shardId).firms.get(firmId);
        long u = obs == null ? 0 : obs.creditBudget;
        for (final TrackedCommand c : engine.tracker().forKey(key(shardId, firmId)))
        {
            u = Math.max(u, c.budget);
        }
        return u;
    }

    /** One allocation pass for a firm. @return the non-refresh commands issued */
    public List<TrackedCommand> runFirm(final Engine engine, final Store.FirmRow firm, final Long auditId)
    {
        final List<TrackedCommand> issued = new ArrayList<>();
        final ControlPlaneConfig cfg = engine.config();
        final int n = engine.shardCount();
        final int firmId = firm.firmId();
        final long limit = Math.max(0, firm.creditLimit());
        final long maxPos = firm.maxPositionPerSymbol();
        final boolean enabled = firm.enabled();
        final long now = engine.clock().monoNs();

        final long[] applied = new long[n];
        final long[] used = new long[n];
        final long[] upper = new long[n];
        final boolean[] seen = new boolean[n];
        final boolean[] active = new boolean[n];
        final TrackedCommand[] latest = new TrackedCommand[n];
        for (int s = 0; s < n; s++)
        {
            final ShardState.Firm obs = engine.shard(s).firms.get(firmId);
            seen[s] = obs != null;
            applied[s] = seen[s] ? obs.creditBudget : 0;
            used[s] = seen[s] ? Math.max(0, obs.creditUsed) : 0;
            upper[s] = upperBound(engine, s, firmId);
            latest[s] = engine.tracker().latest(key(s, firmId));
            active[s] = engine.tracker().hasActive(key(s, firmId));
        }

        // 1. operator changes to maxPos / enabled: urgent, supersede whatever is in flight, keep the budget.
        for (int s = 0; s < n; s++)
        {
            final ShardState sh = engine.shard(s);
            final boolean known = latest[s] != null || seen[s];
            final long intentBudget = latest[s] != null ? latest[s].budget : applied[s];
            final long intentMax = latest[s] != null ? latest[s].maxPos : (seen[s] ? sh.firms.get(firmId).maxPositionPerSymbol : -1);
            final boolean intentEnabled = latest[s] != null ? latest[s].enabled : seen[s] && sh.firms.get(firmId).enabled;
            final boolean differs = known ? intentMax != maxPos || intentEnabled != enabled : !enabled;
            if (differs && (!enabled || sh.ready()))
            {
                engine.tracker().supersedeActive(key(s, firmId));
                issued.add(send(engine, s, firm, known ? intentBudget : 0, enabled ? "LIMITS" : "KILL", auditId));
                active[s] = true;
            }
        }

        final long[] t = targets(used, limit, cfg.allocatorAlpha);
        for (int s = 0; s < n; s++)
        {
            final String k = firmId + ":" + s;
            final Long old = targets.put(k, t[s]);
            if (old == null || old != t[s])
            {
                dirtyTargets.put(k, t[s]);
            }
        }
        if (!enabled)
        {
            return issued;
        }

        final long minStep = Math.max(1, (long)(limit * cfg.allocatorMinStepFraction));
        long sumUpper = 0;
        for (int s = 0; s < n; s++)
        {
            sumUpper += upper[s];
        }

        // 2. shrinks: never raise an upper bound, so always safe.
        for (int s = 0; s < n; s++)
        {
            if (active[s] || !engine.shard(s).ready() || !seen[s])
            {
                continue;
            }
            if (t[s] < applied[s] && (applied[s] - t[s] >= minStep || sumUpper > limit))
            {
                issued.add(send(engine, s, firm, t[s], "REBALANCE", auditId));
                active[s] = true;
            }
        }

        // 3. raises, most pressured shard first, each only within what the other shards' upper bounds leave.
        final List<Integer> order = new ArrayList<>();
        for (int s = 0; s < n; s++)
        {
            order.add(s);
        }
        order.sort(Comparator.comparingDouble((Integer s) -> -pressure(used[s], applied[s])).thenComparingInt(s -> s));
        for (final int s : order)
        {
            if (active[s] || !engine.shard(s).ready() || t[s] <= applied[s])
            {
                continue;
            }
            final long room = limit - (sumUpper - upper[s]);
            final long v = Math.min(t[s], room);
            if (v <= applied[s] || (v - applied[s] < minStep && v != t[s]))
            {
                continue;
            }
            issued.add(send(engine, s, firm, v, "REBALANCE", auditId));
            active[s] = true;
            final long newUpper = Math.max(upper[s], v);
            sumUpper += newUpper - upper[s];
            upper[s] = newUpper;
        }

        // 4. refresh: re-send the current limits so the shard echoes its usage (or creates the firm).
        for (int s = 0; s < n; s++)
        {
            final ShardState sh = engine.shard(s);
            if (active[s] || !sh.ready())
            {
                continue;
            }
            final ShardState.Firm obs = sh.firms.get(firmId);
            if (obs == null && latest[s] == null)
            {
                send(engine, s, firm, 0, "REFRESH", null);
            }
            else if (obs != null && now - obs.observedMonoNs >= cfg.allocatorRefreshMs * 1_000_000L)
            {
                send(engine, s, firm, applied[s], "REFRESH", null);
            }
        }
        return issued;
    }

    private static double pressure(final long used, final long applied)
    {
        return applied > 0 ? (double)used / applied : used > 0 ? Double.MAX_VALUE : 0;
    }

    /** Target budgets for the given usage; their sum never exceeds limit. */
    public static long[] targets(final long[] used, final long limit, final double alpha)
    {
        final int n = used.length;
        final long[] t = new long[n];
        long sumUsed = 0;
        for (final long u : used)
        {
            sumUsed += u;
        }
        if (limit <= 0)
        {
            return t;
        }
        if (sumUsed >= limit)
        {
            for (int s = 0; s < n; s++)
            {
                t[s] = (long)Math.floor((double)limit * used[s] / sumUsed);
            }
        }
        else
        {
            final long headroom = limit - sumUsed;
            for (int s = 0; s < n; s++)
            {
                final double share = sumUsed > 0 ? (double)used[s] / sumUsed : 1.0 / n;
                t[s] = used[s] + (long)Math.floor(headroom * (alpha / n + (1 - alpha) * share));
            }
        }
        long sum = 0;
        for (final long v : t)
        {
            sum += v;
        }
        for (int s = n - 1; sum > limit && s >= 0; s--)
        {
            final long cut = Math.min(t[s], sum - limit);
            t[s] -= cut;
            sum -= cut;
        }
        return t;
    }

    private static TrackedCommand send(final Engine engine, final int shardId, final Store.FirmRow firm, final long budget,
        final String origin, final Long auditId)
    {
        final int firmId = firm.firmId();
        final long maxPos = firm.maxPositionPerSymbol();
        final boolean enabled = firm.enabled();
        final TrackedCommand c = engine.issue(shardId, Kind.RISK_LIMIT, key(shardId, firmId), origin,
            "firm " + firmId + " shard " + shardId + " budget " + budget + " maxPos " + maxPos + (enabled ? "" : " DISABLED"),
            0, auditId, corr -> Commands.riskLimit(corr, firmId, budget, maxPos, enabled));
        c.firmId = firmId;
        c.budget = budget;
        c.maxPos = maxPos;
        c.enabled = enabled;
        return c;
    }

    @Override
    public void persist(final Engine engine, final Connection c) throws SQLException
    {
        final long now = engine.clock().epochMs();
        for (final Map.Entry<String, Long> e : dirtyTargets.entrySet())
        {
            final String[] p = e.getKey().split(":");
            Store.saveBudget(c, new Store.BudgetRow(Integer.parseInt(p[0]), Integer.parseInt(p[1]), e.getValue(), now));
        }
        dirtyTargets.clear();
    }
}
