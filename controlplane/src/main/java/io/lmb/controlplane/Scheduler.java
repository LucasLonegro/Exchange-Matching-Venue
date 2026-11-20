package io.lmb.controlplane;

import io.lmb.controlplane.db.Store;
import io.lmb.sbe.SymbolState;

import java.util.HashMap;
import java.util.Map;

/**
 * Trading-day driver. Two jobs:
 * <ol>
 * <li>Calendar transitions sent ahead of time as timer commands (see {@link CalendarScheduler}).</li>
 * <li>Enforcement (level-triggered): each symbol whose observed state differs from what the calendar
 * (or autoOpen, or an operator override) wants gets one immediate transition, one hop at a time along the
 * allowed-transition graph. This repairs lost timer commands, and is how autoOpen opens the market.</li>
 * </ol>
 */
public final class Scheduler implements Engine.Duty
{
    private final CalendarScheduler calendar;
    private final Map<String, Long> lastEnforceNs = new HashMap<>();

    public Scheduler(final CalendarScheduler calendar)
    {
        this.calendar = calendar;
    }

    @Override
    public void onStepUp(final Engine engine)
    {
        lastEnforceNs.clear();
        if (calendar != null)
        {
            calendar.onStepUp(engine);
        }
    }

    @Override
    public void persistWithCommands(final Engine engine, final java.sql.Connection c) throws java.sql.SQLException
    {
        if (calendar != null)
        {
            calendar.persistWithCommands(engine, c);
        }
    }

    @Override
    public void tick(final Engine engine)
    {
        final ControlPlaneConfig cfg = engine.config();
        final long now = engine.clock().monoNs();
        if (calendar != null && !cfg.autoOpen && cfg.schedulerEnabled)
        {
            calendar.tick(engine);
        }
        if (!cfg.enforce && !cfg.autoOpen)
        {
            return;
        }
        final long cooldown = cfg.commandTimeoutMs * 1_000_000L;
        for (int s = 0; s < engine.shardCount(); s++)
        {
            final ShardState sh = engine.shard(s);
            if (!sh.ready() || engine.tracker().hasActive("state:" + s + ":*"))
            {
                continue;
            }
            final SymbolState target = target(engine, sh);
            for (final Store.SymbolRow r : engine.ref().symbols())
            {
                if (r.shardId() != s || !r.active())
                {
                    continue;
                }
                final ShardState.Symbol obs = sh.symbols.get(r.symbolId());
                if (obs == null)
                {
                    continue;
                }
                final String key = "state:" + s + ":" + r.symbolId();
                if (engine.tracker().hasActive(key))
                {
                    continue;
                }
                final SymbolState hop = SymbolStates.desiredHop(obs.state, target, r.operatorState());
                if (hop == null)
                {
                    continue;
                }
                final Long last = lastEnforceNs.get(key);
                if (last != null && now - last < cooldown)
                {
                    continue;
                }
                lastEnforceNs.put(key, now);
                Reconciler.state(engine, s, r.symbolId(), hop, 0, "ENFORCE", null);
            }
        }
    }

    /** What the trading day wants on this shard right now (null = no opinion). */
    public SymbolState target(final Engine engine, final ShardState sh)
    {
        final ControlPlaneConfig cfg = engine.config();
        if (cfg.autoOpen)
        {
            return engine.clock().monoNs() - engine.stepUpMonoNs() >= cfg.autoOpenDelayMs * 1_000_000L ? SymbolState.CONTINUOUS : null;
        }
        if (calendar == null || !cfg.enforce)
        {
            return null;
        }
        return calendar.enforcementTarget(engine, sh);
    }
}
