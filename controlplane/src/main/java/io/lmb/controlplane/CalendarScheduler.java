package io.lmb.controlplane;

import io.lmb.controlplane.TradingCalendar.Phase;
import io.lmb.controlplane.TradingCalendar.Transition;
import io.lmb.controlplane.TrackedCommand.Kind;
import io.lmb.controlplane.db.Store;
import io.lmb.sbe.SymbolState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sends each calendar state transition ahead of time (within leadMs of it) as a SymbolStateCommand with
 * effectiveTimeNs, so the transition itself is a cluster timer and replays identically. One shard-wide
 * command per shard, or per-symbol commands that skip symbols under an operator override (except CLOSE,
 * which applies to everyone). What was sent is recorded in {@code schedule_sent} in the same fenced
 * transaction as the commands, so a new leader never sends a transition twice.
 *
 * <p>Day roll has no timer form in the schema; it is sent when due, level-triggered: while the latest
 * transition is DAY_ROLL (or the PRE_OPEN right after it), any shard whose trading date is older than the
 * roll's target gets DayRollover. Never during trading hours, so a late roll cannot expire live orders.
 * A lost timer command is never re-sent late (it could apply out of order); {@link Scheduler}'s
 * enforcement brings symbols to {@link #enforcementTarget} once graceMs past the transition instead.
 */
public final class CalendarScheduler
{
    private static final Logger LOG = LoggerFactory.getLogger(CalendarScheduler.class);

    private record SentRow(int date, String phase, int shardId, long correlationId, long effectiveNs)
    {
    }

    private TradingCalendar calendar;
    private long calendarRefVersion = -1;
    private final Set<String> sent = new HashSet<>();
    private final List<SentRow> unsaved = new ArrayList<>();

    public void onStepUp(final Engine engine)
    {
        calendarRefVersion = -1;
        sent.clear();
        unsaved.clear();
        final int from = TradingCalendar.yyyymmdd(calendar(engine).localDate(engine.clock().epochNs()).minusDays(3));
        sent.addAll(engine.db().read(c -> Store.scheduleSentKeys(c, from)));
    }

    public TradingCalendar calendar(final Engine engine)
    {
        if (calendar == null || calendarRefVersion != engine.refVersion())
        {
            calendar = TradingCalendar.from(engine.config(), engine.ref().calendar());
            calendarRefVersion = engine.refVersion();
        }
        return calendar;
    }

    public void tick(final Engine engine)
    {
        final TradingCalendar cal = calendar(engine);
        final long now = engine.clock().epochNs();
        final long lead = engine.config().schedulerLeadMs * 1_000_000L;
        for (final Transition t : cal.between(now, now + lead))
        {
            if (t.phase() == Phase.DAY_ROLL)
            {
                continue;
            }
            final Transition prev = cal.lastAtOrBefore(t.epochNs() - 1);
            if (t.phase() == Phase.PRE_OPEN && prev != null && prev.phase() == Phase.DAY_ROLL)
            {
                continue; // the rollover already leaves every symbol PRE_OPEN
            }
            for (int s = 0; s < engine.shardCount(); s++)
            {
                final String key = t.tradingDate() + ":" + t.phase() + ":" + s;
                if (!sent.contains(key) && engine.shard(s).connected)
                {
                    scheduleOnShard(engine, s, t);
                    sent.add(key);
                }
            }
        }

        final Transition last = cal.lastAtOrBefore(now);
        final Transition roll = pendingRoll(cal, last);
        if (roll != null)
        {
            for (int s = 0; s < engine.shardCount(); s++)
            {
                final ShardState sh = engine.shard(s);
                if (sh.ready() && sh.tradingDate < roll.rollToDate() && !engine.tracker().hasActive("roll:" + s))
                {
                    LOG.info("day roll of shard {} to {}", s, roll.rollToDate());
                    AdminExecutor.dayRoll(engine, s, roll.rollToDate(), "SCHEDULER", null);
                }
            }
        }

        if (last != null && last.phase() == Phase.CLOSE)
        {
            final boolean auctionOverrides = engine.ref().symbols().stream().anyMatch(r -> "AUCTION_CALL".equals(r.operatorState()));
            if (auctionOverrides)
            {
                engine.fencedTx(c -> Store.clearOperatorState(c, "AUCTION_CALL", engine.clock().epochMs()));
                engine.reloadRefData();
            }
        }
    }

    /** The DAY_ROLL in force if no trading phase has started since it, else null. */
    private static Transition pendingRoll(final TradingCalendar cal, final Transition last)
    {
        if (last == null)
        {
            return null;
        }
        if (last.phase() == Phase.DAY_ROLL)
        {
            return last;
        }
        if (last.phase() == Phase.PRE_OPEN)
        {
            final Transition prev = cal.lastAtOrBefore(last.epochNs() - 1);
            return prev != null && prev.phase() == Phase.DAY_ROLL ? prev : null;
        }
        return null;
    }

    private void scheduleOnShard(final Engine engine, final int shardId, final Transition t)
    {
        final SymbolState target = t.phase().target;
        final List<Store.SymbolRow> eligible = new ArrayList<>();
        boolean anyOverride = false;
        for (final Store.SymbolRow r : engine.ref().symbols())
        {
            if (r.shardId() != shardId || !r.active())
            {
                continue;
            }
            if (r.operatorState() != null && target != SymbolState.CLOSED)
            {
                anyOverride = true;
            }
            else
            {
                eligible.add(r);
            }
        }
        final String base = "sched:" + t.tradingDate() + ":" + t.phase() + ":" + shardId;
        TrackedCommand first = null;
        if (!anyOverride)
        {
            first = issue(engine, shardId, -1, target, t, base);
        }
        else
        {
            for (final Store.SymbolRow r : eligible)
            {
                final TrackedCommand c = issue(engine, shardId, r.symbolId(), target, t, base + ":" + r.symbolId());
                first = first == null ? c : first;
            }
        }
        unsaved.add(new SentRow(t.tradingDate(), t.phase().name(), shardId, first == null ? 0 : first.correlationId, t.epochNs()));
    }

    private static TrackedCommand issue(final Engine engine, final int shardId, final int symbolId, final SymbolState target,
        final Transition t, final String key)
    {
        final TrackedCommand c = engine.issue(shardId, Kind.SYMBOL_STATE, key, "SCHEDULER",
            t.phase() + " " + t.tradingDate() + " -> " + target + (symbolId < 0 ? " all symbols" : " symbol " + symbolId) +
                " on shard " + shardId, t.epochNs(), null,
            corr -> Commands.symbolState(corr, symbolId < 0 ? Commands.ALL_SYMBOLS : symbolId, target, t.epochNs(), 0));
        c.symbolId = symbolId;
        c.target = target;
        return c;
    }

    public void persistWithCommands(final Engine engine, final Connection c) throws SQLException
    {
        final long now = engine.clock().epochMs();
        for (final SentRow r : unsaved)
        {
            Store.insertScheduleSent(c, r.date(), r.phase(), r.shardId(), r.correlationId(), r.effectiveNs(), now);
        }
        unsaved.clear();
    }

    /**
     * The state the calendar wants for this shard now, or null within graceMs of a transition (its timer
     * may not have fired / been observed yet) or while a day roll is still pending on the shard.
     */
    public SymbolState enforcementTarget(final Engine engine, final ShardState sh)
    {
        final TradingCalendar cal = calendar(engine);
        final long now = engine.clock().epochNs();
        final Transition last = cal.lastAtOrBefore(now);
        if (last == null || now - last.epochNs() < engine.config().schedulerGraceMs * 1_000_000L)
        {
            return null;
        }
        final Transition roll = pendingRoll(cal, last);
        if (roll != null && sh.tradingDate < roll.rollToDate())
        {
            return null;
        }
        return last.state();
    }
}
