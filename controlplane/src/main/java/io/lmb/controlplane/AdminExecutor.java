package io.lmb.controlplane;

import io.lmb.controlplane.TrackedCommand.Kind;
import io.lmb.controlplane.db.Store;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.SymbolState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Executes operator requests on the leader. The REST API (on any instance) only validates a request and
 * inserts it into {@code audit_log} as PENDING; the leader picks it up here, applies the database change
 * and issues the shard commands (linked to the audit row by audit_id) in fenced transactions, and
 * finalises the row once every command it issued has a confirmed outcome: COMPLETED (all confirmed),
 * PARTIAL (some), FAILED (none), or REJECTED (refused before any command, e.g. unknown symbol).
 * A request that needs no shard command completes immediately.
 */
public final class AdminExecutor implements Engine.Duty
{
    private static final Logger LOG = LoggerFactory.getLogger(AdminExecutor.class);

    public static final List<String> ACTIONS = List.of("HALT_SYMBOL", "RESUME_SYMBOL", "AUCTION_START", "AUCTION_STOP",
        "HALT_MARKET", "RESUME_MARKET", "UPDATE_LIMITS", "KILL_FIRM", "ENABLE_FIRM", "MASS_CANCEL", "DAY_ROLL",
        "UPSERT_SYMBOL", "REMOVE_SYMBOL", "SET_CALENDAR", "DELETE_CALENDAR", "RESYNC_SHARD");

    private final RiskBudgetAllocator allocator;
    private Scheduler scheduler;
    private long lastPollNs = Long.MIN_VALUE / 2;
    private final TreeSet<Long> dirtyAudits = new TreeSet<>();

    /** Thrown to reject a request without side effects. */
    static final class Rejected extends RuntimeException
    {
        Rejected(final String message)
        {
            super(message);
        }
    }

    private record Result(List<TrackedCommand> commands, String note)
    {
    }

    public AdminExecutor(final RiskBudgetAllocator allocator)
    {
        this.allocator = allocator;
    }

    public void scheduler(final Scheduler scheduler)
    {
        this.scheduler = scheduler;
    }

    @Override
    public void onStepUp(final Engine engine)
    {
        dirtyAudits.clear();
        lastPollNs = Long.MIN_VALUE / 2;
        final long now = engine.clock().epochMs();
        engine.fencedTx(c ->
        {
            for (final Store.AuditRow a : Store.auditsByStatus(c, "EXECUTING", 1_000))
            {
                if (Store.commandsForAudit(c, a.id()).isEmpty())
                {
                    Store.updateAudit(c, a.id(), "FAILED", "interrupted by a leader change before its commands were " +
                        "recorded; re-submit if still needed", null, null, now);
                }
                else
                {
                    dirtyAudits.add(a.id());
                }
            }
            return null;
        });
    }

    @Override
    public void onResolved(final Engine engine, final TrackedCommand command)
    {
        if (command.auditId != null)
        {
            dirtyAudits.add(command.auditId);
        }
    }

    @Override
    public void tick(final Engine engine)
    {
        final long now = engine.clock().monoNs();
        if (now - lastPollNs < engine.config().adminPollMs * 1_000_000L)
        {
            return;
        }
        lastPollNs = now;
        final List<Store.AuditRow> pending = engine.db().read(c -> Store.auditsByStatus(c, "PENDING", 20));
        for (final Store.AuditRow a : pending)
        {
            execute(engine, a);
        }
    }

    private void execute(final Engine engine, final Store.AuditRow a)
    {
        final String me = engine.config().instanceId;
        final long nowMs = engine.clock().epochMs();
        Result result;
        try
        {
            final Map<String, Object> p = Json.parseObject(a.params());
            result = run(engine, a.id(), a.action(), p);
        }
        catch (final Rejected | IllegalArgumentException e)
        {
            LOG.info("audit {} {} rejected: {}", a.id(), a.action(), e.getMessage());
            engine.fencedTx(c ->
            {
                Store.updateAudit(c, a.id(), "REJECTED", e.getMessage(), null, me, nowMs);
                return null;
            });
            return;
        }
        final List<TrackedCommand> cmds = result.commands();
        final String ids = cmds.stream().map(c -> Long.toString(c.correlationId)).collect(Collectors.joining(","));
        final String status = cmds.isEmpty() ? "COMPLETED" : "EXECUTING";
        final String outcome = cmds.isEmpty() ? (result.note() == null ? "no shard command needed" : result.note()) :
            "issued " + cmds.size() + " command(s)" + (result.note() == null ? "" : "; " + result.note());
        engine.fencedTx(c ->
        {
            Store.updateAudit(c, a.id(), status, outcome, ids.isEmpty() ? null : ids, me, nowMs);
            return null;
        });
        LOG.info("audit {} {} by {}: {}", a.id(), a.action(), a.actor(), outcome);
    }

    @Override
    public void persist(final Engine engine, final Connection c) throws SQLException
    {
        final long now = engine.clock().epochMs();
        for (final Long id : new ArrayList<>(dirtyAudits))
        {
            final List<Store.CommandRow> rows = Store.commandsForAudit(c, id);
            int confirmed = 0;
            boolean done = true;
            final StringBuilder failures = new StringBuilder();
            for (final Store.CommandRow r : rows)
            {
                switch (r.status())
                {
                    case "CONFIRMED" -> confirmed++;
                    case "PENDING", "SENT", "SCHEDULED", "SUPERSEDED" -> done = false;
                    default -> failures.append("; shard ").append(r.shardId()).append(' ').append(r.detail()).append(": ")
                        .append(r.status()).append(r.rejectReason() == null ? "" : " " + r.rejectReason());
                }
            }
            if (!done)
            {
                continue;
            }
            dirtyAudits.remove(id);
            final String status = confirmed == rows.size() ? "COMPLETED" : confirmed > 0 ? "PARTIAL" : "FAILED";
            Store.updateAudit(c, id, status, confirmed + "/" + rows.size() + " confirmed" + failures, null, null, now);
        }
    }

    // ------------------------------------------------------------------ actions

    private Result run(final Engine engine, final long auditId, final String action, final Map<String, Object> p)
    {
        final long nowMs = engine.clock().epochMs();
        return switch (action)
        {
            case "HALT_SYMBOL" ->
            {
                final Store.SymbolRow r = symbol(engine, p);
                setOperatorState(engine, r.symbolId(), "HALTED", nowMs);
                yield transition(engine, auditId, r, SymbolState.HALTED, "symbol stays halted until resumed");
            }
            case "RESUME_SYMBOL" ->
            {
                final Store.SymbolRow r = symbol(engine, p);
                final boolean auction = bool(p, "auction", false);
                final boolean force = bool(p, "force", false);
                if (!auction && !force && inVolatilityAuction(engine, r))
                {
                    throw new Rejected("symbol " + r.symbolId() + " is in a volatility auction; the venue ends it " +
                        "(resume with auction=true takes it over, force=true ends it now)");
                }
                setOperatorState(engine, r.symbolId(), auction ? "AUCTION_CALL" : null, nowMs);
                yield transition(engine, auditId, r, auction ? SymbolState.AUCTION_CALL : calendarTarget(engine, r.shardId()), null, force);
            }
            case "AUCTION_START" ->
            {
                final Store.SymbolRow r = symbol(engine, p);
                setOperatorState(engine, r.symbolId(), "AUCTION_CALL", nowMs);
                yield transition(engine, auditId, r, SymbolState.AUCTION_CALL, null);
            }
            case "AUCTION_STOP" ->
            {
                final Store.SymbolRow r = symbol(engine, p);
                final SymbolState target = SymbolState.valueOf(str(p, "target", "CONTINUOUS"));
                if (target != SymbolState.CONTINUOUS && target != SymbolState.CLOSED)
                {
                    throw new Rejected("auction stop target must be CONTINUOUS or CLOSED");
                }
                final ShardState.Symbol obs = engine.shard(r.shardId()).symbols.get(r.symbolId());
                if (obs == null || obs.state != SymbolState.AUCTION_CALL)
                {
                    throw new Rejected("symbol " + r.symbolId() + " is not in AUCTION_CALL (" + (obs == null ? "undefined" : obs.state) + ")");
                }
                final boolean force = bool(p, "force", false);
                if (obs.volatilityAuction && !force)
                {
                    throw new Rejected("symbol " + r.symbolId() + " is in a volatility auction; the venue ends it " +
                        "(force=true ends it now)");
                }
                setOperatorState(engine, r.symbolId(), null, nowMs);
                yield transition(engine, auditId, r, target, null, force);
            }
            case "HALT_MARKET", "RESUME_MARKET" ->
            {
                final boolean halt = action.equals("HALT_MARKET");
                final List<Integer> shards = shards(engine, p);
                engine.fencedTx(c ->
                {
                    for (final int s : shards)
                    {
                        Store.setOperatorState(c, null, s, halt ? "HALTED" : null, nowMs);
                    }
                    return null;
                });
                engine.reloadRefData();
                final boolean force = bool(p, "force", false);
                final List<TrackedCommand> out = new ArrayList<>();
                int spared = 0;
                for (final int s : shards)
                {
                    final SymbolState target = halt ? SymbolState.HALTED : calendarTarget(engine, s);
                    final ShardState sh = engine.shard(s);
                    final boolean anyVolatility = sh.symbols.values().stream().anyMatch(o -> o.volatilityAuction);
                    if (target != SymbolState.CONTINUOUS || force || !anyVolatility)
                    {
                        out.add(Reconciler.state(engine, s, -1, target, 0, "ADMIN", auditId));
                        continue;
                    }
                    // A shard-wide CONTINUOUS would end the volatility auctions too: send what the matcher's shard-wide
                    // loop would apply, per symbol, leaving the volatility auctions to their timers.
                    for (final ShardState.Symbol o : sh.symbols.values())
                    {
                        if (o.volatilityAuction)
                        {
                            spared++;
                        }
                        else if (o.state != target && SymbolStates.allowed(o.state, target))
                        {
                            out.add(Reconciler.state(engine, s, o.symbolId, target, 0, "ADMIN", auditId));
                        }
                    }
                }
                yield new Result(out, spared == 0 ? null : spared + " symbol(s) in a volatility auction left to the venue's timer");
            }
            case "UPDATE_LIMITS" ->
            {
                final Store.FirmRow f = firm(engine, p);
                final long credit = lng(p, "creditLimit", f.creditLimit());
                final long maxPos = lng(p, "maxPositionPerSymbol", f.maxPositionPerSymbol());
                if (credit < 0 || maxPos < 0)
                {
                    throw new Rejected("limits must be >= 0");
                }
                engine.fencedTx(c -> Store.setLimits(c, f.firmId(), credit, maxPos, nowMs));
                yield firmPass(engine, auditId, f.firmId(), null);
            }
            case "KILL_FIRM", "ENABLE_FIRM" ->
            {
                final Store.FirmRow f = firm(engine, p);
                final boolean enable = action.equals("ENABLE_FIRM");
                engine.fencedTx(c -> Store.setFirmEnabled(c, f.firmId(), enable, nowMs));
                final Result r = firmPass(engine, auditId, f.firmId(), enable ? null : "new orders rejected on every shard");
                if (!enable && bool(p, "cancelOrders", false))
                {
                    final List<TrackedCommand> all = new ArrayList<>(r.commands());
                    for (int s = 0; s < engine.shardCount(); s++)
                    {
                        all.add(massCancel(engine, auditId, s, MassCancelScope.FIRM, f.firmId(), 0, 0));
                    }
                    yield new Result(all, r.note());
                }
                yield r;
            }
            case "MASS_CANCEL" ->
            {
                final MassCancelScope scope = MassCancelScope.valueOf(str(p, "scope", null));
                final List<TrackedCommand> out = new ArrayList<>();
                switch (scope)
                {
                    case FIRM ->
                    {
                        final Store.FirmRow f = firm(engine, p);
                        for (final int s : shards(engine, p))
                        {
                            out.add(massCancel(engine, auditId, s, scope, f.firmId(), 0, 0));
                        }
                    }
                    case SESSION ->
                    {
                        final long session = lng(p, "sessionId", -1);
                        final Store.FirmRow f = engine.ref().firms().stream().filter(x -> x.sessionIds().contains(session))
                            .findFirst().orElseThrow(() -> new Rejected("unknown session " + session));
                        for (final int s : shards(engine, p))
                        {
                            out.add(massCancel(engine, auditId, s, scope, f.firmId(), 0, session));
                        }
                    }
                    case SYMBOL ->
                    {
                        final Store.SymbolRow r = symbol(engine, p);
                        out.add(massCancel(engine, auditId, r.shardId(), scope, 0, r.symbolId(), 0));
                    }
                    case FIRM_SYMBOL ->
                    {
                        final Store.FirmRow f = firm(engine, p);
                        final Store.SymbolRow r = symbol(engine, p);
                        out.add(massCancel(engine, auditId, r.shardId(), scope, f.firmId(), r.symbolId(), 0));
                    }
                    default -> throw new Rejected("scope " + scope + " is not available to operators");
                }
                yield new Result(out, null);
            }
            case "DAY_ROLL" ->
            {
                final int date = (int)lng(p, "tradingDate", -1);
                final List<TrackedCommand> out = new ArrayList<>();
                for (final int s : shards(engine, p))
                {
                    out.add(dayRoll(engine, s, date, "ADMIN", auditId));
                }
                yield new Result(out, null);
            }
            case "UPSERT_SYMBOL", "REMOVE_SYMBOL" ->
            {
                final Store.SymbolRow row;
                if (action.equals("REMOVE_SYMBOL"))
                {
                    final Store.SymbolRow r = symbol(engine, p);
                    row = new Store.SymbolRow(r.symbolId(), r.ticker(), r.shardId(), r.tickSize(), r.lotSize(), r.referencePrice(),
                        r.priceBandBps(), r.maxOrderQty(), r.dynamicBandBps(), r.volatilityAuctionNs(), "REMOVED", r.operatorState(), nowMs);
                }
                else
                {
                    row = symbolFromParams(engine, p, nowMs);
                }
                engine.fencedTx(c ->
                {
                    Store.upsertSymbol(c, row, nowMs);
                    return null;
                });
                engine.reloadRefData();
                final ShardState sh = engine.shard(row.shardId());
                if (!sh.ready())
                {
                    yield new Result(List.of(), "shard " + row.shardId() + " not reachable; the reconciler will apply it");
                }
                yield new Result(Reconciler.reconcileShard(engine, sh, auditId), null);
            }
            case "SET_CALENDAR", "DELETE_CALENDAR" ->
            {
                final int date = (int)lng(p, "tradingDate", -1);
                if (action.equals("DELETE_CALENDAR"))
                {
                    engine.fencedTx(c -> Store.deleteCalendar(c, date));
                }
                else
                {
                    final Store.CalendarRow row = new Store.CalendarRow(date, bool(p, "tradingDay", true), str(p, "preOpen", null),
                        str(p, "openAuction", null), str(p, "continuous", null), str(p, "closeAuction", null),
                        str(p, "close", null), str(p, "dayRoll", null), str(p, "note", null));
                    engine.fencedTx(c ->
                    {
                        Store.upsertCalendar(c, row, nowMs);
                        return null;
                    });
                }
                engine.reloadRefData();
                yield new Result(List.of(), "calendar updated");
            }
            case "RESYNC_SHARD" ->
            {
                final int s = (int)lng(p, "shardId", -1);
                if (s < 0 || s >= engine.shardCount())
                {
                    throw new Rejected("unknown shard " + s);
                }
                engine.fencedTx(c ->
                {
                    Store.resetShard(c, s);
                    return null;
                });
                engine.requestRestart();
                yield new Result(List.of(), "shard " + s + " will be re-read from event 0");
            }
            default -> throw new Rejected("unknown action " + action);
        };
    }

    private Result firmPass(final Engine engine, final long auditId, final int firmId, final String note)
    {
        engine.reloadRefData();
        final Store.FirmRow f = engine.ref().firmById().get(firmId);
        return new Result(allocator.runFirm(engine, f, auditId), note);
    }

    private Result transition(final Engine engine, final long auditId, final Store.SymbolRow r, final SymbolState target,
        final String noteIfIdle)
    {
        return transition(engine, auditId, r, target, noteIfIdle, false);
    }

    /**
     * One command towards target. In a volatility auction, AUCTION_CALL takes it over and CONTINUOUS is not sent
     * (the venue's timer ends it) unless force.
     */
    private Result transition(final Engine engine, final long auditId, final Store.SymbolRow r, final SymbolState target,
        final String noteIfIdle, final boolean force)
    {
        final ShardState sh = engine.shard(r.shardId());
        final ShardState.Symbol obs = sh.symbols.get(r.symbolId());
        if (target == null)
        {
            return new Result(List.of(), "override cleared; the calendar decides the state");
        }
        final SymbolState hop = obs == null ? null : SymbolStates.nextHop(obs.state, obs.volatilityAuction && !force, target);
        if (hop == null || (target == SymbolState.HALTED && hop != SymbolState.HALTED))
        {
            return new Result(List.of(), "symbol is " + (obs == null ? "not defined yet" : obs.state +
                (obs.volatilityAuction ? " (volatility auction)" : "")) +
                (noteIfIdle == null ? "; nothing to send" : "; " + noteIfIdle));
        }
        return new Result(List.of(Reconciler.state(engine, r.shardId(), r.symbolId(), hop, 0, "ADMIN", auditId)),
            hop == target ? null : "first step towards " + target);
    }

    private static boolean inVolatilityAuction(final Engine engine, final Store.SymbolRow r)
    {
        final ShardState.Symbol obs = engine.shard(r.shardId()).symbols.get(r.symbolId());
        return obs != null && obs.volatilityAuction;
    }

    private SymbolState calendarTarget(final Engine engine, final int shardId)
    {
        final SymbolState t = scheduler == null ? null : scheduler.target(engine, engine.shard(shardId));
        return t == null ? SymbolState.CONTINUOUS : t;
    }

    private static void setOperatorState(final Engine engine, final int symbolId, final String state, final long nowMs)
    {
        engine.fencedTx(c -> Store.setOperatorState(c, symbolId, null, state, nowMs));
        engine.reloadRefData();
    }

    static TrackedCommand massCancel(final Engine engine, final Long auditId, final int shardId, final MassCancelScope scope,
        final int firmId, final int symbolId, final long sessionId)
    {
        final int gw = engine.config().adminGatewayId;
        return engine.issue(shardId, Kind.MASS_CANCEL, "mass:" + shardId + ":" + scope + ":" + firmId + ":" + symbolId + ":" + sessionId,
            "ADMIN", "mass cancel " + scope + " firm " + firmId + " symbol " + symbolId + " session " + sessionId, 0, auditId,
            corr -> Commands.massCancel(corr, scope, firmId, symbolId, sessionId, gw, 0));
    }

    static TrackedCommand dayRoll(final Engine engine, final int shardId, final int tradingDate, final String origin, final Long auditId)
    {
        final String key = "roll:" + shardId;
        engine.tracker().supersedeActive(key);
        final TrackedCommand c = engine.issue(shardId, Kind.DAY_ROLLOVER, key, origin, "day roll to " + tradingDate + " on shard " + shardId,
            0, auditId, corr -> Commands.dayRollover(corr, tradingDate));
        c.tradingDate = tradingDate;
        return c;
    }

    // ------------------------------------------------------------------ parameter helpers

    private static Store.SymbolRow symbol(final Engine engine, final Map<String, Object> p)
    {
        final long id = lng(p, "symbolId", -1);
        final Store.SymbolRow r = engine.ref().symbolById().get((int)id);
        if (r == null || !r.active())
        {
            throw new Rejected("unknown or removed symbol " + id);
        }
        return r;
    }

    private static Store.FirmRow firm(final Engine engine, final Map<String, Object> p)
    {
        final long id = lng(p, "firmId", -1);
        final Store.FirmRow f = engine.ref().firmById().get((int)id);
        if (f == null)
        {
            throw new Rejected("unknown firm " + id);
        }
        return f;
    }

    private static List<Integer> shards(final Engine engine, final Map<String, Object> p)
    {
        final long s = lng(p, "shardId", -1);
        if (s >= engine.shardCount())
        {
            throw new Rejected("unknown shard " + s);
        }
        final List<Integer> out = new ArrayList<>();
        for (int i = 0; i < engine.shardCount(); i++)
        {
            if (s < 0 || s == i)
            {
                out.add(i);
            }
        }
        return out;
    }

    private static Store.SymbolRow symbolFromParams(final Engine engine, final Map<String, Object> p, final long nowMs)
    {
        final int id = (int)lng(p, "symbolId", -1);
        final Store.SymbolRow existing = engine.ref().symbolById().get(id);
        final int shard = (int)lng(p, "shard", existing == null ? -1 : existing.shardId());
        if (existing != null && existing.shardId() != shard)
        {
            throw new Rejected("a symbol cannot move between shards (" + existing.shardId() + " -> " + shard + ")");
        }
        if (shard < 0 || shard >= engine.shardCount())
        {
            throw new Rejected("unknown shard " + shard);
        }
        final String ticker = str(p, "ticker", existing == null ? null : existing.ticker());
        for (final Store.SymbolRow other : engine.ref().symbols())
        {
            if (other.symbolId() != id && other.ticker().equals(ticker))
            {
                throw new Rejected("ticker " + ticker + " already used by symbol " + other.symbolId());
            }
        }
        return new Store.SymbolRow(id, ticker, shard,
            lng(p, "tickSize", existing == null ? -1 : existing.tickSize()),
            lng(p, "lotSize", existing == null ? -1 : existing.lotSize()),
            lng(p, "referencePrice", existing == null ? -1 : existing.referencePrice()),
            (int)lng(p, "priceBandBps", existing == null ? -1 : existing.priceBandBps()),
            lng(p, "maxOrderQty", existing == null ? -1 : existing.maxOrderQty()),
            (int)lng(p, "dynamicBandBps", existing == null ? 0 : existing.dynamicBandBps()),
            lng(p, "volatilityAuctionNs", existing == null ? 300_000_000_000L : existing.volatilityAuctionNs()),
            "ACTIVE", existing == null ? null : existing.operatorState(), nowMs);
    }

    static long lng(final Map<String, Object> p, final String key, final long def)
    {
        final Object v = p.get(key);
        if (v == null)
        {
            return def;
        }
        if (v instanceof Number n)
        {
            return n.longValue();
        }
        return Long.parseLong(v.toString().trim());
    }

    static String str(final Map<String, Object> p, final String key, final String def)
    {
        final Object v = p.get(key);
        return v == null ? def : v.toString();
    }

    static boolean bool(final Map<String, Object> p, final String key, final boolean def)
    {
        final Object v = p.get(key);
        return v == null ? def : Boolean.parseBoolean(v.toString());
    }
}
