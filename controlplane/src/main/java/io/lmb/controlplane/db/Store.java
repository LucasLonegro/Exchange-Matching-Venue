package io.lmb.controlplane.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.lmb.controlplane.db.Database.NULL_BIGINT;
import static io.lmb.controlplane.db.Database.NULL_VARCHAR;
import static io.lmb.controlplane.db.Database.query;
import static io.lmb.controlplane.db.Database.queryOne;
import static io.lmb.controlplane.db.Database.update;

/** All SQL of the control plane. Methods take a Connection so callers choose the transaction. */
public final class Store
{
    /**
     * Desired symbol parameters. referencePrice is the listing price: sent with the first SymbolDefinition, after
     * which the venue owns the reference (see {@link ObservedSymbolRow#venueReference}).
     */
    public record SymbolRow(
        int symbolId, String ticker, int shardId, long tickSize, long lotSize, long referencePrice,
        int priceBandBps, long maxOrderQty, int dynamicBandBps, long volatilityAuctionNs, String status,
        String operatorState, long updatedMs)
    {
        public boolean active()
        {
            return "ACTIVE".equals(status);
        }
    }

    public record FirmRow(
        int firmId, String name, boolean enabled, long creditLimit, long maxPositionPerSymbol, long updatedMs)
    {
    }

    public record CalendarRow(
        int tradingDate, boolean tradingDay, String preOpen, String openAuction, String continuous,
        String closeAuction, String closeTime, String dayRoll, String note)
    {
    }

    public record AuditRow(
        long id, long createdMs, String actor, String instanceId, String action, String params, String status,
        String outcome, String correlationIds, String executedBy, long updatedMs)
    {
        public boolean terminal()
        {
            return !"PENDING".equals(status) && !"EXECUTING".equals(status);
        }
    }

    public record CommandRow(
        long correlationId, int shardId, String kind, String key, String origin, String detail, String payloadHex,
        Long budget, long effectiveNs, String status, int attempts, String rejectReason, Long auditId,
        long leaderEpoch, long createdMs, long updatedMs)
    {
    }

    public record CursorRow(
        int shardId, long eventSeq, int tradingDate, long lastEventTsNs, boolean connected, boolean caughtUp, long updatedMs)
    {
    }

    /**
     * What a shard confirmed. Prices use the SBE null (Long.MIN_VALUE) for "none"; closeSource is null until a
     * ClosingPrice was seen.
     */
    public record ObservedSymbolRow(
        int shardId, int symbolId, String ticker, long tickSize, long lotSize, long referencePrice, int priceBandBps,
        long maxOrderQty, String state, long lastCorrelationId, long eventSeq,
        int dynamicBandBps, long volatilityAuctionNs, boolean volatilityAuction, long volatilityAuctionSinceNs,
        boolean venueReference, long bandLow, long bandHigh,
        long closePrice, long previousClose, String closeSource, int closeTradingDate)
    {
    }

    public record ObservedFirmRow(
        int shardId, int firmId, long creditBudget, long creditUsed, long maxPositionPerSymbol, boolean enabled,
        long lastCorrelationId, long eventSeq, long observedTsNs)
    {
    }

    public record BudgetRow(int firmId, int shardId, long targetBudget, long updatedMs)
    {
    }

    private Store()
    {
    }

    // ------------------------------------------------------------------ reference data

    public static List<SymbolRow> symbols(final Connection c) throws SQLException
    {
        return query(c, "SELECT * FROM symbol ORDER BY symbol_id", Store::symbol);
    }

    public static SymbolRow symbol(final Connection c, final int symbolId) throws SQLException
    {
        return queryOne(c, "SELECT * FROM symbol WHERE symbol_id = ?", Store::symbol, symbolId);
    }

    private static SymbolRow symbol(final ResultSet rs) throws SQLException
    {
        return new SymbolRow(rs.getInt("symbol_id"), rs.getString("ticker"), rs.getInt("shard_id"), rs.getLong("tick_size"),
            rs.getLong("lot_size"), rs.getLong("reference_price"), rs.getInt("price_band_bps"), rs.getLong("max_order_qty"),
            rs.getInt("dynamic_band_bps"), rs.getLong("volatility_auction_ns"),
            rs.getString("status"), rs.getString("operator_state"), rs.getLong("updated_ms"));
    }

    public static void upsertSymbol(final Connection c, final SymbolRow s, final long nowMs) throws SQLException
    {
        final int n = update(c, "UPDATE symbol SET ticker = ?, shard_id = ?, tick_size = ?, lot_size = ?, reference_price = ?, " +
                "price_band_bps = ?, max_order_qty = ?, dynamic_band_bps = ?, volatility_auction_ns = ?, status = ?, updated_ms = ? " +
                "WHERE symbol_id = ?",
            s.ticker(), s.shardId(), s.tickSize(), s.lotSize(), s.referencePrice(), s.priceBandBps(), s.maxOrderQty(),
            s.dynamicBandBps(), s.volatilityAuctionNs(), s.status(), nowMs, s.symbolId());
        if (n == 0)
        {
            update(c, "INSERT INTO symbol (symbol_id, ticker, shard_id, tick_size, lot_size, reference_price, price_band_bps, " +
                    "max_order_qty, dynamic_band_bps, volatility_auction_ns, status, operator_state, updated_ms) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                s.symbolId(), s.ticker(), s.shardId(), s.tickSize(), s.lotSize(), s.referencePrice(), s.priceBandBps(),
                s.maxOrderQty(), s.dynamicBandBps(), s.volatilityAuctionNs(), s.status(),
                s.operatorState() == null ? NULL_VARCHAR : s.operatorState(), nowMs);
        }
    }

    public static int setOperatorState(final Connection c, final Integer symbolIdOrNullForShard, final Integer shardId,
        final String operatorState, final long nowMs) throws SQLException
    {
        final Object v = operatorState == null ? NULL_VARCHAR : operatorState;
        if (symbolIdOrNullForShard != null)
        {
            return update(c, "UPDATE symbol SET operator_state = ?, updated_ms = ? WHERE symbol_id = ?", v, nowMs, symbolIdOrNullForShard);
        }
        if (shardId != null)
        {
            return update(c, "UPDATE symbol SET operator_state = ?, updated_ms = ? WHERE shard_id = ? AND status = 'ACTIVE'",
                v, nowMs, shardId);
        }
        return update(c, "UPDATE symbol SET operator_state = ?, updated_ms = ? WHERE status = 'ACTIVE'", v, nowMs);
    }

    public static int clearOperatorState(final Connection c, final String onlyState, final long nowMs) throws SQLException
    {
        return update(c, "UPDATE symbol SET operator_state = NULL, updated_ms = ? WHERE operator_state = ?", nowMs, onlyState);
    }

    public static List<FirmRow> firms(final Connection c) throws SQLException
    {
        return query(c, "SELECT f.firm_id, f.name, f.enabled, f.updated_ms, l.credit_limit, l.max_position_per_symbol, " +
                "l.updated_ms AS limit_ms FROM firm f LEFT JOIN firm_limit l ON l.firm_id = f.firm_id ORDER BY f.firm_id",
            rs -> new FirmRow(rs.getInt("firm_id"), rs.getString("name"), rs.getBoolean("enabled"), rs.getLong("credit_limit"),
                rs.getLong("max_position_per_symbol"), Math.max(rs.getLong("updated_ms"), rs.getLong("limit_ms"))));
    }

    public static void insertFirm(final Connection c, final FirmRow f, final long nowMs) throws SQLException
    {
        update(c, "INSERT INTO firm (firm_id, name, enabled, updated_ms) VALUES (?, ?, ?, ?)",
            f.firmId(), f.name(), f.enabled(), nowMs);
        update(c, "INSERT INTO firm_limit (firm_id, credit_limit, max_position_per_symbol, updated_ms) VALUES (?, ?, ?, ?)",
            f.firmId(), f.creditLimit(), f.maxPositionPerSymbol(), nowMs);
    }

    public static int setFirmEnabled(final Connection c, final int firmId, final boolean enabled, final long nowMs) throws SQLException
    {
        return update(c, "UPDATE firm SET enabled = ?, updated_ms = ? WHERE firm_id = ?", enabled, nowMs, firmId);
    }

    public static int setLimits(final Connection c, final int firmId, final long creditLimit, final long maxPos, final long nowMs)
        throws SQLException
    {
        return update(c, "UPDATE firm_limit SET credit_limit = ?, max_position_per_symbol = ?, updated_ms = ? WHERE firm_id = ?",
            creditLimit, maxPos, nowMs, firmId);
    }

    public static List<CalendarRow> calendar(final Connection c) throws SQLException
    {
        return query(c, "SELECT * FROM trading_calendar ORDER BY trading_date", rs -> new CalendarRow(rs.getInt("trading_date"),
            rs.getBoolean("is_trading_day"), rs.getString("pre_open"), rs.getString("open_auction"), rs.getString("continuous"),
            rs.getString("close_auction"), rs.getString("close_time"), rs.getString("day_roll"), rs.getString("note")));
    }

    public static void upsertCalendar(final Connection c, final CalendarRow r, final long nowMs) throws SQLException
    {
        update(c, "DELETE FROM trading_calendar WHERE trading_date = ?", r.tradingDate());
        update(c, "INSERT INTO trading_calendar (trading_date, is_trading_day, pre_open, open_auction, continuous, close_auction, " +
                "close_time, day_roll, note, updated_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            r.tradingDate(), r.tradingDay(), nv(r.preOpen()), nv(r.openAuction()), nv(r.continuous()), nv(r.closeAuction()),
            nv(r.closeTime()), nv(r.dayRoll()), nv(r.note()), nowMs);
    }

    public static int deleteCalendar(final Connection c, final int date) throws SQLException
    {
        return update(c, "DELETE FROM trading_calendar WHERE trading_date = ?", date);
    }

    // ------------------------------------------------------------------ audit log

    public static long insertAudit(final Connection c, final String actor, final String instanceId, final String action,
        final String params, final long nowMs) throws SQLException
    {
        return Database.insertReturningId(c, "INSERT INTO audit_log (created_ms, actor, instance_id, action, params, status, " +
            "updated_ms) VALUES (?, ?, ?, ?, ?, 'PENDING', ?)", nowMs, actor, instanceId, action, params, nowMs);
    }

    public static List<AuditRow> auditsByStatus(final Connection c, final String status, final int limit) throws SQLException
    {
        return query(c, "SELECT * FROM audit_log WHERE status = ? ORDER BY id LIMIT ?", Store::audit, status, limit);
    }

    public static List<AuditRow> audits(final Connection c, final long beforeId, final int limit) throws SQLException
    {
        return query(c, "SELECT * FROM audit_log WHERE id < ? ORDER BY id DESC LIMIT ?", Store::audit, beforeId, limit);
    }

    public static AuditRow audit(final Connection c, final long id) throws SQLException
    {
        return queryOne(c, "SELECT * FROM audit_log WHERE id = ?", Store::audit, id);
    }

    private static AuditRow audit(final ResultSet rs) throws SQLException
    {
        return new AuditRow(rs.getLong("id"), rs.getLong("created_ms"), rs.getString("actor"), rs.getString("instance_id"),
            rs.getString("action"), rs.getString("params"), rs.getString("status"), rs.getString("outcome"),
            rs.getString("correlation_ids"), rs.getString("executed_by"), rs.getLong("updated_ms"));
    }

    public static void updateAudit(final Connection c, final long id, final String status, final String outcome,
        final String correlationIds, final String executedBy, final long nowMs) throws SQLException
    {
        update(c, "UPDATE audit_log SET status = ?, outcome = ?, correlation_ids = COALESCE(?, correlation_ids), " +
                "executed_by = COALESCE(?, executed_by), updated_ms = ? WHERE id = ?",
            status, nv(truncate(outcome, 2000)), nv(truncate(correlationIds, 2000)), nv(executedBy), nowMs, id);
    }

    // ------------------------------------------------------------------ commands

    public static long nextCorrelationBlock(final Database db, final Connection c) throws SQLException
    {
        return Database.queryLong(c, db.nextvalSql("correlation_block_seq"));
    }

    public static void insertCommand(final Connection c, final CommandRow r) throws SQLException
    {
        update(c, "INSERT INTO command_log (correlation_id, shard_id, kind, cmd_key, origin, detail, payload_hex, budget, " +
                "effective_ns, status, attempts, reject_reason, audit_id, leader_epoch, created_ms, updated_ms) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            r.correlationId(), r.shardId(), r.kind(), r.key(), r.origin(), truncate(r.detail(), 1000), r.payloadHex(),
            r.budget() == null ? NULL_BIGINT : r.budget(), r.effectiveNs(), r.status(), r.attempts(), nv(r.rejectReason()),
            r.auditId() == null ? NULL_BIGINT : r.auditId(), r.leaderEpoch(), r.createdMs(), r.updatedMs());
    }

    public static void updateCommand(final Connection c, final long correlationId, final String status, final int attempts,
        final String rejectReason, final long nowMs) throws SQLException
    {
        update(c, "UPDATE command_log SET status = ?, attempts = ?, reject_reason = ?, updated_ms = ? WHERE correlation_id = ?",
            status, attempts, nv(rejectReason), nowMs, correlationId);
    }

    public static List<CommandRow> unresolvedCommands(final Connection c) throws SQLException
    {
        return query(c, "SELECT * FROM command_log WHERE status IN ('PENDING', 'SENT', 'SCHEDULED', 'SUPERSEDED') " +
            "ORDER BY correlation_id", Store::command);
    }

    public static List<CommandRow> commands(final Connection c, final String status, final Integer shardId, final int limit)
        throws SQLException
    {
        final StringBuilder sql = new StringBuilder("SELECT * FROM command_log WHERE 1 = 1");
        final List<Object> params = new ArrayList<>();
        if (status != null)
        {
            sql.append(" AND status = ?");
            params.add(status);
        }
        if (shardId != null)
        {
            sql.append(" AND shard_id = ?");
            params.add(shardId);
        }
        sql.append(" ORDER BY correlation_id DESC LIMIT ?");
        params.add(limit);
        return query(c, sql.toString(), Store::command, params.toArray());
    }

    public static CommandRow command(final Connection c, final long correlationId) throws SQLException
    {
        return queryOne(c, "SELECT * FROM command_log WHERE correlation_id = ?", Store::command, correlationId);
    }

    public static List<CommandRow> commandsForAudit(final Connection c, final long auditId) throws SQLException
    {
        return query(c, "SELECT * FROM command_log WHERE audit_id = ? ORDER BY correlation_id", Store::command, auditId);
    }

    private static CommandRow command(final ResultSet rs) throws SQLException
    {
        final long budget = rs.getLong("budget");
        final Long b = rs.wasNull() ? null : budget;
        final long audit = rs.getLong("audit_id");
        final Long a = rs.wasNull() ? null : audit;
        return new CommandRow(rs.getLong("correlation_id"), rs.getInt("shard_id"), rs.getString("kind"), rs.getString("cmd_key"),
            rs.getString("origin"), rs.getString("detail"), rs.getString("payload_hex"), b, rs.getLong("effective_ns"),
            rs.getString("status"), rs.getInt("attempts"), rs.getString("reject_reason"), a, rs.getLong("leader_epoch"),
            rs.getLong("created_ms"), rs.getLong("updated_ms"));
    }

    /** Deletes resolved commands: refreshes after refreshRetention, everything else after retention. */
    public static int pruneCommands(final Connection c, final long nowMs, final long retentionMs, final long refreshRetentionMs)
        throws SQLException
    {
        final String terminal = "status IN ('CONFIRMED', 'REJECTED', 'LOST', 'EXPIRED', 'FAILED')";
        return update(c, "DELETE FROM command_log WHERE origin = 'REFRESH' AND " + terminal + " AND updated_ms < ?",
            nowMs - refreshRetentionMs) +
            update(c, "DELETE FROM command_log WHERE " + terminal + " AND updated_ms < ?", nowMs - retentionMs);
    }

    // ------------------------------------------------------------------ observed shard state

    public static CursorRow cursor(final Connection c, final int shardId) throws SQLException
    {
        return queryOne(c, "SELECT * FROM shard_cursor WHERE shard_id = ?", Store::cursor, shardId);
    }

    public static List<CursorRow> cursors(final Connection c) throws SQLException
    {
        return query(c, "SELECT * FROM shard_cursor ORDER BY shard_id", Store::cursor);
    }

    private static CursorRow cursor(final ResultSet rs) throws SQLException
    {
        return new CursorRow(rs.getInt("shard_id"), rs.getLong("event_seq"), rs.getInt("trading_date"),
            rs.getLong("last_event_ts_ns"), rs.getBoolean("connected"), rs.getBoolean("caught_up"), rs.getLong("updated_ms"));
    }

    public static void saveCursor(final Connection c, final CursorRow r) throws SQLException
    {
        if (update(c, "UPDATE shard_cursor SET event_seq = ?, trading_date = ?, last_event_ts_ns = ?, connected = ?, " +
                "caught_up = ?, updated_ms = ? WHERE shard_id = ?", r.eventSeq(), r.tradingDate(), r.lastEventTsNs(),
            r.connected(), r.caughtUp(), r.updatedMs(), r.shardId()) == 0)
        {
            update(c, "INSERT INTO shard_cursor (shard_id, event_seq, trading_date, last_event_ts_ns, connected, caught_up, " +
                    "updated_ms) VALUES (?, ?, ?, ?, ?, ?, ?)", r.shardId(), r.eventSeq(), r.tradingDate(), r.lastEventTsNs(),
                r.connected(), r.caughtUp(), r.updatedMs());
        }
    }

    public static List<ObservedSymbolRow> observedSymbols(final Connection c) throws SQLException
    {
        return query(c, "SELECT * FROM shard_symbol_state ORDER BY shard_id, symbol_id", rs -> new ObservedSymbolRow(
            rs.getInt("shard_id"), rs.getInt("symbol_id"), rs.getString("ticker"), rs.getLong("tick_size"), rs.getLong("lot_size"),
            rs.getLong("reference_price"), rs.getInt("price_band_bps"), rs.getLong("max_order_qty"), rs.getString("state"),
            rs.getLong("last_correlation_id"), rs.getLong("event_seq"),
            rs.getInt("dynamic_band_bps"), rs.getLong("volatility_auction_ns"), rs.getBoolean("volatility_auction"),
            rs.getLong("volatility_auction_since_ns"), rs.getBoolean("venue_reference"),
            price(rs, "band_low"), price(rs, "band_high"), price(rs, "close_price"), price(rs, "previous_close"),
            rs.getString("close_source"), rs.getInt("close_trading_date")));
    }

    private static final String OBSERVED_SYMBOL_COLUMNS = "ticker, tick_size, lot_size, reference_price, price_band_bps, " +
        "max_order_qty, state, last_correlation_id, event_seq, dynamic_band_bps, volatility_auction_ns, volatility_auction, " +
        "volatility_auction_since_ns, venue_reference, band_low, band_high, close_price, previous_close, close_source, " +
        "close_trading_date";

    public static void saveObservedSymbol(final Connection c, final ObservedSymbolRow r) throws SQLException
    {
        final Object[] values = {r.ticker(), r.tickSize(), r.lotSize(), r.referencePrice(), r.priceBandBps(), r.maxOrderQty(),
            r.state(), r.lastCorrelationId(), r.eventSeq(), r.dynamicBandBps(), r.volatilityAuctionNs(), r.volatilityAuction(),
            r.volatilityAuctionSinceNs(), r.venueReference(), price(r.bandLow()), price(r.bandHigh()), price(r.closePrice()),
            price(r.previousClose()), r.closeSource() == null ? NULL_VARCHAR : r.closeSource(), r.closeTradingDate()};
        final List<Object> params = new ArrayList<>(List.of(values));
        params.add(r.shardId());
        params.add(r.symbolId());
        if (update(c, "UPDATE shard_symbol_state SET " + OBSERVED_SYMBOL_COLUMNS.replace(",", " = ?,") + " = ? " +
            "WHERE shard_id = ? AND symbol_id = ?", params.toArray()) == 0)
        {
            final List<Object> insert = new ArrayList<>(List.of(r.shardId(), r.symbolId()));
            insert.addAll(List.of(values));
            update(c, "INSERT INTO shard_symbol_state (shard_id, symbol_id, " + OBSERVED_SYMBOL_COLUMNS + ") VALUES (" +
                "?, ".repeat(insert.size() - 1) + "?)", insert.toArray());
        }
    }

    /** SBE null price (Long.MIN_VALUE) as SQL NULL. */
    private static Object price(final long p)
    {
        return p == Long.MIN_VALUE ? NULL_BIGINT : (Object)p;
    }

    private static long price(final ResultSet rs, final String column) throws SQLException
    {
        final long v = rs.getLong(column);
        return rs.wasNull() ? Long.MIN_VALUE : v;
    }

    public static List<ObservedFirmRow> observedFirms(final Connection c) throws SQLException
    {
        return query(c, "SELECT * FROM shard_firm_state ORDER BY firm_id, shard_id", rs -> new ObservedFirmRow(
            rs.getInt("shard_id"), rs.getInt("firm_id"), rs.getLong("credit_budget"), rs.getLong("credit_used"),
            rs.getLong("max_position_per_symbol"), rs.getBoolean("enabled"), rs.getLong("last_correlation_id"),
            rs.getLong("event_seq"), rs.getLong("observed_ts_ns")));
    }

    public static void saveObservedFirm(final Connection c, final ObservedFirmRow r) throws SQLException
    {
        if (update(c, "UPDATE shard_firm_state SET credit_budget = ?, credit_used = ?, max_position_per_symbol = ?, enabled = ?, " +
                "last_correlation_id = ?, event_seq = ?, observed_ts_ns = ? WHERE shard_id = ? AND firm_id = ?",
            r.creditBudget(), r.creditUsed(), r.maxPositionPerSymbol(), r.enabled(), r.lastCorrelationId(), r.eventSeq(),
            r.observedTsNs(), r.shardId(), r.firmId()) == 0)
        {
            update(c, "INSERT INTO shard_firm_state (shard_id, firm_id, credit_budget, credit_used, max_position_per_symbol, " +
                    "enabled, last_correlation_id, event_seq, observed_ts_ns) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                r.shardId(), r.firmId(), r.creditBudget(), r.creditUsed(), r.maxPositionPerSymbol(), r.enabled(),
                r.lastCorrelationId(), r.eventSeq(), r.observedTsNs());
        }
    }

    /** Forget everything observed on one shard (operator resync after a shard was rebuilt from scratch). */
    public static void resetShard(final Connection c, final int shardId) throws SQLException
    {
        update(c, "DELETE FROM shard_symbol_state WHERE shard_id = ?", shardId);
        update(c, "DELETE FROM shard_firm_state WHERE shard_id = ?", shardId);
        update(c, "DELETE FROM shard_cursor WHERE shard_id = ?", shardId);
    }

    public static List<BudgetRow> budgets(final Connection c) throws SQLException
    {
        return query(c, "SELECT * FROM risk_budget ORDER BY firm_id, shard_id",
            rs -> new BudgetRow(rs.getInt("firm_id"), rs.getInt("shard_id"), rs.getLong("target_budget"), rs.getLong("updated_ms")));
    }

    public static void saveBudget(final Connection c, final BudgetRow r) throws SQLException
    {
        if (update(c, "UPDATE risk_budget SET target_budget = ?, updated_ms = ? WHERE firm_id = ? AND shard_id = ?",
            r.targetBudget(), r.updatedMs(), r.firmId(), r.shardId()) == 0)
        {
            update(c, "INSERT INTO risk_budget (firm_id, shard_id, target_budget, updated_ms) VALUES (?, ?, ?, ?)",
                r.firmId(), r.shardId(), r.targetBudget(), r.updatedMs());
        }
    }

    // ------------------------------------------------------------------ scheduler bookkeeping

    public static List<String> scheduleSentKeys(final Connection c, final int fromDate) throws SQLException
    {
        return query(c, "SELECT trading_date, phase, shard_id FROM schedule_sent WHERE trading_date >= ?",
            rs -> rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3), fromDate);
    }

    public static void insertScheduleSent(final Connection c, final int date, final String phase, final int shardId,
        final long correlationId, final long effectiveNs, final long nowMs) throws SQLException
    {
        update(c, "INSERT INTO schedule_sent (trading_date, phase, shard_id, correlation_id, effective_ns, sent_ms) " +
            "VALUES (?, ?, ?, ?, ?, ?)", date, phase, shardId, correlationId, effectiveNs, nowMs);
    }

    private static Object nv(final String s)
    {
        return s == null ? NULL_VARCHAR : s;
    }

    private static String truncate(final String s, final int max)
    {
        return s == null || s.length() <= max ? s : s.substring(0, max);
    }
}
