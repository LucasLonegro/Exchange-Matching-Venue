package io.lmb.ledger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * JDBC access of the ledger: schema, state load, the writes of a commit, statement generation and the queries
 * behind the statement files. Portable SQL only (PostgreSQL and H2 in PostgreSQL mode): rows the sink loaded or
 * wrote are UPDATEd, new ones INSERTed, so no upsert syntax is needed. The caller owns the transaction.
 */
final class LedgerStore
{
    private final Connection c;

    LedgerStore(final Connection connection)
    {
        this.c = connection;
    }

    void createSchema() throws SQLException
    {
        try (Statement st = c.createStatement())
        {
            for (final String sql : statements(load("ledger_schema.sql")))
            {
                st.execute(sql);
            }
        }
    }

    // ---------------------------------------------------------------- load

    void loadCursors(final int shardCount, final long[] cursor, final int[] tradingDate) throws SQLException
    {
        final boolean[] seen = new boolean[shardCount];
        try (PreparedStatement ps = c.prepareStatement("SELECT shard_id, event_seq, trading_date FROM ledger_cursor");
            ResultSet rs = ps.executeQuery())
        {
            while (rs.next())
            {
                final int s = rs.getInt(1);
                if (s < shardCount)
                {
                    cursor[s] = rs.getLong(2);
                    tradingDate[s] = rs.getInt(3);
                    seen[s] = true;
                }
            }
        }
        try (PreparedStatement ps = c.prepareStatement(
            "INSERT INTO ledger_cursor (shard_id, event_seq, trading_date) VALUES (?, 0, 0)"))
        {
            for (int s = 0; s < shardCount; s++)
            {
                if (!seen[s])
                {
                    ps.setInt(1, s);
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
    }

    List<LedgerSink.FirmRow> loadFirms() throws SQLException
    {
        final List<LedgerSink.FirmRow> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT firm_id, first_trading_date FROM ledger_firm");
            ResultSet rs = ps.executeQuery())
        {
            while (rs.next())
            {
                final LedgerSink.FirmRow f = new LedgerSink.FirmRow(rs.getInt(1), rs.getInt(2));
                f.persisted = true;
                out.add(f);
            }
        }
        return out;
    }

    List<LedgerSink.SymbolRow> loadSymbols() throws SQLException
    {
        final List<LedgerSink.SymbolRow> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT symbol_id, shard_id, ticker, removed, close_price, close_source, close_date, mark_price, " +
            "mark_source, mark_date FROM ledger_symbol");
            ResultSet rs = ps.executeQuery())
        {
            while (rs.next())
            {
                final LedgerSink.SymbolRow s = new LedgerSink.SymbolRow(rs.getInt(1), rs.getInt(2));
                s.ticker = rs.getString(3);
                s.removed = rs.getBoolean(4);
                s.closePrice = price(rs, 5);
                s.closeSource = rs.getString(6);
                s.closeDate = integer(rs, 7);
                s.markPrice = price(rs, 8);
                s.markSource = rs.getString(9);
                s.markDate = integer(rs, 10);
                s.persisted = true;
                out.add(s);
            }
        }
        return out;
    }

    List<Position> loadPositions() throws SQLException
    {
        final List<Position> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT firm_id, symbol_id, shard_id, quantity, cost_basis, realised_pnl, cash, day_open_qty, " +
            "day_open_market_value, day_bought_qty, day_sold_qty, day_buy_notional, day_sell_notional, " +
            "day_realised_pnl, day_trade_count, last_event_seq FROM ledger_position ORDER BY firm_id, symbol_id");
            ResultSet rs = ps.executeQuery())
        {
            while (rs.next())
            {
                final Position p = new Position(rs.getInt(1), rs.getInt(2), rs.getInt(3));
                p.quantity = rs.getLong(4);
                p.costBasis = rs.getLong(5);
                p.realisedPnl = rs.getLong(6);
                p.cash = rs.getLong(7);
                p.dayOpenQty = rs.getLong(8);
                p.dayOpenMarketValue = rs.getLong(9);
                p.dayBoughtQty = rs.getLong(10);
                p.daySoldQty = rs.getLong(11);
                p.dayBuyNotional = rs.getLong(12);
                p.daySellNotional = rs.getLong(13);
                p.dayRealisedPnl = rs.getLong(14);
                p.dayTradeCount = rs.getInt(15);
                p.lastEventSeq = rs.getLong(16);
                p.persisted = true;
                out.add(p);
            }
        }
        return out;
    }

    /** Dates some shard has ended whose statements are not written, each with the latest end time. */
    Map<Integer, Long> loadPendingDates() throws SQLException
    {
        final Map<Integer, Long> out = new TreeMap<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT d.trading_date, MAX(d.closed_ts_ns) FROM ledger_day d WHERE NOT EXISTS (SELECT 1 FROM " +
            "ledger_statement_day s WHERE s.trading_date = d.trading_date) GROUP BY d.trading_date");
            ResultSet rs = ps.executeQuery())
        {
            while (rs.next())
            {
                out.put(rs.getInt(1), rs.getLong(2));
            }
        }
        return out;
    }

    List<Integer> statementDates() throws SQLException
    {
        final List<Integer> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT trading_date FROM ledger_statement_day ORDER BY trading_date");
            ResultSet rs = ps.executeQuery())
        {
            while (rs.next())
            {
                out.add(rs.getInt(1));
            }
        }
        return out;
    }

    // ---------------------------------------------------------------- writes of a commit

    void saveFirm(final LedgerSink.FirmRow f) throws SQLException
    {
        final String sql = f.persisted ?
            "UPDATE ledger_firm SET first_trading_date = ? WHERE firm_id = ?" :
            "INSERT INTO ledger_firm (first_trading_date, firm_id) VALUES (?, ?)";
        try (PreparedStatement ps = c.prepareStatement(sql))
        {
            ps.setInt(1, f.firstTradingDate);
            ps.setInt(2, f.firmId);
            ps.executeUpdate();
        }
    }

    void saveSymbol(final LedgerSink.SymbolRow s) throws SQLException
    {
        final String sql = s.persisted ?
            "UPDATE ledger_symbol SET shard_id = ?, ticker = ?, removed = ?, close_price = ?, close_source = ?, " +
            "close_date = ?, mark_price = ?, mark_source = ?, mark_date = ? WHERE symbol_id = ?" :
            "INSERT INTO ledger_symbol (shard_id, ticker, removed, close_price, close_source, close_date, mark_price, " +
            "mark_source, mark_date, symbol_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = c.prepareStatement(sql))
        {
            ps.setInt(1, s.shardId);
            ps.setString(2, s.ticker);
            ps.setBoolean(3, s.removed);
            setPrice(ps, 4, s.closePrice);
            ps.setString(5, s.closeSource);
            setInteger(ps, 6, s.closeDate);
            setPrice(ps, 7, s.markPrice);
            ps.setString(8, s.markSource);
            setInteger(ps, 9, s.markDate);
            ps.setInt(10, s.symbolId);
            ps.executeUpdate();
        }
    }

    private PreparedStatement updatePosition;
    private PreparedStatement insertPosition;

    void savePosition(final Position p) throws SQLException
    {
        final PreparedStatement ps;
        if (p.persisted)
        {
            if (updatePosition == null)
            {
                updatePosition = c.prepareStatement(
                    "UPDATE ledger_position SET shard_id = ?, quantity = ?, cost_basis = ?, realised_pnl = ?, cash = ?, " +
                    "day_open_qty = ?, day_open_market_value = ?, day_bought_qty = ?, day_sold_qty = ?, " +
                    "day_buy_notional = ?, day_sell_notional = ?, day_realised_pnl = ?, day_trade_count = ?, " +
                    "last_event_seq = ? WHERE firm_id = ? AND symbol_id = ?");
            }
            ps = updatePosition;
        }
        else
        {
            if (insertPosition == null)
            {
                insertPosition = c.prepareStatement(
                    "INSERT INTO ledger_position (shard_id, quantity, cost_basis, realised_pnl, cash, day_open_qty, " +
                    "day_open_market_value, day_bought_qty, day_sold_qty, day_buy_notional, day_sell_notional, " +
                    "day_realised_pnl, day_trade_count, last_event_seq, firm_id, symbol_id) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            }
            ps = insertPosition;
        }
        ps.setInt(1, p.shardId);
        ps.setLong(2, p.quantity);
        ps.setLong(3, p.costBasis);
        ps.setLong(4, p.realisedPnl);
        ps.setLong(5, p.cash);
        ps.setLong(6, p.dayOpenQty);
        ps.setLong(7, p.dayOpenMarketValue);
        ps.setLong(8, p.dayBoughtQty);
        ps.setLong(9, p.daySoldQty);
        ps.setLong(10, p.dayBuyNotional);
        ps.setLong(11, p.daySellNotional);
        ps.setLong(12, p.dayRealisedPnl);
        ps.setInt(13, p.dayTradeCount);
        ps.setLong(14, p.lastEventSeq);
        ps.setInt(15, p.firmId);
        ps.setInt(16, p.symbolId);
        ps.executeUpdate();
    }

    void insertDailyPositions(final List<Position.DailyPosition> lines) throws SQLException
    {
        if (lines.isEmpty())
        {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement(
            "INSERT INTO ledger_daily_position (trading_date, firm_id, symbol_id, shard_id, open_qty, bought_qty, " +
            "sold_qty, close_qty, buy_notional, sell_notional, cost_basis, close_price, close_source, market_value, " +
            "unrealised_pnl, realised_pnl_day, realised_pnl_total, pnl_day, cash_flow_day, cash_total, trade_count) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
        {
            for (final Position.DailyPosition d : lines)
            {
                ps.setInt(1, d.tradingDate());
                ps.setInt(2, d.firmId());
                ps.setInt(3, d.symbolId());
                ps.setInt(4, d.shardId());
                ps.setLong(5, d.openQty());
                ps.setLong(6, d.boughtQty());
                ps.setLong(7, d.soldQty());
                ps.setLong(8, d.closeQty());
                ps.setLong(9, d.buyNotional());
                ps.setLong(10, d.sellNotional());
                ps.setLong(11, d.costBasis());
                setPrice(ps, 12, d.closePrice());
                ps.setString(13, d.closeSource());
                ps.setLong(14, d.marketValue());
                ps.setLong(15, d.unrealisedPnl());
                ps.setLong(16, d.realisedPnlDay());
                ps.setLong(17, d.realisedPnlTotal());
                ps.setLong(18, d.pnlDay());
                ps.setLong(19, d.cashFlowDay());
                ps.setLong(20, d.cashTotal());
                ps.setInt(21, d.tradeCount());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    void insertDay(final int shardId, final int tradingDate, final long eventSeq, final long tsNs) throws SQLException
    {
        try (PreparedStatement ps = c.prepareStatement(
            "INSERT INTO ledger_day (shard_id, trading_date, closed_event_seq, closed_ts_ns) VALUES (?, ?, ?, ?)"))
        {
            ps.setInt(1, shardId);
            ps.setInt(2, tradingDate);
            ps.setLong(3, eventSeq);
            ps.setLong(4, tsNs);
            ps.executeUpdate();
        }
    }

    private PreparedStatement updateCursor;

    void saveCursor(final int shardId, final long eventSeq, final int tradingDate) throws SQLException
    {
        if (updateCursor == null)
        {
            updateCursor = c.prepareStatement(
                "UPDATE ledger_cursor SET event_seq = ?, trading_date = ? WHERE shard_id = ?");
        }
        updateCursor.setLong(1, eventSeq);
        updateCursor.setInt(2, tradingDate);
        updateCursor.setInt(3, shardId);
        if (updateCursor.executeUpdate() != 1)
        {
            throw new SQLException("ledger_cursor row of shard " + shardId + " is missing");
        }
    }

    /**
     * Writes the statement of every firm in {@code firmIds} for the trading date from its {@code ledger_daily_position}
     * lines (a firm without lines gets a statement of zeros) and marks the date done. Returns the number of statements.
     */
    int writeStatements(final int tradingDate, final List<Integer> firmIds, final long closedTsNs) throws SQLException
    {
        final Map<Integer, long[]> sums = new TreeMap<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT firm_id, SUM(cash_flow_day), SUM(cash_total), SUM(market_value), SUM(realised_pnl_day), " +
            "SUM(realised_pnl_total), SUM(unrealised_pnl), SUM(pnl_day), SUM(buy_notional), SUM(sell_notional), " +
            "SUM(trade_count), SUM(CASE WHEN close_qty <> 0 THEN 1 ELSE 0 END) FROM ledger_daily_position " +
            "WHERE trading_date = ? GROUP BY firm_id"))
        {
            ps.setInt(1, tradingDate);
            try (ResultSet rs = ps.executeQuery())
            {
                while (rs.next())
                {
                    final long[] v = new long[11];
                    for (int i = 0; i < v.length; i++)
                    {
                        v[i] = rs.getLong(i + 2);
                    }
                    sums.put(rs.getInt(1), v);
                }
            }
        }
        int n = 0;
        try (PreparedStatement ps = c.prepareStatement(
            "INSERT INTO ledger_statement (trading_date, firm_id, cash_flow_day, cash_balance, market_value, equity, " +
            "realised_pnl_day, realised_pnl_total, unrealised_pnl, pnl_day, buy_notional, sell_notional, trade_count, " +
            "open_positions) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
        {
            final java.util.TreeSet<Integer> all = new java.util.TreeSet<>(firmIds);
            all.addAll(sums.keySet());
            for (final int firmId : all)
            {
                final long[] v = sums.getOrDefault(firmId, new long[11]);
                ps.setInt(1, tradingDate);
                ps.setInt(2, firmId);
                ps.setLong(3, v[0]);
                ps.setLong(4, v[1]);
                ps.setLong(5, v[2]);
                ps.setLong(6, Math.addExact(v[1], v[2]));
                ps.setLong(7, v[3]);
                ps.setLong(8, v[4]);
                ps.setLong(9, v[5]);
                ps.setLong(10, v[6]);
                ps.setLong(11, v[7]);
                ps.setLong(12, v[8]);
                ps.setInt(13, Math.toIntExact(v[9]));
                ps.setInt(14, Math.toIntExact(v[10]));
                ps.addBatch();
                n++;
            }
            ps.executeBatch();
        }
        try (PreparedStatement ps = c.prepareStatement(
            "INSERT INTO ledger_statement_day (trading_date, firm_count, closed_ts_ns) VALUES (?, ?, ?)"))
        {
            ps.setInt(1, tradingDate);
            ps.setInt(2, n);
            ps.setLong(3, closedTsNs);
            ps.executeUpdate();
        }
        return n;
    }

    // ---------------------------------------------------------------- statement file queries

    /** Rows of {@code ledger_statement} for the date, by firm. Column order: {@link StatementWriter#SUMMARY_COLUMNS}. */
    List<Object[]> statementRows(final int tradingDate) throws SQLException
    {
        return rows("SELECT trading_date, firm_id, cash_flow_day, cash_balance, market_value, equity, realised_pnl_day, " +
            "realised_pnl_total, unrealised_pnl, pnl_day, buy_notional, sell_notional, trade_count, open_positions " +
            "FROM ledger_statement WHERE trading_date = ? ORDER BY firm_id", tradingDate, 14);
    }

    /** Lines of {@code ledger_daily_position} for the date with the ticker. Column order: {@link StatementWriter#POSITION_COLUMNS}. */
    List<Object[]> positionRows(final int tradingDate) throws SQLException
    {
        return rows("SELECT p.trading_date, p.firm_id, p.symbol_id, s.ticker, p.open_qty, p.bought_qty, p.sold_qty, " +
            "p.close_qty, p.buy_notional, p.sell_notional, p.cost_basis, p.close_price, p.close_source, p.market_value, " +
            "p.unrealised_pnl, p.realised_pnl_day, p.realised_pnl_total, p.pnl_day, p.cash_flow_day, p.cash_total, " +
            "p.trade_count FROM ledger_daily_position p LEFT JOIN ledger_symbol s ON s.symbol_id = p.symbol_id " +
            "WHERE p.trading_date = ? ORDER BY p.firm_id, p.symbol_id", tradingDate, 21);
    }

    private List<Object[]> rows(final String sql, final int tradingDate, final int columns) throws SQLException
    {
        final List<Object[]> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql))
        {
            ps.setInt(1, tradingDate);
            try (ResultSet rs = ps.executeQuery())
            {
                while (rs.next())
                {
                    final Object[] row = new Object[columns];
                    for (int i = 0; i < columns; i++)
                    {
                        row[i] = rs.getObject(i + 1);
                    }
                    out.add(row);
                }
            }
        }
        return out;
    }

    // ---------------------------------------------------------------- helpers

    private static long price(final ResultSet rs, final int column) throws SQLException
    {
        final long v = rs.getLong(column);
        return rs.wasNull() ? Ledger.NULL_PRICE : v;
    }

    private static Integer integer(final ResultSet rs, final int column) throws SQLException
    {
        final int v = rs.getInt(column);
        return rs.wasNull() ? null : v;
    }

    private static void setPrice(final PreparedStatement ps, final int column, final long price) throws SQLException
    {
        if (price == Ledger.NULL_PRICE)
        {
            ps.setNull(column, Types.BIGINT);
        }
        else
        {
            ps.setLong(column, price);
        }
    }

    private static void setInteger(final PreparedStatement ps, final int column, final Integer v) throws SQLException
    {
        if (v == null)
        {
            ps.setNull(column, Types.INTEGER);
        }
        else
        {
            ps.setInt(column, v);
        }
    }

    static String[] statements(final String script)
    {
        final StringBuilder sb = new StringBuilder();
        for (final String line : script.split("\n"))
        {
            final int comment = line.indexOf("--");
            sb.append(comment >= 0 ? line.substring(0, comment) : line).append('\n');
        }
        return java.util.Arrays.stream(sb.toString().split(";"))
            .map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
    }

    private static String load(final String script)
    {
        try (InputStream in = LedgerStore.class.getResourceAsStream(script))
        {
            if (in == null)
            {
                throw new IllegalStateException("missing schema script " + script);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (final IOException e)
        {
            throw new IllegalStateException("cannot read schema script " + script, e);
        }
    }
}
