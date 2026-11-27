package io.lmb.ledger;

import io.lmb.api.PostTradeSink;
import io.lmb.api.SinkRunner;
import io.lmb.sbe.SymbolState;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ledger on the real matcher (two shards). Firms 100 and 200 trade, firm 300 never does. AAA (id 1) on shard 0,
 * BBB (id 2) on shard 1.
 *
 * <pre>
 * 20261127  shard 0: 100 buys 10 AAA @ 100.00 from 200, then sells 4 @ 101.00 to 200; AAA closed (close 101.00)
 *           shard 1: 200 buys 20 BBB @ 50.00 from 100; BBB never closed (closed at the roll, close 50.00)
 * 20261130  shard 0: 100 sells 6 AAA @ 102.00 to 200 (both flat in AAA); shard 1: no trade (previous close carried)
 * </pre>
 */
class LedgerSinkTest
{
    private static final int D1 = 20261127;
    private static final int D2 = 20261130;
    private static final int D3 = 20261201;

    @TempDir
    Path tmp;

    private final LedgerTestVenue venue = new LedgerTestVenue(2);
    private final List<SinkRunner> runners = new ArrayList<>();

    @AfterEach
    void tearDown()
    {
        runners.forEach(SinkRunner::close);
        venue.close();
    }

    private LedgerConfig config(final String name, final int commitBatch)
    {
        final LedgerConfig c = new LedgerConfig();
        c.dataDir = tmp.resolve(name);
        c.dbUrl = "jdbc:h2:file:" + tmp.resolve(name).resolve("db") + ";MODE=PostgreSQL";
        c.statementsDir = tmp.resolve(name).resolve("statements");
        c.commitBatch = commitBatch;
        c.commitIntervalMs = 5;
        return c;
    }

    private void setup()
    {
        for (int s = 0; s < 2; s++)
        {
            venue.firm(s, 100);
            venue.firm(s, 200);
        }
        venue.firm(0, 300);
        venue.symbol(0, 1, "AAA", 10_000, SymbolState.PRE_OPEN);
        venue.symbol(1, 2, "BBB", 5_000, SymbolState.PRE_OPEN);
        venue.roll(0, D1);
        venue.roll(1, D1);
        venue.state(0, 1, SymbolState.CONTINUOUS);
        venue.state(1, 2, SymbolState.CONTINUOUS);
    }

    private void day1Shard0()
    {
        venue.trade(0, 1, 100, 200, 10_000, 10);
        venue.trade(0, 1, 200, 100, 10_100, 4);
        venue.state(0, 1, SymbolState.CLOSED);
        venue.roll(0, D2);
    }

    private void day1Shard1()
    {
        venue.trade(1, 2, 200, 100, 5_000, 20);
        venue.roll(1, D2);
    }

    private void day2()
    {
        venue.state(0, 1, SymbolState.CONTINUOUS);
        venue.state(1, 2, SymbolState.CONTINUOUS);
        venue.trade(0, 1, 200, 100, 10_200, 6);
        venue.state(0, 1, SymbolState.CLOSED);
        venue.state(1, 2, SymbolState.CLOSED);
        venue.roll(0, D3);
        venue.roll(1, D3);
    }

    /**
     * Runs the sink until it has applied every event so far, then stops it cleanly (final commit); again if a failure
     * left the committed cursors short of the end.
     */
    private void consume(final LedgerConfig c, final PostTradeSink sink) throws SQLException
    {
        for (int attempt = 0; attempt < 20; attempt++)
        {
            final SinkRunner r = new SinkRunner(sink, venue, 7, 20).start();
            runners.add(r);
            await(() -> r.isUp() && r.lastEventSeq(0) == venue.eventCount(0) && r.lastEventSeq(1) == venue.eventCount(1),
                "the ledger to apply every event");
            r.close();
            runners.remove(r);
            if (query(c, "SELECT event_seq FROM ledger_cursor ORDER BY shard_id")
                .equals(List.of(List.of(venue.eventCount(0)), List.of(venue.eventCount(1)))))
            {
                return;
            }
        }
        throw new AssertionError("the ledger never committed the end of the stream");
    }

    private void consume(final LedgerConfig c) throws SQLException
    {
        consume(c, new LedgerSink(c, new LedgerMetrics()));
    }

    private static void await(final java.util.function.BooleanSupplier condition, final String what)
    {
        final long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.getAsBoolean())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new AssertionError("timed out waiting for " + what);
            }
            try
            {
                Thread.sleep(5);
            }
            catch (final InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    private static List<List<Object>> query(final LedgerConfig c, final String sql) throws SQLException
    {
        try (Connection conn = DriverManager.getConnection(c.dbUrl, c.dbUser, c.dbPassword);
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql))
        {
            final ResultSetMetaData md = rs.getMetaData();
            final List<List<Object>> out = new ArrayList<>();
            while (rs.next())
            {
                final List<Object> row = new ArrayList<>();
                for (int i = 1; i <= md.getColumnCount(); i++)
                {
                    final Object v = rs.getObject(i);
                    row.add(v instanceof Number n && !(v instanceof Double) ? (Object)n.longValue() : v);
                }
                out.add(row);
            }
            return out;
        }
    }

    private static List<Object> statement(final LedgerConfig c, final int date, final int firm) throws SQLException
    {
        final List<List<Object>> rows = query(c, "SELECT cash_balance, market_value, equity, realised_pnl_total, " +
            "unrealised_pnl, pnl_day, trade_count, open_positions FROM ledger_statement WHERE trading_date = " + date +
            " AND firm_id = " + firm);
        assertEquals(1, rows.size(), "statement " + date + " firm " + firm);
        return rows.get(0);
    }

    private static List<Object> longs(final long... v)
    {
        final List<Object> out = new ArrayList<>();
        for (final long x : v)
        {
            out.add(x);
        }
        return out;
    }

    @Test
    void statementsWaitForEveryShardAndMarkToTheOfficialClose() throws Exception
    {
        final LedgerConfig c = config("a", 1000);
        final LedgerMetrics metrics = new LedgerMetrics();
        setup();
        day1Shard0();
        consume(c, new LedgerSink(c, metrics));
        assertEquals(1, metrics.pendingStatementDates.get(), "D1 waits for shard 1");
        // shard 0 has ended the day: its lines are written, marked at AAA's close; no statement yet
        assertEquals(List.of(
                List.of(100L, 1L, 0L, 10L, 4L, 6L, 60_000L, 10_100L, "LAST_TRADE", 60_600L, 600L, 400L, 1_000L),
                List.of(200L, 1L, 0L, 4L, 10L, -6L, -60_000L, 10_100L, "LAST_TRADE", -60_600L, -600L, -400L, -1_000L)),
            query(c, "SELECT firm_id, symbol_id, open_qty, bought_qty, sold_qty, close_qty, cost_basis, close_price, " +
                "close_source, market_value, unrealised_pnl, realised_pnl_day, pnl_day FROM ledger_daily_position " +
                "WHERE trading_date = " + D1 + " ORDER BY firm_id"));
        assertEquals(0, query(c, "SELECT * FROM ledger_statement").size());
        assertFalse(Files.exists(c.statementsDir.resolve("20261127")));

        day1Shard1();
        consume(c, new LedgerSink(c, metrics));
        assertEquals(0, metrics.pendingStatementDates.get());
        // cash_balance, market_value, equity, realised_pnl_total, unrealised_pnl, pnl_day, trade_count, open_positions
        assertEquals(longs(-59_600 + 100_000, 60_600 - 100_000, 1_000, 400, 600, 1_000, 3, 2), statement(c, D1, 100));
        assertEquals(longs(59_600 - 100_000, -60_600 + 100_000, -1_000, -400, -600, -1_000, 3, 2), statement(c, D1, 200));
        assertEquals(longs(0, 0, 0, 0, 0, 0, 0, 0), statement(c, D1, 300));
        assertEquals(List.of(List.of(2L, "BBB", 5_000L, "LAST_TRADE", (long)D1)),
            query(c, "SELECT symbol_id, ticker, mark_price, mark_source, mark_date FROM ledger_symbol WHERE symbol_id = 2"));
        // no statements for the date before the first trading day
        assertEquals(List.of(List.of((long)D1, 3L)), query(c, "SELECT trading_date, firm_count FROM ledger_statement_day"));
    }

    @Test
    void positionsCarryOverAndDailyPnlIsTheChangeInEquity() throws Exception
    {
        final LedgerConfig c = config("b", 1000);
        setup();
        day1Shard0();
        day1Shard1();
        day2();
        consume(c);
        assertEquals(longs(40_400 + 61_200, -100_000, 1_600, 1_600, 0, 600, 1, 1), statement(c, D2, 100));
        assertEquals(longs(-40_400 - 61_200, 100_000, -1_600, -1_600, 0, -600, 1, 1), statement(c, D2, 200));
        assertEquals(List.of(List.of(100L, 2L, -20L, -20L, 5_000L, "PREVIOUS_CLOSE", 0L)),
            query(c, "SELECT firm_id, symbol_id, open_qty, close_qty, close_price, close_source, pnl_day " +
                "FROM ledger_daily_position WHERE trading_date = " + D2 + " AND firm_id = 100 AND symbol_id = 2"));
        // the venue is zero sum: every day's equities and PnLs add up to 0 over the firms
        assertEquals(List.of(List.of((long)D1, 0L, 0L, 0L), List.of((long)D2, 0L, 0L, 0L)),
            query(c, "SELECT trading_date, SUM(equity), SUM(pnl_day), SUM(cash_balance + market_value - equity) " +
                "FROM ledger_statement GROUP BY trading_date ORDER BY trading_date"));
        // and each firm's pnl_day is its change in equity
        assertEquals(0, query(c, "SELECT * FROM ledger_statement a JOIN ledger_statement b ON a.firm_id = b.firm_id " +
            "AND a.trading_date = " + D1 + " AND b.trading_date = " + D2 + " WHERE b.equity - a.equity <> b.pnl_day").size());
        // current positions: flat in AAA, BBB carried
        assertEquals(List.of(List.of(100L, 1L, 0L, 0L, 1_600L, 1_600L), List.of(100L, 2L, -20L, -100_000L, 0L, 100_000L)),
            query(c, "SELECT firm_id, symbol_id, quantity, cost_basis, realised_pnl, cash FROM ledger_position " +
                "WHERE firm_id = 100 ORDER BY symbol_id"));
    }

    @Test
    void statementFilesHoldTheSummaryAndEachFirmsLines() throws Exception
    {
        final LedgerConfig c = config("c", 1000);
        setup();
        day1Shard0();
        day1Shard1();
        consume(c);
        final Path d = c.statementsDir.resolve("20261127");
        final List<String> summary = Files.readAllLines(d.resolve("summary.csv"));
        assertEquals(String.join(",", StatementWriter.SUMMARY_COLUMNS), summary.get(0));
        assertEquals(List.of("20261127,100,40400,40400,-39400,1000,400,400,600,1000,100000,140400,3,2",
            "20261127,200,-40400,-40400,39400,-1000,-400,-400,-600,-1000,140400,100000,3,2",
            "20261127,300,0,0,0,0,0,0,0,0,0,0,0,0"), summary.subList(1, summary.size()));
        final List<String> firm = Files.readAllLines(d.resolve("firm-100.csv"));
        assertTrue(firm.get(0).startsWith("trading_date,firm_id,symbol_id,ticker,open_qty,bought_qty,sold_qty,close_qty," +
            "avg_cost,buy_notional"), firm.get(0));
        assertEquals("20261127,100,1,AAA,0,10,4,6,10000.0000,100000,40400,60000,10100,LAST_TRADE,60600,600,400,400,1000," +
            "-59600,-59600,2", firm.get(1));
        assertEquals("20261127,100,2,BBB,0,0,20,-20,5000.0000,0,100000,-100000,5000,LAST_TRADE,-100000,0,0,0,0,100000," +
            "100000,1", firm.get(2));
        assertEquals(1, Files.readAllLines(d.resolve("firm-300.csv")).size());

        // missing files are rebuilt from the database on the next start
        Files.delete(d.resolve("summary.csv"));
        Files.delete(d.resolve("firm-100.csv"));
        Files.delete(d.resolve("firm-200.csv"));
        Files.delete(d.resolve("firm-300.csv"));
        Files.delete(d);
        consume(c);
        assertEquals(summary, Files.readAllLines(d.resolve("summary.csv")));
    }

    @Test
    void restartsFailuresAndRedeliveryGiveTheSameLedgerAsOneCleanRun() throws Exception
    {
        setup();
        day1Shard0();
        day1Shard1();
        day2();
        final LedgerConfig reference = config("reference", 1000);
        consume(reference);

        final LedgerConfig c = config("interrupted", 3);
        venue.rewind = 5;
        final AtomicLong seen = new AtomicLong();
        final PostTradeSink flaky = new PostTradeSink()
        {
            private final LedgerSink inner = new LedgerSink(c, new LedgerMetrics());

            public String name()
            {
                return inner.name();
            }

            public long[] open(final int shardCount) throws Exception
            {
                return inner.open(shardCount);
            }

            public void onEvent(final int shardId, final long eventSeq, final DirectBuffer buffer, final int offset,
                final int length) throws Exception
            {
                if (seen.incrementAndGet() % 23 == 0)
                {
                    throw new IllegalStateException("injected failure");
                }
                inner.onEvent(shardId, eventSeq, buffer, offset, length);
            }

            public int doWork(final long nowMs) throws Exception
            {
                if (seen.get() % 31 == 0)
                {
                    seen.incrementAndGet();
                    throw new IllegalStateException("injected failure before a commit");
                }
                return inner.doWork(nowMs);
            }

            public void close(final boolean clean)
            {
                inner.close(clean);
            }
        };
        consume(c, flaky);
        assertTrue(seen.get() > venue.eventCount(0) + venue.eventCount(1), "the failures forced re-reads");
        for (final String table : new String[] {"ledger_cursor", "ledger_firm", "ledger_symbol", "ledger_position",
            "ledger_day", "ledger_daily_position", "ledger_statement", "ledger_statement_day"})
        {
            final String sql = "SELECT * FROM " + table + (table.equals("ledger_firm") ? " ORDER BY 1" : " ORDER BY 1, 2, 3");
            assertEquals(query(reference, sql), query(c, sql), table);
        }
        assertEquals(Files.readAllLines(reference.statementsDir.resolve("20261130").resolve("summary.csv")),
            Files.readAllLines(c.statementsDir.resolve("20261130").resolve("summary.csv")));
    }
}
