package io.lmb.posttrade;

import io.lmb.posttrade.store.SchemaMigrator;
import io.lmb.posttrade.store.TradeStore;
import io.lmb.sbe.TradeDecoder;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TradeStoreTest
{
    private static final AtomicInteger DB_IDS = new AtomicInteger();

    private final String url = "jdbc:h2:mem:pt" + DB_IDS.incrementAndGet() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    private final PostTradeMetrics metrics = new PostTradeMetrics();
    private final List<SinkRunner> runners = new ArrayList<>();
    private final TestVenue venue = new TestVenue(2).standardSetup();

    @AfterEach
    void tearDown() throws SQLException
    {
        runners.forEach(SinkRunner::close);
        try (Connection c = DriverManager.getConnection(url, "sa", ""); Statement st = c.createStatement())
        {
            st.execute("SHUTDOWN");
        }
    }

    private TradeStore store()
    {
        return new TradeStore(url, "sa", "", "tradestore-0", 20, 100, metrics);
    }

    private SinkRunner run(final PostTradeSink sink)
    {
        final SinkRunner r = new SinkRunner(sink, venue, 64, 50).start();
        runners.add(r);
        return r;
    }

    private void awaitApplied(final SinkRunner r)
    {
        TestVenue.await(() ->
        {
            for (int s = 0; s < venue.shardCount(); s++)
            {
                if (r.lastEventSeq(s) < venue.venue.eventCount(s))
                {
                    return false;
                }
            }
            return true;
        }, 10_000, "runner to apply every event");
    }

    /** Waits until the cursor rows in the database cover every event (the store committed). */
    private void awaitCommitted()
    {
        TestVenue.await(() ->
        {
            for (int s = 0; s < venue.shardCount(); s++)
            {
                if (query("SELECT COALESCE(MAX(event_seq), 0) FROM pt_cursor WHERE shard_id = " + s) <
                    venue.venue.eventCount(s))
                {
                    return false;
                }
            }
            return true;
        }, 10_000, "trade store commit");
    }

    private long query(final String sql)
    {
        try (Connection c = DriverManager.getConnection(url, "sa", ""); Statement st = c.createStatement();
            ResultSet rs = st.executeQuery(sql))
        {
            return rs.next() ? rs.getLong(1) : -1;
        }
        catch (final SQLException e)
        {
            return -1; // schema not created yet
        }
    }

    private void assertStoreMatchesVenue()
    {
        long trades = 0;
        for (int s = 0; s < venue.shardCount(); s++)
        {
            final long t = venue.count(s, TradeDecoder.TEMPLATE_ID);
            assertEquals(t, query("SELECT COUNT(*) FROM pt_trade WHERE shard_id = " + s), "trades on shard " + s);
            trades += t;
        }
        assertTrue(trades > 0);
        assertEquals(2 * trades, query("SELECT COUNT(*) FROM pt_fill"), "two fills per trade");
    }

    @Test
    void storesEveryTradeAndBothFillsWithTheirFields()
    {
        venue.cross(0, 10_000, 100);
        venue.cross(1, 10_001, 50);
        final long resting = venue.order(0, 200, io.lmb.sbe.Side.SELL, 10_002, 300);
        venue.order(0, 100, io.lmb.sbe.Side.BUY, 10_002, 120); // partial fill of the resting order
        venue.cancel(0, 200, resting);

        final SinkRunner r = run(store());
        awaitApplied(r);
        awaitCommitted();
        assertStoreMatchesVenue();
        assertEquals(10_001L * 50, query("SELECT price * quantity FROM pt_trade WHERE shard_id = 1"));
        assertEquals(1, query("SELECT COUNT(*) FROM pt_trade WHERE shard_id = 1 AND buy_firm_id = 100 AND " +
            "sell_firm_id = 200 AND aggressor_side = 'B' AND is_auction = FALSE"));
        assertEquals(180, query("SELECT leaves_qty FROM pt_fill WHERE shard_id = 0 AND firm_id = 200 AND last_qty = 120"));
        assertEquals(1, query("SELECT COUNT(*) FROM pt_fill WHERE shard_id = 0 AND firm_id = 200 AND liquidity = 'M' " +
            "AND last_qty = 120 AND side = 'S'"));
        assertEquals(0, r.restarts.get());
    }

    @Test
    void restartResumesAfterTheCommittedCursorAndDropsRedeliveries()
    {
        venue.cross(0, 10_000, 10);
        SinkRunner r = run(store());
        awaitApplied(r);
        r.close();
        runners.remove(r);
        final long trades0 = query("SELECT COUNT(*) FROM pt_trade");

        venue.cross(0, 10_000, 20);
        venue.cross(1, 10_000, 30);
        venue.rewind = 7; // the stream redelivers events the store already has
        r = run(store());
        awaitApplied(r);
        awaitCommitted();
        assertStoreMatchesVenue();
        assertTrue(query("SELECT COUNT(*) FROM pt_trade") > trades0);
        assertTrue(r.duplicates.get() > 0, "redelivered events were seen and dropped");
    }

    @Test
    void failureMidBatchRollsBackAndReappliesFromTheCursor()
    {
        for (int i = 0; i < 20; i++)
        {
            venue.cross(i % 2, 10_000 + i, 10 + i);
        }
        final TradeStore inner = store();
        final AtomicInteger seen = new AtomicInteger();
        final PostTradeSink flaky = new PostTradeSink()
        {
            public String name()
            {
                return inner.name();
            }

            public long[] open(final int shardCount) throws Exception
            {
                return inner.open(shardCount);
            }

            public void onEvent(final int shardId, final long eventSeq, final DirectBuffer b, final int o, final int l)
                throws Exception
            {
                if (seen.incrementAndGet() == 60)
                {
                    throw new java.sql.SQLException("injected failure");
                }
                inner.onEvent(shardId, eventSeq, b, o, l);
            }

            public int doWork(final long nowMs) throws Exception
            {
                return inner.doWork(nowMs);
            }

            public void close(final boolean clean)
            {
                inner.close(clean);
            }
        };
        final SinkRunner r = run(flaky);
        awaitApplied(r);
        awaitCommitted();
        assertEquals(1, r.restarts.get());
        assertStoreMatchesVenue();
    }

    @Test
    void lostCursorDoesNotDuplicateRows()
    {
        venue.cross(0, 10_000, 10);
        venue.cross(1, 10_000, 10);
        SinkRunner r = run(store());
        awaitApplied(r);
        r.close();
        runners.remove(r);
        assertEquals(1, executeUpdateCount("UPDATE pt_cursor SET event_seq = 0 WHERE shard_id = 0"));
        r = run(store());
        awaitApplied(r);
        awaitCommitted();
        assertStoreMatchesVenue();
    }

    @Test
    void gapFailsTheSinkInsteadOfSkippingAnEvent()
    {
        venue.cross(0, 10_000, 10);
        final long gapAt = venue.venue.eventCount(0) + 2;
        venue.skipEventSeq = gapAt;
        venue.cross(0, 10_000, 10);
        final SinkRunner r = run(store());
        TestVenue.await(() -> r.restarts.get() >= 1, 5_000, "gap failure");
        assertEquals(gapAt - 1, r.lastEventSeq(0));
        venue.skipEventSeq = -1; // the next attempt reads the event and catches up
        awaitApplied(r);
        awaitCommitted();
        assertStoreMatchesVenue();
    }

    @Test
    void migrationIsIdempotentAndVersioned() throws SQLException
    {
        try (Connection c = DriverManager.getConnection(url, "sa", ""))
        {
            assertEquals(SchemaMigrator.latestVersion(), SchemaMigrator.migrate(c));
            assertEquals(SchemaMigrator.latestVersion(), SchemaMigrator.migrate(c));
        }
        assertEquals(SchemaMigrator.latestVersion(), query("SELECT COUNT(*) FROM pt_schema_version"));
    }

    private int executeUpdateCount(final String sql)
    {
        try (Connection c = DriverManager.getConnection(url, "sa", ""); Statement st = c.createStatement())
        {
            return st.executeUpdate(sql);
        }
        catch (final SQLException e)
        {
            throw new AssertionError(e);
        }
    }
}
