package io.lmb.posttrade.store;

import io.lmb.common.Events;
import io.lmb.posttrade.PostTradeMetrics;
import io.lmb.posttrade.PostTradeSink;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.LiquidityFlag;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.Side;
import io.lmb.sbe.TradeDecoder;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * JDBC trade store (PostgreSQL in production, H2 in PostgreSQL mode for tests and the local profile).
 * Writes one {@code pt_trade} row per Trade event and one {@code pt_fill} row per fill ExecutionReport.
 *
 * <p>Exactly-once: the consumer's per-shard cursor ({@code pt_cursor}) is updated in the same transaction
 * as the rows, and the runner resumes after it. Inserts are additionally idempotent on
 * (shard_id, event_seq) ({@code ON CONFLICT DO NOTHING}), so two instances, or a lost cursor, cannot
 * duplicate rows. Transactions commit every {@code commitIntervalMs} or {@code commitBatch} rows.
 */
public final class TradeStore implements PostTradeSink
{
    private static final Logger LOG = LoggerFactory.getLogger(TradeStore.class);

    private final String url;
    private final String user;
    private final String password;
    private final String consumer;
    private final long commitIntervalMs;
    private final int commitBatch;
    private final PostTradeMetrics metrics;

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final TradeDecoder trade = new TradeDecoder();
    private final ExecutionReportDecoder report = new ExecutionReportDecoder();

    private Connection connection;
    private PreparedStatement insertTrade;
    private PreparedStatement insertFill;
    private PreparedStatement updateCursor;
    private long[] cursor;
    private boolean[] cursorDirty;
    private int pendingTrades;
    private int pendingFills;
    private long nextCommitMs;

    public TradeStore(
        final String url, final String user, final String password, final String consumer,
        final long commitIntervalMs, final int commitBatch, final PostTradeMetrics metrics)
    {
        this.url = url;
        this.user = user;
        this.password = password;
        this.consumer = consumer;
        this.commitIntervalMs = commitIntervalMs;
        this.commitBatch = commitBatch;
        this.metrics = metrics;
    }

    @Override
    public String name()
    {
        return "tradestore";
    }

    @Override
    public long[] open(final int shardCount) throws SQLException
    {
        connection = DriverManager.getConnection(url, user, password);
        SchemaMigrator.migrate(connection);
        connection.setAutoCommit(false);
        cursor = new long[shardCount];
        cursorDirty = new boolean[shardCount];
        try (PreparedStatement ps = connection.prepareStatement(
            "SELECT shard_id, event_seq FROM pt_cursor WHERE consumer = ?"))
        {
            ps.setString(1, consumer);
            final boolean[] seen = new boolean[shardCount];
            try (ResultSet rs = ps.executeQuery())
            {
                while (rs.next())
                {
                    final int s = rs.getInt(1);
                    if (s < shardCount)
                    {
                        cursor[s] = rs.getLong(2);
                        seen[s] = true;
                    }
                }
            }
            try (PreparedStatement ins = connection.prepareStatement(
                "INSERT INTO pt_cursor (consumer, shard_id, event_seq) VALUES (?, ?, 0) ON CONFLICT DO NOTHING"))
            {
                for (int s = 0; s < shardCount; s++)
                {
                    if (!seen[s])
                    {
                        ins.setString(1, consumer);
                        ins.setInt(2, s);
                        ins.addBatch();
                    }
                }
                ins.executeBatch();
            }
        }
        connection.commit();
        insertTrade = connection.prepareStatement(
            "INSERT INTO pt_trade (shard_id, event_seq, exec_id, match_id, symbol_id, price, quantity, buy_order_id, " +
            "sell_order_id, buy_firm_id, sell_firm_id, aggressor_side, is_auction, ts_ns, log_position) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING");
        insertFill = connection.prepareStatement(
            "INSERT INTO pt_fill (shard_id, event_seq, exec_id, match_id, order_id, cl_ord_id, session_id, firm_id, " +
            "account_id, symbol_id, side, last_qty, last_px, leaves_qty, cum_qty, liquidity, ts_ns) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING");
        updateCursor = connection.prepareStatement(
            "UPDATE pt_cursor SET event_seq = ? WHERE consumer = ? AND shard_id = ? AND event_seq < ?");
        pendingTrades = 0;
        pendingFills = 0;
        nextCommitMs = 0;
        LOG.info("trade store open ({}), consumer {}", url, consumer);
        return cursor.clone();
    }

    @Override
    public void onEvent(final int shardId, final long eventSeq, final DirectBuffer buffer, final int offset, final int length)
        throws SQLException
    {
        header.wrap(buffer, offset);
        final int templateId = header.templateId();
        if (templateId == TradeDecoder.TEMPLATE_ID)
        {
            trade.wrapAndApplyHeader(buffer, offset, header);
            final PreparedStatement ps = insertTrade;
            ps.setInt(1, shardId);
            ps.setLong(2, eventSeq);
            ps.setString(3, Events.execId(buffer, offset));
            ps.setLong(4, trade.matchId());
            ps.setInt(5, (int)trade.symbolId());
            ps.setLong(6, trade.price());
            ps.setLong(7, trade.quantity());
            ps.setLong(8, trade.buyOrderId());
            ps.setLong(9, trade.sellOrderId());
            ps.setInt(10, (int)trade.buyFirmId());
            ps.setInt(11, (int)trade.sellFirmId());
            final Side aggressor = trade.aggressorSide();
            if (aggressor == Side.BUY || aggressor == Side.SELL)
            {
                ps.setString(12, aggressor == Side.BUY ? "B" : "S");
            }
            else
            {
                ps.setNull(12, Types.CHAR);
            }
            ps.setBoolean(13, trade.isAuction() == io.lmb.sbe.BooleanType.TRUE);
            ps.setLong(14, Events.timestampNs(buffer, offset));
            ps.setLong(15, Events.logPosition(buffer, offset));
            ps.addBatch();
            pendingTrades++;
        }
        else if (templateId == ExecutionReportDecoder.TEMPLATE_ID)
        {
            report.wrapAndApplyHeader(buffer, offset, header);
            final ExecType execType = report.execType();
            if (execType == ExecType.FILL || execType == ExecType.PARTIAL_FILL)
            {
                final PreparedStatement ps = insertFill;
                ps.setInt(1, shardId);
                ps.setLong(2, eventSeq);
                ps.setString(3, Events.execId(buffer, offset));
                ps.setLong(4, report.matchId());
                ps.setLong(5, report.orderId());
                ps.setLong(6, report.clOrdId());
                ps.setLong(7, report.sessionId());
                ps.setInt(8, (int)report.firmId());
                ps.setInt(9, (int)report.accountId());
                ps.setInt(10, (int)report.symbolId());
                ps.setString(11, report.side() == Side.BUY ? "B" : "S");
                ps.setLong(12, report.lastQty());
                ps.setLong(13, report.lastPx());
                ps.setLong(14, report.leavesQty());
                ps.setLong(15, report.cumQty());
                ps.setString(16, liquidity(report.liquidity()));
                ps.setLong(17, Events.timestampNs(buffer, offset));
                ps.addBatch();
                pendingFills++;
            }
        }
        cursor[shardId] = eventSeq;
        cursorDirty[shardId] = true;
    }

    private static String liquidity(final LiquidityFlag flag)
    {
        return switch (flag)
        {
            case MAKER -> "M";
            case TAKER -> "T";
            case AUCTION -> "A";
            default -> "N";
        };
    }

    @Override
    public int doWork(final long nowMs) throws SQLException
    {
        if (pendingTrades + pendingFills >= commitBatch || (nowMs >= nextCommitMs && anyDirty()))
        {
            final int rows = commit();
            nextCommitMs = nowMs + commitIntervalMs;
            return Math.max(1, rows);
        }
        return 0;
    }

    private boolean anyDirty()
    {
        for (final boolean d : cursorDirty)
        {
            if (d)
            {
                return true;
            }
        }
        return false;
    }

    private int commit() throws SQLException
    {
        final long start = System.nanoTime();
        final int trades = pendingTrades;
        final int fills = pendingFills;
        if (trades > 0)
        {
            insertTrade.executeBatch();
        }
        if (fills > 0)
        {
            insertFill.executeBatch();
        }
        for (int s = 0; s < cursor.length; s++)
        {
            if (cursorDirty[s])
            {
                updateCursor.setLong(1, cursor[s]);
                updateCursor.setString(2, consumer);
                updateCursor.setInt(3, s);
                updateCursor.setLong(4, cursor[s]);
                updateCursor.addBatch();
            }
        }
        updateCursor.executeBatch();
        connection.commit();
        java.util.Arrays.fill(cursorDirty, false);
        pendingTrades = 0;
        pendingFills = 0;
        metrics.tradesStored.addAndGet(trades);
        metrics.fillsStored.addAndGet(fills);
        metrics.storeCommits.incrementAndGet();
        metrics.storeCommitNs.record(System.nanoTime() - start);
        return trades + fills;
    }

    @Override
    public void close(final boolean clean)
    {
        if (connection == null)
        {
            return;
        }
        try
        {
            if (clean && anyDirty())
            {
                commit();
            }
            else if (!clean)
            {
                connection.rollback();
            }
        }
        catch (final SQLException e)
        {
            LOG.warn("trade store: final commit failed (the rows will be re-applied from the cursor)", e);
        }
        try
        {
            connection.close();
        }
        catch (final SQLException e)
        {
            LOG.warn("trade store: close failed", e);
        }
        connection = null;
    }
}
