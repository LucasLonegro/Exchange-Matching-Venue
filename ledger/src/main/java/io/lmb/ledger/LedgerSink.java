package io.lmb.ledger;

import io.lmb.api.PostTradeSink;
import io.lmb.common.Events;
import io.lmb.sbe.ClosingPriceDecoder;
import io.lmb.sbe.DayRolledDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.RiskLimitAppliedDecoder;
import io.lmb.sbe.SymbolDefinedDecoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.TradeDecoder;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The ledger as a {@link PostTradeSink}: consumes every shard's event stream and keeps, per firm and symbol, the
 * position, average cost, realised PnL and cash; at each DayRolled it marks the shard's positions to the official
 * close (the last ClosingPrice of the day) and writes the day's statement lines; once every shard has ended a
 * trading day it writes each firm's daily statement and the statement files.
 *
 * <p>Inputs: Trade (both sides; one record per match), ClosingPrice, DayRolled, SymbolDefined (ticker, shard,
 * removal) and RiskLimitApplied (the firm list). Nothing else in the stream changes the ledger, and the ledger
 * needs no other source: it never reads the control plane.
 *
 * <p>Exactly-once: the whole state is in memory, loaded at {@link #open}; every commit writes what changed together
 * with the per-shard cursors in one transaction. A failure rolls back and the runner reopens from the committed
 * cursors, so the state is always that of a prefix of each shard's stream. A position depends only on its symbol's
 * shard, and a firm statement only on lines that are final, so the result does not depend on how the shards'
 * streams interleave.
 */
final class LedgerSink implements PostTradeSink
{
    private static final Logger LOG = LoggerFactory.getLogger(LedgerSink.class);

    private final LedgerConfig config;
    private final LedgerMetrics metrics;
    private final StatementWriter statements;

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final TradeDecoder trade = new TradeDecoder();
    private final ClosingPriceDecoder closing = new ClosingPriceDecoder();
    private final DayRolledDecoder rolled = new DayRolledDecoder();
    private final SymbolDefinedDecoder defined = new SymbolDefinedDecoder();
    private final RiskLimitAppliedDecoder limit = new RiskLimitAppliedDecoder();

    private Connection connection;
    private LedgerStore store;

    // state (valid between open and close)
    private long[] cursor;
    private int[] tradingDate;
    private boolean[] shardDirty;
    /** Firm id to the first trading date on which some shard saw it (the earliest over all shards). */
    private final TreeMap<Integer, FirmRow> firms = new TreeMap<>();
    private final TreeMap<Integer, SymbolRow> symbols = new TreeMap<>();
    private final Map<Long, Position> positions = new HashMap<>();
    private final List<Position.DailyPosition> newLines = new ArrayList<>();
    private final List<long[]> newDays = new ArrayList<>();
    /** Trading dates some shard has ended and whose firm statements are not written yet, with the latest end time. */
    private final TreeMap<Integer, Long> pendingDates = new TreeMap<>();
    private int pendingEvents;
    private long nextCommitMs;

    LedgerSink(final LedgerConfig config, final LedgerMetrics metrics)
    {
        this.config = config;
        this.metrics = metrics;
        this.statements = new StatementWriter(config.statementsDir);
    }

    @Override
    public String name()
    {
        return "ledger";
    }

    @Override
    public long[] open(final int shardCount) throws SQLException, IOException
    {
        connection = DriverManager.getConnection(config.dbUrl, config.dbUser, config.dbPassword);
        store = new LedgerStore(connection);
        store.createSchema();
        connection.setAutoCommit(false);
        cursor = new long[shardCount];
        tradingDate = new int[shardCount];
        shardDirty = new boolean[shardCount];
        firms.clear();
        symbols.clear();
        positions.clear();
        newLines.clear();
        newDays.clear();
        pendingDates.clear();
        store.loadCursors(shardCount, cursor, tradingDate);
        for (final FirmRow f : store.loadFirms())
        {
            firms.put(f.firmId, f);
        }
        for (final SymbolRow s : store.loadSymbols())
        {
            symbols.put(s.symbolId, s);
        }
        for (final Position p : store.loadPositions())
        {
            positions.put(Position.key(p.firmId, p.symbolId), p);
            symbol(p.symbolId, p.shardId).positions.add(p);
        }
        pendingDates.putAll(store.loadPendingDates());
        connection.commit();
        metrics.pendingStatementDates.set(pendingDates.size());
        pendingEvents = 0;
        nextCommitMs = 0;
        for (final int date : store.statementDates())
        {
            if (!statements.exists(date))
            {
                statements.write(store, date);
                metrics.statementFiles.incrementAndGet();
            }
        }
        connection.commit();
        LOG.info("ledger open ({}): {} firms, {} symbols, {} positions, trading dates {}", config.dbUrl, firms.size(),
            symbols.size(), positions.size(), java.util.Arrays.toString(tradingDate));
        return cursor.clone();
    }

    @Override
    public void onEvent(final int shardId, final long eventSeq, final DirectBuffer buffer, final int offset, final int length)
    {
        header.wrap(buffer, offset);
        switch (header.templateId())
        {
            case TradeDecoder.TEMPLATE_ID -> onTrade(shardId, eventSeq, buffer, offset);
            case ClosingPriceDecoder.TEMPLATE_ID -> onClosingPrice(shardId, buffer, offset);
            case DayRolledDecoder.TEMPLATE_ID -> onDayRolled(shardId, eventSeq, buffer, offset);
            case SymbolDefinedDecoder.TEMPLATE_ID -> onSymbolDefined(shardId, buffer, offset);
            case RiskLimitAppliedDecoder.TEMPLATE_ID ->
            {
                limit.wrapAndApplyHeader(buffer, offset, header);
                firm((int)limit.firmId(), shardId);
            }
            default ->
            {
            }
        }
        cursor[shardId] = eventSeq;
        shardDirty[shardId] = true;
        pendingEvents++;
    }

    private void onTrade(final int shardId, final long eventSeq, final DirectBuffer buffer, final int offset)
    {
        trade.wrapAndApplyHeader(buffer, offset, header);
        final int symbolId = (int)trade.symbolId();
        final long price = trade.price();
        final long qty = trade.quantity();
        final int buyer = (int)trade.buyFirmId();
        final int seller = (int)trade.sellFirmId();
        final SymbolRow s = symbol(symbolId, shardId);
        firm(buyer, shardId);
        firm(seller, shardId);
        final Position b = position(buyer, s);
        b.fill(true, price, qty);
        b.lastEventSeq = eventSeq;
        final Position a = position(seller, s);
        a.fill(false, price, qty);
        a.lastEventSeq = eventSeq;
        metrics.trades.incrementAndGet();
    }

    private void onClosingPrice(final int shardId, final DirectBuffer buffer, final int offset)
    {
        closing.wrapAndApplyHeader(buffer, offset, header);
        final SymbolRow s = symbol((int)closing.symbolId(), shardId);
        s.closePrice = closing.price();
        s.closeSource = closing.source().name();
        s.closeDate = closing.tradingDate();
        s.dirty = true;
    }

    /** The shard's trading day ends: mark its positions to the official close and write the day's lines. */
    private void onDayRolled(final int shardId, final long eventSeq, final DirectBuffer buffer, final int offset)
    {
        rolled.wrapAndApplyHeader(buffer, offset, header);
        final int closingDate = tradingDate[shardId];
        final long ts = Events.timestampNs(buffer, offset);
        // The first DayRollover starts the first trading day; it ends none. Anything traded before it is carried into
        // that day (its lines show it as bought or sold that day, from an opening position of 0).
        final boolean endsADay = closingDate != 0;
        for (final SymbolRow s : symbols.values())
        {
            if (s.shardId != shardId)
            {
                continue;
            }
            if (s.closeDate != null && s.closeDate == closingDate)
            {
                // the last ClosingPrice of the day is the official close (a null price, source NONE, keeps the mark)
                if (s.closePrice != Ledger.NULL_PRICE)
                {
                    s.markPrice = s.closePrice;
                    s.markSource = s.closeSource;
                    s.markDate = closingDate;
                }
            }
            s.closePrice = Ledger.NULL_PRICE;
            s.closeSource = null;
            s.closeDate = null;
            s.dirty = true;
            final String source = s.markPrice == Ledger.NULL_PRICE ? "NONE" :
                s.markDate != null && s.markDate == closingDate ? s.markSource : "PREVIOUS_MARK";
            if (endsADay)
            {
                for (final Position p : s.positions)
                {
                    newLines.add(p.closeDay(closingDate, s.markPrice, source));
                }
            }
        }
        if (endsADay)
        {
            newDays.add(new long[] {shardId, closingDate, eventSeq, ts});
            pendingDates.merge(closingDate, ts, Math::max);
            metrics.daysClosed.incrementAndGet();
        }
        tradingDate[shardId] = rolled.tradingDate();
    }

    private void onSymbolDefined(final int shardId, final DirectBuffer buffer, final int offset)
    {
        defined.wrapAndApplyHeader(buffer, offset, header);
        final SymbolRow s = symbol((int)defined.symbolId(), shardId);
        s.ticker = ticker(defined.ticker());
        s.removed = defined.state() == SymbolState.REMOVED;
        s.dirty = true;
    }

    private static String ticker(final String raw)
    {
        int end = raw.length();
        while (end > 0 && (raw.charAt(end - 1) == '\0' || raw.charAt(end - 1) == ' '))
        {
            end--;
        }
        return raw.substring(0, end);
    }

    /**
     * A firm is in the statements of every trading date from the first on which any shard saw it. The minimum over
     * all shards is final for date D once every shard has ended D, which is when D's statements are written.
     */
    private void firm(final int firmId, final int shardId)
    {
        final int date = tradingDate[shardId];
        final FirmRow f = firms.get(firmId);
        if (f == null)
        {
            final FirmRow n = new FirmRow(firmId, date);
            n.dirty = true;
            firms.put(firmId, n);
        }
        else if (date < f.firstTradingDate)
        {
            f.firstTradingDate = date;
            f.dirty = true;
        }
    }

    private SymbolRow symbol(final int symbolId, final int shardId)
    {
        SymbolRow s = symbols.get(symbolId);
        if (s == null)
        {
            s = new SymbolRow(symbolId, shardId);
            s.ticker = Integer.toString(symbolId);
            s.dirty = true;
            symbols.put(symbolId, s);
        }
        return s;
    }

    private Position position(final int firmId, final SymbolRow s)
    {
        final long key = Position.key(firmId, s.symbolId);
        Position p = positions.get(key);
        if (p == null)
        {
            p = new Position(firmId, s.symbolId, s.shardId);
            positions.put(key, p);
            s.positions.add(p);
        }
        return p;
    }

    @Override
    public int doWork(final long nowMs) throws SQLException, IOException
    {
        if (pendingEvents >= config.commitBatch || (pendingEvents > 0 && nowMs >= nextCommitMs))
        {
            final int n = pendingEvents;
            commit();
            nextCommitMs = nowMs + config.commitIntervalMs;
            return Math.max(1, n);
        }
        return 0;
    }

    private void commit() throws SQLException, IOException
    {
        final long start = System.nanoTime();
        for (final FirmRow f : firms.values())
        {
            if (f.dirty)
            {
                store.saveFirm(f);
                f.persisted = true;
                f.dirty = false;
            }
        }
        for (final SymbolRow s : symbols.values())
        {
            if (s.dirty)
            {
                store.saveSymbol(s);
                s.persisted = true;
                s.dirty = false;
            }
        }
        for (final Position p : positions.values())
        {
            if (p.dirty)
            {
                store.savePosition(p);
                p.persisted = true;
                p.dirty = false;
            }
        }
        store.insertDailyPositions(newLines);
        newLines.clear();
        for (final long[] d : newDays)
        {
            store.insertDay((int)d[0], (int)d[1], d[2], d[3]);
        }
        newDays.clear();
        for (int s = 0; s < cursor.length; s++)
        {
            if (shardDirty[s])
            {
                store.saveCursor(s, cursor[s], tradingDate[s]);
                shardDirty[s] = false;
            }
        }
        // every shard has moved past a pending date: its lines are all written, the firm statements can be
        final int minDate = minTradingDate();
        final List<Integer> written = new ArrayList<>();
        while (!pendingDates.isEmpty() && pendingDates.firstKey() < minDate)
        {
            final Map.Entry<Integer, Long> e = pendingDates.pollFirstEntry();
            final List<Integer> firmIds = new ArrayList<>();
            for (final FirmRow f : firms.values())
            {
                if (f.firstTradingDate <= e.getKey())
                {
                    firmIds.add(f.firmId);
                }
            }
            final int firmCount = store.writeStatements(e.getKey(), firmIds, e.getValue());
            written.add(e.getKey());
            metrics.statements.addAndGet(firmCount);
        }
        connection.commit();
        pendingEvents = 0;
        metrics.commits.incrementAndGet();
        metrics.pendingStatementDates.set(pendingDates.size());
        metrics.lastCommitNs.set(System.nanoTime() - start);
        for (final int date : written)
        {
            statements.write(store, date);
            metrics.statementFiles.incrementAndGet();
            LOG.info("ledger: statements for trading date {} written", date);
        }
        if (!written.isEmpty())
        {
            connection.commit();
        }
    }

    private int minTradingDate()
    {
        int min = Integer.MAX_VALUE;
        for (final int d : tradingDate)
        {
            min = Math.min(min, d);
        }
        return min;
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
            if (clean && pendingEvents > 0)
            {
                commit();
            }
            else if (!clean)
            {
                connection.rollback();
            }
        }
        catch (final SQLException | IOException | RuntimeException e)
        {
            LOG.warn("ledger: final commit failed (the events will be re-applied from the cursor)", e);
        }
        try
        {
            connection.close();
        }
        catch (final SQLException e)
        {
            LOG.warn("ledger: close failed", e);
        }
        connection = null;
    }

    /** In-memory copy of a {@code ledger_firm} row. */
    static final class FirmRow
    {
        final int firmId;
        int firstTradingDate;
        boolean persisted;
        boolean dirty;

        FirmRow(final int firmId, final int firstTradingDate)
        {
            this.firmId = firmId;
            this.firstTradingDate = firstTradingDate;
        }
    }

    /** In-memory copy of a {@code ledger_symbol} row and the positions in the symbol. */
    static final class SymbolRow
    {
        final int symbolId;
        final int shardId;
        String ticker;
        boolean removed;
        long closePrice = Ledger.NULL_PRICE;
        String closeSource;
        Integer closeDate;
        long markPrice = Ledger.NULL_PRICE;
        String markSource;
        Integer markDate;
        boolean persisted;
        boolean dirty;
        final List<Position> positions = new ArrayList<>();

        SymbolRow(final int symbolId, final int shardId)
        {
            this.symbolId = symbolId;
            this.shardId = shardId;
        }
    }
}
