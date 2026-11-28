package io.lmb.posttrade.dropcopy;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.common.Events;
import io.lmb.fix.LastLiquidityInd;
import io.lmb.fix.builder.ExecutionReportEncoder;
import io.lmb.posttrade.CursorFile;
import io.lmb.posttrade.PostTradeMetrics;
import io.lmb.api.PostTradeSink;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.OrdStatus;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.Side;
import io.lmb.sbe.TimeInForce;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.artio.fields.EpochFractionFormat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionAcquiredInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * FIX drop copy sink: sends ExecutionReports (the configured exec types, by default fills and cancels) to the
 * sessions of the {@link DropCopyEngine}. A firm session (Username = firm id) gets only that firm's reports; an
 * {@code ALL} session gets every firm's.
 *
 * <p>Each client (keyed by SenderCompID) has its own per-shard cursor: the eventSeq of the last event it has been
 * offered (sent, or filtered out). A client that logs on catches up from its cursor with a private event stream
 * reader per shard and switches to the live stream (the runner's) once it reaches the runner's position. A
 * back-pressured send puts that shard back into catch-up from the cursor, so nothing is skipped.
 *
 * <p>Durability: {@code dropcopy.ckpt} holds the runner position ({@code live.<shard>}) and every client's cursors.
 * After a clean stop, a client resumes exactly after the last report handed to Artio: nothing is sent twice.
 * After a crash the cursors can lag up to one checkpoint interval; reports in that window are sent again with
 * PossResend(97)=Y (every report of an already known client whose event is older than the process start).
 * Logon tag LmbResumeFrom(20010) "shard:eventSeq;..." overrides the stored cursor. A new client starts at the
 * beginning of the retained event stream.
 *
 * <p>FIX-level ResendRequests within one FIX session are served by Artio from its archive (the engine keeps the
 * session sequence numbers across restarts unless {@code deleteLogsOnStart}).
 */
public final class DropCopySink implements PostTradeSink
{
    private static final Logger LOG = LoggerFactory.getLogger(DropCopySink.class);
    private static final int CATCHUP_LIMIT = 512;
    private static final long CATCHUP_RETRY_MS = 10;

    private final DropCopyEngine engine;
    private final VenueClients venue;
    private final IntFunction<String> tickers;
    private final EnumSet<ExecType> execTypes;
    private final long checkpointIntervalMs;
    private final PostTradeMetrics metrics;
    private final Path checkpointFile;

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final ExecutionReportDecoder report = new ExecutionReportDecoder();
    private final ExecutionReportEncoder er = new ExecutionReportEncoder();
    private final UtcTimestampEncoder ts = new UtcTimestampEncoder(EpochFractionFormat.NANOSECONDS);
    private final Map<String, Client> clients = new LinkedHashMap<>();
    private final List<Client> online = new ArrayList<>();
    private final CatchUpHandler catchUpHandler = new CatchUpHandler();

    private int shards;
    private long[] live;
    private FixLibrary library;
    private boolean dirty;
    private long nextCheckpointMs;
    /** After an unclean stop: events older than this may already have reached known clients. */
    private long possResendBeforeNs;

    public DropCopySink(
        final DropCopyEngine engine, final VenueClients venue, final IntFunction<String> tickers, final String execTypes,
        final long checkpointIntervalMs, final PostTradeMetrics metrics)
    {
        this.engine = engine;
        this.venue = venue;
        this.tickers = tickers;
        this.execTypes = parseExecTypes(execTypes);
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.metrics = metrics;
        this.checkpointFile = engine.baseDir().resolve("dropcopy.ckpt");
    }

    static EnumSet<ExecType> parseExecTypes(final String text)
    {
        final EnumSet<ExecType> set = EnumSet.noneOf(ExecType.class);
        for (final String part : text.split(","))
        {
            if (!part.isBlank())
            {
                set.add(ExecType.valueOf(part.trim().toUpperCase()));
            }
        }
        return set;
    }

    @Override
    public String name()
    {
        return "dropcopy";
    }

    @Override
    public long[] open(final int shardCount) throws Exception
    {
        shards = shardCount;
        live = new long[shardCount];
        clients.clear();
        online.clear();
        final Map<String, String> ckpt = CursorFile.read(checkpointFile);
        for (final Map.Entry<String, String> e : ckpt.entrySet())
        {
            final String k = e.getKey();
            if (k.startsWith("live."))
            {
                final int s = Integer.parseInt(k.substring(5));
                if (s < shardCount)
                {
                    live[s] = Long.parseLong(e.getValue());
                }
            }
            else if (k.startsWith("session."))
            {
                final int dot = k.lastIndexOf('.');
                final int s = Integer.parseInt(k.substring(dot + 1));
                if (s < shardCount)
                {
                    client(k.substring("session.".length(), dot)).cursor[s] = Long.parseLong(e.getValue());
                }
            }
        }
        final boolean clean = ckpt.isEmpty() || "true".equals(ckpt.get("clean"));
        possResendBeforeNs = clean ? 0 : TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        for (final Client c : clients.values())
        {
            c.known = true;
        }
        writeCheckpoint(false);

        library = engine.library(this::onSessionAcquired);
        LOG.info("drop copy open: {} known clients, live {}, {} start", clients.size(), java.util.Arrays.toString(live),
            clean ? "clean" : "unclean (PossResend on replays)");
        return live.clone();
    }

    private Client client(final String compId)
    {
        return clients.computeIfAbsent(compId, id -> new Client(id, shards));
    }

    // ------------------------------------------------------------------ live path

    @Override
    public void onEvent(final int shardId, final long eventSeq, final DirectBuffer buffer, final int offset, final int length)
    {
        live[shardId] = eventSeq;
        dirty = true;
        final boolean relevant = isReport(buffer, offset);
        for (int i = 0, n = online.size(); i < n; i++)
        {
            final Client c = online.get(i);
            if (c.reader[shardId] != null || c.catchUp[shardId] || eventSeq <= c.cursor[shardId])
            {
                continue; // catching up (its reader will get there) or already offered
            }
            if (eventSeq != c.cursor[shardId] + 1)
            {
                c.catchUp[shardId] = true; // cannot happen; recover through the reader instead of skipping
                continue;
            }
            if (relevant && c.wants(report.firmId()) && !send(c, shardId, eventSeq, buffer, offset, false))
            {
                c.catchUp[shardId] = true;
                c.retryAtMs = System.currentTimeMillis() + CATCHUP_RETRY_MS;
                continue;
            }
            c.cursor[shardId] = eventSeq;
        }
    }

    /** Leaves {@link #report} wrapped when true. */
    private boolean isReport(final DirectBuffer buffer, final int offset)
    {
        header.wrap(buffer, offset);
        if (header.templateId() != ExecutionReportDecoder.TEMPLATE_ID)
        {
            return false;
        }
        report.wrapAndApplyHeader(buffer, offset, header);
        return execTypes.contains(report.execType());
    }

    // ------------------------------------------------------------------ duty cycle

    @Override
    public int doWork(final long nowMs)
    {
        int work = library.poll(10);
        for (int i = 0; i < online.size(); i++)
        {
            final Client c = online.get(i);
            if (c.session.isActive() && nowMs >= c.retryAtMs)
            {
                for (int s = 0; s < shards; s++)
                {
                    if (c.catchUp[s])
                    {
                        work += catchUp(c, s, nowMs);
                    }
                }
            }
        }
        if (dirty && nowMs >= nextCheckpointMs)
        {
            writeCheckpoint(false);
            nextCheckpointMs = nowMs + checkpointIntervalMs;
            work++;
        }
        return work;
    }

    private int catchUp(final Client c, final int s, final long nowMs)
    {
        if (c.cursor[s] >= live[s])
        {
            switchToLive(c, s);
            return 1;
        }
        if (c.reader[s] == null)
        {
            c.reader[s] = venue.eventStream(s, c.cursor[s]);
        }
        catchUpHandler.client = c;
        catchUpHandler.shard = s;
        catchUpHandler.blocked = false;
        final int n = c.reader[s].poll(catchUpHandler, CATCHUP_LIMIT);
        if (catchUpHandler.blocked)
        {
            // the reader has consumed past the cursor: reopen it from the cursor after a short pause
            CloseHelper.quietClose(c.reader[s]::close);
            c.reader[s] = null;
            c.retryAtMs = nowMs + CATCHUP_RETRY_MS;
        }
        else if (c.cursor[s] >= live[s])
        {
            switchToLive(c, s);
        }
        return n;
    }

    private void switchToLive(final Client c, final int s)
    {
        if (c.reader[s] != null)
        {
            CloseHelper.quietClose(c.reader[s]::close);
            c.reader[s] = null;
        }
        c.catchUp[s] = false;
        LOG.debug("drop copy {}: shard {} live at eventSeq {}", c.compId, s, c.cursor[s]);
    }

    private final class CatchUpHandler implements EventStreamClient.EventHandler
    {
        Client client;
        int shard;
        boolean blocked;

        @Override
        public void onEvent(final DirectBuffer buffer, final int offset, final int length)
        {
            final Client c = client;
            final int s = shard;
            final long seq = Events.eventSeq(buffer, offset);
            if (blocked || seq <= c.cursor[s])
            {
                return;
            }
            if (c.cursor[s] != 0 && seq != c.cursor[s] + 1)
            {
                throw new IllegalStateException("drop copy " + c.compId + ": event gap on shard " + s + ": expected " +
                    (c.cursor[s] + 1) + ", got " + seq);
            }
            if (isReport(buffer, offset) && c.wants(report.firmId()))
            {
                if (!send(c, s, seq, buffer, offset, true))
                {
                    blocked = true;
                    return;
                }
                metrics.dropCopyReplayed.incrementAndGet();
            }
            c.cursor[s] = seq;
            dirty = true;
        }
    }

    // ------------------------------------------------------------------ encoding

    /** {@link #report} must be wrapped on the event. */
    private boolean send(
        final Client c, final int shardId, final long eventSeq, final DirectBuffer buffer, final int offset,
        final boolean replay)
    {
        final ExecutionReportDecoder d = report;
        final long timestampNs = Events.timestampNs(buffer, offset);
        final int tsLen = ts.encodeFrom(timestampNs, TimeUnit.NANOSECONDS);
        er.reset();
        er.header().possResend(replay && c.known && timestampNs < possResendBeforeNs);
        er.orderID(d.orderId() == 0 ? "NONE" : Long.toString(d.orderId()))
            .execID(Events.execId(buffer, offset))
            .clOrdID(Long.toString(d.clOrdId()))
            .execType(fixExecType(d.execType()))
            .ordStatus(fixOrdStatus(d.ordStatus()))
            .side(d.side() == Side.SELL ? io.lmb.fix.Side.SELL : io.lmb.fix.Side.BUY)
            .ordType(d.ordType() == OrdType.MARKET ? io.lmb.fix.OrdType.MARKET : io.lmb.fix.OrdType.LIMIT)
            .timeInForce(d.timeInForce() == TimeInForce.IOC ? io.lmb.fix.TimeInForce.IMMEDIATE_OR_CANCEL :
                io.lmb.fix.TimeInForce.DAY)
            .leavesQty(d.leavesQty(), 0)
            .cumQty(d.cumQty(), 0)
            .transactTime(ts.buffer(), tsLen)
            .lmbShardID(shardId)
            .lmbEventSeq(eventSeq)
            .lmbFirmID((int)d.firmId())
            .lmbSessionID(d.sessionId());
        er.instrument().symbol(tickers.apply((int)d.symbolId()));
        er.orderQtyData().orderQty(d.orderQty(), 0);
        if (d.origClOrdId() != 0)
        {
            er.origClOrdID(Long.toString(d.origClOrdId()));
        }
        if (d.price() != ExecutionReportDecoder.priceNullValue())
        {
            er.price(d.price(), 2);
        }
        if (d.lastQty() != 0)
        {
            er.lastQty(d.lastQty(), 0);
            er.lastPx(d.lastPx(), 2);
            er.trdMatchID(Long.toString(d.matchId()));
            switch (d.liquidity())
            {
                case MAKER -> er.lastLiquidityInd(LastLiquidityInd.ADDED_LIQUIDITY);
                case TAKER -> er.lastLiquidityInd(LastLiquidityInd.REMOVED_LIQUIDITY);
                case AUCTION -> er.lastLiquidityInd(LastLiquidityInd.AUCTION);
                default ->
                {
                }
            }
        }
        if (d.accountId() != 0)
        {
            er.account(Long.toString(d.accountId()));
        }
        if (d.cancelReason() != io.lmb.sbe.CancelReason.NONE && d.cancelReason() != io.lmb.sbe.CancelReason.NULL_VAL)
        {
            er.text(d.cancelReason().name());
        }
        if (c.session.trySend(er) < 0)
        {
            metrics.dropCopyBackPressure.incrementAndGet();
            return false;
        }
        metrics.dropCopyMessages.incrementAndGet();
        return true;
    }

    private static io.lmb.fix.ExecType fixExecType(final ExecType t)
    {
        return switch (t)
        {
            case NEW -> io.lmb.fix.ExecType.NEW;
            case PARTIAL_FILL, FILL -> io.lmb.fix.ExecType.TRADE;
            case CANCELED -> io.lmb.fix.ExecType.CANCELED;
            case REPLACED -> io.lmb.fix.ExecType.REPLACED;
            case EXPIRED -> io.lmb.fix.ExecType.EXPIRED;
            default -> io.lmb.fix.ExecType.REJECTED;
        };
    }

    private static io.lmb.fix.OrdStatus fixOrdStatus(final OrdStatus s)
    {
        return switch (s)
        {
            case NEW -> io.lmb.fix.OrdStatus.NEW;
            case PARTIALLY_FILLED -> io.lmb.fix.OrdStatus.PARTIALLY_FILLED;
            case FILLED -> io.lmb.fix.OrdStatus.FILLED;
            case CANCELED -> io.lmb.fix.OrdStatus.CANCELED;
            case EXPIRED -> io.lmb.fix.OrdStatus.EXPIRED;
            default -> io.lmb.fix.OrdStatus.REJECTED;
        };
    }

    // ------------------------------------------------------------------ sessions

    private SessionHandler onSessionAcquired(final Session session, final SessionAcquiredInfo info)
    {
        final String compId = session.compositeKey().remoteCompId();
        final DropCopyEngine.Logon logon = engine.pendingLogons.remove(compId);
        final ClientHandler handler = new ClientHandler();
        if (logon == null || clients.containsKey(compId) && clients.get(compId).session != null)
        {
            LOG.info("drop copy session {} refused (no logon record or already connected)", compId);
            session.logoutAndDisconnect();
            return handler;
        }
        final Client c = client(compId);
        c.firmId = logon.firmId();
        c.session = session;
        if (logon.resumeFrom() != null && !logon.resumeFrom().isBlank())
        {
            applyResumeFrom(c, logon.resumeFrom());
        }
        for (int s = 0; s < shards; s++)
        {
            c.catchUp[s] = true;
        }
        c.retryAtMs = 0;
        handler.client = c;
        online.add(c);
        dirty = true;
        metrics.dropCopySessions.set(online.size());
        LOG.info("drop copy session {} (firm {}) from cursors {}", compId,
            c.firmId == DropCopyEngine.ALL_FIRMS ? "ALL" : c.firmId, java.util.Arrays.toString(c.cursor));
        return handler;
    }

    /** "shard:eventSeq;shard:eventSeq" (',' also accepted); malformed entries are ignored. */
    static void applyResumeFrom(final Client c, final String text)
    {
        for (final String part : text.split("[;,]"))
        {
            final int colon = part.indexOf(':');
            try
            {
                final int s = Integer.parseInt(part.substring(0, Math.max(colon, 0)).trim());
                final long seq = Long.parseLong(part.substring(colon + 1).trim());
                if (s >= 0 && s < c.cursor.length && seq >= 0)
                {
                    c.cursor[s] = seq;
                }
            }
            catch (final RuntimeException ignore)
            {
                // skip
            }
        }
    }

    private void offline(final Client c)
    {
        if (c == null || c.session == null)
        {
            return;
        }
        c.session = null;
        for (int s = 0; s < shards; s++)
        {
            if (c.reader[s] != null)
            {
                CloseHelper.quietClose(c.reader[s]::close);
                c.reader[s] = null;
            }
        }
        online.remove(c);
        metrics.dropCopySessions.set(online.size());
        dirty = true;
        LOG.info("drop copy session {} disconnected at cursors {}", c.compId, java.util.Arrays.toString(c.cursor));
    }

    private final class ClientHandler implements SessionHandler
    {
        Client client;

        @Override
        public Action onMessage(
            final DirectBuffer buffer, final int offset, final int length, final int libraryId, final Session session,
            final int sequenceIndex, final long messageType, final long timestampInNs, final long position,
            final OnMessageInfo messageInfo)
        {
            return Action.CONTINUE; // a drop copy is send-only; session-level messages are handled by Artio
        }

        @Override
        public void onTimeout(final int libraryId, final Session session)
        {
        }

        @Override
        public void onSlowStatus(final int libraryId, final Session session, final boolean hasBecomeSlow)
        {
        }

        @Override
        public Action onDisconnect(final int libraryId, final Session session, final DisconnectReason reason)
        {
            offline(client);
            return Action.CONTINUE;
        }

        @Override
        public void onSessionStart(final Session session)
        {
        }
    }

    // ------------------------------------------------------------------ checkpoint and close

    private void writeCheckpoint(final boolean clean)
    {
        final Map<String, String> m = new LinkedHashMap<>();
        m.put("clean", Boolean.toString(clean));
        for (int s = 0; s < shards; s++)
        {
            m.put("live." + s, Long.toString(live[s]));
        }
        for (final Client c : clients.values())
        {
            for (int s = 0; s < shards; s++)
            {
                m.put("session." + c.compId + "." + s, Long.toString(c.cursor[s]));
            }
        }
        try
        {
            CursorFile.write(checkpointFile, m);
        }
        catch (final java.io.IOException e)
        {
            throw new java.io.UncheckedIOException(e);
        }
        dirty = false;
    }

    @Override
    public void close(final boolean clean)
    {
        for (final Client c : new ArrayList<>(online))
        {
            final Session s = c.session;
            offline(c);
            s.logoutAndDisconnect();
        }
        if (library != null)
        {
            final long until = System.currentTimeMillis() + 200;
            while (System.currentTimeMillis() < until)
            {
                library.poll(10); // let the logouts reach the engine
            }
            library = null; // owned by the engine: reconnecting would wait for the old library's timeout
        }
        if (clean && live != null)
        {
            try
            {
                writeCheckpoint(true);
            }
            catch (final RuntimeException e)
            {
                LOG.warn("drop copy: final checkpoint failed", e);
            }
        }
    }

    /** Package-private for tests: the cursors of a client, or null. */
    long[] cursors(final String compId)
    {
        final Client c = clients.get(compId);
        return c == null ? null : c.cursor.clone();
    }

    static final class Client
    {
        final String compId;
        final long[] cursor;
        final boolean[] catchUp;
        final EventStreamClient[] reader;
        int firmId;
        Session session;
        long retryAtMs;
        /** Present in the checkpoint at open: may have received reports the checkpoint does not cover. */
        boolean known;

        Client(final String compId, final int shards)
        {
            this.compId = compId;
            this.cursor = new long[shards];
            this.catchUp = new boolean[shards];
            this.reader = new EventStreamClient[shards];
        }

        boolean wants(final long reportFirmId)
        {
            return firmId == DropCopyEngine.ALL_FIRMS || firmId == reportFirmId;
        }
    }
}
