package io.lmb.gateway.core;

import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.common.Events;
import io.lmb.common.FirmDirectory;
import io.lmb.common.SymbolDirectory;
import io.lmb.gateway.GatewayConfig;
import io.lmb.sbe.BooleanType;
import io.lmb.sbe.CancelOrderEncoder;
import io.lmb.sbe.CancelReason;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.ExecutionReportEncoder;
import io.lmb.sbe.MassCancelDoneDecoder;
import io.lmb.sbe.MassCancelEncoder;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.NewOrderEncoder;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.ReplaceOrderEncoder;
import io.lmb.sbe.SymbolDefinedDecoder;
import io.lmb.sbe.SymbolStateChangedDecoder;
import io.lmb.sbe.TimeInForce;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * The protocol-independent heart of the gateway. One instance runs as an Agrona {@link Agent} on a
 * single dedicated thread (the "core thread") and owns, without locks: every shard's
 * {@code CommandClient} and live {@code EventStreamClient}, the session registry, sequence numbers,
 * token buckets, the per-shard lanes and in-flight buffers, per-session catch-up readers, the order
 * cache and the symbol states. Front ends talk to it in two ways: from foreign threads (gRPC) with
 * {@link #submit(Runnable)}, a lock-free MPSC task queue drained by the core; or directly from the
 * core thread (the Artio FIX library is polled by the core as a {@link Poller}).
 */
public final class GatewayCore implements Agent
{
    private static final Logger LOG = LoggerFactory.getLogger(GatewayCore.class);
    private static final int TASK_LIMIT = 512;
    private static final int LIVE_POLL_LIMIT = 256;
    private static final int CATCHUP_POLL_LIMIT = 64;
    private static final long TIMER_PERIOD_NS = 5_000_000L;
    private static final int HEADER = MessageHeaderEncoder.ENCODED_LENGTH;

    /** Something else the core thread must poll (the FIX library). */
    public interface Poller
    {
        default void onStart()
        {
        }

        int poll(long nowNs);

        default void onClose()
        {
        }
    }

    /** Input to {@link #logon}. */
    public static final class LogonRequest
    {
        public int firmId;
        public long sessionId;
        public String apiKey;
        /** Set when the transport already authenticated the credentials (FIX: Artio's AuthenticationStrategy). */
        public boolean preAuthenticated;
        public boolean cancelOnDisconnect;
        public long nextClientSeq = 1;
        /** Explicit resume cursors: shard ids and event seqs, same length. Null = none. */
        public int[] resumeShards;
        public long[] resumeSeqs;
        /** Without explicit cursors, continue from what this gateway delivered last time (FIX). */
        public boolean resumeFromOwnCursors;
    }

    /** Snapshot for OrderQuery.GetSessionStatus. */
    public record SessionStatusSnapshot(
        boolean connected, int gatewayId, long lastClientSeq, long lastServerSeq, long[] cursors)
    {
    }

    public record SymbolSnapshot(SymbolDirectory.Symbol symbol, String state)
    {
    }

    private final GatewayConfig cfg;
    private final VenueClients venue;
    private final LiveSymbols symbols;
    private final FirmDirectory firms;
    private final GatewayMetrics metrics;
    private final EpochNanoClock epochClock = new OffsetEpochNanoClock();
    private final ManyToOneConcurrentLinkedQueue<Runnable> tasks = new ManyToOneConcurrentLinkedQueue<>();
    private final List<Poller> pollers = new ArrayList<>();
    private final ShardChannel[] shards;
    private final LiveHandler[] liveHandlers;
    private final Long2ObjectHashMap<SessionState> sessions = new Long2ObjectHashMap<>();
    private final List<CatchupHandler> catchups = new ArrayList<>();
    private final OrderCache orderCache;
    private final Int2ObjectHashMap<String> symbolStates = new Int2ObjectHashMap<>();
    private final ReportView reportView;

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ExecutionReportDecoder erDecoder = new ExecutionReportDecoder();
    private final MassCancelDoneDecoder mcdDecoder = new MassCancelDoneDecoder();
    private final SymbolStateChangedDecoder sscDecoder = new SymbolStateChangedDecoder();
    private final SymbolDefinedDecoder sdDecoder = new SymbolDefinedDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final NewOrderEncoder newOrderEncoder = new NewOrderEncoder();
    private final CancelOrderEncoder cancelEncoder = new CancelOrderEncoder();
    private final ReplaceOrderEncoder replaceEncoder = new ReplaceOrderEncoder();
    private final MassCancelEncoder massCancelEncoder = new MassCancelEncoder();
    private final UnsafeBuffer cachedReportBuffer = new UnsafeBuffer(new byte[0]);

    private volatile boolean ready;
    private volatile boolean started;
    private long startNs;
    private long nextTimerNs;
    private long nextMetricsNs;
    private long internalRequestIds;
    private int connectedSessions;
    private boolean shuttingDown;
    private long shutdownDeadlineNs;
    private final CountDownLatch shutdownFlushed = new CountDownLatch(1);
    private final CountDownLatch startedLatch = new CountDownLatch(1);
    private volatile Throwable startFailure;

    public GatewayCore(
        final GatewayConfig cfg, final VenueClients venue, final SymbolDirectory symbols, final FirmDirectory firms,
        final GatewayMetrics metrics)
    {
        this.cfg = cfg;
        this.venue = venue;
        this.symbols = new LiveSymbols(symbols);
        this.firms = firms;
        this.metrics = metrics;
        this.orderCache = new OrderCache(cfg.orderCacheMax);
        this.reportView = new ReportView(this.symbols);
        final int n = venue.shardCount();
        this.shards = new ShardChannel[n];
        this.liveHandlers = new LiveHandler[n];
        for (int s = 0; s < n; s++)
        {
            shards[s] = new ShardChannel(s);
            liveHandlers[s] = new LiveHandler(shards[s]);
        }
    }

    // ------------------------------------------------------------------ wiring

    /** Registers a poller; call before the agent starts. */
    public void addPoller(final Poller poller)
    {
        pollers.add(poller);
    }

    /** Thread-safe: runs the task on the core thread. */
    public void submit(final Runnable task)
    {
        tasks.offer(task);
    }

    public int gatewayId()
    {
        return cfg.gatewayId;
    }

    public GatewayConfig config()
    {
        return cfg;
    }

    public LiveSymbols symbols()
    {
        return symbols;
    }

    public FirmDirectory firms()
    {
        return firms;
    }

    public GatewayMetrics metrics()
    {
        return metrics;
    }

    public int shardCount()
    {
        return shards.length;
    }

    /** Epoch nanoseconds, the time base of gatewayInTsNs. Thread-safe. */
    public long epochNs()
    {
        return epochClock.nanoTime();
    }

    /** True once the live event streams have caught up and logons are accepted. Thread-safe. */
    public boolean isReady()
    {
        return ready;
    }

    /** Waits until the core thread has started (clients created). Rethrows a start failure. */
    public void awaitStarted(final long timeoutMs) throws InterruptedException
    {
        if (!startedLatch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS))
        {
            throw new IllegalStateException("gateway core did not start within " + timeoutMs + " ms");
        }
        if (startFailure != null)
        {
            throw new IllegalStateException("gateway core failed to start", startFailure);
        }
    }

    // ------------------------------------------------------------------ agent

    @Override
    public String roleName()
    {
        return "gateway-core-" + cfg.gatewayId;
    }

    @Override
    public void onStart()
    {
        try
        {
            final long from = cfg.streamFromStart ? 0 : VenueClients.LATEST;
            for (final ShardChannel ch : shards)
            {
                ch.commands = venue.commandClient(ch.shardId);
                ch.live = venue.eventStream(ch.shardId, from);
                ch.connected = ch.commands.isConnected();
            }
            for (final Poller p : pollers)
            {
                p.onStart();
            }
            startNs = System.nanoTime();
            started = true;
        }
        catch (final Throwable t)
        {
            startFailure = t;
            throw t;
        }
        finally
        {
            startedLatch.countDown();
        }
    }

    @Override
    public int doWork()
    {
        if (!started)
        {
            return 0;
        }
        final long now = System.nanoTime();
        int work = drainTasks();
        for (int i = 0; i < pollers.size(); i++)
        {
            work += pollers.get(i).poll(now);
        }
        for (final ShardChannel ch : shards)
        {
            work += pollShard(ch, now);
        }
        work += pollCatchups(now);
        if (now >= nextTimerNs)
        {
            nextTimerNs = now + TIMER_PERIOD_NS;
            work += onTimer(now);
        }
        return work;
    }

    @Override
    public void onClose()
    {
        for (final CatchupHandler h : catchups)
        {
            h.closeReader();
        }
        catchups.clear();
        for (final Poller p : pollers)
        {
            try
            {
                p.onClose();
            }
            catch (final RuntimeException e)
            {
                LOG.warn("poller close failed", e);
            }
        }
        for (final ShardChannel ch : shards)
        {
            if (ch.live != null)
            {
                ch.live.close();
            }
            if (ch.commands != null)
            {
                ch.commands.close();
            }
        }
    }

    private int drainTasks()
    {
        int n = 0;
        Runnable r;
        while (n < TASK_LIMIT && (r = tasks.poll()) != null)
        {
            try
            {
                r.run();
            }
            catch (final RuntimeException e)
            {
                LOG.error("core task failed", e);
            }
            n++;
        }
        return n;
    }

    // ------------------------------------------------------------------ shutdown

    /**
     * Thread-safe. Disconnects every session (cancel-on-disconnect applies), then lets the core keep
     * running until the resulting commands are offered or the flush deadline passes.
     */
    public void beginShutdown()
    {
        submit(() ->
        {
            shuttingDown = true;
            shutdownDeadlineNs = System.nanoTime() + cfg.shutdownFlushMs * 1_000_000L;
            final List<SessionState> connected = new ArrayList<>();
            sessions.values().forEach(s ->
            {
                if (s.outlet != null)
                {
                    connected.add(s);
                }
            });
            for (final SessionState s : connected)
            {
                final SessionOutlet o = s.outlet;
                detach(s);
                o.disconnect(Reject.UNAVAILABLE, "gateway shutting down");
            }
        });
    }

    public boolean awaitShutdownFlushed(final long timeoutMs) throws InterruptedException
    {
        return shutdownFlushed.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private void checkShutdown(final long now)
    {
        boolean empty = true;
        for (final ShardChannel ch : shards)
        {
            empty &= ch.urgent.isEmpty() && ch.normal.isEmpty();
        }
        if (empty || now >= shutdownDeadlineNs)
        {
            shutdownFlushed.countDown();
        }
    }

    // ------------------------------------------------------------------ logon / logoff

    /**
     * Logs a session on. Core thread. Returns null on success (the outlet received
     * {@link SessionOutlet#onLogonAccepted}), otherwise the reason; the caller reports it.
     */
    public Reject logon(final LogonRequest req, final SessionOutlet outlet)
    {
        if (!ready || shuttingDown)
        {
            return Reject.UNAVAILABLE;
        }
        final FirmDirectory.Firm firm = firms.byId(req.firmId);
        if (firm == null || !firm.sessionIds().contains(req.sessionId) ||
            (!req.preAuthenticated && !constantTimeEquals(firm.apiKey(), req.apiKey)))
        {
            return Reject.AUTH_FAILED;
        }
        SessionState s = sessions.get(req.sessionId);
        if (s != null && s.outlet != null)
        {
            return Reject.DUPLICATE_SESSION;
        }
        final long now = System.nanoTime();
        final boolean known = s != null;
        if (s == null)
        {
            s = new SessionState(req.sessionId, req.firmId, shards.length);
            sessions.put(req.sessionId, s);
        }
        s.nextExpectedClientSeq = known && s.nextExpectedClientSeq > 0 ? s.nextExpectedClientSeq : Math.max(1, req.nextClientSeq);
        s.cancelOnDisconnect = req.cancelOnDisconnect;
        s.bucket = new TokenBucket(cfg.throttleRatePerSec, cfg.throttleBurst, now);
        s.outlet = outlet;
        s.connectedAtNs = now;
        s.lastInboundNs = now;
        s.lastOutboundNs = now;
        s.notWritableSinceNs = 0;
        s.massCancels.clear();

        final boolean ownCursors = known && req.resumeFromOwnCursors && req.resumeShards == null;
        for (int sh = 0; sh < shards.length; sh++)
        {
            final long liveEdge = shards[sh].live.lastEventSeq();
            s.possDupUpTo[sh] = 0;
            s.catchingUp[sh] = false;
            if (!ownCursors)
            {
                s.cursor[sh] = liveEdge;
            }
            else if (s.cursor[sh] < liveEdge)
            {
                startCatchup(s, sh, s.cursor[sh]); // never delivered: not poss dup
            }
        }
        if (req.resumeShards != null)
        {
            for (int i = 0; i < req.resumeShards.length; i++)
            {
                final int sh = req.resumeShards[i];
                if (sh < 0 || sh >= shards.length)
                {
                    continue;
                }
                final long from = Math.max(0, req.resumeSeqs[i]);
                final long liveEdge = shards[sh].live.lastEventSeq();
                if (from < liveEdge)
                {
                    s.possDupUpTo[sh] = liveEdge;
                    startCatchup(s, sh, from);
                }
                else
                {
                    s.cursor[sh] = from;
                }
            }
        }
        connectedSessions++;
        metrics.sessions(connectedSessions);
        LOG.info("session {} (firm {}) logged on via {}, next client seq {}", s.sessionId, s.firmId,
            outlet.transport(), s.nextExpectedClientSeq);
        outlet.onLogonAccepted(s, s.nextExpectedClientSeq, cfg.heartbeatIntervalMs);
        return null;
    }

    /** The transport went away (client close, network, logout). Core thread. Idempotent. */
    public void onTransportClosed(final SessionState s, final SessionOutlet outlet)
    {
        if (s != null && s.outlet == outlet)
        {
            detach(s);
        }
    }

    /** Core-initiated disconnect (heartbeat timeout, slow consumer). */
    private void disconnect(final SessionState s, final Reject reason, final String text)
    {
        final SessionOutlet o = s.outlet;
        if (o == null)
        {
            return;
        }
        detach(s);
        o.disconnect(reason, text);
    }

    private void detach(final SessionState s)
    {
        LOG.info("session {} disconnected", s.sessionId);
        for (int sh = 0; sh < shards.length; sh++)
        {
            if (s.catchingUp[sh])
            {
                final CatchupHandler h = s.catchupHandler[sh];
                if (h != null)
                {
                    h.closeReader();
                    catchups.remove(h);
                }
            }
            else
            {
                s.cursor[sh] = Math.max(s.cursor[sh], shards[sh].live.lastEventSeq());
            }
        }
        s.outlet = null;
        s.massCancels.clear();
        connectedSessions--;
        metrics.sessions(connectedSessions);
        metrics.catchups(catchups.size());
        if (s.cancelOnDisconnect)
        {
            for (final ShardChannel ch : shards)
            {
                final InFlight f = new InFlight();
                f.kind = OrderRequest.Kind.MASS_CANCEL;
                f.internal = true;
                f.urgent = true;
                f.firmId = s.firmId;
                f.sessionId = s.sessionId;
                f.requestId = -(++internalRequestIds);
                massCancelEncoder.wrapAndApplyHeader(f.buffer, 0, headerEncoder)
                    .requestId(f.requestId).sessionId(s.sessionId).gatewayInTsNs(epochNs())
                    .firmId(s.firmId).symbolId(0).gatewayId(cfg.gatewayId).targetGatewayId(0)
                    .scope(MassCancelScope.SESSION).reason(CancelReason.DISCONNECT);
                f.length = HEADER + massCancelEncoder.encodedLength();
                enqueue(ch, f, System.nanoTime());
                metrics.cancelOnDisconnect();
            }
        }
    }

    // ------------------------------------------------------------------ inbound requests

    public static final int SEQ_OK = 0;
    public static final int SEQ_POSSIBLE_RESEND = 1;
    public static final int SEQ_GAP = -1;

    /**
     * Client sequence check (gRPC; FIX sequence numbers are Artio's). Exactly the next expected
     * number advances it; a lower one is a possible resend (processed with possResend=TRUE); a
     * higher one is a gap (rejected, nothing advances).
     */
    public int checkClientSeq(final SessionState s, final long seq)
    {
        s.lastInboundNs = System.nanoTime();
        if (seq == s.nextExpectedClientSeq)
        {
            s.nextExpectedClientSeq++;
            return SEQ_OK;
        }
        return seq < s.nextExpectedClientSeq ? SEQ_POSSIBLE_RESEND : SEQ_GAP;
    }

    /** Liveness signal without a sequence number (heartbeats). */
    public void onClientActivity(final SessionState s)
    {
        s.lastInboundNs = System.nanoTime();
    }

    /** Stateless checks, throttle, routing, submission. Core thread. */
    public void handleRequest(final SessionState s, final OrderRequest req, final boolean possibleResend)
    {
        if (s.outlet == null)
        {
            return;
        }
        final long now = System.nanoTime();
        s.lastInboundNs = now;
        metrics.request(req.kind);
        final SymbolDirectory.Symbol sym = req.symbol == null || req.symbol.isEmpty() ? null : symbols.byTicker(req.symbol);
        switch (req.kind)
        {
            case NEW ->
            {
                if (req.clOrdId <= 0)
                {
                    reject(s, req, Reject.INVALID_FIELD, "cl_ord_id must be a positive integer");
                }
                else if (sym == null)
                {
                    reject(s, req, Reject.UNKNOWN_SYMBOL, "unknown symbol '" + req.symbol + "'");
                }
                else if (req.side == null)
                {
                    reject(s, req, Reject.INVALID_FIELD, "side is required");
                }
                else if (req.ordType == null)
                {
                    reject(s, req, Reject.INVALID_FIELD, "order type is required");
                }
                else if (req.quantity <= 0)
                {
                    reject(s, req, Reject.INVALID_FIELD, "quantity must be positive");
                }
                else if (req.quantity > sym.maxOrderQty())
                {
                    reject(s, req, Reject.MAX_ORDER_SIZE, "quantity above " + sym.maxOrderQty());
                }
                else if (req.ordType == OrdType.LIMIT && req.priceCents <= 0)
                {
                    reject(s, req, Reject.INVALID_FIELD, "limit order needs a positive price");
                }
                else if ("REMOVED".equals(symbolStates.get(sym.symbolId())))
                {
                    reject(s, req, Reject.SYMBOL_NOT_TRADING, "symbol removed");
                }
                else if (!s.bucket.tryAcquire(now))
                {
                    reject(s, req, Reject.THROTTLED, "order rate above " + cfg.throttleRatePerSec + "/s");
                }
                else
                {
                    submitOrder(s, req, sym, possibleResend, now);
                }
            }
            case CANCEL, REPLACE ->
            {
                if (req.clOrdId <= 0 || req.origClOrdId <= 0)
                {
                    reject(s, req, Reject.INVALID_FIELD, "cl_ord_id and orig_cl_ord_id must be positive integers");
                }
                else if (sym == null)
                {
                    reject(s, req, Reject.UNKNOWN_SYMBOL, "unknown symbol '" + req.symbol + "'");
                }
                else if (req.kind == OrderRequest.Kind.REPLACE && req.quantity <= 0)
                {
                    reject(s, req, Reject.INVALID_FIELD, "new quantity must be positive");
                }
                else if (req.kind == OrderRequest.Kind.REPLACE && req.quantity > sym.maxOrderQty())
                {
                    reject(s, req, Reject.MAX_ORDER_SIZE, "quantity above " + sym.maxOrderQty());
                }
                else if (req.kind == OrderRequest.Kind.REPLACE && req.priceCents < 0)
                {
                    reject(s, req, Reject.INVALID_FIELD, "price must not be negative");
                }
                else if (req.kind == OrderRequest.Kind.REPLACE && !s.bucket.tryAcquire(now))
                {
                    reject(s, req, Reject.THROTTLED, "order rate above " + cfg.throttleRatePerSec + "/s");
                }
                else
                {
                    submitOrder(s, req, sym, possibleResend, now);
                }
            }
            case MASS_CANCEL ->
            {
                if (req.symbol != null && !req.symbol.isEmpty() && sym == null)
                {
                    reject(s, req, Reject.UNKNOWN_SYMBOL, "unknown symbol '" + req.symbol + "'");
                }
                else
                {
                    submitMassCancel(s, req, sym, now);
                }
            }
        }
    }

    private void reject(final SessionState s, final OrderRequest req, final Reject reason, final String text)
    {
        metrics.reject(reason);
        sendReject(s, reason, req.clientSeq, req.kind == OrderRequest.Kind.MASS_CANCEL ? req.requestId : req.clOrdId, text);
    }

    public void sendReject(final SessionState s, final Reject reason, final long refSeq, final long refId, final String text)
    {
        if (s.outlet != null)
        {
            s.outlet.sendReject(reason, refSeq, refId, text);
            s.lastOutboundNs = System.nanoTime();
        }
    }

    /** Counts a reject produced by a front end (bad sequence, not logged on, ...). */
    public void countReject(final Reject reason)
    {
        metrics.reject(reason);
    }

    private void submitOrder(
        final SessionState s, final OrderRequest req, final SymbolDirectory.Symbol sym, final boolean possibleResend,
        final long now)
    {
        final ShardChannel ch = shards[sym.shard()];
        if (possibleResend)
        {
            final OrderCache.Entry known = orderCache.get(s.firmId, req.clOrdId);
            if (known != null)
            {
                // Outcome already in the stream: never resubmit; repeat the latest report to its owner.
                metrics.resendFromCache();
                if (known.sessionId == s.sessionId)
                {
                    cachedReportBuffer.wrap(known.lastReport);
                    reportView.wrap(cachedReportBuffer, 0, known.lastReport.length);
                    if (s.outlet.sendExecutionReport(reportView, true))
                    {
                        metrics.reportOut(true);
                    }
                }
                return;
            }
        }
        final InFlight existing = ch.active(s.firmId, req.clOrdId);
        if (existing != null)
        {
            if (!possibleResend)
            {
                reject(s, req, Reject.INVALID_FIELD, "a request with cl_ord_id " + req.clOrdId + " is already in flight");
            }
            return;
        }
        if (!ch.connected)
        {
            reject(s, req, Reject.UNAVAILABLE, "shard " + ch.shardId + " unavailable");
            return;
        }
        final boolean isCancel = req.kind == OrderRequest.Kind.CANCEL;
        final int limit = isCancel ? cfg.inflightMax + cfg.cancelHeadroom : cfg.inflightMax;
        if (ch.depth() >= limit)
        {
            reject(s, req, Reject.UNAVAILABLE, "shard " + ch.shardId + " in-flight queue full");
            return;
        }
        final InFlight f = new InFlight();
        f.kind = req.kind;
        f.firmId = s.firmId;
        f.clOrdId = req.clOrdId;
        f.sessionId = s.sessionId;
        f.clientSeq = req.clientSeq;
        f.session = s;
        final BooleanType possResend = possibleResend ? BooleanType.TRUE : BooleanType.FALSE;
        switch (req.kind)
        {
            case NEW ->
            {
                newOrderEncoder.wrapAndApplyHeader(f.buffer, 0, headerEncoder)
                    .sessionId(s.sessionId).clOrdId(req.clOrdId)
                    .price(req.ordType == OrdType.MARKET ? NewOrderEncoder.priceNullValue() : req.priceCents)
                    .quantity(req.quantity).gatewayInTsNs(req.gatewayInTsNs).firmId(s.firmId)
                    .accountId(req.accountId).symbolId(sym.symbolId()).gatewayId(cfg.gatewayId)
                    .side(req.side).ordType(req.ordType)
                    .timeInForce(req.timeInForce == null ? TimeInForce.DAY : req.timeInForce)
                    .possResend(possResend);
                f.length = HEADER + newOrderEncoder.encodedLength();
                f.possResendOffset = HEADER + NewOrderEncoder.possResendEncodingOffset();
            }
            case CANCEL ->
            {
                cancelEncoder.wrapAndApplyHeader(f.buffer, 0, headerEncoder)
                    .sessionId(s.sessionId).clOrdId(req.clOrdId).origClOrdId(req.origClOrdId)
                    .gatewayInTsNs(req.gatewayInTsNs).firmId(s.firmId).symbolId(sym.symbolId())
                    .gatewayId(cfg.gatewayId).possResend(possResend);
                f.length = HEADER + cancelEncoder.encodedLength();
                f.possResendOffset = HEADER + CancelOrderEncoder.possResendEncodingOffset();
            }
            case REPLACE ->
            {
                replaceEncoder.wrapAndApplyHeader(f.buffer, 0, headerEncoder)
                    .sessionId(s.sessionId).clOrdId(req.clOrdId).origClOrdId(req.origClOrdId)
                    .newPrice(req.priceCents > 0 ? req.priceCents : ReplaceOrderEncoder.newPriceNullValue())
                    .newQuantity(req.quantity).gatewayInTsNs(req.gatewayInTsNs).firmId(s.firmId)
                    .symbolId(sym.symbolId()).gatewayId(cfg.gatewayId).possResend(possResend);
                f.length = HEADER + replaceEncoder.encodedLength();
                f.possResendOffset = HEADER + ReplaceOrderEncoder.possResendEncodingOffset();
            }
            default -> throw new IllegalStateException();
        }
        // A cancel jumps the queue, unless its order is itself still queued (it would arrive first and miss).
        f.urgent = isCancel && !ch.pendingInNormalLane(s.firmId, req.origClOrdId);
        enqueue(ch, f, now);
    }

    private void submitMassCancel(final SessionState s, final OrderRequest req, final SymbolDirectory.Symbol sym, final long now)
    {
        final List<ShardChannel> targets = new ArrayList<>();
        if (sym != null)
        {
            targets.add(shards[sym.shard()]);
        }
        else
        {
            for (final ShardChannel ch : shards)
            {
                targets.add(ch);
            }
        }
        final SessionState.MassCancelAggregate agg = new SessionState.MassCancelAggregate(req.requestId, 0);
        for (final ShardChannel ch : targets)
        {
            final ShardChannel.MassKey key = new ShardChannel.MassKey(s.sessionId, req.requestId);
            if (ch.massActive.containsKey(key))
            {
                continue; // the same mass cancel is already on its way to this shard
            }
            if (!ch.connected || ch.depth() >= cfg.inflightMax + cfg.cancelHeadroom)
            {
                metrics.reject(Reject.UNAVAILABLE);
                sendReject(s, Reject.UNAVAILABLE, req.clientSeq, req.requestId,
                    "mass cancel not delivered to shard " + ch.shardId + " (unavailable)");
                continue;
            }
            final InFlight f = new InFlight();
            f.kind = OrderRequest.Kind.MASS_CANCEL;
            f.firmId = s.firmId;
            f.sessionId = s.sessionId;
            f.requestId = req.requestId;
            f.clientSeq = req.clientSeq;
            f.session = s;
            massCancelEncoder.wrapAndApplyHeader(f.buffer, 0, headerEncoder)
                .requestId(req.requestId).sessionId(s.sessionId).gatewayInTsNs(req.gatewayInTsNs)
                .firmId(s.firmId).symbolId(sym == null ? 0 : sym.symbolId()).gatewayId(cfg.gatewayId)
                .targetGatewayId(0).scope(sym == null ? MassCancelScope.FIRM : MassCancelScope.FIRM_SYMBOL)
                .reason(CancelReason.MASS_CANCEL);
            f.length = HEADER + massCancelEncoder.encodedLength();
            // Orders of this firm still queued on the shard were sent before the mass cancel: keep that order.
            f.urgent = ch.pendingNormalByFirm.get(s.firmId) == 0;
            agg.pending++;
            enqueue(ch, f, now);
        }
        if (agg.pending > 0)
        {
            s.massCancels.put(req.requestId, agg);
        }
    }

    private void enqueue(final ShardChannel ch, final InFlight f, final long now)
    {
        f.enqueuedNs = now;
        if (f.kind == OrderRequest.Kind.MASS_CANCEL)
        {
            ch.massActive.put(new ShardChannel.MassKey(f.sessionId, f.requestId), f);
        }
        else
        {
            ch.putActive(f);
        }
        if (f.urgent)
        {
            ch.urgent.add(f);
        }
        else
        {
            ch.normal.add(f);
            ch.addPendingNormal(f.firmId, 1);
        }
        flush(ch, now);
    }

    /** Offers queued commands, cancels first. Stops at the first refused offer (back pressure). */
    private int flush(final ShardChannel ch, final long now)
    {
        if (!ch.connected)
        {
            return 0;
        }
        int n = 0;
        InFlight f;
        while ((f = ch.urgent.peek()) != null)
        {
            if (!ch.commands.offer(f.buffer, 0, f.length))
            {
                return n;
            }
            ch.urgent.poll();
            markSent(ch, f, now);
            n++;
        }
        while ((f = ch.normal.peek()) != null)
        {
            if (!ch.commands.offer(f.buffer, 0, f.length))
            {
                return n;
            }
            ch.normal.poll();
            ch.addPendingNormal(f.firmId, -1);
            markSent(ch, f, now);
            n++;
        }
        return n;
    }

    private void markSent(final ShardChannel ch, final InFlight f, final long now)
    {
        f.sent = true;
        f.attempts = 1;
        f.deadlineNs = now + cfg.inflightTimeoutMs * 1_000_000L;
        ch.sent.add(f);
        ch.sentCount++;
    }

    private void complete(final ShardChannel ch, final InFlight f)
    {
        f.done = true;
        ch.removeActive(f);
        if (f.sent)
        {
            ch.sentCount--;
        }
        else if (f.urgent)
        {
            ch.urgent.remove(f);
        }
        else
        {
            ch.normal.remove(f);
            ch.addPendingNormal(f.firmId, -1);
        }
    }

    // ------------------------------------------------------------------ shards

    private int pollShard(final ShardChannel ch, final long now)
    {
        int work = ch.commands.poll();
        final boolean connected = ch.commands.isConnected();
        if (connected && !ch.connected)
        {
            ch.connected = true;
            work += resubmitAll(ch, now);
        }
        ch.connected = connected;
        work += flush(ch, now);
        final int n = ch.live.poll(liveHandlers[ch.shardId], LIVE_POLL_LIMIT);
        if (!ch.drained && n < LIVE_POLL_LIMIT && ch.live.isConnected())
        {
            ch.drained = true;
        }
        return work + n;
    }

    /** After a reconnect every sent command may have been lost with the old leader: resend them all. */
    private int resubmitAll(final ShardChannel ch, final long now)
    {
        final List<InFlight> pending = new ArrayList<>(ch.sent);
        ch.sent.clear();
        int n = 0;
        for (final InFlight f : pending)
        {
            if (f.done)
            {
                continue;
            }
            f.markPossResend();
            if (ch.commands.offer(f.buffer, 0, f.length))
            {
                f.attempts++;
                metrics.resubmit(true);
                n++;
            }
            f.deadlineNs = now + cfg.inflightTimeoutMs * 1_000_000L;
            ch.sent.add(f);
        }
        if (n > 0)
        {
            LOG.info("shard {} reconnected: resubmitted {} in-flight commands", ch.shardId, n);
        }
        return n;
    }

    private int checkInFlight(final ShardChannel ch, final long now)
    {
        int work = 0;
        InFlight f;
        while ((f = ch.sent.peek()) != null)
        {
            if (f.done)
            {
                ch.sent.poll();
                continue;
            }
            if (f.deadlineNs > now || !ch.connected)
            {
                break;
            }
            if (f.attempts >= cfg.inflightMaxAttempts && !f.internal)
            {
                ch.sent.poll();
                f.done = true;
                ch.removeActive(f);
                ch.sentCount--;
                metrics.abandoned();
                LOG.warn("shard {}: no effect seen for {} {} of session {} after {} attempts; giving up",
                    ch.shardId, f.kind, f.kind == OrderRequest.Kind.MASS_CANCEL ? f.requestId : f.clOrdId,
                    f.sessionId, f.attempts);
                if (f.session != null && f.session.outlet != null)
                {
                    sendReject(f.session, Reject.UNAVAILABLE, f.clientSeq,
                        f.kind == OrderRequest.Kind.MASS_CANCEL ? f.requestId : f.clOrdId,
                        "no acknowledgement from shard " + ch.shardId + " after " + f.attempts +
                        " attempts; outcome unknown, query the order");
                }
                continue;
            }
            f.markPossResend();
            if (!ch.commands.offer(f.buffer, 0, f.length))
            {
                break;
            }
            ch.sent.poll();
            f.attempts++;
            f.deadlineNs = now + cfg.inflightTimeoutMs * 1_000_000L;
            ch.sent.add(f);
            metrics.resubmit(false);
            work++;
        }
        work += expireQueued(ch, ch.urgent, now);
        work += expireQueued(ch, ch.normal, now);
        return work;
    }

    /** Client commands that could not even be offered within sendTimeoutMs are refused explicitly. */
    private int expireQueued(final ShardChannel ch, final java.util.ArrayDeque<InFlight> lane, final long now)
    {
        final long timeoutNs = cfg.sendTimeoutMs * 1_000_000L;
        int n = 0;
        final Iterator<InFlight> it = lane.iterator();
        while (it.hasNext())
        {
            final InFlight f = it.next();
            if (now - f.enqueuedNs < timeoutNs)
            {
                break;
            }
            if (f.internal)
            {
                continue;
            }
            it.remove();
            f.done = true;
            ch.removeActive(f);
            if (!f.urgent)
            {
                ch.addPendingNormal(f.firmId, -1);
            }
            final SessionState s = f.session;
            if (s != null && s.outlet != null)
            {
                metrics.reject(Reject.UNAVAILABLE);
                sendReject(s, Reject.UNAVAILABLE, f.clientSeq,
                    f.kind == OrderRequest.Kind.MASS_CANCEL ? f.requestId : f.clOrdId,
                    "shard " + ch.shardId + " did not accept the command within " + cfg.sendTimeoutMs + " ms");
                if (f.kind == OrderRequest.Kind.MASS_CANCEL)
                {
                    final SessionState.MassCancelAggregate agg = s.massCancels.get(f.requestId);
                    if (agg != null && --agg.pending == 0)
                    {
                        s.massCancels.remove(f.requestId);
                    }
                }
            }
            n++;
        }
        return n;
    }

    /** Handler for a shard's live event stream: global bookkeeping, then delivery to the owning session. */
    final class LiveHandler implements EventStreamClient.EventHandler
    {
        private final ShardChannel ch;

        LiveHandler(final ShardChannel ch)
        {
            this.ch = ch;
        }

        @Override
        public void onEvent(final DirectBuffer buffer, final int offset, final int length)
        {
            final int templateId = Events.templateId(buffer, offset);
            switch (templateId)
            {
                case ExecutionReportDecoder.TEMPLATE_ID ->
                {
                    erDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    orderCache.onReport(erDecoder, buffer, offset, length);
                    onReportForInFlight(ch, erDecoder);
                    deliverLive(ch, erDecoder.sessionId(), buffer, offset, length);
                }
                case MassCancelDoneDecoder.TEMPLATE_ID ->
                {
                    mcdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    final InFlight f = ch.massActive.get(new ShardChannel.MassKey(mcdDecoder.sessionId(), mcdDecoder.requestId()));
                    if (f != null)
                    {
                        complete(ch, f);
                    }
                    deliverLive(ch, mcdDecoder.sessionId(), buffer, offset, length);
                }
                case SymbolStateChangedDecoder.TEMPLATE_ID ->
                {
                    sscDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    symbolStates.put((int)sscDecoder.symbolId(), sscDecoder.newState().name());
                }
                case SymbolDefinedDecoder.TEMPLATE_ID ->
                {
                    sdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    symbolStates.put((int)sdDecoder.symbolId(), sdDecoder.state().name());
                    symbols.onDefined(ch.shardId, (int)sdDecoder.symbolId(), sdDecoder.ticker().trim(), sdDecoder.tickSize(),
                        sdDecoder.lotSize(), sdDecoder.referencePrice(), (int)sdDecoder.priceBandBps(), sdDecoder.maxOrderQty());
                }
                default ->
                {
                }
            }
        }
    }

    private void onReportForInFlight(final ShardChannel ch, final ExecutionReportDecoder er)
    {
        final InFlight f = ch.active((int)er.firmId(), er.clOrdId());
        if (f == null || !f.sent)
        {
            return;
        }
        final ExecType t = er.execType();
        final boolean effect = switch (f.kind)
        {
            case NEW -> t == ExecType.NEW || t == ExecType.REJECTED;
            case CANCEL -> t == ExecType.CANCELED || t == ExecType.CANCEL_REJECTED || t == ExecType.REJECTED;
            case REPLACE -> t == ExecType.REPLACED || t == ExecType.CANCEL_REJECTED || t == ExecType.REJECTED;
            default -> false;
        };
        if (effect)
        {
            complete(ch, f);
        }
    }

    // ------------------------------------------------------------------ delivery

    private void deliverLive(final ShardChannel ch, final long sessionId, final DirectBuffer buffer, final int offset, final int length)
    {
        final SessionState s = sessions.get(sessionId);
        if (s == null || s.outlet == null || s.catchingUp[ch.shardId])
        {
            return;
        }
        final long seq = Events.eventSeq(buffer, offset);
        if (seq <= s.cursor[ch.shardId])
        {
            return;
        }
        if (!s.outlet.isWritable() || !deliver(s, ch.shardId, buffer, offset, length, seq))
        {
            startCatchup(s, ch.shardId, seq - 1); // the event stream becomes this session's buffer
            return;
        }
        s.cursor[ch.shardId] = seq;
    }

    /** @return false if the transport refused the report. */
    private boolean deliver(
        final SessionState s, final int shard, final DirectBuffer buffer, final int offset, final int length, final long seq)
    {
        final int templateId = Events.templateId(buffer, offset);
        if (templateId == ExecutionReportDecoder.TEMPLATE_ID)
        {
            final boolean possDup = seq <= s.possDupUpTo[shard];
            reportView.wrap(buffer, offset, length);
            if (!s.outlet.sendExecutionReport(reportView, possDup))
            {
                return false;
            }
            s.lastOutboundNs = System.nanoTime();
            metrics.reportOut(possDup);
            final ExecutionReportDecoder er = reportView.decoder();
            if (!possDup && er.gatewayInTsNs() != 0 && er.gatewayId() == cfg.gatewayId && er.sessionId() == s.sessionId &&
                er.liquidity() != io.lmb.sbe.LiquidityFlag.MAKER)
            {
                metrics.recordLatency(epochNs() - er.gatewayInTsNs());
            }
            return true;
        }
        if (templateId == MassCancelDoneDecoder.TEMPLATE_ID)
        {
            mcdDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
            final SessionState.MassCancelAggregate agg = s.massCancels.get(mcdDecoder.requestId());
            if (agg != null)
            {
                agg.pending--;
                agg.total += mcdDecoder.canceledCount();
                if (agg.pending <= 0)
                {
                    s.massCancels.remove(agg.requestId);
                }
                s.outlet.sendMassCancelAck(agg.requestId, shard, mcdDecoder.canceledCount(), Math.max(0, agg.pending), agg.total);
                s.lastOutboundNs = System.nanoTime();
            }
            return true;
        }
        return true;
    }

    private static boolean isForSession(final DirectBuffer buffer, final int offset, final long sessionId,
        final ExecutionReportDecoder er, final MassCancelDoneDecoder mcd, final MessageHeaderDecoder hdr)
    {
        final int templateId = Events.templateId(buffer, offset);
        if (templateId == ExecutionReportDecoder.TEMPLATE_ID)
        {
            return er.wrapAndApplyHeader(buffer, offset, hdr).sessionId() == sessionId;
        }
        if (templateId == MassCancelDoneDecoder.TEMPLATE_ID)
        {
            return mcd.wrapAndApplyHeader(buffer, offset, hdr).sessionId() == sessionId;
        }
        return false;
    }

    private void startCatchup(final SessionState s, final int shard, final long fromSeq)
    {
        s.cursor[shard] = fromSeq;
        if (s.catchingUp[shard])
        {
            return;
        }
        s.catchingUp[shard] = true;
        CatchupHandler h = s.catchupHandler[shard];
        if (h == null)
        {
            h = new CatchupHandler(s, shard);
            s.catchupHandler[shard] = h;
        }
        catchups.add(h);
        metrics.catchups(catchups.size());
    }

    /** A session's private reader on one shard, used to resume and to absorb slow consumers. */
    final class CatchupHandler implements EventStreamClient.EventHandler
    {
        final SessionState s;
        final int shard;
        EventStreamClient reader;
        boolean failed;
        private final ExecutionReportDecoder er = new ExecutionReportDecoder();
        private final MassCancelDoneDecoder mcd = new MassCancelDoneDecoder();
        private final MessageHeaderDecoder hdr = new MessageHeaderDecoder();

        CatchupHandler(final SessionState s, final int shard)
        {
            this.s = s;
            this.shard = shard;
        }

        @Override
        public void onEvent(final DirectBuffer buffer, final int offset, final int length)
        {
            if (failed)
            {
                return;
            }
            final long seq = Events.eventSeq(buffer, offset);
            if (seq <= s.cursor[shard])
            {
                return;
            }
            if (isForSession(buffer, offset, s.sessionId, er, mcd, hdr) &&
                !deliver(s, shard, buffer, offset, length, seq))
            {
                failed = true;
                return;
            }
            s.cursor[shard] = seq;
        }

        void closeReader()
        {
            if (reader != null)
            {
                reader.close();
                reader = null;
            }
        }
    }

    private int pollCatchups(final long now)
    {
        if (catchups.isEmpty())
        {
            return 0;
        }
        int work = 0;
        for (int i = catchups.size() - 1; i >= 0; i--)
        {
            final CatchupHandler h = catchups.get(i);
            final SessionState s = h.s;
            if (s.outlet == null)
            {
                continue;
            }
            if (!s.outlet.isWritable())
            {
                if (s.notWritableSinceNs == 0)
                {
                    s.notWritableSinceNs = now;
                }
                else if (now - s.notWritableSinceNs > cfg.slowConsumerTimeoutMs * 1_000_000L)
                {
                    metrics.slowConsumerDisconnect();
                    disconnect(s, Reject.UNAVAILABLE, "slow consumer: reports not read for " + cfg.slowConsumerTimeoutMs + " ms");
                }
                continue;
            }
            s.notWritableSinceNs = 0;
            if (h.reader == null)
            {
                h.reader = venue.eventStream(h.shard, s.cursor[h.shard]);
            }
            h.failed = false;
            final int n = h.reader.poll(h, CATCHUP_POLL_LIMIT);
            work += n;
            if (h.failed)
            {
                h.closeReader(); // reopen from the cursor once the transport drains
                continue;
            }
            final long liveEdge = shards[h.shard].live.lastEventSeq();
            if (h.reader.lastEventSeq() >= liveEdge)
            {
                s.cursor[h.shard] = Math.max(s.cursor[h.shard], h.reader.lastEventSeq());
                h.closeReader();
                s.catchingUp[h.shard] = false;
                catchups.remove(i);
                metrics.catchups(catchups.size());
                work++;
            }
        }
        return work;
    }

    // ------------------------------------------------------------------ timers

    private int onTimer(final long now)
    {
        int work = 0;
        for (final ShardChannel ch : shards)
        {
            work += checkInFlight(ch, now);
        }
        if (!ready)
        {
            boolean drained = true;
            for (final ShardChannel ch : shards)
            {
                drained &= ch.drained;
            }
            if (drained || now - startNs > cfg.bootstrapTimeoutMs * 1_000_000L)
            {
                if (!drained)
                {
                    LOG.warn("event streams not caught up after {} ms; accepting logons anyway", cfg.bootstrapTimeoutMs);
                }
                ready = true;
                LOG.info("gateway {} ready: order cache holds {} keys", cfg.gatewayId, orderCache.size());
            }
        }
        final long hbNs = cfg.heartbeatIntervalMs * 1_000_000L;
        List<SessionState> dead = null;
        for (final SessionState s : sessions.values())
        {
            if (s.outlet == null || !s.outlet.coreManagedHeartbeats())
            {
                continue;
            }
            if (now - s.lastInboundNs > hbNs * cfg.heartbeatTimeoutMultiplier)
            {
                if (dead == null)
                {
                    dead = new ArrayList<>();
                }
                dead.add(s);
            }
            else if (now - s.lastOutboundNs >= hbNs)
            {
                s.outlet.sendHeartbeat();
                s.lastOutboundNs = now;
            }
        }
        if (dead != null)
        {
            for (final SessionState s : dead)
            {
                disconnect(s, Reject.UNAVAILABLE, "heartbeat timeout");
            }
        }
        if (now >= nextMetricsNs)
        {
            nextMetricsNs = now + cfg.metricsIntervalMs * 1_000_000L;
            metrics.publishLatency();
        }
        for (final ShardChannel ch : shards)
        {
            metrics.inflightDepth(ch.shardId, ch.depth());
        }
        if (shuttingDown)
        {
            checkShutdown(now);
        }
        return work;
    }

    // ------------------------------------------------------------------ queries (core thread)

    /** Latest report bytes for the order, or null. */
    public byte[] lastReport(final int firmId, final long clOrdId)
    {
        final OrderCache.Entry e = orderCache.get(firmId, clOrdId);
        return e == null ? null : e.lastReport;
    }

    public SessionStatusSnapshot sessionStatus(final long sessionId, final long lastServerSeqIfUnknown)
    {
        final SessionState s = sessions.get(sessionId);
        if (s == null)
        {
            return new SessionStatusSnapshot(false, cfg.gatewayId, 0, lastServerSeqIfUnknown, new long[0]);
        }
        final long[] cursors = new long[shards.length];
        for (int sh = 0; sh < shards.length; sh++)
        {
            cursors[sh] = s.catchingUp[sh] || s.outlet == null ? s.cursor[sh] : Math.max(s.cursor[sh], shards[sh].live.lastEventSeq());
        }
        return new SessionStatusSnapshot(s.outlet != null, cfg.gatewayId, s.nextExpectedClientSeq - 1,
            s.outlet != null ? s.outlet.lastServerSeq() : lastServerSeqIfUnknown, cursors);
    }

    public List<SymbolSnapshot> symbolSnapshots()
    {
        final List<SymbolSnapshot> out = new ArrayList<>();
        for (final SymbolDirectory.Symbol sym : symbols.all())
        {
            out.add(new SymbolSnapshot(sym, symbolStates.getOrDefault(sym.symbolId(), "UNKNOWN")));
        }
        return out;
    }

    /** Wraps raw report bytes (from {@link #lastReport}) for an outlet-independent conversion. */
    public ReportView viewOf(final byte[] report)
    {
        final ReportView v = new ReportView(symbols);
        return v.wrap(new UnsafeBuffer(report), 0, report.length);
    }

    /** Firm owning a session, or null. Thread-safe (directory is immutable). */
    public FirmDirectory.Firm firmOfSession(final long sessionId)
    {
        return firms.bySession(sessionId);
    }

    public static boolean constantTimeEquals(final String expected, final String actual)
    {
        if (expected == null || actual == null)
        {
            return false;
        }
        return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), actual.getBytes(StandardCharsets.UTF_8));
    }

    /** For tests and the FIX front end: the epoch clock is the base of gatewayInTsNs. */
    static int encodedLengthOfReport()
    {
        return HEADER + ExecutionReportEncoder.BLOCK_LENGTH;
    }
}
