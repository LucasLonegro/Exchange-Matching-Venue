package io.lmb.controlplane;

import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.common.Events;
import io.lmb.controlplane.TrackedCommand.Kind;
import io.lmb.controlplane.TrackedCommand.Status;
import io.lmb.controlplane.db.Database;
import io.lmb.controlplane.db.Store;
import io.lmb.sbe.BooleanType;
import io.lmb.sbe.ClosingPriceDecoder;
import io.lmb.sbe.CommandRejectedDecoder;
import io.lmb.sbe.DayRolledDecoder;
import io.lmb.sbe.DayRolloverDecoder;
import io.lmb.sbe.MassCancelDoneDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.ReferencePriceAppliedDecoder;
import io.lmb.sbe.RejectReason;
import io.lmb.sbe.RiskLimitAppliedDecoder;
import io.lmb.sbe.RiskLimitUpdateDecoder;
import io.lmb.sbe.StateChangeCause;
import io.lmb.sbe.SymbolDefinedDecoder;
import io.lmb.sbe.SymbolDefinitionDecoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateChangedDecoder;
import io.lmb.sbe.SymbolStateCommandDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

/**
 * The leader's single-threaded loop. While this instance holds the lease, each {@link #tick()}:
 * polls the shard command clients (detecting reconnects), consumes every shard's event stream into the
 * observed model and the command tracker, runs the duties (admin requests, reconciler, scheduler,
 * allocator), retries commands whose effect has not appeared, then writes new commands to the database
 * <b>before</b> offering them (write-ahead, fenced by the lease) and periodically persists the observed
 * state, cursors and command statuses in one fenced transaction.
 *
 * <p>On any failure, or when the lease is lost, the leader steps down and drops all in-memory state;
 * when it (or another instance) becomes leader again it reloads everything from the database.
 */
public final class Engine
{
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);
    private static final int CORRELATION_BLOCK = 4096;
    private static final int POLL_LIMIT = 10_000;

    /** A leader-only activity run every tick. */
    public interface Duty
    {
        void tick(Engine engine);

        default void onStepUp(final Engine engine)
        {
        }

        default void onStepDown()
        {
        }

        default void onResolved(final Engine engine, final TrackedCommand command)
        {
        }

        /** Extra writes inside the periodic fenced persist transaction. */
        default void persist(final Engine engine, final Connection c) throws SQLException
        {
        }

        /** Extra writes inside the fenced transaction that inserts new commands (before they are offered). */
        default void persistWithCommands(final Engine engine, final Connection c) throws SQLException
        {
        }
    }

    private final ControlPlaneConfig cfg;
    private final Database db;
    private final VenueClients venue;
    private final Clock clock;
    private final LeaderLease lease;
    private final Metrics metrics;
    private final List<Duty> duties = new ArrayList<>();

    private boolean active;
    private boolean restartRequested;
    private long epoch;
    private ShardState[] shards;
    private CommandTracker tracker;
    private RefData ref;
    private long refVersion;
    private long refLoadedNs;
    private long lastPersistNs;
    private long lastPruneNs;
    private long stepUpNs;
    private long corrNext;
    private long corrLimit;
    private final List<TrackedCommand> newlyIssued = new ArrayList<>();

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final SymbolDefinedDecoder symbolDefined = new SymbolDefinedDecoder();
    private final SymbolStateChangedDecoder stateChanged = new SymbolStateChangedDecoder();
    private final RiskLimitAppliedDecoder riskApplied = new RiskLimitAppliedDecoder();
    private final CommandRejectedDecoder rejected = new CommandRejectedDecoder();
    private final DayRolledDecoder dayRolled = new DayRolledDecoder();
    private final MassCancelDoneDecoder massCancelDone = new MassCancelDoneDecoder();
    private final ReferencePriceAppliedDecoder refApplied = new ReferencePriceAppliedDecoder();
    private final ClosingPriceDecoder closingPrice = new ClosingPriceDecoder();
    private final SymbolDefinitionDecoder sentDefinition = new SymbolDefinitionDecoder();
    private final MessageHeaderDecoder sentHeader = new MessageHeaderDecoder();
    private final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[0]);

    public Engine(final ControlPlaneConfig cfg, final Database db, final VenueClients venue, final Clock clock,
        final LeaderLease lease, final Metrics metrics)
    {
        this.cfg = cfg;
        this.db = db;
        this.venue = venue;
        this.clock = clock;
        this.lease = lease;
        this.metrics = metrics;
    }

    public void addDuty(final Duty duty)
    {
        duties.add(duty);
    }

    // ------------------------------------------------------------------ accessors for duties

    public ControlPlaneConfig config()
    {
        return cfg;
    }

    public Database db()
    {
        return db;
    }

    public Clock clock()
    {
        return clock;
    }

    public boolean isActive()
    {
        return active;
    }

    public int shardCount()
    {
        return venue.shardCount();
    }

    public ShardState shard(final int shardId)
    {
        return shards[shardId];
    }

    public CommandTracker tracker()
    {
        return tracker;
    }

    public RefData ref()
    {
        return ref;
    }

    /** Increments whenever the reference data snapshot is reloaded. */
    public long refVersion()
    {
        return refVersion;
    }

    public long stepUpMonoNs()
    {
        return stepUpNs;
    }

    public long epoch()
    {
        return epoch;
    }

    /** Drop in-memory state and reload it from the database at the next tick. */
    public void requestRestart()
    {
        restartRequested = true;
    }

    public void reloadRefData()
    {
        ref = RefData.load(db);
        refVersion++;
        refLoadedNs = clock.monoNs();
    }

    /** Runs work in a transaction that first proves this instance still holds the lease. */
    public <T> T fencedTx(final Database.SqlWork<T> work)
    {
        return db.tx(c ->
        {
            lease.fence(c);
            return work.run(c);
        });
    }

    // ------------------------------------------------------------------ issuing commands

    /**
     * Creates and tracks a command; it is written to the database at the end of the tick and only then offered.
     *
     * @param encoder builds the SBE bytes for the allocated correlation id
     */
    public TrackedCommand issue(final int shardId, final Kind kind, final String key, final String origin, final String detail,
        final long effectiveNs, final Long auditId, final LongFunction<byte[]> encoder)
    {
        final long corr = nextCorrelationId();
        final TrackedCommand c = new TrackedCommand(corr, shardId, kind, key, origin, detail, encoder.apply(corr), effectiveNs,
            auditId, epoch, clock.epochMs());
        tracker.add(c);
        newlyIssued.add(c);
        metrics.commandIssued(kind.name());
        return c;
    }

    private long nextCorrelationId()
    {
        if (corrNext >= corrLimit)
        {
            final long block = db.read(c -> Store.nextCorrelationBlock(db, c));
            corrNext = block * CORRELATION_BLOCK + 1;
            corrLimit = (block + 1) * CORRELATION_BLOCK;
        }
        return corrNext++;
    }

    // ------------------------------------------------------------------ the loop

    /** One iteration. Never throws. */
    public void tick()
    {
        final long start = clock.monoNs();
        if (!lease.isLeader())
        {
            if (active)
            {
                stepDown("lease not held");
            }
            metrics.leader(false);
            return;
        }
        try
        {
            if (!active || epoch != lease.epoch() || restartRequested)
            {
                if (active)
                {
                    // no final persist: a resync has just deleted this state on purpose; anything unpersisted
                    // is re-derived from the event stream and command_log after the reload
                    stepDown(restartRequested ? "restart requested" : "epoch changed");
                }
                restartRequested = false;
                stepUp();
            }
            metrics.leader(true);
            pollClients();
            pollEvents();
            if (clock.monoNs() - refLoadedNs >= cfg.refDataReloadMs * 1_000_000L)
            {
                reloadRefData();
            }
            for (final Duty d : duties)
            {
                d.tick(this);
            }
            retries();
            flush();
            persist(false);
            prune();
            metrics.tickLatency(clock.monoNs() - start);
        }
        catch (final RuntimeException e)
        {
            LOG.warn("leader tick failed, stepping down: {}", e.toString(), e instanceof LeaderLease.NotLeaderException ? null : e);
            stepDown("error: " + e);
        }
    }

    private void stepUp()
    {
        epoch = lease.epoch();
        LOG.info("{} becoming leader (epoch {})", cfg.instanceId, epoch);
        tracker = new CommandTracker(this::onResolved);
        newlyIssued.clear();
        corrNext = corrLimit = 0;
        reloadRefData();
        final int n = venue.shardCount();
        shards = new ShardState[n];
        for (int s = 0; s < n; s++)
        {
            shards[s] = new ShardState(s);
        }
        db.read(c ->
        {
            for (final Store.CursorRow r : Store.cursors(c))
            {
                if (r.shardId() < n)
                {
                    final ShardState sh = shards[r.shardId()];
                    sh.eventSeq = r.eventSeq();
                    sh.tradingDate = r.tradingDate();
                    sh.lastEventTsNs = r.lastEventTsNs();
                }
            }
            for (final Store.ObservedSymbolRow r : Store.observedSymbols(c))
            {
                if (r.shardId() < n)
                {
                    final ShardState.Symbol s = new ShardState.Symbol();
                    s.symbolId = r.symbolId();
                    s.ticker = r.ticker();
                    s.tickSize = r.tickSize();
                    s.lotSize = r.lotSize();
                    s.referencePrice = r.referencePrice();
                    s.priceBandBps = r.priceBandBps();
                    s.maxOrderQty = r.maxOrderQty();
                    s.state = SymbolState.valueOf(r.state());
                    s.lastCorrelationId = r.lastCorrelationId();
                    s.eventSeq = r.eventSeq();
                    s.dynamicBandBps = r.dynamicBandBps();
                    s.volatilityAuctionNs = r.volatilityAuctionNs();
                    s.volatilityAuction = r.volatilityAuction();
                    s.volatilityAuctionSinceNs = r.volatilityAuctionSinceNs();
                    s.venueReference = r.venueReference();
                    s.bandLow = r.bandLow();
                    s.bandHigh = r.bandHigh();
                    s.closePrice = r.closePrice();
                    s.previousClose = r.previousClose();
                    s.closeSource = r.closeSource();
                    s.closeTradingDate = r.closeTradingDate();
                    shards[r.shardId()].symbols.put(s.symbolId, s);
                }
            }
            for (final Store.ObservedFirmRow r : Store.observedFirms(c))
            {
                if (r.shardId() < n)
                {
                    final ShardState.Firm f = new ShardState.Firm();
                    f.firmId = r.firmId();
                    f.creditBudget = r.creditBudget();
                    f.creditUsed = r.creditUsed();
                    f.maxPositionPerSymbol = r.maxPositionPerSymbol();
                    f.enabled = r.enabled();
                    f.lastCorrelationId = r.lastCorrelationId();
                    f.eventSeq = r.eventSeq();
                    f.observedTsNs = r.observedTsNs();
                    shards[r.shardId()].firms.put(f.firmId, f);
                }
            }
            for (final Store.CommandRow r : Store.unresolvedCommands(c))
            {
                if (r.shardId() < n)
                {
                    tracker.add(restore(r));
                }
            }
            return null;
        });
        tracker.drainDirty();
        for (final ShardState sh : shards)
        {
            sh.client = venue.commandClient(sh.shardId);
            sh.stream = venue.eventStream(sh.shardId, sh.eventSeq);
        }
        stepUpNs = clock.monoNs();
        lastPersistNs = stepUpNs;
        active = true;
        for (final Duty d : duties)
        {
            d.onStepUp(this);
        }
    }

    private void stepDown(final String reason)
    {
        LOG.info("{} stepping down: {}", cfg.instanceId, reason);
        active = false;
        if (shards != null)
        {
            for (final ShardState sh : shards)
            {
                closeQuietly(sh.client);
                closeQuietly(sh.stream);
            }
        }
        shards = null;
        tracker = null;
        newlyIssued.clear();
        for (final Duty d : duties)
        {
            d.onStepDown();
        }
    }

    /** Releases transport resources (shutdown). */
    public void close()
    {
        if (active)
        {
            try
            {
                persist(true);
            }
            catch (final RuntimeException e)
            {
                LOG.warn("final persist failed: {}", e.toString());
            }
            stepDown("shutdown");
        }
    }

    private static void closeQuietly(final AutoCloseable c)
    {
        if (c != null)
        {
            try
            {
                c.close();
            }
            catch (final Exception ignore)
            {
                // closing
            }
        }
    }

    private void pollClients()
    {
        for (final ShardState sh : shards)
        {
            sh.client.poll();
            final boolean connected = sh.client.isConnected();
            if (connected && !sh.connected)
            {
                LOG.info("shard {} connected; resending {} outstanding commands and reconciling", sh.shardId,
                    countActive(sh.shardId));
                for (final TrackedCommand c : tracker.unresolved())
                {
                    if (c.shardId == sh.shardId && c.status == Status.SENT)
                    {
                        c.lastSentMonoNs = Long.MIN_VALUE / 2;
                    }
                }
                sh.needsReconcile = true;
            }
            if (connected != sh.connected)
            {
                sh.connected = connected;
                sh.cursorDirty = true;
            }
            metrics.shard(sh.shardId, connected, sh.eventSeq);
        }
    }

    private int countActive(final int shardId)
    {
        int n = 0;
        for (final TrackedCommand c : tracker.unresolved())
        {
            if (c.shardId == shardId && c.status.active())
            {
                n++;
            }
        }
        return n;
    }

    private void pollEvents()
    {
        for (final ShardState sh : shards)
        {
            final EventStreamClient stream = sh.stream;
            final int n = stream.poll((buffer, offset, length) -> onEvent(sh, buffer, offset), POLL_LIMIT);
            final boolean caughtUp = n < POLL_LIMIT && stream.isConnected();
            if (caughtUp != sh.caughtUp)
            {
                sh.caughtUp = caughtUp;
                sh.cursorDirty = true;
            }
        }
    }

    private void onEvent(final ShardState sh, final DirectBuffer buffer, final int offset)
    {
        final long seq = Events.eventSeq(buffer, offset);
        if (seq <= sh.eventSeq)
        {
            return;
        }
        sh.eventSeq = seq;
        sh.lastEventTsNs = Events.timestampNs(buffer, offset);
        sh.cursorDirty = true;
        switch (Events.templateId(buffer, offset))
        {
            case SymbolDefinedDecoder.TEMPLATE_ID ->
            {
                symbolDefined.wrapAndApplyHeader(buffer, offset, header);
                final int id = (int)symbolDefined.symbolId();
                final ShardState.Symbol known = sh.symbols.get(id);
                // A new symbol, or one re-created after REMOVED: the matcher starts it from the definition's reference.
                final boolean fresh = known == null || known.state == null || known.state == SymbolState.REMOVED;
                final ShardState.Symbol s = fresh ? new ShardState.Symbol() : known;
                sh.symbols.put(id, s);
                s.symbolId = id;
                s.ticker = symbolDefined.ticker().trim();
                s.tickSize = symbolDefined.tickSize();
                s.lotSize = symbolDefined.lotSize();
                s.referencePrice = symbolDefined.referencePrice();
                s.priceBandBps = (int)symbolDefined.priceBandBps();
                s.maxOrderQty = symbolDefined.maxOrderQty();
                s.state = symbolDefined.state();
                s.dynamicBandBps = (int)symbolDefined.dynamicBandBps();
                s.volatilityAuctionNs = symbolDefined.volatilityAuctionNs();
                if (s.state != SymbolState.AUCTION_CALL)
                {
                    clearVolatilityAuction(s);
                }
                final long sentRef = fresh ? Long.MIN_VALUE : sentReferencePrice(sh, symbolDefined.correlationId());
                if (sentRef != Long.MIN_VALUE && sentRef != s.referencePrice)
                {
                    s.venueReference = true; // an update whose reference the matcher kept for its own
                }
                s.lastCorrelationId = symbolDefined.correlationId();
                s.eventSeq = seq;
                sh.dirtySymbols.add(id);
                confirm(sh, symbolDefined.correlationId());
            }
            case SymbolStateChangedDecoder.TEMPLATE_ID ->
            {
                stateChanged.wrapAndApplyHeader(buffer, offset, header);
                final ShardState.Symbol s = sh.symbols.get((int)stateChanged.symbolId());
                if (s != null)
                {
                    s.state = stateChanged.newState();
                    // Any state change ends a volatility auction; only an interruption starts one.
                    if (s.state == SymbolState.AUCTION_CALL && stateChanged.cause() == StateChangeCause.VOLATILITY_INTERRUPTION)
                    {
                        s.volatilityAuction = true;
                        s.volatilityAuctionSinceNs = sh.lastEventTsNs;
                    }
                    else
                    {
                        clearVolatilityAuction(s);
                    }
                    s.lastCorrelationId = stateChanged.correlationId();
                    s.eventSeq = seq;
                    sh.dirtySymbols.add(s.symbolId);
                }
                confirm(sh, stateChanged.correlationId());
            }
            case RiskLimitAppliedDecoder.TEMPLATE_ID ->
            {
                riskApplied.wrapAndApplyHeader(buffer, offset, header);
                final int firmId = (int)riskApplied.firmId();
                final ShardState.Firm f = sh.firms.computeIfAbsent(firmId, k -> new ShardState.Firm());
                f.firmId = firmId;
                f.creditBudget = riskApplied.creditBudget();
                f.creditUsed = riskApplied.creditUsed();
                f.maxPositionPerSymbol = riskApplied.maxPositionPerSymbol();
                f.enabled = riskApplied.enabled() == BooleanType.TRUE;
                f.lastCorrelationId = riskApplied.correlationId();
                f.eventSeq = seq;
                f.observedTsNs = sh.lastEventTsNs;
                f.observedMonoNs = clock.monoNs();
                sh.dirtyFirms.add(firmId);
                confirm(sh, riskApplied.correlationId());
            }
            case CommandRejectedDecoder.TEMPLATE_ID ->
            {
                rejected.wrapAndApplyHeader(buffer, offset, header);
                final TrackedCommand c = tracker.get(rejected.correlationId());
                if (c != null && c.shardId == sh.shardId)
                {
                    final RejectReason reason = rejected.rejectReason();
                    // DAY_ROLLOVER: the matcher rejects a roll that does not advance the trading date (idempotent).
                    if ((c.kind == Kind.SYMBOL_STATE || c.kind == Kind.DAY_ROLLOVER)
                        && reason == RejectReason.INVALID_STATE_TRANSITION && levelSatisfied(c))
                    {
                        tracker.resolve(c, Status.CONFIRMED, "already in target state");
                    }
                    else
                    {
                        metrics.commandRejected();
                        LOG.warn("shard {} rejected {}: {}", sh.shardId, c, reason);
                        tracker.resolve(c, Status.REJECTED, String.valueOf(reason));
                    }
                }
            }
            case DayRolledDecoder.TEMPLATE_ID ->
            {
                dayRolled.wrapAndApplyHeader(buffer, offset, header);
                sh.tradingDate = dayRolled.tradingDate();
                for (final ShardState.Symbol s : sh.symbols.values())
                {
                    if (s.state != SymbolState.REMOVED && s.state != SymbolState.PRE_OPEN)
                    {
                        s.state = SymbolState.PRE_OPEN;
                        clearVolatilityAuction(s);
                        s.eventSeq = seq;
                        sh.dirtySymbols.add(s.symbolId);
                    }
                }
                confirm(sh, dayRolled.correlationId());
            }
            case MassCancelDoneDecoder.TEMPLATE_ID ->
            {
                massCancelDone.wrapAndApplyHeader(buffer, offset, header);
                if (massCancelDone.gatewayId() == cfg.adminGatewayId)
                {
                    final TrackedCommand c = tracker.get(massCancelDone.requestId());
                    if (c != null && c.shardId == sh.shardId && c.kind == Kind.MASS_CANCEL)
                    {
                        tracker.resolve(c, Status.CONFIRMED, "canceled " + massCancelDone.canceledCount());
                    }
                }
            }
            case ReferencePriceAppliedDecoder.TEMPLATE_ID ->
            {
                // day-cycle / band-stp: the venue moved the reference (day rollover to the close, end of a volatility
                // auction that traded, or an external ReferencePrice). It owns the reference from now on.
                refApplied.wrapAndApplyHeader(buffer, offset, header);
                final ShardState.Symbol s = sh.symbols.get((int)refApplied.symbolId());
                if (s != null)
                {
                    s.referencePrice = refApplied.referencePrice();
                    s.bandLow = refApplied.bandLow();
                    s.bandHigh = refApplied.bandHigh();
                    s.venueReference = true;
                    s.eventSeq = seq;
                    sh.dirtySymbols.add(s.symbolId);
                }
            }
            case ClosingPriceDecoder.TEMPLATE_ID ->
            {
                closingPrice.wrapAndApplyHeader(buffer, offset, header);
                final ShardState.Symbol s = sh.symbols.get((int)closingPrice.symbolId());
                if (s != null)
                {
                    s.closePrice = closingPrice.price();
                    s.previousClose = closingPrice.previousClose();
                    s.closeSource = String.valueOf(closingPrice.source());
                    s.closeTradingDate = closingPrice.tradingDate();
                    s.eventSeq = seq;
                    sh.dirtySymbols.add(s.symbolId);
                }
            }
            default ->
            {
                // order flow and market data events are not the control plane's business
            }
        }
    }

    /**
     * The symbol shows the state a command asked for. A volatility auction does not satisfy an AUCTION_CALL target:
     * the command asks for a takeover (cause COMMAND, AUCTION_CALL -> AUCTION_CALL), which clears the flag.
     */
    static boolean stateReached(final ShardState.Symbol s, final SymbolState target)
    {
        return s.state == target && !(target == SymbolState.AUCTION_CALL && s.volatilityAuction);
    }

    private static void clearVolatilityAuction(final ShardState.Symbol s)
    {
        s.volatilityAuction = false;
        s.volatilityAuctionSinceNs = 0;
    }

    /** Reference price in the SymbolDefinition we sent with this correlation id, or the SBE null if not ours / unknown. */
    private long sentReferencePrice(final ShardState sh, final long correlationId)
    {
        final TrackedCommand c = tracker.get(correlationId);
        if (c == null || c.shardId != sh.shardId || c.kind != Kind.SYMBOL_DEFINITION || c.payload == null)
        {
            return Long.MIN_VALUE;
        }
        return sentDefinition.wrapAndApplyHeader(new UnsafeBuffer(c.payload), 0, sentHeader).referencePrice();
    }

    private void confirm(final ShardState sh, final long correlationId)
    {
        final TrackedCommand c = tracker.get(correlationId);
        if (c != null && c.shardId == sh.shardId && c.kind != Kind.MASS_CANCEL)
        {
            final long latency = c.lastSentMonoNs > Long.MIN_VALUE / 2 && c.effectiveNs == 0 ? clock.monoNs() - c.lastSentMonoNs : -1;
            tracker.resolve(c, Status.CONFIRMED, null);
            if (latency >= 0)
            {
                metrics.confirmLatency(latency);
            }
        }
    }

    /** True if the shard already shows the effect a state/definition/rollover command asks for. */
    public boolean levelSatisfied(final TrackedCommand c)
    {
        final ShardState sh = shards[c.shardId];
        switch (c.kind)
        {
            case SYMBOL_STATE ->
            {
                if (c.symbolId >= 0)
                {
                    final ShardState.Symbol s = sh.symbols.get(c.symbolId);
                    return s != null && (stateReached(s, c.target) || (c.target != SymbolState.REMOVED && s.state == SymbolState.REMOVED));
                }
                for (final ShardState.Symbol s : sh.symbols.values())
                {
                    if (!stateReached(s, c.target) && (SymbolStates.allowed(s.state, c.target) || s.state == c.target))
                    {
                        return false;
                    }
                }
                return true;
            }
            case SYMBOL_DEFINITION ->
            {
                final ShardState.Symbol s = sh.symbols.get(c.symbolId);
                final Store.SymbolRow r = ref.symbolById().get(c.symbolId);
                return s != null && r != null && Reconciler.matches(s, r);
            }
            case DAY_ROLLOVER ->
            {
                return sh.tradingDate >= c.tradingDate;
            }
            default ->
            {
                return false;
            }
        }
    }

    private void onResolved(final TrackedCommand c)
    {
        for (final Duty d : duties)
        {
            d.onResolved(this, c);
        }
    }

    private void retries()
    {
        final long now = clock.monoNs();
        final long nowEpochNs = clock.epochNs();
        final long timeoutNs = cfg.commandTimeoutMs * 1_000_000L;
        for (final TrackedCommand c : new ArrayList<>(tracker.unresolved()))
        {
            if (!c.persisted)
            {
                continue;
            }
            final ShardState sh = shards[c.shardId];
            if (c.effectiveNs > 0 && c.status.active() && nowEpochNs > c.effectiveNs + timeoutNs)
            {
                // A timer command whose time has passed without its effect: never resend it late (it could
                // apply out of order); the scheduler's enforcement brings the symbol to the expected state.
                tracker.resolve(c, sh.ready() && levelSatisfied(c) ? Status.CONFIRMED : Status.EXPIRED, "effective time passed");
                continue;
            }
            if (c.status == Status.PENDING)
            {
                offer(c);
                continue;
            }
            if (c.status != Status.SENT)
            {
                continue;
            }
            final long backoff = timeoutNs * Math.min(8, 1L << Math.min(3, Math.max(0, c.attempts - 1)));
            if (now - c.lastSentMonoNs < backoff)
            {
                continue;
            }
            if (sh.ready() && c.kind != Kind.RISK_LIMIT && c.kind != Kind.MASS_CANCEL && levelSatisfied(c))
            {
                tracker.resolve(c, Status.CONFIRMED, "effect observed");
                continue;
            }
            if (c.kind != Kind.RISK_LIMIT && c.attempts >= cfg.commandMaxAttempts)
            {
                LOG.warn("giving up on {}", c);
                tracker.resolve(c, Status.FAILED, "no effect after " + c.attempts + " attempts");
                continue;
            }
            if (c.kind == Kind.DAY_ROLLOVER && sh.tradingDate >= c.tradingDate)
            {
                tracker.resolve(c, Status.CONFIRMED, "trading date reached");
                continue;
            }
            metrics.retry();
            LOG.debug("retrying {}", c);
            offer(c);
        }
    }

    private void offer(final TrackedCommand c)
    {
        final ShardState sh = shards[c.shardId];
        offerBuffer.wrap(c.payload);
        if (sh.client.offer(offerBuffer, 0, c.payload.length))
        {
            c.attempts++;
            c.lastSentMonoNs = clock.monoNs();
            c.status = c.effectiveNs > 0 ? Status.SCHEDULED : Status.SENT;
            tracker.markDirty(c);
        }
    }

    private void flush()
    {
        if (!newlyIssued.isEmpty())
        {
            final long now = clock.epochMs();
            final List<TrackedCommand> batch = new ArrayList<>(newlyIssued);
            fencedTx(c ->
            {
                for (final TrackedCommand t : batch)
                {
                    Store.insertCommand(c, row(t, now));
                }
                for (final Duty d : duties)
                {
                    d.persistWithCommands(this, c);
                }
                return null;
            });
            newlyIssued.clear();
            for (final TrackedCommand t : batch)
            {
                t.persisted = true;
                if (t.status == Status.PENDING)
                {
                    offer(t);
                }
            }
        }
    }

    private static Store.CommandRow row(final TrackedCommand t, final long nowMs)
    {
        return new Store.CommandRow(t.correlationId, t.shardId, t.kind.name(), t.key, t.origin, t.detail, Commands.hex(t.payload),
            t.kind == Kind.RISK_LIMIT ? t.budget : null, t.effectiveNs, t.status.name(), t.attempts, t.rejectReason, t.auditId,
            t.leaderEpoch, t.createdMs, nowMs);
    }

    /** Writes observed state, cursors and command statuses; at most every persistIntervalMs unless forced. */
    public void persist(final boolean force)
    {
        final long now = clock.monoNs();
        if (!force && now - lastPersistNs < cfg.persistIntervalMs * 1_000_000L)
        {
            return;
        }
        lastPersistNs = now;
        final long nowMs = clock.epochMs();
        final List<TrackedCommand> commands = tracker.drainDirty();
        try
        {
            fencedTx(c ->
            {
                for (final ShardState sh : shards)
                {
                    for (final int id : sh.dirtySymbols)
                    {
                        final ShardState.Symbol s = sh.symbols.get(id);
                        Store.saveObservedSymbol(c, new Store.ObservedSymbolRow(sh.shardId, id, s.ticker, s.tickSize, s.lotSize,
                            s.referencePrice, s.priceBandBps, s.maxOrderQty, s.state.name(), s.lastCorrelationId, s.eventSeq,
                            s.dynamicBandBps, s.volatilityAuctionNs, s.volatilityAuction, s.volatilityAuctionSinceNs,
                            s.venueReference, s.bandLow, s.bandHigh, s.closePrice, s.previousClose, s.closeSource,
                            s.closeTradingDate));
                    }
                    for (final int id : sh.dirtyFirms)
                    {
                        final ShardState.Firm f = sh.firms.get(id);
                        Store.saveObservedFirm(c, new Store.ObservedFirmRow(sh.shardId, id, f.creditBudget, f.creditUsed,
                            f.maxPositionPerSymbol, f.enabled, f.lastCorrelationId, f.eventSeq, f.observedTsNs));
                    }
                    if (sh.cursorDirty)
                    {
                        Store.saveCursor(c, new Store.CursorRow(sh.shardId, sh.eventSeq, sh.tradingDate, sh.lastEventTsNs,
                            sh.connected, sh.caughtUp, nowMs));
                    }
                }
                for (final TrackedCommand t : commands)
                {
                    if (t.persisted)
                    {
                        Store.updateCommand(c, t.correlationId, t.status.name(), t.attempts, t.rejectReason, nowMs);
                    }
                }
                for (final Duty d : duties)
                {
                    d.persist(this, c);
                }
                return null;
            });
        }
        catch (final RuntimeException e)
        {
            tracker.undrain(commands);
            throw e;
        }
        for (final ShardState sh : shards)
        {
            sh.dirtySymbols.clear();
            sh.dirtyFirms.clear();
            sh.cursorDirty = false;
        }
        metrics.pending(tracker.size());
    }

    private void prune()
    {
        final long now = clock.monoNs();
        if (now - lastPruneNs < 60_000_000_000L)
        {
            return;
        }
        lastPruneNs = now;
        final int n = fencedTx(c -> Store.pruneCommands(c, clock.epochMs(), cfg.commandRetentionMs, cfg.refreshRetentionMs));
        if (n > 0)
        {
            LOG.debug("pruned {} resolved commands", n);
        }
    }

    /** Rebuilds a tracked command from its command_log row (used when a new leader takes over). */
    static TrackedCommand restore(final Store.CommandRow r)
    {
        final byte[] payload = Commands.unhex(r.payloadHex());
        final TrackedCommand t = new TrackedCommand(r.correlationId(), r.shardId(), Kind.valueOf(r.kind()), r.key(), r.origin(),
            r.detail(), payload, r.effectiveNs(), r.auditId(), r.leaderEpoch(), r.createdMs());
        final UnsafeBuffer b = new UnsafeBuffer(payload);
        final MessageHeaderDecoder h = new MessageHeaderDecoder();
        switch (t.kind)
        {
            case RISK_LIMIT ->
            {
                final RiskLimitUpdateDecoder d = new RiskLimitUpdateDecoder().wrapAndApplyHeader(b, 0, h);
                t.firmId = (int)d.firmId();
                t.budget = d.creditBudget();
                t.maxPos = d.maxPositionPerSymbol();
                t.enabled = d.enabled() == BooleanType.TRUE;
            }
            case SYMBOL_STATE ->
            {
                final SymbolStateCommandDecoder d = new SymbolStateCommandDecoder().wrapAndApplyHeader(b, 0, h);
                t.symbolId = d.symbolId() == Commands.ALL_SYMBOLS ? -1 : (int)d.symbolId();
                t.target = d.targetState();
            }
            case SYMBOL_DEFINITION ->
            {
                final SymbolDefinitionDecoder d = new SymbolDefinitionDecoder().wrapAndApplyHeader(b, 0, h);
                t.symbolId = (int)d.symbolId();
            }
            case DAY_ROLLOVER -> t.tradingDate = new DayRolloverDecoder().wrapAndApplyHeader(b, 0, h).tradingDate();
            default ->
            {
            }
        }
        t.status = Status.valueOf(r.status());
        t.attempts = r.attempts();
        t.persisted = true;
        // A new leader resends everything it inherited (same correlation id, so a duplicate is harmless).
        t.lastSentMonoNs = Long.MIN_VALUE / 2;
        if (t.status == Status.PENDING && t.effectiveNs == 0)
        {
            t.status = Status.SENT;
        }
        return t;
    }
}
