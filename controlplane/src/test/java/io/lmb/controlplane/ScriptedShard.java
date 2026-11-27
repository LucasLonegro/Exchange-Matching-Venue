package io.lmb.controlplane;

import io.lmb.api.EventSink;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.SnapshotSink;
import io.lmb.api.TimerScheduler;
import io.lmb.sbe.BooleanType;
import io.lmb.sbe.ClosePriceSource;
import io.lmb.sbe.ClosingPriceEncoder;
import io.lmb.sbe.ReferencePriceAppliedEncoder;
import io.lmb.sbe.StateChangeCause;
import io.lmb.sbe.CommandAppliedEncoder;
import io.lmb.sbe.CommandRejectedEncoder;
import io.lmb.sbe.DayRolledEncoder;
import io.lmb.sbe.DayRolloverDecoder;
import io.lmb.sbe.EventHeaderEncoder;
import io.lmb.sbe.MassCancelDecoder;
import io.lmb.sbe.MassCancelDoneEncoder;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.RejectReason;
import io.lmb.sbe.RiskLimitAppliedEncoder;
import io.lmb.sbe.RiskLimitUpdateDecoder;
import io.lmb.sbe.SymbolDefinedEncoder;
import io.lmb.sbe.SymbolDefinitionDecoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateChangedEncoder;
import io.lmb.sbe.SymbolStateCommandDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Just enough of ARCHITECTURE.md section 3.1 for control plane tests: symbol definition, state
 * transitions (immediate and timer-scheduled), risk limit echo with a usage model, day rollover,
 * admin mass cancel and rejections. Records every command it applies. Test hooks model the venue's volatility
 * interruptions and their timer (with the AUCTION_CALL takeover), ReferencePriceApplied, and ClosingPrice on CLOSED.
 */
final class ScriptedShard implements ShardStateMachine
{
    static final class Sym
    {
        int id;
        String ticker;
        long tick;
        long lot;
        long ref;
        int band;
        long maxQty;
        int dyn;
        long vol;
        SymbolState state;
        // band-stp / day-cycle model
        boolean volAuction;
        long volTimer;
        long volEndPrice = Long.MIN_VALUE;
        boolean venueRef;
        long lastTrade = Long.MIN_VALUE;
        long close = Long.MIN_VALUE;
        long prevClose = Long.MIN_VALUE;
    }

    static final class Limits
    {
        long budget;
        long used;
        long maxPos;
        boolean enabled;
    }

    private record Pending(long correlationId, long symbolId, SymbolState target)
    {
    }

    /** Cross-shard check of the allocator invariant: the sum of budgets the shards enforce never exceeds the firm limit. */
    static final class BudgetLedger
    {
        private final Map<Integer, Long> limits = new HashMap<>();
        private final Map<String, Long> budgets = new HashMap<>();
        final List<String> violations = Collections.synchronizedList(new ArrayList<>());

        synchronized void limit(final int firmId, final long limit)
        {
            limits.put(firmId, limit);
        }

        synchronized void set(final int shardId, final int firmId, final long budget)
        {
            budgets.put(shardId + ":" + firmId, budget);
            final Long limit = limits.get(firmId);
            if (limit != null)
            {
                long sum = 0;
                for (final Map.Entry<String, Long> e : budgets.entrySet())
                {
                    if (e.getKey().endsWith(":" + firmId))
                    {
                        sum += e.getValue();
                    }
                }
                if (sum > limit)
                {
                    violations.add("firm " + firmId + " budgets " + budgets + " sum " + sum + " > " + limit);
                }
            }
        }
    }

    private final int shardId;
    private final BudgetLedger ledger;
    private EventSink sink;
    private TimerScheduler timers;
    private long eventSeq;
    private int eventIdx;
    private long logPosition;
    private long timestampNs;
    private int tradingDate;
    private long nextTimerId;
    private final TreeMap<Integer, Sym> symbols = new TreeMap<>();
    private final TreeMap<Integer, Limits> firms = new TreeMap<>();
    private final Map<Integer, Long> demand = new HashMap<>();
    private final TreeMap<Long, Pending> pendingTimers = new TreeMap<>();
    private final TreeMap<Long, Integer> volTimers = new TreeMap<>();
    final List<String> received = Collections.synchronizedList(new ArrayList<>());

    private final UnsafeBuffer out = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDec = new MessageHeaderDecoder();

    ScriptedShard(final int shardId, final BudgetLedger ledger)
    {
        this.shardId = shardId;
        this.ledger = ledger;
    }

    // ---------------------------------------------------------------- test accessors (call under the venue's shard lock or after quiescence)

    synchronized SymbolState state(final int symbolId)
    {
        final Sym s = symbols.get(symbolId);
        return s == null ? null : s.state;
    }

    synchronized Sym symbol(final int symbolId)
    {
        return symbols.get(symbolId);
    }

    synchronized int symbolCount()
    {
        return symbols.size();
    }

    synchronized Limits limits(final int firmId)
    {
        return firms.get(firmId);
    }

    synchronized int tradingDate()
    {
        return tradingDate;
    }

    /** Usage the firm would like to have on this shard; applied at the next RiskLimitUpdate. */
    synchronized void demand(final int firmId, final long notional)
    {
        demand.put(firmId, notional);
    }

    synchronized List<String> received()
    {
        return new ArrayList<>(received);
    }

    long count(final String prefix)
    {
        return received().stream().filter(s -> s.startsWith(prefix)).count();
    }

    synchronized boolean inVolatilityAuction(final int symbolId)
    {
        final Sym s = symbols.get(symbolId);
        return s != null && s.volAuction;
    }

    /**
     * The matcher interrupts a CONTINUOUS symbol into a volatility auction (cause VOLATILITY_INTERRUPTION) and starts a
     * timer of the symbol's volatilityAuctionNs. At the timer the symbol returns to CONTINUOUS (VOLATILITY_AUCTION_END),
     * followed by ReferencePriceApplied at endPrice unless endPrice is the SBE null (the auction did not trade).
     */
    synchronized void interrupt(final int symbolId, final long nowNs, final long endPrice)
    {
        final Sym s = symbols.get(symbolId);
        if (s == null || s.state != SymbolState.CONTINUOUS || s.vol <= 0)
        {
            throw new IllegalStateException("cannot interrupt " + symbolId);
        }
        begin(logPosition, nowNs);
        received.add("VOL " + symbolId);
        changed(0, s, SymbolState.AUCTION_CALL, StateChangeCause.VOLATILITY_INTERRUPTION);
        s.volAuction = true;
        s.volEndPrice = endPrice;
        s.volTimer = ++nextTimerId;
        volTimers.put(s.volTimer, symbolId);
        timers.schedule(s.volTimer, nowNs + s.vol);
        applied(0);
    }

    /** The venue applies a reference of its own (as for an external ReferencePrice). */
    synchronized void reference(final int symbolId, final long nowNs, final long price)
    {
        begin(logPosition, nowNs);
        referenceApplied(symbols.get(symbolId), price);
        applied(0);
    }

    /** A trade print: the price CLOSED reports as LAST_TRADE. */
    synchronized void trade(final int symbolId, final long price)
    {
        symbols.get(symbolId).lastTrade = price;
    }

    // ---------------------------------------------------------------- ShardStateMachine

    @Override
    public int shardId()
    {
        return shardId;
    }

    @Override
    public void bind(final EventSink events, final TimerScheduler timers)
    {
        this.sink = events;
        this.timers = timers;
    }

    @Override
    public synchronized void onCommand(final DirectBuffer buffer, final int offset, final int length, final long logPosition,
        final long timestampNs)
    {
        begin(logPosition, timestampNs);
        headerDec.wrap(buffer, offset);
        final int templateId = headerDec.templateId();
        switch (templateId)
        {
            case SymbolDefinitionDecoder.TEMPLATE_ID -> define(new SymbolDefinitionDecoder().wrapAndApplyHeader(buffer, offset, headerDec));
            case SymbolStateCommandDecoder.TEMPLATE_ID -> stateCommand(
                new SymbolStateCommandDecoder().wrapAndApplyHeader(buffer, offset, headerDec));
            case RiskLimitUpdateDecoder.TEMPLATE_ID -> risk(new RiskLimitUpdateDecoder().wrapAndApplyHeader(buffer, offset, headerDec));
            case DayRolloverDecoder.TEMPLATE_ID -> roll(new DayRolloverDecoder().wrapAndApplyHeader(buffer, offset, headerDec));
            case MassCancelDecoder.TEMPLATE_ID -> massCancel(new MassCancelDecoder().wrapAndApplyHeader(buffer, offset, headerDec));
            default -> reject(0, templateId, RejectReason.MALFORMED);
        }
        applied(templateId);
    }

    @Override
    public synchronized void onTimer(final long correlationId, final long logPosition, final long timestampNs)
    {
        begin(logPosition, timestampNs);
        final Integer volSymbol = volTimers.remove(correlationId);
        final Sym vs = volSymbol == null ? null : symbols.get(volSymbol);
        if (vs != null && vs.volAuction && vs.volTimer == correlationId)
        {
            received.add("VOLEND " + vs.id + " @" + timestampNs);
            final long px = vs.volEndPrice;
            changed(0, vs, SymbolState.CONTINUOUS, StateChangeCause.VOLATILITY_AUCTION_END);
            if (px != Long.MIN_VALUE)
            {
                referenceApplied(vs, px);
            }
        }
        final Pending p = pendingTimers.remove(correlationId);
        if (p != null)
        {
            received.add("FIRE " + (p.symbolId == Commands.ALL_SYMBOLS ? "*" : p.symbolId) + " " + p.target + " @" + timestampNs);
            transition(p.correlationId, p.symbolId, p.target);
        }
        applied(0);
    }

    private void define(final SymbolDefinitionDecoder d)
    {
        final int id = (int)d.symbolId();
        received.add("DEF " + id);
        Sym s = symbols.get(id);
        if (s == null || s.state == SymbolState.REMOVED)
        {
            s = new Sym();
            s.id = id;
            s.state = d.initialState();
            symbols.put(id, s);
        }
        s.ticker = d.ticker();
        s.tick = d.tickSize();
        s.lot = d.lotSize();
        if (!s.venueRef)
        {
            s.ref = d.referencePrice(); // like the matcher: the venue's own reference wins once applied
        }
        s.band = (int)d.priceBandBps();
        s.maxQty = d.maxOrderQty();
        s.dyn = (int)d.dynamicBandBps();
        s.vol = d.volatilityAuctionNs();
        final SymbolDefinedEncoder e = new SymbolDefinedEncoder().wrapAndApplyHeader(out, 0, headerEnc);
        header(e.eventHeader());
        e.correlationId(d.correlationId()).tickSize(s.tick).lotSize(s.lot).referencePrice(s.ref).maxOrderQty(s.maxQty)
            .symbolId(id).priceBandBps(s.band).ticker(s.ticker).state(s.state)
            .volatilityAuctionNs(s.vol).dynamicBandBps(s.dyn);
        emit(e.encodedLength());
    }

    private void stateCommand(final SymbolStateCommandDecoder d)
    {
        final long symbolId = d.symbolId();
        final SymbolState target = d.targetState();
        final long eff = d.effectiveTimeNs();
        received.add("STATE " + (symbolId == Commands.ALL_SYMBOLS ? "*" : symbolId) + " " + target + (eff > 0 ? " eff=" + eff : ""));
        if (eff > 0)
        {
            final long timerId = ++nextTimerId;
            pendingTimers.put(timerId, new Pending(d.correlationId(), symbolId, target));
            timers.schedule(timerId, eff);
            return;
        }
        transition(d.correlationId(), symbolId, target);
    }

    private void transition(final long corr, final long symbolId, final SymbolState target)
    {
        if (symbolId == Commands.ALL_SYMBOLS)
        {
            int changed = 0;
            for (final Sym s : symbols.values())
            {
                if (allowed(s, target))
                {
                    changed(corr, s, target, StateChangeCause.COMMAND);
                    changed++;
                }
            }
            if (changed == 0)
            {
                reject(corr, SymbolStateCommandDecoder.TEMPLATE_ID, RejectReason.INVALID_STATE_TRANSITION);
            }
            return;
        }
        final Sym s = symbols.get((int)symbolId);
        if (s == null)
        {
            reject(corr, SymbolStateCommandDecoder.TEMPLATE_ID, RejectReason.UNKNOWN_SYMBOL);
        }
        else if (!allowed(s, target))
        {
            reject(corr, SymbolStateCommandDecoder.TEMPLATE_ID, RejectReason.INVALID_STATE_TRANSITION);
        }
        else
        {
            changed(corr, s, target, StateChangeCause.COMMAND);
        }
    }

    /** The table plus the takeover: AUCTION_CALL -> AUCTION_CALL while a volatility auction runs. */
    private static boolean allowed(final Sym s, final SymbolState target)
    {
        return SymbolStates.allowed(s.state, target) || (target == SymbolState.AUCTION_CALL && s.volAuction);
    }

    private void changed(final long corr, final Sym s, final SymbolState target, final StateChangeCause cause)
    {
        final SymbolState prev = s.state;
        endVolatility(s);
        s.state = target;
        final SymbolStateChangedEncoder e = new SymbolStateChangedEncoder().wrapAndApplyHeader(out, 0, headerEnc);
        header(e.eventHeader());
        e.correlationId(corr).symbolId(s.id).reasonCode(0).previousState(prev).newState(target).cause(cause);
        emit(e.encodedLength());
        if (target == SymbolState.CLOSED)
        {
            s.close = s.lastTrade != Long.MIN_VALUE ? s.lastTrade : s.prevClose;
            final ClosePriceSource src = s.lastTrade != Long.MIN_VALUE ? ClosePriceSource.LAST_TRADE
                : s.prevClose != Long.MIN_VALUE ? ClosePriceSource.PREVIOUS_CLOSE : ClosePriceSource.NONE;
            final ClosingPriceEncoder c = new ClosingPriceEncoder().wrapAndApplyHeader(out, 0, headerEnc);
            header(c.eventHeader());
            c.price(s.close).previousClose(s.prevClose).symbolId(s.id).tradingDate(tradingDate).source(src);
            emit(c.encodedLength());
        }
    }

    /** Any transition (and the day roll) ends a running volatility auction and cancels its timer. */
    private void endVolatility(final Sym s)
    {
        if (s.volAuction)
        {
            s.volAuction = false;
            volTimers.remove(s.volTimer);
            timers.cancel(s.volTimer);
        }
    }

    private void referenceApplied(final Sym s, final long price)
    {
        s.ref = price;
        s.venueRef = true;
        final long half = price * s.band / 10_000L;
        final ReferencePriceAppliedEncoder e = new ReferencePriceAppliedEncoder().wrapAndApplyHeader(out, 0, headerEnc);
        header(e.eventHeader());
        e.referencePrice(price).nbbBid(Long.MIN_VALUE).nbbOffer(Long.MIN_VALUE).bandLow(price - half).bandHigh(price + half)
            .sourceTsNs(timestampNs).symbolId(s.id);
        emit(e.encodedLength());
    }

    private void risk(final RiskLimitUpdateDecoder d)
    {
        final int firmId = (int)d.firmId();
        received.add("RISK " + firmId + " " + d.creditBudget() + " " + d.maxPositionPerSymbol() + " " + d.enabled());
        final Limits l = firms.computeIfAbsent(firmId, k -> new Limits());
        l.budget = d.creditBudget();
        l.maxPos = d.maxPositionPerSymbol();
        l.enabled = d.enabled() == BooleanType.TRUE;
        final long want = demand.getOrDefault(firmId, l.used);
        l.used = want <= l.used ? want : Math.max(l.used, Math.min(want, l.budget));
        if (ledger != null)
        {
            ledger.set(shardId, firmId, l.budget);
        }
        final RiskLimitAppliedEncoder e = new RiskLimitAppliedEncoder().wrapAndApplyHeader(out, 0, headerEnc);
        header(e.eventHeader());
        e.correlationId(d.correlationId()).creditBudget(l.budget).creditUsed(l.used).maxPositionPerSymbol(l.maxPos).firmId(firmId)
            .enabled(l.enabled ? BooleanType.TRUE : BooleanType.FALSE);
        emit(e.encodedLength());
    }

    private void roll(final DayRolloverDecoder d)
    {
        received.add("ROLL " + d.tradingDate());
        tradingDate = d.tradingDate();
        for (final Sym s : symbols.values())
        {
            if (s.state != SymbolState.REMOVED)
            {
                endVolatility(s);
                s.state = SymbolState.PRE_OPEN;
            }
        }
        for (final Limits l : firms.values())
        {
            l.used = 0;
        }
        final DayRolledEncoder e = new DayRolledEncoder().wrapAndApplyHeader(out, 0, headerEnc);
        header(e.eventHeader());
        e.correlationId(d.correlationId()).tradingDate(d.tradingDate()).expiredOrders(0);
        emit(e.encodedLength());
        // day-cycle: the official close becomes the next day's reference
        for (final Sym s : symbols.values())
        {
            if (s.state != SymbolState.REMOVED && s.close != Long.MIN_VALUE)
            {
                s.prevClose = s.close;
                s.lastTrade = Long.MIN_VALUE;
                referenceApplied(s, s.close);
            }
        }
    }

    private void massCancel(final MassCancelDecoder d)
    {
        received.add("MASS " + d.scope() + " firm=" + d.firmId() + " symbol=" + d.symbolId() + " reason=" + d.reason());
        final MassCancelDoneEncoder e = new MassCancelDoneEncoder().wrapAndApplyHeader(out, 0, headerEnc);
        header(e.eventHeader());
        e.requestId(d.requestId()).sessionId(d.sessionId()).firmId(d.firmId()).canceledCount(0).gatewayId(d.gatewayId())
            .scope(d.scope());
        emit(e.encodedLength());
    }

    private void reject(final long corr, final int templateId, final RejectReason reason)
    {
        final CommandRejectedEncoder e = new CommandRejectedEncoder().wrapAndApplyHeader(out, 0, headerEnc);
        header(e.eventHeader());
        e.correlationId(corr).commandTemplateId(templateId).rejectReason(reason);
        emit(e.encodedLength());
    }

    private void begin(final long logPosition, final long timestampNs)
    {
        this.logPosition = logPosition;
        this.timestampNs = timestampNs;
        this.eventIdx = 0;
    }

    private void applied(final int templateId)
    {
        final CommandAppliedEncoder e = new CommandAppliedEncoder().wrapAndApplyHeader(out, 0, headerEnc);
        final int count = eventIdx;
        header(e.eventHeader());
        e.eventCount(count).commandTemplateId(templateId);
        emit(e.encodedLength());
    }

    private void header(final EventHeaderEncoder h)
    {
        h.eventSeq(++eventSeq).logPosition(logPosition).timestampNs(timestampNs).eventIdx(eventIdx++).shardId(shardId).reserved(0);
    }

    private void emit(final int bodyLength)
    {
        sink.onEvent(out, 0, MessageHeaderEncoder.ENCODED_LENGTH + bodyLength);
    }

    @Override
    public void takeSnapshot(final SnapshotSink sink)
    {
    }

    @Override
    public void onSnapshotChunk(final DirectBuffer buffer, final int offset, final int length)
    {
    }

    @Override
    public void onSnapshotLoaded()
    {
    }

    @Override
    public long lastEventSeq()
    {
        return eventSeq;
    }
}
