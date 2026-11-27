package io.lmb.matcher;

import io.lmb.api.EventSink;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.SnapshotSink;
import io.lmb.api.TimerScheduler;
import io.lmb.sbe.CancelOrderDecoder;
import io.lmb.sbe.CancelReason;
import io.lmb.sbe.DayRolloverDecoder;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.LiquidityFlag;
import io.lmb.sbe.MassCancelDecoder;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.NewOrderDecoder;
import io.lmb.sbe.OrdStatus;
import io.lmb.sbe.ReferencePriceDecoder;
import io.lmb.sbe.RejectReason;
import io.lmb.sbe.ReplaceOrderDecoder;
import io.lmb.sbe.RiskLimitUpdateDecoder;
import io.lmb.sbe.StateChangeCause;
import io.lmb.sbe.SymbolDefinitionDecoder;
import io.lmb.sbe.SymbolStateCommandDecoder;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;

import static io.lmb.matcher.Codes.AUCTION_CALL;
import static io.lmb.matcher.Codes.BUY;
import static io.lmb.matcher.Codes.CLOSED;
import static io.lmb.matcher.Codes.CONTINUOUS;
import static io.lmb.matcher.Codes.HALTED;
import static io.lmb.matcher.Codes.MARKET;
import static io.lmb.matcher.Codes.MAX_PRICE;
import static io.lmb.matcher.Codes.MAX_QTY;
import static io.lmb.matcher.Codes.NULL_PRICE;
import static io.lmb.matcher.Codes.PRE_OPEN;
import static io.lmb.matcher.Codes.REMOVED;
import static io.lmb.matcher.Codes.SELL;

/**
 * The deterministic state machine of one shard: pre-trade risk, order books, price-time matching,
 * idempotency, symbol states, auctions, mass cancel, day roll, audit hash chain and snapshots.
 * See docs/modules/matcher-core.md for the exact behaviour and event order.
 */
public final class MatcherShard implements ShardStateMachine
{
    private static final Logger LOG = LoggerFactory.getLogger(MatcherShard.class);
    private static final int HDR = MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int SCHEMA_ID = 1;

    /** Dedupe table values: >= 0 is the pool index of the live order holding that clOrdId. */
    static final int SEEN = -1;
    static final int DONE_FILLED = -2;
    static final int DONE_CANCELED = -3;
    static final int DONE_EXPIRED = -4;

    // band-stp: why matchAggressor stopped before the aggressor was filled or reached its limit
    private static final byte STOP_NONE = 0;
    private static final byte STOP_SELF_TRADE = 1;
    private static final byte STOP_VOLATILITY = 2;
    /** Longest volatility auction accepted: one day. */
    private static final long MAX_VOLATILITY_AUCTION_NS = 86_400_000_000_000L;
    // /band-stp

    private static final int PRICE_BITS = 43;
    private static final long PRICE_MASK = (1L << PRICE_BITS) - 1;

    final int shardId;
    final long idBase;
    final MatcherConfig config;
    final EventWriter ew;
    TimerScheduler timers;

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final NewOrderDecoder newOrderDecoder = new NewOrderDecoder();
    private final CancelOrderDecoder cancelDecoder = new CancelOrderDecoder();
    private final ReplaceOrderDecoder replaceDecoder = new ReplaceOrderDecoder();
    private final MassCancelDecoder massCancelDecoder = new MassCancelDecoder();
    private final ReferencePriceDecoder refDecoder = new ReferencePriceDecoder();
    private final SymbolDefinitionDecoder symbolDefDecoder = new SymbolDefinitionDecoder();
    private final SymbolStateCommandDecoder stateDecoder = new SymbolStateCommandDecoder();
    private final RiskLimitUpdateDecoder riskDecoder = new RiskLimitUpdateDecoder();
    private final DayRolloverDecoder dayRollDecoder = new DayRolloverDecoder();

    // ---- state (package-private for the snapshot codec and tests) ----
    final OrderPool orderPool;
    final LevelPool levelPool;
    final ArrayList<Symbol> symbols = new ArrayList<>();
    final Int2IntHashMap symbolIdxById = new Int2IntHashMap(-1);
    final ArrayList<Firm> firms = new ArrayList<>();
    final Int2IntHashMap firmIdxById = new Int2IntHashMap(-1);
    final ArrayList<FirmSymbol> firmSymbols = new ArrayList<>();
    final Long2ObjectHashMap<FirmSymbol> fsByKey = new Long2ObjectHashMap<>();
    final ArrayList<Session> sessions = new ArrayList<>();
    final LongPairIntMap sessionIdx = new LongPairIntMap(1024);
    LongPairIntMap dedupe;
    final Long2LongHashMap orderById = new Long2LongHashMap(-1L);
    final OrderList allOrders = new OrderList(Order.L_GLOBAL);
    final ArrayList<PendingTransition> pending = new ArrayList<>();
    /** band-stp: running volatility auction timers, timerId -> symbol index (derived from Symbol.volTimerId, not snapshotted). */
    final Long2LongHashMap volTimers = new Long2LongHashMap(-1L);
    long orderSeq;
    long matchSeq;
    long timerSeq;
    /** Trading date of the last applied DayRollover (0 = none yet); makes the roll idempotent. */
    int tradingDate;
    long internalErrors;

    // ---- per-step scratch ----
    private PriceLevel[] dirty = new PriceLevel[64];
    private int dirtyCount;
    private long[] dirtyKeys = new long[64];
    private int[] touched = new int[64];
    private long aPrice;
    private long aMatched;
    private long aImbalance;
    private byte aSide;
    private byte matchStop = STOP_NONE;

    // ---- snapshot load ----
    private SnapshotCodec.Loader loader;

    public MatcherShard(final int shardId)
    {
        this(shardId, MatcherConfig.DEFAULT);
    }

    public MatcherShard(final int shardId, final MatcherConfig config)
    {
        if (shardId < 0 || shardId > 0xFFFF)
        {
            throw new IllegalArgumentException("shardId out of range: " + shardId);
        }
        this.shardId = shardId;
        this.idBase = (long)shardId << 48;
        this.config = config;
        this.ew = new EventWriter(shardId);
        this.orderPool = new OrderPool(config.orderPoolSize());
        this.levelPool = new LevelPool(config.levelPoolSize());
        this.dedupe = new LongPairIntMap(config.dedupeCapacity());
    }

    @Override
    public int shardId()
    {
        return shardId;
    }

    @Override
    public void bind(final EventSink events, final TimerScheduler timers)
    {
        this.ew.sink = events;
        this.timers = timers;
    }

    @Override
    public long lastEventSeq()
    {
        return ew.eventSeq;
    }

    /** Commands that threw unexpectedly (a bug). Exposed as a metric; 0 in a healthy system. */
    public long internalErrors()
    {
        return internalErrors;
    }

    /** Current chain hash (copy). */
    public byte[] chainHash()
    {
        return ew.chain.current().clone();
    }

    // =====================================================================================
    // Entry points
    // =====================================================================================

    @Override
    public void onCommand(final DirectBuffer buffer, final int offset, final int length, final long logPosition, final long timestampNs)
    {
        ew.begin(logPosition, timestampNs);
        ew.hashInput(buffer, offset, length);
        final int templateId = length >= HDR ? buffer.getShort(offset + 2, ByteOrder.LITTLE_ENDIAN) & 0xFFFF : 0;
        try
        {
            dispatch(buffer, offset, length, templateId, timestampNs);
            flushBook(false);
        }
        catch (final RuntimeException ex)
        {
            onInternalError(ex, templateId, logPosition);
        }
        ew.commandApplied(templateId);
    }

    @Override
    public void onTimer(final long correlationId, final long logPosition, final long timestampNs)
    {
        ew.begin(logPosition, timestampNs);
        ew.hashTimer(correlationId);
        try
        {
            PendingTransition p = null;
            for (int i = 0; i < pending.size(); i++)
            {
                if (pending.get(i).timerId == correlationId)
                {
                    p = pending.remove(i);
                    break;
                }
            }
            if (p != null)
            {
                applyStateCommand(p.symbolId, p.targetState, p.correlationId, p.reasonCode);
            }
            else
            {
                onVolatilityTimer(correlationId);
            }
            flushBook(false);
        }
        catch (final RuntimeException ex)
        {
            onInternalError(ex, 0, logPosition);
        }
        ew.commandApplied(0);
    }

    private void onInternalError(final RuntimeException ex, final int templateId, final long logPosition)
    {
        internalErrors++;
        for (int i = 0; i < dirtyCount; i++)
        {
            dirty[i].dirty = false;
        }
        dirtyCount = 0;
        matchStop = STOP_NONE;
        LOG.error("internal error applying template {} at log position {}", templateId, logPosition, ex);
    }

    private static boolean wellFormed(final DirectBuffer b, final int off, final int len, final int minBlock)
    {
        if (len < HDR)
        {
            return false;
        }
        final int blockLength = b.getShort(off, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final int schemaId = b.getShort(off + 4, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        return schemaId == SCHEMA_ID && blockLength >= minBlock && len >= HDR + blockLength;
    }

    private static int blockLength(final DirectBuffer b, final int off)
    {
        return b.getShort(off, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
    }

    private void dispatch(final DirectBuffer b, final int off, final int len, final int templateId, final long now)
    {
        switch (templateId)
        {
            case NewOrderDecoder.TEMPLATE_ID:
                if (wellFormed(b, off, len, NewOrderDecoder.BLOCK_LENGTH))
                {
                    newOrderDecoder.wrap(b, off + HDR, blockLength(b, off), 0);
                    onNewOrder(newOrderDecoder);
                    return;
                }
                break;
            case CancelOrderDecoder.TEMPLATE_ID:
                if (wellFormed(b, off, len, CancelOrderDecoder.BLOCK_LENGTH))
                {
                    cancelDecoder.wrap(b, off + HDR, blockLength(b, off), 0);
                    onCancel(cancelDecoder);
                    return;
                }
                break;
            case ReplaceOrderDecoder.TEMPLATE_ID:
                if (wellFormed(b, off, len, ReplaceOrderDecoder.BLOCK_LENGTH))
                {
                    replaceDecoder.wrap(b, off + HDR, blockLength(b, off), 0);
                    onReplace(replaceDecoder);
                    return;
                }
                break;
            case MassCancelDecoder.TEMPLATE_ID:
                if (wellFormed(b, off, len, MassCancelDecoder.BLOCK_LENGTH))
                {
                    massCancelDecoder.wrap(b, off + HDR, blockLength(b, off), 0);
                    onMassCancel(massCancelDecoder);
                    return;
                }
                break;
            case ReferencePriceDecoder.TEMPLATE_ID:
                if (wellFormed(b, off, len, ReferencePriceDecoder.BLOCK_LENGTH))
                {
                    refDecoder.wrap(b, off + HDR, blockLength(b, off), 0);
                    onReferencePrice(refDecoder);
                    return;
                }
                break;
            case SymbolDefinitionDecoder.TEMPLATE_ID:
                if (wellFormed(b, off, len, SymbolDefinitionDecoder.BLOCK_LENGTH))
                {
                    symbolDefDecoder.wrap(b, off + HDR, blockLength(b, off), 0);
                    onSymbolDefinition(symbolDefDecoder);
                    return;
                }
                break;
            case SymbolStateCommandDecoder.TEMPLATE_ID:
                if (wellFormed(b, off, len, SymbolStateCommandDecoder.BLOCK_LENGTH))
                {
                    stateDecoder.wrap(b, off + HDR, blockLength(b, off), 0);
                    onSymbolState(stateDecoder, now);
                    return;
                }
                break;
            case RiskLimitUpdateDecoder.TEMPLATE_ID:
                if (wellFormed(b, off, len, RiskLimitUpdateDecoder.BLOCK_LENGTH))
                {
                    riskDecoder.wrap(b, off + HDR, blockLength(b, off), 0);
                    onRiskLimit(riskDecoder);
                    return;
                }
                break;
            case DayRolloverDecoder.TEMPLATE_ID:
                if (wellFormed(b, off, len, DayRolloverDecoder.BLOCK_LENGTH))
                {
                    dayRollDecoder.wrap(b, off + HDR, blockLength(b, off), 0);
                    onDayRollover(dayRollDecoder, now);
                    return;
                }
                break;
            default:
                break;
        }
        ew.commandRejected(0, templateId, RejectReason.MALFORMED);
    }

    // =====================================================================================
    // Lookups
    // =====================================================================================

    private Firm firm(final int firmId)
    {
        final int i = firmIdxById.get(firmId);
        return i < 0 ? null : firms.get(i);
    }

    /** The symbol if defined and not REMOVED. */
    private Symbol liveSymbol(final long symbolId)
    {
        if (symbolId < 0 || symbolId > 0xFFFF_FFFFL)
        {
            return null;
        }
        final int i = symbolIdxById.get((int)symbolId);
        if (i < 0)
        {
            return null;
        }
        final Symbol s = symbols.get(i);
        return s.state == REMOVED ? null : s;
    }

    private FirmSymbol findFs(final Firm f, final Symbol s)
    {
        return fsByKey.get(FirmSymbol.key(f.idx, s.idx));
    }

    FirmSymbol fs(final Firm f, final Symbol s)
    {
        final long key = FirmSymbol.key(f.idx, s.idx);
        FirmSymbol fs = fsByKey.get(key);
        if (fs == null)
        {
            fs = new FirmSymbol(f, s);
            fsByKey.put(key, fs);
            firmSymbols.add(fs);
        }
        return fs;
    }

    Session session(final int firmId, final long sessionId)
    {
        final long a = firmId & 0xFFFF_FFFFL;
        int i = sessionIdx.get(a, sessionId);
        if (i == LongPairIntMap.MISSING)
        {
            i = sessions.size();
            sessions.add(new Session());
            sessionIdx.insert(a, sessionId, i);
        }
        return sessions.get(i);
    }

    private static boolean validSide(final short v)
    {
        return v == 1 || v == 2;
    }

    private static boolean validBool(final short v)
    {
        return v == 0 || v == 1;
    }

    private static OrdStatus liveStatus(final Order o)
    {
        return o.cumQty == 0 ? OrdStatus.NEW : o.leaves() == 0 ? OrdStatus.FILLED : OrdStatus.PARTIALLY_FILLED;
    }

    private static OrdStatus doneStatus(final int code)
    {
        switch (code)
        {
            case DONE_FILLED:
                return OrdStatus.FILLED;
            case DONE_CANCELED:
                return OrdStatus.CANCELED;
            case DONE_EXPIRED:
                return OrdStatus.EXPIRED;
            default:
                return OrdStatus.REJECTED;
        }
    }

    // =====================================================================================
    // NewOrder
    // =====================================================================================

    private void onNewOrder(final NewOrderDecoder d)
    {
        final long clOrdId = d.clOrdId();
        final short sideRaw = d.sideRaw();
        final short typeRaw = d.ordTypeRaw();
        final short tifRaw = d.timeInForceRaw();
        final short resendRaw = d.possResendRaw();
        if (!validSide(sideRaw) || !validSide(typeRaw) || !validSide(tifRaw) || !validBool(resendRaw))
        {
            ew.commandRejected(clOrdId, NewOrderDecoder.TEMPLATE_ID, RejectReason.MALFORMED);
            return;
        }
        final int firmId = (int)d.firmId();
        final long firmKey = d.firmId();
        int entry = dedupe.find(firmKey, clOrdId);
        if (entry >= 0 && resendRaw == 1)
        {
            return; // possResend duplicate: dropped silently
        }

        final byte side = (byte)sideRaw;
        final byte ordType = (byte)typeRaw;
        final byte tif = (byte)tifRaw;
        final boolean buy = side == BUY;
        final boolean market = ordType == MARKET;
        final long sessionId = d.sessionId();
        final long price = market ? NULL_PRICE : d.price();
        final long qty = d.quantity();
        final long gwTs = d.gatewayInTsNs();
        final int accountId = (int)d.accountId();
        final long symbolId = d.symbolId();
        final int gatewayId = d.gatewayId();

        final Firm firm = firm(firmId);
        Symbol sym = null;
        RejectReason reason = null;
        long riskPrice = 0;
        if (firm == null)
        {
            reason = RejectReason.UNKNOWN_FIRM;
        }
        else if (!firm.enabled)
        {
            reason = RejectReason.FIRM_DISABLED;
        }
        else if ((sym = liveSymbol(symbolId)) == null)
        {
            reason = RejectReason.UNKNOWN_SYMBOL;
        }
        else if (sym.state == PRE_OPEN || sym.state == CLOSED)
        {
            reason = RejectReason.SYMBOL_CLOSED;
        }
        else if (sym.state == HALTED)
        {
            reason = RejectReason.SYMBOL_HALTED;
        }
        else if (sym.state == AUCTION_CALL && market)
        {
            reason = RejectReason.MARKET_ORDER_NOT_ALLOWED;
        }
        else if (qty <= 0 || qty > sym.maxOrderQty || qty % sym.lotSize != 0)
        {
            reason = RejectReason.INVALID_QTY;
        }
        else if (!market && (price <= 0 || price > MAX_PRICE || price % sym.tickSize != 0))
        {
            reason = RejectReason.INVALID_PRICE;
        }
        else if (!market && sym.refPrice != NULL_PRICE && (price < sym.bandLow || price > sym.bandHigh))
        {
            reason = RejectReason.PRICE_BAND;
        }
        else if (market && sym.refPrice == NULL_PRICE)
        {
            reason = RejectReason.NO_REFERENCE_PRICE;
        }
        else if (entry >= 0)
        {
            reason = RejectReason.DUPLICATE_CLORDID;
        }
        else
        {
            riskPrice = market ? (buy ? sym.bandHigh : sym.bandLow) : price;
            if (riskPrice < 0)
            {
                riskPrice = 0;
            }
            final FirmSymbol fs = findFs(firm, sym);
            if (qty * riskPrice > firm.creditBudget - firm.creditUsed())
            {
                reason = RejectReason.CREDIT_LIMIT;
            }
            else if (positionBreached(firm, fs, buy, qty))
            {
                reason = RejectReason.POSITION_LIMIT;
            }
            else if (market && (buy ? sym.asks : sym.bids).size == 0)
            {
                reason = RejectReason.NO_LIQUIDITY;
            }
        }

        if (entry < 0)
        {
            entry = dedupe.insert(firmKey, clOrdId, SEEN);
        }
        if (reason != null)
        {
            ew.executionReport(
                sessionId, 0, clOrdId, 0, 0, price, qty, 0, NULL_PRICE, 0, 0, gwTs, firmId, accountId, (int)symbolId,
                gatewayId, ExecType.REJECTED, OrdStatus.REJECTED, side, ordType, tif, LiquidityFlag.NONE, reason,
                CancelReason.NONE);
            return;
        }

        final Order o = orderPool.acquire();
        o.orderId = idBase | ++orderSeq;
        o.clOrdId = clOrdId;
        o.sessionId = sessionId;
        o.price = price;
        o.riskPrice = riskPrice;
        o.qty = qty;
        o.cumQty = 0;
        o.accountId = accountId;
        o.gatewayId = gatewayId;
        o.side = side;
        o.ordType = ordType;
        o.tif = tif;
        o.firm = firm;
        o.symbol = sym;
        o.fs = fs(firm, sym);
        o.dedupeEntry = entry;
        o.linked = false;
        dedupe.setValue(entry, o.poolIndex);
        orderById.put(o.orderId, o.poolIndex);
        firm.openNotional += qty * riskPrice;
        if (buy)
        {
            o.fs.openBuyQty += qty;
        }
        else
        {
            o.fs.openSellQty += qty;
        }

        emitOrder(o, ExecType.NEW, OrdStatus.NEW, o.clOrdId, 0, gwTs, CancelReason.NONE, o.leaves());
        if (sym.state == CONTINUOUS)
        {
            matchAggressor(o, gwTs);
        }
        if (finishAggressor(o, gwTs))
        {
            startVolatilityAuction(sym);
        }
        flushBook(true);
    }

    private static boolean positionBreached(final Firm firm, final FirmSymbol fs, final boolean buy, final long qty)
    {
        final long net = fs == null ? 0 : fs.netPosition;
        final long max = firm.maxPositionPerSymbol;
        if (buy)
        {
            final long open = fs == null ? 0 : fs.openBuyQty;
            return net + open + qty > max;
        }
        final long open = fs == null ? 0 : fs.openSellQty;
        return net - open - qty < -max;
    }

    /**
     * After matching: rest a DAY limit remainder, cancel an IOC/market remainder, or retire if filled.
     * A remainder stopped by self-trade prevention is canceled ({@code SELF_TRADE}) whatever its type.
     *
     * @return true if matching stopped at the dynamic collar (the caller starts the volatility auction)
     */
    private boolean finishAggressor(final Order o, final long gwTs)
    {
        final byte stop = matchStop;
        matchStop = STOP_NONE;
        if (o.leaves() == 0)
        {
            retire(o, DONE_FILLED);
        }
        else if (stop == STOP_SELF_TRADE)
        {
            releaseRisk(o);
            emitOrder(o, ExecType.CANCELED, OrdStatus.CANCELED, o.clOrdId, 0, gwTs, CancelReason.SELF_TRADE, 0);
            retire(o, DONE_CANCELED);
        }
        else if (o.ordType == Codes.LIMIT && o.tif == Codes.DAY)
        {
            if (!o.linked)
            {
                linkLists(o);
            }
            addToLevel(o);
        }
        else
        {
            final CancelReason why = o.ordType == MARKET ? CancelReason.MARKET_COLLAR : CancelReason.IOC_REMAINDER;
            releaseRisk(o);
            emitOrder(o, ExecType.CANCELED, OrdStatus.CANCELED, o.clOrdId, 0, gwTs, why, 0);
            retire(o, DONE_CANCELED);
        }
        return stop == STOP_VOLATILITY;
    }

    // =====================================================================================
    // Cancel
    // =====================================================================================

    private void onCancel(final CancelOrderDecoder d)
    {
        final long clOrdId = d.clOrdId();
        final short resendRaw = d.possResendRaw();
        if (!validBool(resendRaw))
        {
            ew.commandRejected(clOrdId, CancelOrderDecoder.TEMPLATE_ID, RejectReason.MALFORMED);
            return;
        }
        final long firmKey = d.firmId();
        final int firmId = (int)firmKey;
        int entry = dedupe.find(firmKey, clOrdId);
        if (entry >= 0 && resendRaw == 1)
        {
            return;
        }
        final long sessionId = d.sessionId();
        final long origClOrdId = d.origClOrdId();
        final long gwTs = d.gatewayInTsNs();
        final long symbolId = d.symbolId();
        final int gatewayId = d.gatewayId();

        final Firm firm = firm(firmId);
        RejectReason reason = null;
        Order o = null;
        int target = SEEN;
        if (firm == null)
        {
            reason = RejectReason.UNKNOWN_FIRM;
        }
        else if (entry >= 0)
        {
            reason = RejectReason.DUPLICATE_CLORDID;
        }
        else
        {
            target = dedupe.get(firmKey, origClOrdId);
            if (target >= 0)
            {
                o = orderPool.byIndex(target);
                if (o.symbol.symbolId != (int)symbolId || symbolId > 0xFFFF_FFFFL)
                {
                    o = null;
                    reason = RejectReason.UNKNOWN_ORDER;
                }
            }
            else if (target == DONE_FILLED || target == DONE_CANCELED || target == DONE_EXPIRED)
            {
                reason = RejectReason.TOO_LATE_TO_CANCEL;
            }
            else
            {
                reason = RejectReason.UNKNOWN_ORDER;
            }
        }
        if (entry < 0)
        {
            entry = dedupe.insert(firmKey, clOrdId, SEEN);
        }
        if (reason != null)
        {
            emitCancelReject(o, target, sessionId, clOrdId, origClOrdId, gwTs, firmId, symbolId, gatewayId, reason);
            return;
        }
        dedupe.setValue(entry, DONE_CANCELED);
        cancelResting(o, ExecType.CANCELED, CancelReason.CLIENT, clOrdId, origClOrdId, sessionId, gwTs);
        flushBook(true);
    }

    private void emitCancelReject(
        final Order o, final int target, final long sessionId, final long clOrdId, final long origClOrdId,
        final long gwTs, final int firmId, final long symbolId, final int gatewayId, final RejectReason reason)
    {
        if (o != null)
        {
            ew.executionReport(
                sessionId, o.orderId, clOrdId, origClOrdId, 0, o.price, o.qty, 0, NULL_PRICE, o.leaves(), o.cumQty,
                gwTs, firmId, o.accountId, o.symbol.symbolId, o.gatewayId, ExecType.CANCEL_REJECTED, liveStatus(o),
                o.side, o.ordType, o.tif, LiquidityFlag.NONE, reason, CancelReason.NONE);
        }
        else
        {
            ew.executionReport(
                sessionId, 0, clOrdId, origClOrdId, 0, NULL_PRICE, 0, 0, NULL_PRICE, 0, 0, gwTs, firmId, 0,
                (int)symbolId, gatewayId, ExecType.CANCEL_REJECTED, doneStatus(target), (byte)0, Codes.LIMIT, Codes.DAY,
                LiquidityFlag.NONE, reason, CancelReason.NONE);
        }
    }

    // =====================================================================================
    // Replace
    // =====================================================================================

    private void onReplace(final ReplaceOrderDecoder d)
    {
        final long clOrdId = d.clOrdId();
        final short resendRaw = d.possResendRaw();
        if (!validBool(resendRaw))
        {
            ew.commandRejected(clOrdId, ReplaceOrderDecoder.TEMPLATE_ID, RejectReason.MALFORMED);
            return;
        }
        final long firmKey = d.firmId();
        final int firmId = (int)firmKey;
        int entry = dedupe.find(firmKey, clOrdId);
        if (entry >= 0 && resendRaw == 1)
        {
            return;
        }
        final long sessionId = d.sessionId();
        final long origClOrdId = d.origClOrdId();
        final long newPrice = d.newPrice();
        final long newQty = d.newQuantity();
        final long gwTs = d.gatewayInTsNs();
        final long symbolId = d.symbolId();
        final int gatewayId = d.gatewayId();

        final Firm firm = firm(firmId);
        Symbol sym = null;
        Order o = null;
        int target = SEEN;
        RejectReason reason = null;
        if (firm == null)
        {
            reason = RejectReason.UNKNOWN_FIRM;
        }
        else if (!firm.enabled)
        {
            reason = RejectReason.FIRM_DISABLED;
        }
        else if ((sym = liveSymbol(symbolId)) == null)
        {
            reason = RejectReason.UNKNOWN_SYMBOL;
        }
        else
        {
            target = dedupe.get(firmKey, origClOrdId);
            if (target >= 0)
            {
                o = orderPool.byIndex(target);
                if (o.symbol != sym)
                {
                    o = null;
                    target = SEEN;
                    reason = RejectReason.UNKNOWN_ORDER;
                }
            }
            else if (target == DONE_FILLED || target == DONE_CANCELED || target == DONE_EXPIRED)
            {
                reason = RejectReason.TOO_LATE_TO_CANCEL;
            }
            else
            {
                reason = RejectReason.UNKNOWN_ORDER;
            }
        }
        if (reason == null)
        {
            final long oldLeaves = o.leaves();
            final long newLeaves = newQty - o.cumQty;
            if (sym.state == HALTED)
            {
                reason = RejectReason.SYMBOL_HALTED;
            }
            else if (sym.state == PRE_OPEN || sym.state == CLOSED)
            {
                reason = RejectReason.SYMBOL_CLOSED;
            }
            else if (newQty <= 0 || newQty > sym.maxOrderQty || newQty % sym.lotSize != 0)
            {
                reason = RejectReason.INVALID_QTY;
            }
            else if (newQty <= o.cumQty)
            {
                reason = RejectReason.INVALID_REPLACE;
            }
            else if (newPrice <= 0 || newPrice > MAX_PRICE || newPrice % sym.tickSize != 0)
            {
                reason = RejectReason.INVALID_PRICE;
            }
            else if (sym.refPrice != NULL_PRICE && (newPrice < sym.bandLow || newPrice > sym.bandHigh))
            {
                reason = RejectReason.PRICE_BAND;
            }
            else if (entry >= 0)
            {
                reason = RejectReason.DUPLICATE_CLORDID;
            }
            else
            {
                final long oldN = oldLeaves * o.riskPrice;
                final long newN = newLeaves * newPrice;
                if (newN > oldN && newN - oldN > firm.creditBudget - firm.creditUsed())
                {
                    reason = RejectReason.CREDIT_LIMIT;
                }
                else if (newLeaves > oldLeaves && positionBreached(firm, o.fs, o.isBuy(), newLeaves - oldLeaves))
                {
                    reason = RejectReason.POSITION_LIMIT;
                }
            }
        }
        if (entry < 0)
        {
            entry = dedupe.insert(firmKey, clOrdId, SEEN);
        }
        if (reason != null)
        {
            emitCancelReject(o, target, sessionId, clOrdId, origClOrdId, gwTs, firmId, symbolId, gatewayId, reason);
            return;
        }

        // apply
        final boolean keepPriority = newPrice == o.price && newQty <= o.qty;
        final long oldLeaves = o.leaves();
        final long newLeaves = newQty - o.cumQty;
        o.firm.openNotional += newLeaves * newPrice - oldLeaves * o.riskPrice;
        if (o.isBuy())
        {
            o.fs.openBuyQty += newLeaves - oldLeaves;
        }
        else
        {
            o.fs.openSellQty += newLeaves - oldLeaves;
        }
        dedupe.setValue(o.dedupeEntry, SEEN);
        dedupe.setValue(entry, o.poolIndex);
        o.dedupeEntry = entry;
        o.clOrdId = clOrdId;
        o.gatewayId = gatewayId;
        if (o.sessionId != sessionId)
        {
            o.session.orders.remove(o);
            o.sessionId = sessionId;
            o.session = session(firmId, sessionId);
            o.session.orders.insertByOrderId(o);
        }
        if (keepPriority)
        {
            final PriceLevel l = o.level;
            markDirty(l);
            l.totalQty -= oldLeaves - newLeaves;
            o.qty = newQty;
            emitOrder(o, ExecType.REPLACED, liveStatus(o), clOrdId, origClOrdId, gwTs, CancelReason.NONE, o.leaves());
        }
        else
        {
            removeFromLevel(o);
            o.qty = newQty;
            o.price = newPrice;
            o.riskPrice = newPrice;
            emitOrder(o, ExecType.REPLACED, liveStatus(o), clOrdId, origClOrdId, gwTs, CancelReason.NONE, o.leaves());
            if (sym.state == CONTINUOUS)
            {
                matchAggressor(o, gwTs);
            }
            if (finishAggressor(o, gwTs))
            {
                startVolatilityAuction(sym);
            }
        }
        flushBook(true);
    }

    // =====================================================================================
    // Mass cancel
    // =====================================================================================

    private void onMassCancel(final MassCancelDecoder d)
    {
        final long requestId = d.requestId();
        final short scopeRaw = d.scopeRaw();
        final short reasonRaw = d.reasonRaw();
        if (scopeRaw < 1 || scopeRaw > 5 || (reasonRaw != 2 && reasonRaw != 3 && reasonRaw != 8))
        {
            ew.commandRejected(requestId, MassCancelDecoder.TEMPLATE_ID, RejectReason.MALFORMED);
            return;
        }
        final MassCancelScope scope = MassCancelScope.get(scopeRaw);
        final CancelReason reason = CancelReason.get(reasonRaw);
        final long sessionId = d.sessionId();
        final long gwTs = d.gatewayInTsNs();
        final int firmId = (int)d.firmId();
        final long symbolId = d.symbolId();
        final int gatewayId = d.gatewayId();
        final int targetGateway = d.targetGatewayId();

        final Firm firm = firm(firmId);
        OrderList list = null;
        boolean filterGateway = false;
        switch (scope)
        {
            case FIRM:
                list = firm == null ? null : firm.orders;
                break;
            case SESSION:
                if (firm != null)
                {
                    final int si = sessionIdx.get(firmId & 0xFFFF_FFFFL, sessionId);
                    list = si == LongPairIntMap.MISSING ? null : sessions.get(si).orders;
                }
                break;
            case FIRM_SYMBOL:
            {
                final Symbol s = liveSymbol(symbolId);
                final FirmSymbol fs = firm == null || s == null ? null : findFs(firm, s);
                list = fs == null ? null : fs.orders;
                break;
            }
            case SYMBOL:
            {
                final Symbol s = liveSymbol(symbolId);
                list = s == null ? null : s.orders;
                break;
            }
            case GATEWAY:
            default:
                list = allOrders;
                filterGateway = true;
                break;
        }
        int count = 0;
        if (list != null)
        {
            Order o = list.head;
            while (o != null)
            {
                final Order next = o.next[list.k];
                if (!filterGateway || o.gatewayId == targetGateway)
                {
                    final long ts = o.firm.firmId == firmId ? gwTs : 0;
                    cancelResting(o, ExecType.CANCELED, reason, o.clOrdId, 0, o.sessionId, ts);
                    count++;
                }
                o = next;
            }
        }
        flushBook(true);
        ew.massCancelDone(requestId, sessionId, firmId, count, gatewayId, scope);
    }

    // =====================================================================================
    // Control commands
    // =====================================================================================

    private void onReferencePrice(final ReferencePriceDecoder d)
    {
        final long sourceTs = d.sourceTsNs();
        final Symbol s = liveSymbol(d.symbolId());
        if (s == null)
        {
            ew.commandRejected(sourceTs, ReferencePriceDecoder.TEMPLATE_ID, RejectReason.UNKNOWN_SYMBOL);
            return;
        }
        if (sourceTs <= s.lastRefSourceTs)
        {
            return; // stale or duplicate (two consolidators): ignored
        }
        final long ref = d.referencePrice();
        if (ref <= 0 || ref > MAX_PRICE)
        {
            ew.commandRejected(sourceTs, ReferencePriceDecoder.TEMPLATE_ID, RejectReason.INVALID_PRICE);
            return;
        }
        s.refPrice = ref;
        s.lastRefSourceTs = sourceTs;
        s.computeBand();
        s.bids.maybeRecentre(ref);
        s.asks.maybeRecentre(ref);
        ew.referencePriceApplied(ref, d.nbbBid(), d.nbbOffer(), s.bandLow, s.bandHigh, sourceTs, s.symbolId);
        if (s.state == AUCTION_CALL)
        {
            maybeEmitIndicative(s);
        }
    }

    private void onSymbolDefinition(final SymbolDefinitionDecoder d)
    {
        final long corr = d.correlationId();
        final short initRaw = d.initialStateRaw();
        if (initRaw < 1 || initRaw > 6)
        {
            ew.commandRejected(corr, SymbolDefinitionDecoder.TEMPLATE_ID, RejectReason.MALFORMED);
            return;
        }
        final long tick = d.tickSize();
        final long lot = d.lotSize();
        final long maxQty = d.maxOrderQty();
        final long ref = d.referencePrice();
        final long symbolId = d.symbolId();
        final long bps = d.priceBandBps();
        final long dynBps = d.dynamicBandBps();
        final long volNs = d.volatilityAuctionNs();
        RejectReason reason = null;
        if (volNs < 0 || volNs > MAX_VOLATILITY_AUCTION_NS || (dynBps > 0 && volNs == 0))
        {
            reason = RejectReason.MALFORMED;
        }
        else if (tick <= 0 || tick > MAX_PRICE || (ref != NULL_PRICE && (ref <= 0 || ref > MAX_PRICE)))
        {
            reason = RejectReason.INVALID_PRICE;
        }
        else if (lot <= 0 || lot > MAX_QTY || maxQty <= 0 || maxQty > MAX_QTY)
        {
            reason = RejectReason.INVALID_QTY;
        }
        else if (symbolId == Codes.NULL_SYMBOL)
        {
            reason = RejectReason.UNKNOWN_SYMBOL;
        }
        final int existingIdx = symbolIdxById.get((int)symbolId);
        Symbol s = existingIdx < 0 ? null : symbols.get(existingIdx);
        final boolean fresh = s == null || s.state == REMOVED;
        if (reason == null && fresh)
        {
            if (initRaw == REMOVED)
            {
                reason = RejectReason.INVALID_STATE_TRANSITION;
            }
            else if (s == null && symbols.size() >= Codes.MAX_SYMBOLS)
            {
                reason = RejectReason.OTHER;
            }
        }
        if (reason != null)
        {
            ew.commandRejected(corr, SymbolDefinitionDecoder.TEMPLATE_ID, reason);
            return;
        }
        if (s == null)
        {
            s = new Symbol(symbols.size(), (int)symbolId, config.levelWindowTicks());
            symbols.add(s);
            symbolIdxById.put((int)symbolId, s.idx);
        }
        // change B: the band before this definition (a fresh symbol has none to compare against)
        final long oldRef = fresh ? NULL_PRICE : s.refPrice;
        final long oldBps = s.bandBps;
        final long oldLow = fresh ? NULL_PRICE : s.bandLow;
        final long oldHigh = fresh ? NULL_PRICE : s.bandHigh;
        if (fresh)
        {
            s.state = (byte)initRaw;
            s.refPrice = ref;
            s.lastRefSourceTs = Long.MIN_VALUE;
            s.lastTradePx = NULL_PRICE;
            s.resetAuction();
            s.resetDayCycle();
            s.bids.clearAll();
            s.asks.clearAll();
            s.bids.indexTick = tick;
            s.asks.indexTick = tick;
        }
        else
        {
            if (ref != NULL_PRICE && s.lastRefSourceTs == Long.MIN_VALUE)
            {
                s.refPrice = ref;
            }
            if (tick != s.tickSize)
            {
                final boolean empty = s.bids.size == 0 && s.asks.size == 0;
                final long it = empty ? tick : Codes.gcd(s.bids.indexTick, tick);
                s.bids.reindex(it, s.refPrice);
                s.asks.reindex(it, s.refPrice);
            }
        }
        s.tickSize = tick;
        s.lotSize = lot;
        s.maxOrderQty = maxQty;
        s.bandBps = bps;
        s.dynamicBandBps = dynBps;
        s.volatilityAuctionNs = volNs;
        d.getTicker(s.ticker, 0);
        s.computeBand();
        if (s.refPrice != NULL_PRICE)
        {
            s.bids.maybeRecentre(s.refPrice);
            s.asks.maybeRecentre(s.refPrice);
        }
        ew.symbolDefined(corr, s);
        // change B: publish the band whenever the definition set or moved it, so market data always knows it.
        // sourceTsNs is the symbol's effective reference timestamp (Long.MIN_VALUE while only definitions set it).
        if (s.refPrice != NULL_PRICE
            && (fresh || s.refPrice != oldRef || s.bandBps != oldBps || s.bandLow != oldLow || s.bandHigh != oldHigh))
        {
            ew.referencePriceApplied(
                s.refPrice, NULL_PRICE, NULL_PRICE, s.bandLow, s.bandHigh, s.lastRefSourceTs, s.symbolId);
        }
        if (s.state == AUCTION_CALL)
        {
            maybeEmitIndicative(s);
        }
    }

    private void onSymbolState(final SymbolStateCommandDecoder d, final long now)
    {
        final long corr = d.correlationId();
        final short targetRaw = d.targetStateRaw();
        if (targetRaw < 1 || targetRaw > 6)
        {
            ew.commandRejected(corr, SymbolStateCommandDecoder.TEMPLATE_ID, RejectReason.MALFORMED);
            return;
        }
        final long symbolId = d.symbolId();
        final int reasonCode = d.reasonCode();
        final long eff = d.effectiveTimeNs();
        if (symbolId != Codes.NULL_SYMBOL && liveSymbol(symbolId) == null)
        {
            ew.commandRejected(corr, SymbolStateCommandDecoder.TEMPLATE_ID, RejectReason.UNKNOWN_SYMBOL);
            return;
        }
        if (eff > 0)
        {
            final long timerId = ++timerSeq;
            pending.add(new PendingTransition(timerId, corr, symbolId, reasonCode, (byte)targetRaw, eff));
            timers.schedule(timerId, eff);
            return;
        }
        applyStateCommand(symbolId, (byte)targetRaw, corr, reasonCode);
    }

    private void applyStateCommand(final long symbolId, final byte target, final long corr, final int reasonCode)
    {
        if (symbolId == Codes.NULL_SYMBOL)
        {
            for (int i = 0; i < symbols.size(); i++)
            {
                final Symbol s = symbols.get(i);
                if (s.state != REMOVED && allowed(s, target))
                {
                    transition(s, target, corr, reasonCode, StateChangeCause.COMMAND);
                }
            }
            return;
        }
        final Symbol s = liveSymbol(symbolId);
        if (s == null)
        {
            ew.commandRejected(corr, SymbolStateCommandDecoder.TEMPLATE_ID, RejectReason.UNKNOWN_SYMBOL);
        }
        else if (!allowed(s, target))
        {
            ew.commandRejected(corr, SymbolStateCommandDecoder.TEMPLATE_ID, RejectReason.INVALID_STATE_TRANSITION);
        }
        else
        {
            transition(s, target, corr, reasonCode, StateChangeCause.COMMAND);
        }
    }

    /**
     * The transition table, plus (band-stp) AUCTION_CALL -> AUCTION_CALL while a volatility auction runs: a scheduled
     * auction (e.g. the closing auction) arriving then takes the auction over, so it is not lost and the volatility
     * timer no longer ends it.
     */
    private static boolean allowed(final Symbol s, final byte target)
    {
        return Codes.allowed(s.state, target) || (target == AUCTION_CALL && s.inVolatilityAuction());
    }

    private void transition(
        final Symbol s, final byte target, final long corr, final int reasonCode, final StateChangeCause cause)
    {
        final byte prev = s.state;
        final long matchSeqBefore = matchSeq;
        // band-stp: any transition ends a running volatility auction (its timer is canceled)
        final boolean volatility = s.inVolatilityAuction();
        if (volatility)
        {
            clearVolatilityTimer(s);
        }
        if ((prev == AUCTION_CALL && (target == CONTINUOUS || target == CLOSED)) || (target == CONTINUOUS && s.crossed()))
        {
            final long px = uncross(s);
            if (volatility && target == CONTINUOUS && px != NULL_PRICE)
            {
                recentre(s, px);
            }
        }
        s.state = target;
        ew.symbolStateChanged(corr, s.symbolId, reasonCode, prev, target, cause);
        if (target == CLOSED || target == REMOVED)
        {
            final boolean closed = target == CLOSED;
            Order o = s.orders.head;
            while (o != null)
            {
                final Order next = o.next[Order.L_SYMBOL];
                cancelResting(
                    o, closed ? ExecType.EXPIRED : ExecType.CANCELED,
                    closed ? CancelReason.END_OF_DAY : CancelReason.SYMBOL_REMOVED, o.clOrdId, 0, o.sessionId, 0);
                o = next;
            }
            flushBook(false);
            s.resetAuction();
            if (closed)
            {
                closeSymbol(s, prev == AUCTION_CALL && matchSeq != matchSeqBefore);
            }
        }
        else if (target == AUCTION_CALL)
        {
            s.resetAuction();
            maybeEmitIndicative(s);
        }
    }

    private void onRiskLimit(final RiskLimitUpdateDecoder d)
    {
        final long corr = d.correlationId();
        final short enabledRaw = d.enabledRaw();
        if (!validBool(enabledRaw))
        {
            ew.commandRejected(corr, RiskLimitUpdateDecoder.TEMPLATE_ID, RejectReason.MALFORMED);
            return;
        }
        final long budget = d.creditBudget();
        final long maxPos = d.maxPositionPerSymbol();
        if (budget < 0)
        {
            ew.commandRejected(corr, RiskLimitUpdateDecoder.TEMPLATE_ID, RejectReason.CREDIT_LIMIT);
            return;
        }
        if (maxPos < 0)
        {
            ew.commandRejected(corr, RiskLimitUpdateDecoder.TEMPLATE_ID, RejectReason.POSITION_LIMIT);
            return;
        }
        final int firmId = (int)d.firmId();
        Firm f = firm(firmId);
        if (f == null)
        {
            f = new Firm(firmId, firms.size());
            firms.add(f);
            firmIdxById.put(firmId, f.idx);
        }
        f.creditBudget = budget;
        f.maxPositionPerSymbol = maxPos;
        f.enabled = enabledRaw == 1;
        ew.riskLimitApplied(corr, budget, f.creditUsed(), maxPos, firmId, f.enabled);
    }

    private void onDayRollover(final DayRolloverDecoder d, final long now)
    {
        final long corr = d.correlationId();
        // Idempotent: a roll that does not advance the trading date (a resend whose first copy was applied
        // but whose DayRolled the sender had not seen yet) must not expire the new day's orders or clear
        // the dedupe table.
        if (d.tradingDate() <= tradingDate)
        {
            ew.commandRejected(corr, DayRolloverDecoder.TEMPLATE_ID, RejectReason.INVALID_STATE_TRANSITION);
            return;
        }
        final int closingDate = tradingDate;
        tradingDate = d.tradingDate();
        int expired = 0;
        Order o = allOrders.head;
        while (o != null)
        {
            final Order next = o.next[Order.L_GLOBAL];
            cancelResting(o, ExecType.EXPIRED, CancelReason.END_OF_DAY, o.clOrdId, 0, o.sessionId, 0);
            expired++;
            o = next;
        }
        flushBook(false);
        dedupe.clear();
        for (int i = 0; i < firms.size(); i++)
        {
            final Firm f = firms.get(i);
            f.tradedNet = 0;
            f.openNotional = 0;
        }
        // day-cycle: positions carry over to the next day; only open quantities (all orders expired) reset.
        for (int i = 0; i < firmSymbols.size(); i++)
        {
            final FirmSymbol fs = firmSymbols.get(i);
            fs.openBuyQty = 0;
            fs.openSellQty = 0;
        }
        for (int i = 0; i < symbols.size(); i++)
        {
            final Symbol s = symbols.get(i);
            if (s.state == REMOVED)
            {
                continue;
            }
            // day-cycle: a symbol whose close is not current (never closed today, or traded since) closes now
            if (!s.hasClose)
            {
                closeSymbol(s, false, closingDate);
            }
            if (s.state != PRE_OPEN)
            {
                final byte prev = s.state;
                if (s.inVolatilityAuction())
                {
                    clearVolatilityTimer(s);
                }
                s.state = PRE_OPEN;
                s.resetAuction();
                ew.symbolStateChanged(corr, s.symbolId, 0, prev, PRE_OPEN, StateChangeCause.DAY_ROLLOVER);
            }
            rollDayCycle(s, now);
        }
        ew.dayRolled(corr, d.tradingDate(), expired);
    }

    // =====================================================================================
    // day-cycle: last trade, official close, next day's reference price
    // =====================================================================================

    private static void recordTrade(final Symbol s, final long px)
    {
        s.lastTradePx = px;
        s.tradedToday = true;
        s.hasClose = false;
    }

    private void closeSymbol(final Symbol s, final boolean auctionTraded)
    {
        closeSymbol(s, auctionTraded, tradingDate);
    }

    /**
     * Determines the symbol's close for the trading day and emits ClosingPrice. A close that is still current
     * (determined earlier today, no trade since) is repeated unchanged unless the closing auction just traded.
     */
    private void closeSymbol(final Symbol s, final boolean auctionTraded, final int date)
    {
        if (auctionTraded)
        {
            s.closePx = s.lastTradePx;
            s.closeSource = Codes.CLOSE_AUCTION;
        }
        else if (!s.hasClose)
        {
            if (s.tradedToday)
            {
                s.closePx = s.lastTradePx;
                s.closeSource = Codes.CLOSE_LAST_TRADE;
            }
            else if (s.prevClosePx != NULL_PRICE)
            {
                s.closePx = s.prevClosePx;
                s.closeSource = Codes.CLOSE_PREVIOUS_CLOSE;
            }
            else
            {
                s.closePx = NULL_PRICE;
                s.closeSource = Codes.CLOSE_NONE;
            }
        }
        s.hasClose = true;
        ew.closingPrice(s, date);
    }

    /**
     * Start of a new trading day for one symbol: the close becomes the previous close and, when it is a price,
     * the reference price. The reference counts as applied with sourceTsNs = max(last applied, roll time), so an
     * external ReferencePrice observed at or before the roll is ignored as stale and a SymbolDefinition update
     * no longer overrides it.
     */
    private void rollDayCycle(final Symbol s, final long now)
    {
        final long close = s.closePx;
        if (close != NULL_PRICE)
        {
            s.refPrice = close;
            s.lastRefSourceTs = Math.max(s.lastRefSourceTs, now);
            s.computeBand();
            s.bids.maybeRecentre(close);
            s.asks.maybeRecentre(close);
            ew.referencePriceApplied(close, NULL_PRICE, NULL_PRICE, s.bandLow, s.bandHigh, s.lastRefSourceTs, s.symbolId);
        }
        s.prevClosePx = close;
        s.closePx = NULL_PRICE;
        s.closeSource = Codes.CLOSE_NONE;
        s.hasClose = false;
        s.tradedToday = false;
    }

    // =====================================================================================
    // band-stp: volatility interruption
    // =====================================================================================

    /**
     * Matching stopped because the next print would leave the dynamic collar: the symbol goes to AUCTION_CALL
     * for volatilityAuctionNs (a cluster timer). Book deltas of the continuous part are flushed first.
     */
    private void startVolatilityAuction(final Symbol s)
    {
        flushBook(false);
        final long timerId = ++timerSeq;
        final long end = ew.timestampNs() + s.volatilityAuctionNs;
        s.state = AUCTION_CALL;
        s.volTimerId = timerId;
        s.volEndNs = end;
        volTimers.put(timerId, s.idx);
        ew.symbolStateChanged(0, s.symbolId, 0, CONTINUOUS, AUCTION_CALL, StateChangeCause.VOLATILITY_INTERRUPTION);
        timers.schedule(timerId, end);
        s.resetAuction();
        maybeEmitIndicative(s);
    }

    /** A timer that is not a scheduled transition: the end of a volatility auction, unless it was superseded. */
    private void onVolatilityTimer(final long timerId)
    {
        final long idx = volTimers.get(timerId);
        if (idx < 0)
        {
            return; // superseded (halt, close, scheduled auction, day roll, removal): nothing to do
        }
        transition(symbols.get((int)idx), CONTINUOUS, 0, 0, StateChangeCause.VOLATILITY_AUCTION_END);
    }

    private void clearVolatilityTimer(final Symbol s)
    {
        volTimers.remove(s.volTimerId);
        timers.cancel(s.volTimerId);
        s.volTimerId = 0;
        s.volEndNs = 0;
    }

    /**
     * After a volatility auction that traded: the static band is re-centred on the uncross price. Like the reference
     * a DayRollover sets, it counts as applied at max(last applied sourceTsNs, cluster time), which the event carries.
     */
    private void recentre(final Symbol s, final long px)
    {
        final long now = ew.timestampNs();
        s.refPrice = px;
        if (now > s.lastRefSourceTs)
        {
            s.lastRefSourceTs = now;
        }
        s.computeBand();
        s.bids.maybeRecentre(px);
        s.asks.maybeRecentre(px);
        ew.referencePriceApplied(px, NULL_PRICE, NULL_PRICE, s.bandLow, s.bandHigh, s.lastRefSourceTs, s.symbolId);
    }

    // =====================================================================================
    // Matching
    // =====================================================================================

    private void matchAggressor(final Order o, final long gwTs)
    {
        final Symbol sym = o.symbol;
        final boolean buy = o.isBuy();
        final BookSide opp = buy ? sym.asks : sym.bids;
        final long limit = o.riskPrice;
        // band-stp: the collar is centred on the last print before this order started matching, so one
        // order cannot walk the price past it in small steps
        final long centre = sym.collarCentre();
        matchStop = STOP_NONE;
        while (o.qty > o.cumQty)
        {
            final PriceLevel lvl = opp.best();
            if (lvl == null)
            {
                break;
            }
            final long px = lvl.price;
            if (buy ? px > limit : px < limit)
            {
                break;
            }
            if (sym.outsideCollar(centre, px))
            {
                matchStop = STOP_VOLATILITY;
                break;
            }
            final Order m = lvl.orders.head;
            if (m.firm == o.firm)
            {
                matchStop = STOP_SELF_TRADE;
                break;
            }
            final long q = Math.min(o.leaves(), m.leaves());
            final long matchId = idBase | ++matchSeq;
            applyFill(o, q, px);
            applyFill(m, q, px);
            markDirty(lvl);
            lvl.totalQty -= q;
            if (buy)
            {
                ew.trade(matchId, px, q, o.orderId, m.orderId, sym.symbolId, o.firm.firmId, m.firm.firmId, o.side, false);
            }
            else
            {
                ew.trade(matchId, px, q, m.orderId, o.orderId, sym.symbolId, m.firm.firmId, o.firm.firmId, o.side, false);
            }
            recordTrade(sym, px);
            emitFill(o, matchId, q, px, gwTs, LiquidityFlag.TAKER);
            emitFill(m, matchId, q, px, 0, LiquidityFlag.MAKER);
            if (m.leaves() == 0)
            {
                removeFromLevel(m);
                retire(m, DONE_FILLED);
            }
        }
    }

    private static void applyFill(final Order o, final long q, final long px)
    {
        o.cumQty += q;
        final Firm f = o.firm;
        final FirmSymbol fs = o.fs;
        f.openNotional -= q * o.riskPrice;
        if (o.side == BUY)
        {
            f.tradedNet += q * px;
            fs.netPosition += q;
            fs.openBuyQty -= q;
        }
        else
        {
            f.tradedNet -= q * px;
            fs.netPosition -= q;
            fs.openSellQty -= q;
        }
    }

    private static void releaseRisk(final Order o)
    {
        final long leaves = o.leaves();
        o.firm.openNotional -= leaves * o.riskPrice;
        if (o.side == BUY)
        {
            o.fs.openBuyQty -= leaves;
        }
        else
        {
            o.fs.openSellQty -= leaves;
        }
    }

    // =====================================================================================
    // Auctions
    // =====================================================================================

    /**
     * Indicative uncross into aPrice/aMatched/aImbalance/aSide. Candidates are every limit price in the
     * book plus the reference price (when on tick). Max volume, then min imbalance, then closest to the
     * reference price, then lowest price.
     */
    void computeAuction(final Symbol s)
    {
        final BookSide bids = s.bids;
        final BookSide asks = s.asks;
        final long ref = s.refPrice;
        final long totalAsk = asks.totalQty();
        long bestV = 0;
        long bestImb = 0;
        long bestP = NULL_PRICE;
        long bestDist = 0;
        long bestBuy = 0;
        long bestSell = 0;
        int i = bids.size - 1;
        int j = 0;
        long cumBid = 0;
        long askAbove = 0;
        while (i >= 0 || j < asks.size)
        {
            final long pb = i >= 0 ? bids.sorted[i].price : Long.MIN_VALUE;
            final long pa = j < asks.size ? asks.sorted[j].price : Long.MIN_VALUE;
            final long p = Math.max(pb, pa);
            while (i >= 0 && bids.sorted[i].price == p)
            {
                cumBid += bids.sorted[i].totalQty;
                i--;
            }
            final long sellLe = totalAsk - askAbove;
            while (j < asks.size && asks.sorted[j].price == p)
            {
                askAbove += asks.sorted[j].totalQty;
                j++;
            }
            final long v = Math.min(cumBid, sellLe);
            if (v > 0)
            {
                final long imb = Math.abs(cumBid - sellLe);
                final long dist = ref == NULL_PRICE ? 0 : Math.abs(p - ref);
                if (bestP == NULL_PRICE || better(v, imb, dist, p, bestV, bestImb, bestDist, bestP))
                {
                    bestV = v;
                    bestImb = imb;
                    bestDist = dist;
                    bestP = p;
                    bestBuy = cumBid;
                    bestSell = sellLe;
                }
            }
        }
        if (ref != NULL_PRICE && bestP != NULL_PRICE && ref % s.tickSize == 0)
        {
            long buyV = 0;
            for (int k = bids.size - 1; k >= 0 && bids.sorted[k].price >= ref; k--)
            {
                buyV += bids.sorted[k].totalQty;
            }
            long sellV = 0;
            for (int k = asks.size - 1; k >= 0 && asks.sorted[k].price <= ref; k--)
            {
                sellV += asks.sorted[k].totalQty;
            }
            final long v = Math.min(buyV, sellV);
            final long imb = Math.abs(buyV - sellV);
            if (v > 0 && better(v, imb, 0, ref, bestV, bestImb, bestDist, bestP))
            {
                bestV = v;
                bestImb = imb;
                bestP = ref;
                bestBuy = buyV;
                bestSell = sellV;
            }
        }
        aPrice = bestP;
        aMatched = bestV;
        aImbalance = bestImb;
        aSide = bestP == NULL_PRICE || bestBuy == bestSell ? Codes.NO_SIDE : bestBuy > bestSell ? BUY : SELL;
    }

    private static boolean better(
        final long v, final long imb, final long dist, final long p,
        final long bv, final long bimb, final long bdist, final long bp)
    {
        if (v != bv)
        {
            return v > bv;
        }
        if (imb != bimb)
        {
            return imb < bimb;
        }
        if (dist != bdist)
        {
            return dist < bdist;
        }
        return p < bp;
    }

    private void maybeEmitIndicative(final Symbol s)
    {
        computeAuction(s);
        if (aPrice != s.auctPrice || aMatched != s.auctMatched || aImbalance != s.auctImbalance || aSide != s.auctSide)
        {
            s.auctPrice = aPrice;
            s.auctMatched = aMatched;
            s.auctImbalance = aImbalance;
            s.auctSide = aSide;
            ew.auctionUpdate(aPrice, aMatched, aImbalance, s.symbolId, aSide, false);
        }
    }

    /** @return the uncross price if anything traded, else null price */
    private long uncross(final Symbol s)
    {
        computeAuction(s);
        final long p = aPrice;
        final long v = aMatched;
        final long imb = aImbalance;
        final byte imbSide = aSide;
        if (v > 0)
        {
            while (true)
            {
                final PriceLevel bl = s.bids.best();
                final PriceLevel al = s.asks.best();
                if (bl == null || al == null || bl.price < p || al.price > p)
                {
                    break;
                }
                final Order b = bl.orders.head;
                final Order a = al.orders.head;
                final long q = Math.min(b.leaves(), a.leaves());
                final long matchId = idBase | ++matchSeq;
                applyFill(b, q, p);
                applyFill(a, q, p);
                markDirty(bl);
                bl.totalQty -= q;
                markDirty(al);
                al.totalQty -= q;
                ew.trade(matchId, p, q, b.orderId, a.orderId, s.symbolId, b.firm.firmId, a.firm.firmId, Codes.NO_SIDE, true);
                recordTrade(s, p);
                emitFill(b, matchId, q, p, 0, LiquidityFlag.AUCTION);
                emitFill(a, matchId, q, p, 0, LiquidityFlag.AUCTION);
                if (b.leaves() == 0)
                {
                    removeFromLevel(b);
                    retire(b, DONE_FILLED);
                }
                if (a.leaves() == 0)
                {
                    removeFromLevel(a);
                    retire(a, DONE_FILLED);
                }
            }
        }
        flushBook(false);
        ew.auctionUpdate(p, v, imb, s.symbolId, imbSide, true);
        s.resetAuction();
        return v > 0 ? p : NULL_PRICE;
    }

    // =====================================================================================
    // Book and list maintenance
    // =====================================================================================

    private void linkLists(final Order o)
    {
        allOrders.append(o);
        o.firm.orders.append(o);
        o.fs.orders.append(o);
        o.session = session(o.firm.firmId, o.sessionId);
        o.session.orders.append(o);
        o.symbol.orders.append(o);
        o.linked = true;
    }

    private void unlinkLists(final Order o)
    {
        allOrders.remove(o);
        o.firm.orders.remove(o);
        o.fs.orders.remove(o);
        o.session.orders.remove(o);
        o.symbol.orders.remove(o);
        o.linked = false;
    }

    private void addToLevel(final Order o)
    {
        final BookSide side = o.isBuy() ? o.symbol.bids : o.symbol.asks;
        PriceLevel l = side.find(o.price);
        if (l == null)
        {
            l = levelPool.acquire(side, o.price);
            side.indexPut(l);
        }
        markDirty(l);
        if (!l.attached)
        {
            side.attach(l);
        }
        l.orders.append(o);
        l.totalQty += o.leaves();
        o.level = l;
    }

    private void removeFromLevel(final Order o)
    {
        final PriceLevel l = o.level;
        markDirty(l);
        l.orders.remove(o);
        l.totalQty -= o.leaves();
        o.level = null;
        if (l.orders.size == 0)
        {
            l.side.detach(l);
        }
    }

    /** Removes a resting order completely, releasing its risk, and emits its CANCELED/EXPIRED report. */
    private void cancelResting(
        final Order o, final ExecType execType, final CancelReason reason, final long clOrdId, final long origClOrdId,
        final long sessionId, final long gwTs)
    {
        removeFromLevel(o);
        releaseRisk(o);
        final OrdStatus status = execType == ExecType.EXPIRED ? OrdStatus.EXPIRED : OrdStatus.CANCELED;
        ew.executionReport(
            sessionId, o.orderId, clOrdId, origClOrdId, 0, o.price, o.qty, 0, NULL_PRICE, 0, o.cumQty, gwTs,
            o.firm.firmId, o.accountId, o.symbol.symbolId, o.gatewayId, execType, status, o.side, o.ordType, o.tif,
            LiquidityFlag.NONE, RejectReason.NONE, reason);
        retire(o, execType == ExecType.EXPIRED ? DONE_EXPIRED : DONE_CANCELED);
    }

    /** Order leaves the machine: unlink, mark its clOrdId done, release to the pool. */
    private void retire(final Order o, final int doneCode)
    {
        if (o.linked)
        {
            unlinkLists(o);
        }
        dedupe.setValue(o.dedupeEntry, doneCode);
        orderById.remove(o.orderId);
        orderPool.release(o);
    }

    private void emitOrder(
        final Order o, final ExecType execType, final OrdStatus status, final long clOrdId, final long origClOrdId,
        final long gwTs, final CancelReason cancelReason, final long leaves)
    {
        ew.executionReport(
            o.sessionId, o.orderId, clOrdId, origClOrdId, 0, o.price, o.qty, 0, NULL_PRICE, leaves, o.cumQty, gwTs,
            o.firm.firmId, o.accountId, o.symbol.symbolId, o.gatewayId, execType, status, o.side, o.ordType, o.tif,
            LiquidityFlag.NONE, RejectReason.NONE, cancelReason);
    }

    private void emitFill(final Order o, final long matchId, final long q, final long px, final long gwTs, final LiquidityFlag liq)
    {
        final boolean full = o.leaves() == 0;
        ew.executionReport(
            o.sessionId, o.orderId, o.clOrdId, 0, matchId, o.price, o.qty, q, px, o.leaves(), o.cumQty, gwTs,
            o.firm.firmId, o.accountId, o.symbol.symbolId, o.gatewayId, full ? ExecType.FILL : ExecType.PARTIAL_FILL,
            full ? OrdStatus.FILLED : OrdStatus.PARTIALLY_FILLED, o.side, o.ordType, o.tif, liq, RejectReason.NONE,
            CancelReason.NONE);
    }

    private void markDirty(final PriceLevel l)
    {
        if (!l.dirty)
        {
            l.dirty = true;
            l.baseQty = l.totalQty;
            l.baseCount = l.orders.size;
            if (dirtyCount == dirty.length)
            {
                dirty = Arrays.copyOf(dirty, dirtyCount * 2);
                dirtyKeys = new long[dirtyCount * 2];
                touched = new int[dirtyCount * 2];
            }
            dirty[dirtyCount++] = l;
        }
    }

    private static long dirtyKey(final PriceLevel l)
    {
        final BookSide side = l.side;
        final long p = side.buy ? PRICE_MASK - l.price : l.price;
        return ((long)side.symbol.idx << (PRICE_BITS + 1)) | (side.buy ? 0 : 1L << PRICE_BITS) | p;
    }

    /**
     * Emits one BookDelta per level whose quantity or order count changed during the step, ordered by
     * symbol, then BUY before SELL, then best price first; frees emptied levels; then (if asked) an
     * AuctionUpdate for each touched symbol in AUCTION_CALL whose indicative changed.
     */
    private void flushBook(final boolean indicatives)
    {
        final int n = dirtyCount;
        if (n == 0)
        {
            return;
        }
        final long[] keys = dirtyKeys;
        for (int i = 0; i < n; i++)
        {
            keys[i] = dirtyKey(dirty[i]);
            dirty[i] = null;
        }
        dirtyCount = 0;
        if (n <= 32)
        {
            for (int i = 1; i < n; i++)
            {
                final long k = keys[i];
                int j = i - 1;
                while (j >= 0 && keys[j] > k)
                {
                    keys[j + 1] = keys[j];
                    j--;
                }
                keys[j + 1] = k;
            }
        }
        else
        {
            Arrays.sort(keys, 0, n);
        }
        int touchedCount = 0;
        int lastSym = -1;
        for (int i = 0; i < n; i++)
        {
            final long k = keys[i];
            final int symIdx = (int)(k >>> (PRICE_BITS + 1));
            final boolean sell = (k & (1L << PRICE_BITS)) != 0;
            final long raw = k & PRICE_MASK;
            final long price = sell ? raw : PRICE_MASK - raw;
            final Symbol s = symbols.get(symIdx);
            final BookSide side = sell ? s.asks : s.bids;
            final PriceLevel l = side.find(price);
            if (l.totalQty != l.baseQty || l.orders.size != l.baseCount)
            {
                ew.bookDelta(price, l.totalQty, s.symbolId, l.orders.size, side.sideCode);
            }
            l.dirty = false;
            if (l.orders.size == 0)
            {
                side.indexRemove(l);
                levelPool.release(l);
            }
            if (symIdx != lastSym)
            {
                touched[touchedCount++] = symIdx;
                lastSym = symIdx;
            }
        }
        if (indicatives)
        {
            for (int i = 0; i < touchedCount; i++)
            {
                final Symbol s = symbols.get(touched[i]);
                if (s.state == AUCTION_CALL)
                {
                    maybeEmitIndicative(s);
                }
            }
        }
    }

    // =====================================================================================
    // Snapshot
    // =====================================================================================

    @Override
    public void takeSnapshot(final SnapshotSink sink)
    {
        SnapshotCodec.write(this, sink);
    }

    @Override
    public void onSnapshotChunk(final DirectBuffer buffer, final int offset, final int length)
    {
        if (loader == null)
        {
            loader = new SnapshotCodec.Loader();
        }
        loader.append(buffer, offset, length);
    }

    @Override
    public void onSnapshotLoaded()
    {
        if (loader == null)
        {
            throw new IllegalStateException("no snapshot chunks received");
        }
        SnapshotCodec.load(this, loader);
        loader = null;
    }

    /**
     * Re-registers every pending scheduled transition with the TimerScheduler. Aeron Cluster restores its
     * own timers with the snapshot, so the cluster service does not need this; a host whose scheduler
     * does not persist timers calls it after {@link #onSnapshotLoaded()}. Idempotent.
     */
    public void rescheduleTimers()
    {
        for (int i = 0; i < pending.size(); i++)
        {
            final PendingTransition p = pending.get(i);
            timers.schedule(p.timerId, p.effectiveTimeNs);
        }
        for (int i = 0; i < symbols.size(); i++)
        {
            final Symbol s = symbols.get(i);
            if (s.inVolatilityAuction())
            {
                timers.schedule(s.volTimerId, s.volEndNs);
            }
        }
    }

    // ---- used by SnapshotCodec.load ----

    void restoreVolatilityTimer(final Symbol s)
    {
        if (s.inVolatilityAuction())
        {
            volTimers.put(s.volTimerId, s.idx);
        }
    }

    Symbol restoreSymbol(final int symbolId)
    {
        final Symbol s = new Symbol(symbols.size(), symbolId, config.levelWindowTicks());
        symbols.add(s);
        symbolIdxById.put(symbolId, s.idx);
        return s;
    }

    Firm restoreFirm(final int firmId)
    {
        final Firm f = new Firm(firmId, firms.size());
        firms.add(f);
        firmIdxById.put(firmId, f.idx);
        return f;
    }

    void restoreRestingOrder(final Order o)
    {
        linkLists(o);
        orderById.put(o.orderId, o.poolIndex);
    }

    void restoreAppendToLevel(final BookSide side, final PriceLevel levelOrNull, final Order o)
    {
        o.level = levelOrNull;
        levelOrNull.orders.append(o);
        levelOrNull.totalQty += o.leaves();
    }

    PriceLevel restoreLevel(final BookSide side, final long price)
    {
        final PriceLevel l = levelPool.acquire(side, price);
        side.indexPut(l);
        // levels are written best first; attach() keeps the array sorted regardless of order
        side.attach(l);
        return l;
    }

    Order orderByOrderId(final long orderId)
    {
        final long idx = orderById.get(orderId);
        return idx < 0 ? null : orderPool.byIndex((int)idx);
    }
}
