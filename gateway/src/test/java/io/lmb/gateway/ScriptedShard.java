package io.lmb.gateway;

import io.lmb.api.EventSink;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.SnapshotSink;
import io.lmb.api.TimerScheduler;
import io.lmb.sbe.BooleanType;
import io.lmb.sbe.CancelOrderDecoder;
import io.lmb.sbe.CancelReason;
import io.lmb.sbe.CommandAppliedEncoder;
import io.lmb.sbe.EventHeaderEncoder;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportEncoder;
import io.lmb.sbe.LiquidityFlag;
import io.lmb.sbe.MassCancelDecoder;
import io.lmb.sbe.MassCancelDoneEncoder;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.NewOrderDecoder;
import io.lmb.sbe.OrdStatus;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.RejectReason;
import io.lmb.sbe.ReplaceOrderDecoder;
import io.lmb.sbe.Side;
import io.lmb.sbe.TimeInForce;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A deliberately small stand-in for the real matcher: acks, rejects (price of 1 cent is rejected
 * with INVALID_PRICE; a limit at 2 cents is acked then cancelled with SELF_TRADE), matches price-time at the
 * resting price, cancels, replaces, mass cancels, and
 * honours the (firmId, clOrdId) dedupe rule including possResend. Records every command it applied.
 */
final class ScriptedShard implements ShardStateMachine
{
    record Command(int templateId, long firmId, long clOrdId, boolean possResend, MassCancelScope scope, CancelReason reason,
        int targetGatewayId)
    {
    }

    static final class Order
    {
        long orderId;
        long firmId;
        long sessionId;
        long clOrdId;
        long symbolId;
        long accountId;
        int gatewayId;
        Side side;
        OrdType ordType;
        TimeInForce tif;
        long price;
        long qty;
        long cum;
        boolean open = true;
    }

    private final int shardId;
    private EventSink sink;
    private final UnsafeBuffer out = new UnsafeBuffer(new byte[512]);
    private final MessageHeaderDecoder hdr = new MessageHeaderDecoder();
    private final MessageHeaderEncoder hdrEnc = new MessageHeaderEncoder();
    private final NewOrderDecoder newOrder = new NewOrderDecoder();
    private final CancelOrderDecoder cancel = new CancelOrderDecoder();
    private final ReplaceOrderDecoder replace = new ReplaceOrderDecoder();
    private final MassCancelDecoder mass = new MassCancelDecoder();
    private final ExecutionReportEncoder er = new ExecutionReportEncoder();
    private final MassCancelDoneEncoder mcd = new MassCancelDoneEncoder();
    private final CommandAppliedEncoder applied = new CommandAppliedEncoder();

    private final Set<String> dedupe = new HashSet<>();
    private final Map<String, Order> byKey = new HashMap<>();
    private final List<Order> book = new ArrayList<>();
    private final List<Command> commands = new ArrayList<>();
    private long eventSeq;
    private int eventIdx;
    private long logPosition;
    private long timestampNs;
    private long nextOrder;
    private long nextMatch;

    ScriptedShard(final int shardId)
    {
        this.shardId = shardId;
    }

    synchronized List<Command> commands()
    {
        return new ArrayList<>(commands);
    }

    synchronized long countCommands(final int templateId)
    {
        return commands.stream().filter(c -> c.templateId() == templateId).count();
    }

    synchronized long openOrders(final long sessionId)
    {
        return book.stream().filter(o -> o.open && o.sessionId == sessionId).count();
    }

    @Override
    public int shardId()
    {
        return shardId;
    }

    @Override
    public void bind(final EventSink events, final TimerScheduler timers)
    {
        this.sink = events;
    }

    @Override
    public synchronized void onCommand(
        final DirectBuffer buffer, final int offset, final int length, final long logPosition, final long timestampNs)
    {
        this.logPosition = logPosition;
        this.timestampNs = timestampNs;
        this.eventIdx = 0;
        hdr.wrap(buffer, offset);
        final int templateId = hdr.templateId();
        switch (templateId)
        {
            case NewOrderDecoder.TEMPLATE_ID -> onNewOrder(buffer, offset);
            case CancelOrderDecoder.TEMPLATE_ID -> onCancel(buffer, offset);
            case ReplaceOrderDecoder.TEMPLATE_ID -> onReplace(buffer, offset);
            case MassCancelDecoder.TEMPLATE_ID -> onMassCancel(buffer, offset);
            default ->
            {
            }
        }
        emitApplied(templateId);
    }

    private boolean dedupe(final long firmId, final long clOrdId, final boolean possResend, final Order rejectTemplate)
    {
        if (dedupe.add(firmId + ":" + clOrdId))
        {
            return false;
        }
        if (!possResend)
        {
            rejectTemplate.clOrdId = clOrdId;
            emit(rejectTemplate, ExecType.REJECTED, OrdStatus.REJECTED, 0, 0, 0, LiquidityFlag.NONE, RejectReason.DUPLICATE_CLORDID,
                CancelReason.NONE, 0, 0, 0);
        }
        return true;
    }

    private void onNewOrder(final DirectBuffer buffer, final int offset)
    {
        newOrder.wrapAndApplyHeader(buffer, offset, hdr);
        final boolean possResend = newOrder.possResend() == BooleanType.TRUE;
        commands.add(new Command(NewOrderDecoder.TEMPLATE_ID, newOrder.firmId(), newOrder.clOrdId(), possResend, null, null, 0));
        final Order o = new Order();
        o.firmId = newOrder.firmId();
        o.sessionId = newOrder.sessionId();
        o.clOrdId = newOrder.clOrdId();
        o.symbolId = newOrder.symbolId();
        o.accountId = newOrder.accountId();
        o.gatewayId = newOrder.gatewayId();
        o.side = newOrder.side();
        o.ordType = newOrder.ordType();
        o.tif = newOrder.timeInForce();
        o.price = newOrder.price();
        o.qty = newOrder.quantity();
        final long inTs = newOrder.gatewayInTsNs();
        if (dedupe(o.firmId, o.clOrdId, possResend, o))
        {
            return;
        }
        if (o.ordType == OrdType.LIMIT && o.price == 1)
        {
            emit(o, ExecType.REJECTED, OrdStatus.REJECTED, 0, 0, 0, LiquidityFlag.NONE, RejectReason.INVALID_PRICE, CancelReason.NONE,
                inTs, 0, 0);
            return;
        }
        o.orderId = ((long)shardId << 48) | ++nextOrder;
        emit(o, ExecType.NEW, OrdStatus.NEW, 0, 0, 0, LiquidityFlag.NONE, RejectReason.NONE, CancelReason.NONE, inTs, 0, 0);
        if (o.ordType == OrdType.LIMIT && o.price == 2)
        {
            // Magic price: what the real matcher's self-trade prevention emits for a cancelled taker.
            o.open = false;
            emit(o, ExecType.CANCELED, OrdStatus.CANCELED, 0, 0, 0, LiquidityFlag.NONE, RejectReason.NONE,
                CancelReason.SELF_TRADE, 0, 0, 0);
            return;
        }
        for (final Order r : book)
        {
            if (o.qty - o.cum == 0)
            {
                break;
            }
            if (!r.open || r.symbolId != o.symbolId || r.side == o.side)
            {
                continue;
            }
            final boolean crosses = o.ordType == OrdType.MARKET ||
                (o.side == Side.BUY ? r.price <= o.price : r.price >= o.price);
            if (!crosses)
            {
                continue;
            }
            final long q = Math.min(o.qty - o.cum, r.qty - r.cum);
            final long matchId = ((long)shardId << 48) | ++nextMatch;
            o.cum += q;
            r.cum += q;
            emit(o, o.cum == o.qty ? ExecType.FILL : ExecType.PARTIAL_FILL,
                o.cum == o.qty ? OrdStatus.FILLED : OrdStatus.PARTIALLY_FILLED, q, r.price, matchId, LiquidityFlag.TAKER,
                RejectReason.NONE, CancelReason.NONE, inTs, 0, 0);
            if (r.cum == r.qty)
            {
                r.open = false;
            }
            emit(r, r.cum == r.qty ? ExecType.FILL : ExecType.PARTIAL_FILL,
                r.cum == r.qty ? OrdStatus.FILLED : OrdStatus.PARTIALLY_FILLED, q, r.price, matchId, LiquidityFlag.MAKER,
                RejectReason.NONE, CancelReason.NONE, 0, 0, 0);
        }
        if (o.cum < o.qty)
        {
            if (o.ordType == OrdType.MARKET || o.tif == TimeInForce.IOC)
            {
                o.open = false;
                emit(o, ExecType.CANCELED, OrdStatus.CANCELED, 0, 0, 0, LiquidityFlag.NONE, RejectReason.NONE,
                    CancelReason.IOC_REMAINDER, 0, 0, 0);
            }
            else
            {
                book.add(o);
                byKey.put(o.firmId + ":" + o.clOrdId, o);
            }
        }
    }

    private void onCancel(final DirectBuffer buffer, final int offset)
    {
        cancel.wrapAndApplyHeader(buffer, offset, hdr);
        final boolean possResend = cancel.possResend() == BooleanType.TRUE;
        commands.add(new Command(CancelOrderDecoder.TEMPLATE_ID, cancel.firmId(), cancel.clOrdId(), possResend, null, null, 0));
        final Order template = new Order();
        template.firmId = cancel.firmId();
        template.sessionId = cancel.sessionId();
        template.symbolId = cancel.symbolId();
        template.side = Side.BUY;
        final long cancelId = cancel.clOrdId();
        final long origId = cancel.origClOrdId();
        final long inTs = cancel.gatewayInTsNs();
        if (dedupe(template.firmId, cancelId, possResend, template))
        {
            return;
        }
        final Order o = byKey.get(template.firmId + ":" + origId);
        if (o == null || !o.open)
        {
            template.clOrdId = cancelId;
            emit(template, ExecType.CANCEL_REJECTED, OrdStatus.REJECTED, 0, 0, 0, LiquidityFlag.NONE,
                o == null ? RejectReason.UNKNOWN_ORDER : RejectReason.TOO_LATE_TO_CANCEL, CancelReason.NONE, inTs, origId, 0);
            return;
        }
        o.open = false;
        final long keep = o.clOrdId;
        o.clOrdId = cancelId;
        emit(o, ExecType.CANCELED, OrdStatus.CANCELED, 0, 0, 0, LiquidityFlag.NONE, RejectReason.NONE, CancelReason.CLIENT, inTs, keep, 0);
        o.clOrdId = keep;
    }

    private void onReplace(final DirectBuffer buffer, final int offset)
    {
        replace.wrapAndApplyHeader(buffer, offset, hdr);
        final boolean possResend = replace.possResend() == BooleanType.TRUE;
        commands.add(new Command(ReplaceOrderDecoder.TEMPLATE_ID, replace.firmId(), replace.clOrdId(), possResend, null, null, 0));
        final Order template = new Order();
        template.firmId = replace.firmId();
        template.sessionId = replace.sessionId();
        template.symbolId = replace.symbolId();
        template.side = Side.BUY;
        final long newId = replace.clOrdId();
        final long origId = replace.origClOrdId();
        final long newPrice = replace.newPrice();
        final long newQty = replace.newQuantity();
        final long inTs = replace.gatewayInTsNs();
        if (dedupe(template.firmId, newId, possResend, template))
        {
            return;
        }
        final Order o = byKey.get(template.firmId + ":" + origId);
        if (o == null || !o.open || newQty <= o.cum)
        {
            template.clOrdId = newId;
            emit(template, ExecType.CANCEL_REJECTED, OrdStatus.REJECTED, 0, 0, 0, LiquidityFlag.NONE,
                o == null ? RejectReason.UNKNOWN_ORDER : RejectReason.INVALID_REPLACE, CancelReason.NONE, inTs, origId, 0);
            return;
        }
        byKey.remove(template.firmId + ":" + origId);
        o.clOrdId = newId;
        o.qty = newQty;
        if (newPrice != ReplaceOrderDecoder.newPriceNullValue())
        {
            o.price = newPrice;
        }
        byKey.put(o.firmId + ":" + newId, o);
        emit(o, ExecType.REPLACED, OrdStatus.NEW, 0, 0, 0, LiquidityFlag.NONE, RejectReason.NONE, CancelReason.NONE, inTs, origId, 0);
    }

    private void onMassCancel(final DirectBuffer buffer, final int offset)
    {
        mass.wrapAndApplyHeader(buffer, offset, hdr);
        final MassCancelScope scope = mass.scope();
        final CancelReason reason = mass.reason();
        commands.add(new Command(MassCancelDecoder.TEMPLATE_ID, mass.firmId(), mass.requestId(), false, scope, reason,
            mass.targetGatewayId()));
        final long requestId = mass.requestId();
        final long sessionId = mass.sessionId();
        final long firmId = mass.firmId();
        final long symbolId = mass.symbolId();
        final int gatewayId = mass.gatewayId();
        final int targetGateway = mass.targetGatewayId();
        int canceled = 0;
        for (final Order o : book)
        {
            if (!o.open)
            {
                continue;
            }
            final boolean match = switch (scope)
            {
                case FIRM -> o.firmId == firmId;
                case SESSION -> o.sessionId == sessionId;
                case SESSION_GATEWAY -> o.sessionId == sessionId && o.gatewayId == targetGateway;
                case FIRM_SYMBOL -> o.firmId == firmId && o.symbolId == symbolId;
                case SYMBOL -> o.symbolId == symbolId;
                default -> false;
            };
            if (match)
            {
                o.open = false;
                canceled++;
                emit(o, ExecType.CANCELED, OrdStatus.CANCELED, 0, 0, 0, LiquidityFlag.NONE, RejectReason.NONE, reason, 0, 0, 0);
            }
        }
        mcd.wrapAndApplyHeader(out, 0, hdrEnc);
        header(mcd.eventHeader());
        mcd.requestId(requestId).sessionId(sessionId).firmId(firmId).canceledCount(canceled).gatewayId(gatewayId).scope(scope);
        sink.onEvent(out, 0, MessageHeaderEncoder.ENCODED_LENGTH + mcd.encodedLength());
    }

    private void header(final EventHeaderEncoder h)
    {
        h.eventSeq(++eventSeq).logPosition(logPosition).timestampNs(timestampNs).eventIdx(eventIdx++).shardId(shardId).reserved(0);
    }

    private void emit(
        final Order o, final ExecType execType, final OrdStatus status, final long lastQty, final long lastPx, final long matchId,
        final LiquidityFlag liquidity, final RejectReason rejectReason, final CancelReason cancelReason, final long inTs,
        final long origClOrdId, final int unused)
    {
        er.wrapAndApplyHeader(out, 0, hdrEnc);
        header(er.eventHeader());
        er.sessionId(o.sessionId).orderId(o.orderId).clOrdId(o.clOrdId).origClOrdId(origClOrdId).matchId(matchId)
            .price(o.price).orderQty(o.qty).lastQty(lastQty).lastPx(lastQty == 0 ? ExecutionReportEncoder.lastPxNullValue() : lastPx)
            .leavesQty(o.open && execType != ExecType.REJECTED ? o.qty - o.cum : 0).cumQty(o.cum).gatewayInTsNs(inTs)
            .firmId(o.firmId).accountId(o.accountId).symbolId(o.symbolId).gatewayId(o.gatewayId)
            .execType(execType).ordStatus(status).side(o.side == null ? Side.BUY : o.side)
            .ordType(o.ordType == null ? OrdType.LIMIT : o.ordType).timeInForce(o.tif == null ? TimeInForce.DAY : o.tif)
            .liquidity(liquidity).rejectReason(rejectReason).cancelReason(cancelReason);
        sink.onEvent(out, 0, MessageHeaderEncoder.ENCODED_LENGTH + er.encodedLength());
    }

    private void emitApplied(final int templateId)
    {
        final int count = eventIdx;
        applied.wrapAndApplyHeader(out, 0, hdrEnc);
        header(applied.eventHeader());
        applied.eventCount(count).commandTemplateId(templateId);
        sink.onEvent(out, 0, MessageHeaderEncoder.ENCODED_LENGTH + applied.encodedLength());
    }

    @Override
    public void onTimer(final long correlationId, final long logPosition, final long timestampNs)
    {
        this.logPosition = logPosition;
        this.timestampNs = timestampNs;
        this.eventIdx = 0;
        emitApplied(0);
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
