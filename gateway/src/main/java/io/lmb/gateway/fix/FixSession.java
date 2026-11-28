package io.lmb.gateway.fix;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.lmb.fix.BusinessRejectReason;
import io.lmb.fix.CxlRejReason;
import io.lmb.fix.CxlRejResponseTo;
import io.lmb.fix.LastLiquidityInd;
import io.lmb.fix.MassCancelRequestType;
import io.lmb.fix.MassCancelResponse;
import io.lmb.fix.OrdRejReason;
import io.lmb.fix.builder.BusinessMessageRejectEncoder;
import io.lmb.fix.builder.ExecutionReportEncoder;
import io.lmb.fix.builder.OrderCancelRejectEncoder;
import io.lmb.fix.builder.OrderMassCancelReportEncoder;
import io.lmb.fix.decoder.HeaderDecoder;
import io.lmb.fix.decoder.LogoutDecoder;
import io.lmb.fix.decoder.NewOrderSingleDecoder;
import io.lmb.fix.decoder.OrderCancelReplaceRequestDecoder;
import io.lmb.fix.decoder.OrderCancelRequestDecoder;
import io.lmb.fix.decoder.OrderMassCancelRequestDecoder;
import io.lmb.gateway.core.GatewayCore;
import io.lmb.gateway.core.OrderRequest;
import io.lmb.gateway.core.Reject;
import io.lmb.gateway.core.ReportView;
import io.lmb.gateway.core.SessionOutlet;
import io.lmb.gateway.core.SessionState;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.OrdStatus;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.Side;
import io.lmb.sbe.TimeInForce;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.EpochFractionFormat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.concurrent.TimeUnit;

/**
 * One FIX session: decodes application messages into core requests and encodes reports. Every
 * callback runs on the core thread (the FixLibrary is polled there).
 */
final class FixSession implements SessionHandler, SessionOutlet
{
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(FixSession.class);
    private final GatewayCore core;
    private final Session session;
    private final MutableAsciiBuffer ascii = new MutableAsciiBuffer();
    private final NewOrderSingleDecoder nos = new NewOrderSingleDecoder();
    private final OrderCancelRequestDecoder cxl = new OrderCancelRequestDecoder();
    private final OrderCancelReplaceRequestDecoder rpl = new OrderCancelReplaceRequestDecoder();
    private final OrderMassCancelRequestDecoder mass = new OrderMassCancelRequestDecoder();
    private final ExecutionReportEncoder er = new ExecutionReportEncoder();
    private final OrderCancelRejectEncoder cxlRej = new OrderCancelRejectEncoder();
    private final OrderMassCancelReportEncoder massReport = new OrderMassCancelReportEncoder();
    private final BusinessMessageRejectEncoder bmr = new BusinessMessageRejectEncoder();
    private final UtcTimestampEncoder ts = new UtcTimestampEncoder(EpochFractionFormat.NANOSECONDS);
    private final OrderRequest request = new OrderRequest();
    /** Pending cancel ('F') or replace ('G') request ids, to fill CxlRejResponseTo. */
    private final Long2ObjectHashMap<Character> cancelKinds = new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<Character> massTypes = new Long2ObjectHashMap<>();
    private SessionState state;
    /** The client sent a Logout (35=5): its disconnect is a logout for CancelOnDisconnectType. */
    private boolean logoutReceived;
    private String currentMsgType = "D";

    FixSession(final GatewayCore core, final Session session)
    {
        this.core = core;
        this.session = session;
    }

    // ------------------------------------------------------------------ inbound

    @Override
    public Action onMessage(
        final DirectBuffer buffer, final int offset, final int length, final int libraryId, final Session session,
        final int sequenceIndex, final long messageType, final long timestampInNs, final long position,
        final OnMessageInfo messageInfo)
    {
        if (messageType == LogoutDecoder.MESSAGE_TYPE)
        {
            // a client that sends Logout and closes at once looks like REMOTE_DISCONNECT to Artio: it still logged out
            logoutReceived = true;
            return Action.CONTINUE;
        }
        if (state == null || !state.isConnected())
        {
            return Action.CONTINUE;
        }
        ascii.wrap(buffer, offset, length);
        final long now = core.epochNs();
        final OrderRequest req = request.reset();
        req.gatewayInTsNs = now;
        final HeaderDecoder header;
        if (messageType == NewOrderSingleDecoder.MESSAGE_TYPE)
        {
            currentMsgType = "D";
            nos.reset();
            nos.decode(ascii, 0, length);
            header = nos.header();
            req.kind = OrderRequest.Kind.NEW;
            req.clientSeq = header.msgSeqNum();
            if (!parseIds(req, nos.clOrdIDAsString(), null) || !nos.validate())
            {
                return Action.CONTINUE;
            }
            req.symbol = nos.symbolAsString();
            req.side = side(nos.side());
            req.ordType = nos.ordType() == '1' ? OrdType.MARKET : nos.ordType() == '2' ? OrdType.LIMIT : null;
            req.timeInForce = !nos.hasTimeInForce() || nos.timeInForce() == '0' ? TimeInForce.DAY :
                nos.timeInForce() == '3' ? TimeInForce.IOC : null;
            if (req.timeInForce == null)
            {
                return refuse(req, Reject.INVALID_FIELD, "TimeInForce must be 0 (DAY) or 3 (IOC)");
            }
            req.quantity = wholeNumber(nos.orderQty());
            req.priceCents = nos.hasPrice() ? cents(nos.price()) : 0;
            req.accountId = nos.hasAccount() ? FixFrontEnd.parseLong(nos.accountAsString()) : 0;
            if (req.accountId < 0 || req.quantity < 0 || req.priceCents < 0)
            {
                return refuse(req, Reject.INVALID_FIELD, "Account, OrderQty and Price must be non-negative numbers (price in cents precision)");
            }
        }
        else if (messageType == OrderCancelRequestDecoder.MESSAGE_TYPE)
        {
            currentMsgType = "F";
            cxl.reset();
            cxl.decode(ascii, 0, length);
            header = cxl.header();
            req.kind = OrderRequest.Kind.CANCEL;
            req.clientSeq = header.msgSeqNum();
            if (!parseIds(req, cxl.clOrdIDAsString(), cxl.origClOrdIDAsString()) || !cxl.validate())
            {
                return Action.CONTINUE;
            }
            req.symbol = cxl.symbolAsString();
            cancelKinds.put(req.clOrdId, Character.valueOf('F'));
        }
        else if (messageType == OrderCancelReplaceRequestDecoder.MESSAGE_TYPE)
        {
            currentMsgType = "G";
            rpl.reset();
            rpl.decode(ascii, 0, length);
            header = rpl.header();
            req.kind = OrderRequest.Kind.REPLACE;
            req.clientSeq = header.msgSeqNum();
            if (!parseIds(req, rpl.clOrdIDAsString(), rpl.origClOrdIDAsString()) || !rpl.validate())
            {
                return Action.CONTINUE;
            }
            req.symbol = rpl.symbolAsString();
            req.quantity = wholeNumber(rpl.orderQty());
            req.priceCents = rpl.hasPrice() ? cents(rpl.price()) : 0;
            if (req.quantity < 0 || req.priceCents < 0)
            {
                return refuse(req, Reject.INVALID_FIELD, "OrderQty and Price must be non-negative numbers");
            }
            cancelKinds.put(req.clOrdId, Character.valueOf('G'));
        }
        else if (messageType == OrderMassCancelRequestDecoder.MESSAGE_TYPE)
        {
            currentMsgType = "q";
            mass.reset();
            mass.decode(ascii, 0, length);
            header = mass.header();
            req.kind = OrderRequest.Kind.MASS_CANCEL;
            req.clientSeq = header.msgSeqNum();
            final long id = numericId(mass.clOrdIDAsString());
            if (id <= 0)
            {
                return refuse(req, Reject.INVALID_FIELD, "ClOrdID must be a positive integer");
            }
            req.requestId = id;
            final char type = mass.massCancelRequestType();
            if (type == '1')
            {
                if (mass.symbolLength() == 0)
                {
                    return refuse(req, Reject.INVALID_FIELD, "MassCancelRequestType 1 needs Symbol");
                }
                req.symbol = mass.symbolAsString();
            }
            else if (type == '7')
            {
                req.symbol = "";
            }
            else
            {
                return refuse(req, Reject.INVALID_FIELD, "MassCancelRequestType must be 1 or 7");
            }
            massTypes.put(id, Character.valueOf(type));
        }
        else
        {
            return Action.CONTINUE; // session-level messages are Artio's
        }
        final boolean possResend = header.hasPossResend() && header.possResend();
        core.handleRequest(state, req, possResend);
        return Action.CONTINUE;
    }

    private Action refuse(final OrderRequest req, final Reject reason, final String text)
    {
        core.countReject(reason);
        sendReject(reason, req.clientSeq, req.kind == OrderRequest.Kind.MASS_CANCEL ? req.requestId : req.clOrdId, text);
        return Action.CONTINUE;
    }

    /** The venue's clOrdId is an int64: FIX ClOrdID/OrigClOrdID must be decimal integers. */
    private boolean parseIds(final OrderRequest req, final String clOrdId, final String origClOrdId)
    {
        req.clOrdId = numericId(clOrdId);
        if (req.clOrdId <= 0)
        {
            refuse(req, Reject.INVALID_FIELD, "ClOrdID must be a positive integer, got '" + clOrdId + "'");
            return false;
        }
        if (origClOrdId != null)
        {
            req.origClOrdId = numericId(origClOrdId);
            if (req.origClOrdId <= 0)
            {
                refuse(req, Reject.INVALID_FIELD, "OrigClOrdID must be a positive integer, got '" + origClOrdId + "'");
                return false;
            }
        }
        return true;
    }

    static long numericId(final String s)
    {
        if (s == null || s.isEmpty() || s.length() > 18)
        {
            return -1;
        }
        long v = 0;
        for (int i = 0; i < s.length(); i++)
        {
            final char c = s.charAt(i);
            if (c < '0' || c > '9')
            {
                return -1;
            }
            v = v * 10 + (c - '0');
        }
        return v;
    }

    private static Side side(final char c)
    {
        return c == '1' ? Side.BUY : c == '2' ? Side.SELL : null;
    }

    /** Price in cents, or -1 if it has more precision than a cent or is negative. */
    static long cents(final DecimalFloat d)
    {
        long v = d.value();
        int scale = d.scale();
        while (scale < 2)
        {
            v *= 10;
            scale++;
        }
        while (scale > 2)
        {
            if (v % 10 != 0)
            {
                return -1;
            }
            v /= 10;
            scale--;
        }
        return v < 0 ? -1 : v;
    }

    /** Whole shares, or -1. */
    static long wholeNumber(final DecimalFloat d)
    {
        long v = d.value();
        int scale = d.scale();
        while (scale > 0)
        {
            if (v % 10 != 0)
            {
                return -1;
            }
            v /= 10;
            scale--;
        }
        while (scale < 0)
        {
            v *= 10;
            scale++;
        }
        return v < 0 ? -1 : v;
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
        if (state != null)
        {
            final boolean logout = logoutReceived || reason == DisconnectReason.LOGOUT;
            LOG.info("FIX session {} disconnected: {}{}", state.sessionId(), reason, logout ? " (logged out)" : "");
            core.onTransportClosed(state, this, logout);
        }
        return Action.CONTINUE;
    }

    @Override
    public void onSessionStart(final Session session)
    {
    }

    // ------------------------------------------------------------------ outlet

    @Override
    public Transport transport()
    {
        return Transport.FIX;
    }

    @Override
    public void onLogonAccepted(final SessionState s, final long nextExpectedClientSeq, final int heartbeatIntervalMs)
    {
        this.state = s;
    }

    @Override
    public boolean isWritable()
    {
        return session.canSendMessage();
    }

    @Override
    public boolean sendExecutionReport(final ReportView v, final boolean possDup)
    {
        final ExecutionReportDecoder d = v.decoder();
        final int tsLen = ts.encodeFrom(v.timestampNs(), TimeUnit.NANOSECONDS);
        if (d.execType() == ExecType.CANCEL_REJECTED)
        {
            final Character kind = cancelKinds.remove(d.clOrdId());
            cxlRej.reset();
            cxlRej.header().possResend(possDup);
            cxlRej.orderID(d.orderId() == 0 ? "NONE" : Long.toString(d.orderId()))
                .clOrdID(Long.toString(d.clOrdId()))
                .origClOrdID(Long.toString(d.origClOrdId()))
                .ordStatus(fixOrdStatus(d.ordStatus()))
                .transactTime(ts.buffer(), tsLen)
                .cxlRejResponseTo(kind != null && kind == 'G' ? CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST :
                    CxlRejResponseTo.ORDER_CANCEL_REQUEST)
                .cxlRejReason(switch (d.rejectReason())
                {
                    case UNKNOWN_ORDER -> CxlRejReason.UNKNOWN_ORDER;
                    case TOO_LATE_TO_CANCEL -> CxlRejReason.TOO_LATE_TO_CANCEL;
                    case DUPLICATE_CLORDID -> CxlRejReason.DUPLICATE_CLORDID;
                    default -> CxlRejReason.OTHER;
                })
                .text(d.rejectReason().name())
                .lmbShardID(v.shardId())
                .lmbEventSeq(v.eventSeq());
            if (d.accountId() != 0)
            {
                cxlRej.account(Long.toString(d.accountId()));
            }
            return session.trySend(cxlRej) >= 0;
        }
        er.reset();
        er.header().possResend(possDup);
        er.orderID(d.orderId() == 0 ? "NONE" : Long.toString(d.orderId()))
            .execID(v.execId())
            .clOrdID(Long.toString(d.clOrdId()))
            .execType(fixExecType(d.execType()))
            .ordStatus(fixOrdStatus(d.ordStatus()))
            .side(d.side() == Side.SELL ? io.lmb.fix.Side.SELL : io.lmb.fix.Side.BUY)
            .ordType(d.ordType() == OrdType.MARKET ? io.lmb.fix.OrdType.MARKET : io.lmb.fix.OrdType.LIMIT)
            .timeInForce(d.timeInForce() == TimeInForce.IOC ? io.lmb.fix.TimeInForce.IMMEDIATE_OR_CANCEL : io.lmb.fix.TimeInForce.DAY)
            .leavesQty(d.leavesQty(), 0)
            .cumQty(d.cumQty(), 0)
            .transactTime(ts.buffer(), tsLen)
            .lmbShardID(v.shardId())
            .lmbEventSeq(v.eventSeq());
        er.instrument().symbol(v.ticker());
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
        if (d.execType() == ExecType.REJECTED)
        {
            er.ordRejReason(switch (d.rejectReason())
            {
                case UNKNOWN_SYMBOL -> OrdRejReason.UNKNOWN_SYMBOL;
                case SYMBOL_CLOSED, SYMBOL_HALTED -> OrdRejReason.EXCHANGE_CLOSED;
                case DUPLICATE_CLORDID -> OrdRejReason.DUPLICATE_ORDER;
                case CREDIT_LIMIT, POSITION_LIMIT -> OrdRejReason.ORDER_EXCEEDS_LIMIT;
                case INVALID_QTY -> OrdRejReason.INCORRECT_QUANTITY;
                default -> OrdRejReason.OTHER;
            }).text(d.rejectReason().name());
        }
        else if (d.cancelReason() != io.lmb.sbe.CancelReason.NONE && d.cancelReason() != io.lmb.sbe.CancelReason.NULL_VAL)
        {
            er.text(d.cancelReason().name());
        }
        if (d.execType() == ExecType.REPLACED || d.execType() == ExecType.CANCELED)
        {
            cancelKinds.remove(d.clOrdId());
        }
        return session.trySend(er) >= 0;
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

    @Override
    public void sendMassCancelAck(
        final long requestId, final int shardId, final int canceledCount, final int shardsPending, final int totalCanceled)
    {
        if (shardsPending > 0)
        {
            return; // FIX gets one aggregated report
        }
        final Character type = massTypes.remove(requestId);
        massReport.reset();
        massReport.clOrdID(Long.toString(requestId))
            .orderID("MC-" + requestId)
            .massCancelRequestType(type != null && type == '1' ? MassCancelRequestType.CANCEL_ORDERS_FOR_A_SECURITY :
                MassCancelRequestType.CANCEL_ALL_ORDERS)
            .massCancelResponse(type != null && type == '1' ? MassCancelResponse.CANCEL_ORDERS_FOR_A_SECURITY :
                MassCancelResponse.CANCEL_ALL_ORDERS)
            .totalAffectedOrders(totalCanceled);
        session.trySend(massReport);
    }

    @Override
    public void sendReject(final Reject reason, final long refSeq, final long refClOrdId, final String text)
    {
        bmr.reset();
        bmr.refMsgType(currentMsgType)
            .businessRejectReason(switch (reason)
            {
                case UNKNOWN_SYMBOL -> BusinessRejectReason.UNKNOWN_SECURITY;
                case UNAVAILABLE, SYMBOL_NOT_TRADING -> BusinessRejectReason.APPLICATION_NOT_AVAILABLE;
                case THROTTLED -> BusinessRejectReason.THROTTLE_LIMIT_EXCEEDED;
                case NOT_LOGGED_ON, AUTH_FAILED, NOT_PERMITTED -> BusinessRejectReason.NOT_AUTHORIZED;
                default -> BusinessRejectReason.OTHER;
            })
            .text(reason.name() + (text == null ? "" : ": " + text));
        if (refSeq > 0)
        {
            bmr.refSeqNum((int)refSeq);
        }
        if (refClOrdId != 0)
        {
            bmr.businessRejectRefID(Long.toString(refClOrdId));
        }
        session.trySend(bmr);
    }

    @Override
    public void sendHeartbeat()
    {
    }

    @Override
    public boolean coreManagedHeartbeats()
    {
        return false;
    }

    @Override
    public long lastServerSeq()
    {
        return session.lastSentMsgSeqNum();
    }

    @Override
    public void disconnect(final Reject reason, final String text)
    {
        session.logoutAndDisconnect();
    }
}
