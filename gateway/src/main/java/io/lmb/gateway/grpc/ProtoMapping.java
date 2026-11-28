package io.lmb.gateway.grpc;

import io.lmb.gateway.core.ReportView;
import io.lmb.gateway.core.Reject;
import io.lmb.proto.orderentry.ExecType;
import io.lmb.proto.orderentry.ExecutionReport;
import io.lmb.proto.orderentry.OrdStatus;
import io.lmb.proto.orderentry.SessionReject;
import io.lmb.proto.orderentry.Side;
import io.lmb.sbe.CancelReason;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.LiquidityFlag;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.RejectReason;
import io.lmb.sbe.TimeInForce;

/** Conversions between the order entry protobuf messages and the venue's SBE / core types. */
public final class ProtoMapping
{
    private ProtoMapping()
    {
    }

    public static ExecutionReport toProto(final ReportView v, final boolean possDup)
    {
        final ExecutionReportDecoder er = v.decoder();
        final ExecutionReport.Builder b = ExecutionReport.newBuilder()
            .setShardId(v.shardId())
            .setEventSeq(v.eventSeq())
            .setExecId(v.execId())
            .setOrderId(er.orderId())
            .setClOrdId(er.clOrdId())
            .setOrigClOrdId(er.origClOrdId())
            .setSymbol(v.ticker())
            .setPriceCents(nullToZero(er.price()))
            .setOrderQty(er.orderQty())
            .setLastQty(er.lastQty())
            .setLastPxCents(nullToZero(er.lastPx()))
            .setLeavesQty(er.leavesQty())
            .setCumQty(er.cumQty())
            .setMatchId(er.matchId())
            .setTransactTimeNs(v.timestampNs())
            .setAccountId((int)er.accountId())
            .setPossDup(possDup);
        final Side side = Side.forNumber(er.sideRaw());
        if (side != null)
        {
            b.setSide(side);
        }
        final ExecType execType = ExecType.forNumber(er.execTypeRaw());
        if (execType != null)
        {
            b.setExecType(execType);
        }
        final OrdStatus status = OrdStatus.forNumber(er.ordStatusRaw());
        if (status != null)
        {
            b.setOrdStatus(status);
        }
        final RejectReason rr = er.rejectReason();
        if (rr != RejectReason.NONE && rr != RejectReason.NULL_VAL)
        {
            b.setRejectReason(rr.name());
        }
        final CancelReason cr = er.cancelReason();
        if (cr != CancelReason.NONE && cr != CancelReason.NULL_VAL)
        {
            b.setCancelReason(cr.name());
        }
        final LiquidityFlag lf = er.liquidity();
        if (lf != LiquidityFlag.NONE && lf != LiquidityFlag.NULL_VAL)
        {
            b.setLiquidity(lf.name());
        }
        return b.build();
    }

    private static long nullToZero(final long price)
    {
        return price == ExecutionReportDecoder.priceNullValue() ? 0 : price;
    }

    public static SessionReject.Reason toProto(final Reject r)
    {
        return switch (r)
        {
            case NOT_LOGGED_ON -> SessionReject.Reason.NOT_LOGGED_ON;
            case AUTH_FAILED -> SessionReject.Reason.AUTH_FAILED;
            case THROTTLED -> SessionReject.Reason.THROTTLED;
            case UNAVAILABLE -> SessionReject.Reason.UNAVAILABLE;
            case BAD_SEQUENCE -> SessionReject.Reason.BAD_SEQUENCE;
            case UNKNOWN_SYMBOL -> SessionReject.Reason.UNKNOWN_SYMBOL;
            case INVALID_FIELD -> SessionReject.Reason.INVALID_FIELD;
            case SYMBOL_NOT_TRADING -> SessionReject.Reason.SYMBOL_NOT_TRADING;
            case MAX_ORDER_SIZE -> SessionReject.Reason.MAX_ORDER_SIZE;
            case DUPLICATE_SESSION -> SessionReject.Reason.DUPLICATE_SESSION;
            case NOT_PERMITTED -> SessionReject.Reason.NOT_PERMITTED;
        };
    }

    public static io.lmb.sbe.Side side(final Side side)
    {
        return switch (side)
        {
            case BUY -> io.lmb.sbe.Side.BUY;
            case SELL -> io.lmb.sbe.Side.SELL;
            default -> null;
        };
    }

    public static OrdType ordType(final io.lmb.proto.orderentry.OrdType t)
    {
        return switch (t)
        {
            case LIMIT -> OrdType.LIMIT;
            case MARKET -> OrdType.MARKET;
            default -> null;
        };
    }

    /** Unspecified time in force means DAY (as FIX tag 59 absent). */
    public static TimeInForce tif(final io.lmb.proto.orderentry.TimeInForce t)
    {
        return t == io.lmb.proto.orderentry.TimeInForce.IOC ? TimeInForce.IOC : TimeInForce.DAY;
    }
}
