package io.lmb.matcher;

import io.lmb.common.Events;
import io.lmb.sbe.AuctionUpdateDecoder;
import io.lmb.sbe.BookDeltaDecoder;
import io.lmb.sbe.CommandAppliedDecoder;
import io.lmb.sbe.CommandRejectedDecoder;
import io.lmb.sbe.DayRolledDecoder;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.MassCancelDoneDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.ReferencePriceAppliedDecoder;
import io.lmb.sbe.RiskLimitAppliedDecoder;
import io.lmb.sbe.SymbolDefinedDecoder;
import io.lmb.sbe.SymbolStateChangedDecoder;
import io.lmb.sbe.TradeDecoder;
import org.agrona.concurrent.UnsafeBuffer;

/** One captured event with typed accessors and a canonical text form (header and chain hash excluded). */
final class Ev
{
    static final int HDR = MessageHeaderDecoder.ENCODED_LENGTH;

    final byte[] bytes;
    final UnsafeBuffer buf;
    final int templateId;

    Ev(final byte[] bytes)
    {
        this.bytes = bytes;
        this.buf = new UnsafeBuffer(bytes);
        this.templateId = Events.templateId(buf, 0);
    }

    long seq()
    {
        return Events.eventSeq(buf, 0);
    }

    long idx()
    {
        return Events.eventIdx(buf, 0);
    }

    ExecutionReportDecoder er()
    {
        check(ExecutionReportDecoder.TEMPLATE_ID);
        return new ExecutionReportDecoder().wrap(buf, HDR, ExecutionReportDecoder.BLOCK_LENGTH, 1);
    }

    TradeDecoder trade()
    {
        check(TradeDecoder.TEMPLATE_ID);
        return new TradeDecoder().wrap(buf, HDR, TradeDecoder.BLOCK_LENGTH, 1);
    }

    BookDeltaDecoder delta()
    {
        check(BookDeltaDecoder.TEMPLATE_ID);
        return new BookDeltaDecoder().wrap(buf, HDR, BookDeltaDecoder.BLOCK_LENGTH, 1);
    }

    AuctionUpdateDecoder auction()
    {
        check(AuctionUpdateDecoder.TEMPLATE_ID);
        return new AuctionUpdateDecoder().wrap(buf, HDR, AuctionUpdateDecoder.BLOCK_LENGTH, 1);
    }

    SymbolStateChangedDecoder stateChanged()
    {
        check(SymbolStateChangedDecoder.TEMPLATE_ID);
        return new SymbolStateChangedDecoder().wrap(buf, HDR, SymbolStateChangedDecoder.BLOCK_LENGTH, 1);
    }

    CommandRejectedDecoder rejected()
    {
        check(CommandRejectedDecoder.TEMPLATE_ID);
        return new CommandRejectedDecoder().wrap(buf, HDR, CommandRejectedDecoder.BLOCK_LENGTH, 1);
    }

    CommandAppliedDecoder applied()
    {
        check(CommandAppliedDecoder.TEMPLATE_ID);
        return new CommandAppliedDecoder().wrap(buf, HDR, CommandAppliedDecoder.BLOCK_LENGTH, 1);
    }

    MassCancelDoneDecoder massDone()
    {
        check(MassCancelDoneDecoder.TEMPLATE_ID);
        return new MassCancelDoneDecoder().wrap(buf, HDR, MassCancelDoneDecoder.BLOCK_LENGTH, 1);
    }

    RiskLimitAppliedDecoder riskApplied()
    {
        check(RiskLimitAppliedDecoder.TEMPLATE_ID);
        return new RiskLimitAppliedDecoder().wrap(buf, HDR, RiskLimitAppliedDecoder.BLOCK_LENGTH, 1);
    }

    ReferencePriceAppliedDecoder refApplied()
    {
        check(ReferencePriceAppliedDecoder.TEMPLATE_ID);
        return new ReferencePriceAppliedDecoder().wrap(buf, HDR, ReferencePriceAppliedDecoder.BLOCK_LENGTH, 1);
    }

    SymbolDefinedDecoder symbolDefined()
    {
        check(SymbolDefinedDecoder.TEMPLATE_ID);
        return new SymbolDefinedDecoder().wrap(buf, HDR, SymbolDefinedDecoder.BLOCK_LENGTH, 1);
    }

    DayRolledDecoder dayRolled()
    {
        check(DayRolledDecoder.TEMPLATE_ID);
        return new DayRolledDecoder().wrap(buf, HDR, DayRolledDecoder.BLOCK_LENGTH, 1);
    }

    private void check(final int t)
    {
        if (templateId != t)
        {
            throw new AssertionError("expected template " + t + " but was " + this);
        }
    }

    boolean is(final int t)
    {
        return templateId == t;
    }

    /** Canonical text: everything except the EventHeader and the chain hash. */
    String text()
    {
        switch (templateId)
        {
            case ExecutionReportDecoder.TEMPLATE_ID:
            {
                final ExecutionReportDecoder d = er();
                return erText(d.sessionId(), d.orderId(), d.clOrdId(), d.origClOrdId(), d.matchId(), d.price(),
                    d.orderQty(), d.lastQty(), d.lastPx(), d.leavesQty(), d.cumQty(), d.gatewayInTsNs(), d.firmId(),
                    d.accountId(), d.symbolId(), d.gatewayId(), d.execType().name(), d.ordStatus().name(),
                    d.sideRaw(), d.ordType().name(), d.timeInForce().name(), d.liquidity().name(),
                    d.rejectReason().name(), d.cancelReason().name());
            }
            case TradeDecoder.TEMPLATE_ID:
            {
                final TradeDecoder d = trade();
                return "TRADE m=" + d.matchId() + " px=" + d.price() + " q=" + d.quantity() + " b=" + d.buyOrderId() +
                    " s=" + d.sellOrderId() + " sym=" + d.symbolId() + " bf=" + d.buyFirmId() + " sf=" + d.sellFirmId() +
                    " agg=" + d.aggressorSideRaw() + " auct=" + d.isAuction();
            }
            case BookDeltaDecoder.TEMPLATE_ID:
            {
                final BookDeltaDecoder d = delta();
                return "DELTA sym=" + d.symbolId() + " side=" + d.sideRaw() + " px=" + d.price() + " q=" + d.quantity() +
                    " n=" + d.orderCount();
            }
            case SymbolStateChangedDecoder.TEMPLATE_ID:
            {
                final SymbolStateChangedDecoder d = stateChanged();
                return "STATE corr=" + d.correlationId() + " sym=" + d.symbolId() + " rc=" + d.reasonCode() + " " +
                    d.previousState() + "->" + d.newState();
            }
            case AuctionUpdateDecoder.TEMPLATE_ID:
            {
                final AuctionUpdateDecoder d = auction();
                return "AUCT sym=" + d.symbolId() + " px=" + d.price() + " v=" + d.matchedQuantity() + " imb=" +
                    d.imbalanceQuantity() + " side=" + d.imbalanceSideRaw() + " final=" + d.isFinal();
            }
            case ReferencePriceAppliedDecoder.TEMPLATE_ID:
            {
                final ReferencePriceAppliedDecoder d = refApplied();
                return "REF sym=" + d.symbolId() + " ref=" + d.referencePrice() + " bid=" + d.nbbBid() + " ofr=" +
                    d.nbbOffer() + " band=" + d.bandLow() + ".." + d.bandHigh() + " ts=" + d.sourceTsNs();
            }
            case MassCancelDoneDecoder.TEMPLATE_ID:
            {
                final MassCancelDoneDecoder d = massDone();
                return "MASSDONE req=" + d.requestId() + " sess=" + d.sessionId() + " f=" + d.firmId() + " n=" +
                    d.canceledCount() + " g=" + d.gatewayId() + " scope=" + d.scope();
            }
            case RiskLimitAppliedDecoder.TEMPLATE_ID:
            {
                final RiskLimitAppliedDecoder d = riskApplied();
                return "RISK corr=" + d.correlationId() + " f=" + d.firmId() + " budget=" + d.creditBudget() + " used=" +
                    d.creditUsed() + " maxPos=" + d.maxPositionPerSymbol() + " en=" + d.enabled();
            }
            case SymbolDefinedDecoder.TEMPLATE_ID:
            {
                final SymbolDefinedDecoder d = symbolDefined();
                return "SYMDEF corr=" + d.correlationId() + " sym=" + d.symbolId() + " tick=" + d.tickSize() + " lot=" +
                    d.lotSize() + " ref=" + d.referencePrice() + " max=" + d.maxOrderQty() + " bps=" + d.priceBandBps() +
                    " ticker=" + d.ticker().trim() + " state=" + d.state();
            }
            case CommandRejectedDecoder.TEMPLATE_ID:
            {
                final CommandRejectedDecoder d = rejected();
                return "CMDREJ corr=" + d.correlationId() + " t=" + d.commandTemplateId() + " r=" + d.rejectReason();
            }
            case DayRolledDecoder.TEMPLATE_ID:
            {
                final DayRolledDecoder d = dayRolled();
                return "DAYROLLED corr=" + d.correlationId() + " date=" + d.tradingDate() + " n=" + d.expiredOrders();
            }
            case CommandAppliedDecoder.TEMPLATE_ID:
            {
                final CommandAppliedDecoder d = applied();
                return "APPLIED n=" + d.eventCount() + " t=" + d.commandTemplateId();
            }
            default:
                return "UNKNOWN " + templateId;
        }
    }

    @SuppressWarnings("ParameterNumber")
    static String erText(
        final long sessionId, final long orderId, final long clOrdId, final long orig, final long matchId,
        final long price, final long orderQty, final long lastQty, final long lastPx, final long leaves,
        final long cum, final long gwTs, final long firm, final long account, final long symbol, final int gateway,
        final String execType, final String status, final int side, final String ordType, final String tif,
        final String liq, final String rej, final String cxl)
    {
        return "ER " + execType + " " + status + " sess=" + sessionId + " oid=" + orderId + " cl=" + clOrdId +
            " orig=" + orig + " m=" + matchId + " px=" + price + " q=" + orderQty + " lq=" + lastQty + " lpx=" + lastPx +
            " lv=" + leaves + " cum=" + cum + " gwts=" + gwTs + " f=" + firm + " a=" + account + " sym=" + symbol +
            " g=" + gateway + " side=" + side + " " + ordType + " " + tif + " liq=" + liq + " rej=" + rej + " cxl=" + cxl;
    }

    @Override
    public String toString()
    {
        return "#" + seq() + "/" + idx() + " " + text();
    }
}
