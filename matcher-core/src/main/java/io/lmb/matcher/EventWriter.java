package io.lmb.matcher;

import io.lmb.api.EventSink;
import io.lmb.common.HashChain;
import io.lmb.sbe.AuctionUpdateEncoder;
import io.lmb.sbe.BookDeltaEncoder;
import io.lmb.sbe.BooleanType;
import io.lmb.sbe.CancelReason;
import io.lmb.sbe.ClosePriceSource;
import io.lmb.sbe.ClosingPriceEncoder;
import io.lmb.sbe.CommandAppliedEncoder;
import io.lmb.sbe.CommandRejectedEncoder;
import io.lmb.sbe.DayRolledEncoder;
import io.lmb.sbe.EventHeaderEncoder;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportEncoder;
import io.lmb.sbe.LiquidityFlag;
import io.lmb.sbe.MassCancelDoneEncoder;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.OrdStatus;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.ReferencePriceAppliedEncoder;
import io.lmb.sbe.RejectReason;
import io.lmb.sbe.RiskLimitAppliedEncoder;
import io.lmb.sbe.Side;
import io.lmb.sbe.StateChangeCause;
import io.lmb.sbe.SymbolDefinedEncoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateChangedEncoder;
import io.lmb.sbe.TimeInForce;
import io.lmb.sbe.TradeEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Encodes events into one reusable buffer, assigns the event identity (eventSeq, eventIdx), feeds
 * every event except CommandApplied into the hash chain, and hands it to the sink. Allocation-free.
 */
final class EventWriter
{
    private static final int HDR = MessageHeaderEncoder.ENCODED_LENGTH;

    private final int shardId;
    private final UnsafeBuffer buf = new UnsafeBuffer(new byte[512]);
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final ExecutionReportEncoder er = new ExecutionReportEncoder();
    private final TradeEncoder trade = new TradeEncoder();
    private final BookDeltaEncoder delta = new BookDeltaEncoder();
    private final SymbolStateChangedEncoder stateChanged = new SymbolStateChangedEncoder();
    private final AuctionUpdateEncoder auction = new AuctionUpdateEncoder();
    private final ReferencePriceAppliedEncoder refApplied = new ReferencePriceAppliedEncoder();
    private final MassCancelDoneEncoder massDone = new MassCancelDoneEncoder();
    private final RiskLimitAppliedEncoder riskApplied = new RiskLimitAppliedEncoder();
    private final SymbolDefinedEncoder symbolDefined = new SymbolDefinedEncoder();
    private final CommandRejectedEncoder rejected = new CommandRejectedEncoder();
    private final DayRolledEncoder dayRolled = new DayRolledEncoder();
    private final ClosingPriceEncoder closingPrice = new ClosingPriceEncoder();
    private final CommandAppliedEncoder applied = new CommandAppliedEncoder();

    final HashChain chain = new HashChain();
    EventSink sink;
    long eventSeq;
    private int eventIdx;
    private long logPosition;
    private long timestampNs;

    EventWriter(final int shardId)
    {
        this.shardId = shardId;
    }

    void begin(final long logPosition, final long timestampNs)
    {
        this.logPosition = logPosition;
        this.timestampNs = timestampNs;
        this.eventIdx = 0;
        chain.begin(logPosition, timestampNs);
    }

    void hashInput(final DirectBuffer buffer, final int offset, final int length)
    {
        chain.update(buffer, offset, length);
    }

    void hashTimer(final long correlationId)
    {
        chain.updateLong(correlationId);
    }

    /** Cluster time of the log entry being applied. */
    long timestampNs()
    {
        return timestampNs;
    }

    int eventIdx()
    {
        return eventIdx;
    }

    private void eventHeader(final EventHeaderEncoder h)
    {
        h.eventSeq(++eventSeq)
            .logPosition(logPosition)
            .timestampNs(timestampNs)
            .eventIdx(eventIdx++)
            .shardId(shardId)
            .reserved(0);
    }

    private void publish(final int length)
    {
        chain.update(buf, 0, length);
        sink.onEvent(buf, 0, length);
    }

    static Side side(final byte code)
    {
        return code == Codes.BUY ? Side.BUY : code == Codes.SELL ? Side.SELL : Side.NULL_VAL;
    }

    @SuppressWarnings("ParameterNumber")
    void executionReport(
        final long sessionId, final long orderId, final long clOrdId, final long origClOrdId, final long matchId,
        final long price, final long orderQty, final long lastQty, final long lastPx, final long leavesQty,
        final long cumQty, final long gatewayInTsNs, final int firmId, final int accountId, final int symbolId,
        final int gatewayId, final ExecType execType, final OrdStatus ordStatus, final byte side, final byte ordType,
        final byte tif, final LiquidityFlag liquidity, final RejectReason rejectReason, final CancelReason cancelReason)
    {
        er.wrapAndApplyHeader(buf, 0, header);
        eventHeader(er.eventHeader());
        er.sessionId(sessionId)
            .orderId(orderId)
            .clOrdId(clOrdId)
            .origClOrdId(origClOrdId)
            .matchId(matchId)
            .price(price)
            .orderQty(orderQty)
            .lastQty(lastQty)
            .lastPx(lastPx)
            .leavesQty(leavesQty)
            .cumQty(cumQty)
            .gatewayInTsNs(gatewayInTsNs)
            .firmId(firmId & 0xFFFF_FFFFL)
            .accountId(accountId & 0xFFFF_FFFFL)
            .symbolId(symbolId & 0xFFFF_FFFFL)
            .gatewayId(gatewayId)
            .execType(execType)
            .ordStatus(ordStatus)
            .side(side(side))
            .ordType(ordType == Codes.MARKET ? OrdType.MARKET : OrdType.LIMIT)
            .timeInForce(tif == Codes.IOC ? TimeInForce.IOC : TimeInForce.DAY)
            .liquidity(liquidity)
            .rejectReason(rejectReason)
            .cancelReason(cancelReason);
        publish(HDR + ExecutionReportEncoder.BLOCK_LENGTH);
    }

    void trade(
        final long matchId, final long price, final long qty, final long buyOrderId, final long sellOrderId,
        final int symbolId, final int buyFirmId, final int sellFirmId, final byte aggressorSide, final boolean auction)
    {
        trade.wrapAndApplyHeader(buf, 0, header);
        eventHeader(trade.eventHeader());
        trade.matchId(matchId)
            .price(price)
            .quantity(qty)
            .buyOrderId(buyOrderId)
            .sellOrderId(sellOrderId)
            .symbolId(symbolId & 0xFFFF_FFFFL)
            .buyFirmId(buyFirmId & 0xFFFF_FFFFL)
            .sellFirmId(sellFirmId & 0xFFFF_FFFFL)
            .aggressorSide(side(aggressorSide))
            .isAuction(auction ? BooleanType.TRUE : BooleanType.FALSE);
        publish(HDR + TradeEncoder.BLOCK_LENGTH);
    }

    void bookDelta(final long price, final long qty, final int symbolId, final int orderCount, final byte side)
    {
        delta.wrapAndApplyHeader(buf, 0, header);
        eventHeader(delta.eventHeader());
        delta.price(price).quantity(qty).symbolId(symbolId & 0xFFFF_FFFFL).orderCount(orderCount).side(side(side));
        publish(HDR + BookDeltaEncoder.BLOCK_LENGTH);
    }

    static SymbolState state(final byte code)
    {
        return SymbolState.get(code);
    }

    void symbolStateChanged(
        final long correlationId, final int symbolId, final int reasonCode, final byte prev, final byte next,
        final StateChangeCause cause)
    {
        stateChanged.wrapAndApplyHeader(buf, 0, header);
        eventHeader(stateChanged.eventHeader());
        stateChanged.correlationId(correlationId)
            .symbolId(symbolId & 0xFFFF_FFFFL)
            .reasonCode(reasonCode)
            .previousState(state(prev))
            .newState(state(next))
            .cause(cause); // band-stp
        publish(HDR + SymbolStateChangedEncoder.BLOCK_LENGTH);
    }

    void auctionUpdate(
        final long price, final long matched, final long imbalance, final int symbolId, final byte imbalanceSide,
        final boolean isFinal)
    {
        auction.wrapAndApplyHeader(buf, 0, header);
        eventHeader(auction.eventHeader());
        auction.price(price)
            .matchedQuantity(matched)
            .imbalanceQuantity(imbalance)
            .symbolId(symbolId & 0xFFFF_FFFFL)
            .imbalanceSide(side(imbalanceSide))
            .isFinal(isFinal ? BooleanType.TRUE : BooleanType.FALSE);
        publish(HDR + AuctionUpdateEncoder.BLOCK_LENGTH);
    }

    void referencePriceApplied(
        final long ref, final long nbbBid, final long nbbOffer, final long bandLow, final long bandHigh,
        final long sourceTsNs, final int symbolId)
    {
        refApplied.wrapAndApplyHeader(buf, 0, header);
        eventHeader(refApplied.eventHeader());
        refApplied.referencePrice(ref)
            .nbbBid(nbbBid)
            .nbbOffer(nbbOffer)
            .bandLow(bandLow)
            .bandHigh(bandHigh)
            .sourceTsNs(sourceTsNs)
            .symbolId(symbolId & 0xFFFF_FFFFL);
        publish(HDR + ReferencePriceAppliedEncoder.BLOCK_LENGTH);
    }

    void massCancelDone(
        final long requestId, final long sessionId, final int firmId, final int count, final int gatewayId,
        final MassCancelScope scope)
    {
        massDone.wrapAndApplyHeader(buf, 0, header);
        eventHeader(massDone.eventHeader());
        massDone.requestId(requestId)
            .sessionId(sessionId)
            .firmId(firmId & 0xFFFF_FFFFL)
            .canceledCount(count)
            .gatewayId(gatewayId)
            .scope(scope);
        publish(HDR + MassCancelDoneEncoder.BLOCK_LENGTH);
    }

    void riskLimitApplied(
        final long correlationId, final long budget, final long used, final long maxPos, final int firmId,
        final boolean enabled)
    {
        riskApplied.wrapAndApplyHeader(buf, 0, header);
        eventHeader(riskApplied.eventHeader());
        riskApplied.correlationId(correlationId)
            .creditBudget(budget)
            .creditUsed(used)
            .maxPositionPerSymbol(maxPos)
            .firmId(firmId & 0xFFFF_FFFFL)
            .enabled(enabled ? BooleanType.TRUE : BooleanType.FALSE);
        publish(HDR + RiskLimitAppliedEncoder.BLOCK_LENGTH);
    }

    void symbolDefined(final long correlationId, final Symbol s)
    {
        symbolDefined.wrapAndApplyHeader(buf, 0, header);
        eventHeader(symbolDefined.eventHeader());
        symbolDefined.correlationId(correlationId)
            .tickSize(s.tickSize)
            .lotSize(s.lotSize)
            .referencePrice(s.refPrice)
            .maxOrderQty(s.maxOrderQty)
            .symbolId(s.symbolId & 0xFFFF_FFFFL)
            .priceBandBps(s.bandBps)
            .putTicker(s.ticker, 0)
            .state(state(s.state))
            .volatilityAuctionNs(s.volatilityAuctionNs) // band-stp
            .dynamicBandBps(s.dynamicBandBps);
        publish(HDR + SymbolDefinedEncoder.BLOCK_LENGTH);
    }

    void commandRejected(final long correlationId, final int templateId, final RejectReason reason)
    {
        rejected.wrapAndApplyHeader(buf, 0, header);
        eventHeader(rejected.eventHeader());
        rejected.correlationId(correlationId).commandTemplateId(templateId).rejectReason(reason);
        publish(HDR + CommandRejectedEncoder.BLOCK_LENGTH);
    }

    void dayRolled(final long correlationId, final int tradingDate, final int expired)
    {
        dayRolled.wrapAndApplyHeader(buf, 0, header);
        eventHeader(dayRolled.eventHeader());
        dayRolled.correlationId(correlationId).tradingDate(tradingDate).expiredOrders(expired);
        publish(HDR + DayRolledEncoder.BLOCK_LENGTH);
    }

    /** Ends the log entry: CommandApplied with the new chain hash. Not itself hashed. */
    void closingPrice(final Symbol s, final int tradingDate)
    {
        closingPrice.wrapAndApplyHeader(buf, 0, header);
        eventHeader(closingPrice.eventHeader());
        closingPrice.price(s.closePx)
            .previousClose(s.prevClosePx)
            .symbolId(s.symbolId & 0xFFFF_FFFFL)
            .tradingDate(tradingDate)
            .source(ClosePriceSource.get(s.closeSource));
        publish(HDR + ClosingPriceEncoder.BLOCK_LENGTH);
    }

    void commandApplied(final int templateId)
    {
        final int count = eventIdx;
        final byte[] hash = chain.finish();
        applied.wrapAndApplyHeader(buf, 0, header);
        eventHeader(applied.eventHeader());
        applied.eventCount(count).commandTemplateId(templateId).putChainHash(hash, 0, HashChain.HASH_LENGTH);
        sink.onEvent(buf, 0, HDR + CommandAppliedEncoder.BLOCK_LENGTH);
    }
}
