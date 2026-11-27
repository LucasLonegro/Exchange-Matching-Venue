package io.lmb.matcher;

import io.lmb.sbe.BooleanType;
import io.lmb.sbe.CancelOrderEncoder;
import io.lmb.sbe.CancelReason;
import io.lmb.sbe.DayRolloverEncoder;
import io.lmb.sbe.MassCancelEncoder;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.NewOrderEncoder;
import io.lmb.sbe.OrdType;
import io.lmb.sbe.ReferencePriceEncoder;
import io.lmb.sbe.ReplaceOrderEncoder;
import io.lmb.sbe.RiskLimitUpdateEncoder;
import io.lmb.sbe.Side;
import io.lmb.sbe.SymbolDefinitionEncoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateCommandEncoder;
import io.lmb.sbe.TimeInForce;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;

/** Encodes commands to byte arrays for tests. */
final class Cmd
{
    static final long NULL = Long.MIN_VALUE;
    static final long ALL_SYMBOLS = 0xFFFF_FFFFL;
    private static final int HDR = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final MessageHeaderEncoder H = new MessageHeaderEncoder();

    private Cmd()
    {
    }

    private static UnsafeBuffer buf()
    {
        return new UnsafeBuffer(new byte[256]);
    }

    private static byte[] bytes(final UnsafeBuffer b, final int blockLength)
    {
        return Arrays.copyOf(b.byteArray(), HDR + blockLength);
    }

    static byte[] newOrder(
        final long sessionId, final long clOrdId, final int firmId, final int symbolId, final Side side,
        final OrdType type, final TimeInForce tif, final long price, final long qty, final boolean possResend)
    {
        return newOrder(sessionId, clOrdId, firmId, 7, symbolId, 1, side, type, tif, price, qty, possResend, 1000 + clOrdId);
    }

    static byte[] newOrder(
        final long sessionId, final long clOrdId, final int firmId, final int accountId, final int symbolId,
        final int gatewayId, final Side side, final OrdType type, final TimeInForce tif, final long price,
        final long qty, final boolean possResend, final long gwTs)
    {
        final UnsafeBuffer b = buf();
        new NewOrderEncoder().wrapAndApplyHeader(b, 0, H)
            .sessionId(sessionId).clOrdId(clOrdId).price(price).quantity(qty).gatewayInTsNs(gwTs)
            .firmId(firmId).accountId(accountId).symbolId(symbolId).gatewayId(gatewayId)
            .side(side).ordType(type).timeInForce(tif).possResend(possResend ? BooleanType.TRUE : BooleanType.FALSE);
        return bytes(b, NewOrderEncoder.BLOCK_LENGTH);
    }

    static byte[] limit(final long session, final long clOrdId, final int firm, final int symbol, final Side side, final long price, final long qty)
    {
        return newOrder(session, clOrdId, firm, symbol, side, OrdType.LIMIT, TimeInForce.DAY, price, qty, false);
    }

    static byte[] ioc(final long session, final long clOrdId, final int firm, final int symbol, final Side side, final long price, final long qty)
    {
        return newOrder(session, clOrdId, firm, symbol, side, OrdType.LIMIT, TimeInForce.IOC, price, qty, false);
    }

    static byte[] market(final long session, final long clOrdId, final int firm, final int symbol, final Side side, final long qty)
    {
        return newOrder(session, clOrdId, firm, symbol, side, OrdType.MARKET, TimeInForce.DAY, NULL, qty, false);
    }

    static byte[] cancel(final long session, final long clOrdId, final long orig, final int firm, final int symbol, final boolean possResend)
    {
        return cancel(session, clOrdId, orig, firm, symbol, 1, possResend);
    }

    static byte[] cancel(
        final long session, final long clOrdId, final long orig, final int firm, final int symbol, final int gatewayId,
        final boolean possResend)
    {
        final UnsafeBuffer b = buf();
        new CancelOrderEncoder().wrapAndApplyHeader(b, 0, H)
            .sessionId(session).clOrdId(clOrdId).origClOrdId(orig).gatewayInTsNs(2000 + clOrdId).firmId(firm)
            .symbolId(symbol).gatewayId(gatewayId).possResend(possResend ? BooleanType.TRUE : BooleanType.FALSE);
        return bytes(b, CancelOrderEncoder.BLOCK_LENGTH);
    }

    static byte[] replace(
        final long session, final long clOrdId, final long orig, final int firm, final int symbol, final long newPrice,
        final long newQty, final boolean possResend)
    {
        final UnsafeBuffer b = buf();
        new ReplaceOrderEncoder().wrapAndApplyHeader(b, 0, H)
            .sessionId(session).clOrdId(clOrdId).origClOrdId(orig).newPrice(newPrice).newQuantity(newQty)
            .gatewayInTsNs(3000 + clOrdId).firmId(firm).symbolId(symbol).gatewayId(1)
            .possResend(possResend ? BooleanType.TRUE : BooleanType.FALSE);
        return bytes(b, ReplaceOrderEncoder.BLOCK_LENGTH);
    }

    static byte[] massCancel(
        final long requestId, final long session, final int firm, final int symbol, final int gatewayId,
        final int targetGateway, final MassCancelScope scope, final CancelReason reason)
    {
        final UnsafeBuffer b = buf();
        new MassCancelEncoder().wrapAndApplyHeader(b, 0, H)
            .requestId(requestId).sessionId(session).gatewayInTsNs(4000 + requestId).firmId(firm).symbolId(symbol)
            .gatewayId(gatewayId).targetGatewayId(targetGateway).scope(scope).reason(reason);
        return bytes(b, MassCancelEncoder.BLOCK_LENGTH);
    }

    static byte[] ref(final int symbol, final long ref, final long sourceTs)
    {
        final UnsafeBuffer b = buf();
        new ReferencePriceEncoder().wrapAndApplyHeader(b, 0, H)
            .nbbBid(ref == NULL ? NULL : ref - 1).nbbOffer(ref == NULL ? NULL : ref + 1).referencePrice(ref)
            .sourceTsNs(sourceTs).symbolId(symbol).sourceId(1).venueCount((short)3);
        return bytes(b, ReferencePriceEncoder.BLOCK_LENGTH);
    }

    static byte[] symbol(
        final long corr, final int symbol, final String ticker, final long tick, final long lot, final long ref,
        final long maxQty, final long bps, final SymbolState initial)
    {
        final UnsafeBuffer b = buf();
        new SymbolDefinitionEncoder().wrapAndApplyHeader(b, 0, H)
            .correlationId(corr).tickSize(tick).lotSize(lot).referencePrice(ref).maxOrderQty(maxQty)
            .symbolId(symbol).priceBandBps(bps).ticker(ticker).initialState(initial);
        return bytes(b, SymbolDefinitionEncoder.BLOCK_LENGTH);
    }

    /** band-stp: a SymbolDefinition with the dynamic collar and the volatility auction length. */
    static byte[] symbol(
        final long corr, final int symbol, final String ticker, final long tick, final long lot, final long ref,
        final long maxQty, final long bps, final SymbolState initial, final long dynBps, final long volNs)
    {
        final byte[] c = symbol(corr, symbol, ticker, tick, lot, ref, maxQty, bps, initial);
        new SymbolDefinitionEncoder().wrap(new UnsafeBuffer(c), HDR).dynamicBandBps(dynBps).volatilityAuctionNs(volNs);
        return c;
    }

    static byte[] state(final long corr, final long symbol, final SymbolState target, final long effectiveTimeNs)
    {
        final UnsafeBuffer b = buf();
        new SymbolStateCommandEncoder().wrapAndApplyHeader(b, 0, H)
            .correlationId(corr).effectiveTimeNs(effectiveTimeNs).symbolId(symbol).reasonCode(7).targetState(target);
        return bytes(b, SymbolStateCommandEncoder.BLOCK_LENGTH);
    }

    static byte[] risk(final long corr, final int firm, final long budget, final long maxPos, final boolean enabled)
    {
        final UnsafeBuffer b = buf();
        new RiskLimitUpdateEncoder().wrapAndApplyHeader(b, 0, H)
            .correlationId(corr).creditBudget(budget).maxPositionPerSymbol(maxPos).firmId(firm)
            .enabled(enabled ? BooleanType.TRUE : BooleanType.FALSE);
        return bytes(b, RiskLimitUpdateEncoder.BLOCK_LENGTH);
    }

    static byte[] dayRoll(final long corr, final int date)
    {
        final UnsafeBuffer b = buf();
        new DayRolloverEncoder().wrapAndApplyHeader(b, 0, H).correlationId(corr).tradingDate(date);
        return bytes(b, DayRolloverEncoder.BLOCK_LENGTH);
    }

    /** Overwrites one byte of a command body (to build malformed enums). */
    static byte[] poke(final byte[] cmd, final int bodyOffset, final int value)
    {
        final byte[] c = cmd.clone();
        c[HDR + bodyOffset] = (byte)value;
        return c;
    }
}
