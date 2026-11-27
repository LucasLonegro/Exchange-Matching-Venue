package io.lmb.controlplane;

import io.lmb.sbe.BooleanType;
import io.lmb.sbe.CancelReason;
import io.lmb.sbe.DayRolloverEncoder;
import io.lmb.sbe.MassCancelEncoder;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.RiskLimitUpdateEncoder;
import io.lmb.sbe.SymbolDefinitionEncoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateCommandEncoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;
import java.util.HexFormat;

/** SBE encoding of the control commands. The control plane is off the hot path, so these return fresh arrays. */
public final class Commands
{
    public static final long ALL_SYMBOLS = SymbolStateCommandEncoder.symbolIdNullValue();
    private static final HexFormat HEX = HexFormat.of();

    private Commands()
    {
    }

    public static byte[] symbolDefinition(final long correlationId, final int symbolId, final String ticker, final long tickSize,
        final long lotSize, final long referencePrice, final int priceBandBps, final long maxOrderQty, final int dynamicBandBps,
        final long volatilityAuctionNs, final SymbolState initial)
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[128]);
        final SymbolDefinitionEncoder e = new SymbolDefinitionEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder());
        e.correlationId(correlationId).tickSize(tickSize).lotSize(lotSize).referencePrice(referencePrice)
            .maxOrderQty(maxOrderQty).symbolId(symbolId).priceBandBps(priceBandBps).ticker(ticker).initialState(initial)
            .volatilityAuctionNs(volatilityAuctionNs).dynamicBandBps(dynamicBandBps);
        return copy(b, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    /** symbolId {@link #ALL_SYMBOLS} = every symbol on the shard. */
    public static byte[] symbolState(final long correlationId, final long symbolId, final SymbolState target,
        final long effectiveTimeNs, final int reasonCode)
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[64]);
        final SymbolStateCommandEncoder e = new SymbolStateCommandEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder());
        e.correlationId(correlationId).effectiveTimeNs(effectiveTimeNs).symbolId(symbolId).reasonCode(reasonCode).targetState(target);
        return copy(b, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    public static byte[] riskLimit(final long correlationId, final int firmId, final long creditBudget, final long maxPos,
        final boolean enabled)
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[64]);
        final RiskLimitUpdateEncoder e = new RiskLimitUpdateEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder());
        e.correlationId(correlationId).creditBudget(creditBudget).maxPositionPerSymbol(maxPos).firmId(firmId)
            .enabled(enabled ? BooleanType.TRUE : BooleanType.FALSE);
        return copy(b, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    public static byte[] dayRollover(final long correlationId, final int tradingDate)
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[64]);
        final DayRolloverEncoder e = new DayRolloverEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder());
        e.correlationId(correlationId).tradingDate(tradingDate);
        return copy(b, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    public static byte[] massCancel(final long requestId, final MassCancelScope scope, final int firmId, final int symbolId,
        final long sessionId, final int gatewayId, final int targetGatewayId)
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[96]);
        final MassCancelEncoder e = new MassCancelEncoder().wrapAndApplyHeader(b, 0, new MessageHeaderEncoder());
        e.requestId(requestId).sessionId(sessionId).gatewayInTsNs(0).firmId(firmId).symbolId(symbolId).gatewayId(gatewayId)
            .targetGatewayId(targetGatewayId).scope(scope).reason(CancelReason.ADMIN);
        return copy(b, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
    }

    public static String hex(final byte[] bytes)
    {
        return HEX.formatHex(bytes);
    }

    public static byte[] unhex(final String hex)
    {
        return HEX.parseHex(hex);
    }

    private static byte[] copy(final UnsafeBuffer b, final int length)
    {
        return Arrays.copyOf(b.byteArray(), length);
    }
}
