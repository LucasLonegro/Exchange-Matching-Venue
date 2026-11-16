package io.lmb.fix;

import io.lmb.fix.builder.ExecutionReportEncoder;
import io.lmb.fix.builder.NewOrderSingleEncoder;
import io.lmb.fix.decoder.ExecutionReportDecoder;
import io.lmb.fix.decoder.LogonDecoder;
import io.lmb.fix.decoder.NewOrderSingleDecoder;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixCodecsTest
{
    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[4096]);

    @Test
    void dictionaryIsGeneratedInTheVenuePackage()
    {
        final FixDictionary dictionary = FixDictionary.of(FixDictionaryImpl.class);
        assertEquals("FIX.4.4", dictionary.beginString());
        assertInstanceOf(LogonDecoder.class, dictionary.makeLogonDecoder());
    }

    @Test
    void newOrderSingleRoundTrip()
    {
        final UtcTimestampEncoder ts = new UtcTimestampEncoder();
        final int tsLength = ts.encode(1_700_000_000_123L);
        final NewOrderSingleEncoder nos = new NewOrderSingleEncoder();
        nos.header().senderCompID("1001").targetCompID("LMB").msgSeqNum(7).sendingTime(ts.buffer(), tsLength);
        nos.clOrdID("123456789012").account("42").side(Side.SELL).ordType(OrdType.LIMIT)
            .price(19050, 2).timeInForce(TimeInForce.IMMEDIATE_OR_CANCEL).transactTime(ts.buffer(), tsLength);
        nos.instrument().symbol("AAPL");
        nos.orderQtyData().orderQty(300, 0);

        final long result = nos.encode(buffer, 0);
        final NewOrderSingleDecoder decoder = new NewOrderSingleDecoder();
        decoder.decode(buffer, Encoder.offset(result), Encoder.length(result));

        assertTrue(decoder.validate(), () -> "invalid: " + decoder.invalidTagId());
        assertEquals("123456789012", decoder.clOrdIDAsString());
        assertEquals("AAPL", decoder.symbolAsString());
        assertEquals(Side.SELL, decoder.sideAsEnum());
        assertEquals(OrdType.LIMIT, decoder.ordTypeAsEnum());
        assertEquals(TimeInForce.IMMEDIATE_OR_CANCEL, decoder.timeInForceAsEnum());
        assertEquals(190.50, decoder.price().toDouble(), 1e-9);
        assertEquals(300.0, decoder.orderQty().toDouble(), 1e-9);
        assertEquals(7, decoder.header().msgSeqNum());
    }

    @Test
    void executionReportCarriesVenueTags()
    {
        final UtcTimestampEncoder ts = new UtcTimestampEncoder();
        final int tsLength = ts.encode(1_700_000_000_123L);
        final ExecutionReportEncoder er = new ExecutionReportEncoder();
        er.header().senderCompID("LMB").targetCompID("1001").msgSeqNum(3).sendingTime(ts.buffer(), tsLength);
        er.orderID("281474976710657").execID("1-4096-0").clOrdID("5").execType(ExecType.TRADE)
            .ordStatus(OrdStatus.PARTIALLY_FILLED).side(Side.BUY).leavesQty(60, 0).cumQty(40, 0)
            .lastQty(40, 0).lastPx(19000, 2).lastLiquidityInd(LastLiquidityInd.REMOVED_LIQUIDITY)
            .lmbShardID(1).lmbEventSeq(5_000_000_000L);
        er.instrument().symbol("GOOG");

        final long result = er.encode(buffer, 0);
        final ExecutionReportDecoder decoder = new ExecutionReportDecoder();
        decoder.decode(buffer, Encoder.offset(result), Encoder.length(result));

        assertTrue(decoder.validate(), () -> "invalid: " + decoder.invalidTagId());
        assertEquals(ExecType.TRADE, decoder.execTypeAsEnum());
        assertEquals(OrdStatus.PARTIALLY_FILLED, decoder.ordStatusAsEnum());
        assertEquals(1, decoder.lmbShardID());
        assertEquals(5_000_000_000L, decoder.lmbEventSeq());
        assertEquals(190.00, decoder.lastPx().toDouble(), 1e-9);
        assertEquals(LastLiquidityInd.REMOVED_LIQUIDITY, decoder.lastLiquidityIndAsEnum());
    }
}
