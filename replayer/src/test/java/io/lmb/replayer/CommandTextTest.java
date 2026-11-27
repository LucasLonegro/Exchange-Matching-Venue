package io.lmb.replayer;

import io.lmb.sbe.ClosingPriceDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.SymbolDefinitionDecoder;
import io.lmb.sbe.SymbolState;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommandTextTest
{
    private final CommandText text = new CommandText();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final SymbolDefinitionDecoder def = new SymbolDefinitionDecoder();

    private SymbolDefinitionDecoder encode(final String line)
    {
        final int length = text.encode(line, buffer);
        def.wrapAndApplyHeader(buffer, 0, header);
        assertEquals(SymbolDefinitionDecoder.SCHEMA_VERSION, header.version());
        assertEquals(SymbolDefinitionDecoder.BLOCK_LENGTH, header.blockLength());
        assertEquals(MessageHeaderDecoder.ENCODED_LENGTH + def.encodedLength(), length);
        return def;
    }

    @Test
    void symbolDefinitionCarriesTheDynamicCollarAndVolatilityAuctionLength()
    {
        final SymbolDefinitionDecoder d = encode("SymbolDefinition correlationId=2 symbolId=2 ticker=BETA tickSize=1 lotSize=1 " +
            "referencePrice=5000 maxOrderQty=100000 priceBandBps=1000 dynamicBandBps=100 volatilityAuctionNs=300000000000 " +
            "initialState=CONTINUOUS");
        assertEquals(2, d.symbolId());
        assertEquals(5000, d.referencePrice());
        assertEquals(1000, d.priceBandBps());
        assertEquals(100, d.dynamicBandBps());
        assertEquals(300_000_000_000L, d.volatilityAuctionNs());
        assertEquals(SymbolState.CONTINUOUS, d.initialState());
    }

    @Test
    void symbolDefinitionWithoutTheNewFieldsHasNoCollar()
    {
        // the buffer is dirty from a previous encode: fields not given must still be 0
        encode("SymbolDefinition symbolId=3 dynamicBandBps=7 volatilityAuctionNs=9 initialState=PRE_OPEN");
        final SymbolDefinitionDecoder d = encode("SymbolDefinition correlationId=1 symbolId=1 ticker=ACME tickSize=1 " +
            "lotSize=1 referencePrice=10000 maxOrderQty=100000 priceBandBps=1000 initialState=CONTINUOUS");
        assertEquals(0, d.dynamicBandBps());
        assertEquals(0, d.volatilityAuctionNs());
    }

    @Test
    void backtestNamesTheClosingPriceEvent()
    {
        assertEquals("ClosingPrice", Backtest.templateName(ClosingPriceDecoder.TEMPLATE_ID));
    }
}
