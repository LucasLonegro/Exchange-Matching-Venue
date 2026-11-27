package io.lmb.tools;

import io.lmb.proto.marketdata.BookSnapshot;
import io.lmb.proto.marketdata.ClosingPrice;
import io.lmb.proto.marketdata.MarketDataUpdate;
import io.lmb.proto.marketdata.PriceLevel;
import io.lmb.proto.marketdata.ReferencePrice;
import io.lmb.proto.marketdata.SymbolStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The watcher's line format for the day-cycle content: reference and band, close, state-change cause. */
class MarketDataWatchMainTest
{
    private static final ReferencePrice REF = ReferencePrice.newBuilder().setReferencePriceCents(10_000)
        .setHasReferencePrice(true).setBandLowCents(9_500).setBandHighCents(10_500).setHasBand(true).build();
    private static final ClosingPrice CLOSE = ClosingPrice.newBuilder().setPriceCents(10_200).setHasPrice(true)
        .setPreviousCloseCents(10_000).setHasPreviousClose(true).setTradingDate(20261127).setSource("AUCTION").build();

    private static MarketDataUpdate.Builder update()
    {
        return MarketDataUpdate.newBuilder().setSymbol("AAPL").setSymbolId(1).setMdSeq(7);
    }

    @Test
    void referenceCloseAndCause()
    {
        assertEquals("MD AAPL seq=7 REFERENCE ref=10000 band=9500..10500",
            MarketDataWatchMain.format(update().setReference(REF).build()));
        assertEquals("MD AAPL seq=7 REFERENCE ref=- band=-",
            MarketDataWatchMain.format(update().setReference(ReferencePrice.getDefaultInstance()).build()));
        assertEquals("MD AAPL seq=7 CLOSE price=10200 prevClose=10000 date=20261127 source=AUCTION",
            MarketDataWatchMain.format(update().setClose(CLOSE).build()));
        assertEquals("MD AAPL seq=7 CLOSE price=- prevClose=- date=0 source=NONE",
            MarketDataWatchMain.format(update().setClose(ClosingPrice.newBuilder().setSource("NONE")).build()));
        assertEquals("MD AAPL seq=7 STATUS state=AUCTION_CALL cause=VOLATILITY_INTERRUPTION reasonCode=0",
            MarketDataWatchMain.format(update().setStatus(SymbolStatus.newBuilder().setState("AUCTION_CALL")
                .setCause("VOLATILITY_INTERRUPTION")).build()));
    }

    @Test
    void snapshotCarriesStateCauseReferenceAndLastClose()
    {
        final BookSnapshot s = BookSnapshot.newBuilder().setState("CONTINUOUS").setStateCause("VOLATILITY_AUCTION_END")
            .addBids(PriceLevel.newBuilder().setPriceCents(9_990).setQuantity(100).setOrderCount(1))
            .setReference(REF).setLastClose(CLOSE).build();
        assertEquals("MD AAPL seq=7 SNAPSHOT state=CONTINUOUS cause=VOLATILITY_AUCTION_END bid=100@9990 ask=- levels=1/0" +
            " ref=10000 band=9500..10500 lastClose: price=10200 prevClose=10000 date=20261127 source=AUCTION",
            MarketDataWatchMain.format(update().setSnapshot(s).build()));
        assertEquals("MD AAPL seq=7 SNAPSHOT state=PRE_OPEN bid=- ask=- levels=0/0",
            MarketDataWatchMain.format(update().setSnapshot(BookSnapshot.newBuilder().setState("PRE_OPEN")).build()));
    }
}
