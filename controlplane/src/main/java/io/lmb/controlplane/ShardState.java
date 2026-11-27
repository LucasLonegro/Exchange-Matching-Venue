package io.lmb.controlplane;

import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.sbe.SymbolState;

import java.util.TreeMap;
import java.util.TreeSet;

/** What the leader knows about one shard, built from its event stream. Engine thread only. */
public final class ShardState
{
    public static final class Symbol
    {
        public int symbolId;
        public String ticker;
        public long tickSize;
        public long lotSize;
        public long referencePrice;
        public int priceBandBps;
        public long maxOrderQty;
        public SymbolState state;
        public long lastCorrelationId;
        public long eventSeq;
        // band-stp
        public int dynamicBandBps;
        public long volatilityAuctionNs;
        /** In a matcher-initiated volatility auction: AUCTION_CALL entered with cause VOLATILITY_INTERRUPTION. */
        public boolean volatilityAuction;
        /** Cluster time the running volatility auction started, 0 if none. */
        public long volatilityAuctionSinceNs;
        /** The venue owns {@link #referencePrice}: it applied a reference of its own since the definition. */
        public boolean venueReference;
        public long bandLow = Long.MIN_VALUE;
        public long bandHigh = Long.MIN_VALUE;
        // day-cycle: last ClosingPrice (closeSource null = none seen)
        public long closePrice = Long.MIN_VALUE;
        public long previousClose = Long.MIN_VALUE;
        public String closeSource;
        public int closeTradingDate;
    }

    public static final class Firm
    {
        public int firmId;
        public long creditBudget;
        public long creditUsed;
        public long maxPositionPerSymbol;
        public boolean enabled;
        public long lastCorrelationId;
        public long eventSeq;
        public long observedTsNs;
        /** Local monotonic time of the last RiskLimitApplied seen by this leader (not persisted). */
        public long observedMonoNs = Long.MIN_VALUE;
    }

    public final int shardId;
    public long eventSeq;
    public int tradingDate;
    public long lastEventTsNs;
    public boolean connected;
    public boolean caughtUp;
    public boolean needsReconcile = true;
    public boolean cursorDirty = true;
    public final TreeMap<Integer, Symbol> symbols = new TreeMap<>();
    public final TreeMap<Integer, Firm> firms = new TreeMap<>();
    public final TreeSet<Integer> dirtySymbols = new TreeSet<>();
    public final TreeSet<Integer> dirtyFirms = new TreeSet<>();
    public CommandClient client;
    public EventStreamClient stream;

    public ShardState(final int shardId)
    {
        this.shardId = shardId;
    }

    public boolean ready()
    {
        return connected && caughtUp;
    }
}
