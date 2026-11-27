package io.lmb.matcher;

/** Symbol parameters, state, reference price / band, book, and the last published auction indicative. */
final class Symbol
{
    final int idx;
    final int symbolId;
    final byte[] ticker = new byte[8];
    long tickSize;
    long lotSize;
    long maxOrderQty;
    long bandBps;
    long refPrice = Codes.NULL_PRICE;
    long lastRefSourceTs = Long.MIN_VALUE;
    long bandLow = Codes.NULL_PRICE;
    long bandHigh = Codes.NULL_PRICE;
    byte state;
    final BookSide bids;
    final BookSide asks;
    final OrderList orders = new OrderList(Order.L_SYMBOL);

    // band-stp: dynamic collar and volatility interruption
    /** Dynamic collar half-width around the last trade price, bps; 0 = disabled. */
    long dynamicBandBps;
    /** Length of a volatility auction, cluster ns. */
    long volatilityAuctionNs;
    /** Timer id of the running volatility auction, 0 = none. Non-zero only in AUCTION_CALL. */
    long volTimerId;
    /** Deadline of that timer (cluster ns). */
    long volEndNs;
    // /band-stp

    long auctPrice = Codes.NULL_PRICE;
    long auctMatched;
    long auctImbalance;
    byte auctSide = Codes.NO_SIDE;

    // ---- day-cycle: last trade and official close ----
    /** Price of the last trade print (continuous or auction). Not reset by DayRollover. */
    long lastTradePx = Codes.NULL_PRICE;
    /** A trade printed since the last DayRollover (the current trading day). */
    boolean tradedToday;
    /** True while closePx/closeSource are today's close and no trade has printed since it was determined. */
    boolean hasClose;
    /** Today's close (valid when hasClose; also kept after a later trade until the next close or roll). */
    long closePx = Codes.NULL_PRICE;
    /** ClosePriceSource raw value of closePx. */
    byte closeSource = Codes.CLOSE_NONE;
    /** Official close of the previous trading day. */
    long prevClosePx = Codes.NULL_PRICE;

    void resetDayCycle()
    {
        lastTradePx = Codes.NULL_PRICE;
        tradedToday = false;
        hasClose = false;
        closePx = Codes.NULL_PRICE;
        closeSource = Codes.CLOSE_NONE;
        prevClosePx = Codes.NULL_PRICE;
    }

    Symbol(final int idx, final int symbolId, final int windowSize)
    {
        this.idx = idx;
        this.symbolId = symbolId;
        this.bids = new BookSide(this, true, windowSize);
        this.asks = new BookSide(this, false, windowSize);
    }

    /** Band [ref*(1-bps/1e4), ref*(1+bps/1e4)] rounded inward to the tick; low clamped to one tick, high to MAX_PRICE. */
    void computeBand()
    {
        if (refPrice == Codes.NULL_PRICE)
        {
            bandLow = Codes.NULL_PRICE;
            bandHigh = Codes.NULL_PRICE;
            return;
        }
        final long denom = 10_000L * tickSize;
        long low = bandBps >= 10_000L ? tickSize : Math.ceilDiv(refPrice * (10_000L - bandBps), denom) * tickSize;
        if (low < tickSize)
        {
            low = tickSize;
        }
        long high = Math.floorDiv(refPrice * (10_000L + bandBps), denom) * tickSize;
        if (high > Codes.MAX_PRICE)
        {
            high = (Codes.MAX_PRICE / tickSize) * tickSize;
        }
        bandLow = low;
        bandHigh = high;
    }

    void resetAuction()
    {
        auctPrice = Codes.NULL_PRICE;
        auctMatched = 0;
        auctImbalance = 0;
        auctSide = Codes.NO_SIDE;
    }

    // band-stp
    /** True in a matcher-initiated volatility auction (AUCTION_CALL whose end is a matcher timer). */
    boolean inVolatilityAuction()
    {
        return volTimerId != 0;
    }

    /**
     * True if a print at px would leave the dynamic collar centred on {@code centre}:
     * px outside [centre*(1-bps/1e4), centre*(1+bps/1e4)] (px is on the tick, so this is the collar rounded inward).
     * Never true when the collar is disabled or there is no centre.
     */
    boolean outsideCollar(final long centre, final long px)
    {
        if (dynamicBandBps <= 0 || centre == Codes.NULL_PRICE)
        {
            return false;
        }
        final long scaled = px * 10_000L;
        return scaled > centre * (10_000L + dynamicBandBps) || scaled < centre * (10_000L - dynamicBandBps);
    }

    /**
     * Centre of the dynamic collar: the last trade price if a trade printed in the current trading day, else the
     * reference price (after a roll, the official close). Either may be null; a null centre disables the collar.
     */
    long collarCentre()
    {
        return tradedToday ? lastTradePx : refPrice;
    }
    // /band-stp

    boolean crossed()
    {
        final PriceLevel b = bids.best();
        final PriceLevel a = asks.best();
        return b != null && a != null && b.price >= a.price;
    }
}
