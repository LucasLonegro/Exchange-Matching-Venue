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

    long auctPrice = Codes.NULL_PRICE;
    long auctMatched;
    long auctImbalance;
    byte auctSide = Codes.NO_SIDE;

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

    boolean crossed()
    {
        final PriceLevel b = bids.best();
        final PriceLevel a = asks.best();
        return b != null && a != null && b.price >= a.price;
    }
}
