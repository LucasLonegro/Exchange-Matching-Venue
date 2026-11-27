package io.lmb.ledger;

import java.math.BigInteger;

/**
 * One firm's holding in one symbol, average cost method. Quantities are shares, amounts integer cents, all
 * arithmetic exact (an overflow throws instead of wrapping).
 *
 * <p>{@code costBasis} is the signed cost of the open quantity: positive for a long, negative for a short, 0 when
 * flat. A fill that adds to the position (or opens it) adds its notional to the cost basis. A fill that reduces it
 * removes the closed share of the cost basis, {@code costBasis * closed / |quantity|} truncated toward zero (the
 * remainder stays with the shares still open and is removed with the last of them), and realises the difference
 * to the fill's notional. A fill that crosses zero closes the whole position, then opens the rest at its price.
 *
 * <p>Invariant: {@code cash == realisedPnl - costBasis}. So at any mark M,
 * {@code cash + quantity * M == realisedPnl + unrealisedPnl(M)}: cash plus market value is total PnL, to the cent.
 */
final class Position
{
    final int firmId;
    final int symbolId;
    final int shardId;

    long quantity;
    long costBasis;
    long realisedPnl;
    /** Cumulative trade settlement: + sell notional, - buy notional. */
    long cash;

    // the current trading day (reset by closeDay)
    long dayOpenQty;
    /** Market value at the previous official close (0 for a position opened today). */
    long dayOpenMarketValue;
    long dayBoughtQty;
    long daySoldQty;
    long dayBuyNotional;
    long daySellNotional;
    long dayRealisedPnl;
    int dayTradeCount;

    long lastEventSeq;
    boolean persisted;
    boolean dirty;

    Position(final int firmId, final int symbolId, final int shardId)
    {
        this.firmId = firmId;
        this.symbolId = symbolId;
        this.shardId = shardId;
    }

    static long key(final int firmId, final int symbolId)
    {
        return ((long)firmId << 32) | (symbolId & 0xFFFF_FFFFL);
    }

    /** Applies one fill of this firm: {@code qty} shares bought ({@code buy}) or sold at {@code price}. */
    void fill(final boolean buy, final long price, final long qty)
    {
        if (qty <= 0)
        {
            throw new IllegalArgumentException("fill quantity must be positive: " + qty);
        }
        final long notional = Math.multiplyExact(price, qty);
        if (buy)
        {
            dayBoughtQty = Math.addExact(dayBoughtQty, qty);
            dayBuyNotional = Math.addExact(dayBuyNotional, notional);
            cash = Math.subtractExact(cash, notional);
        }
        else
        {
            daySoldQty = Math.addExact(daySoldQty, qty);
            daySellNotional = Math.addExact(daySellNotional, notional);
            cash = Math.addExact(cash, notional);
        }
        dayTradeCount++;

        long rest = qty;
        if (quantity != 0 && (quantity > 0) != buy)
        {
            final long open = Math.abs(quantity);
            final long closed = Math.min(rest, open);
            final long removed = closed == open ? costBasis : mulDiv(costBasis, closed, open);
            final long closedNotional = Math.multiplyExact(price, closed);
            final long realised = Math.subtractExact(quantity > 0 ? closedNotional : -closedNotional, removed);
            costBasis = Math.subtractExact(costBasis, removed);
            quantity = buy ? quantity + closed : quantity - closed;
            realisedPnl = Math.addExact(realisedPnl, realised);
            dayRealisedPnl = Math.addExact(dayRealisedPnl, realised);
            rest -= closed;
        }
        if (rest > 0)
        {
            final long opened = Math.multiplyExact(price, rest);
            quantity = buy ? Math.addExact(quantity, rest) : Math.subtractExact(quantity, rest);
            costBasis = buy ? Math.addExact(costBasis, opened) : Math.subtractExact(costBasis, opened);
        }
        dirty = true;
    }

    /**
     * Market value at {@code mark}; a position without a mark (no price was ever established for the symbol) is
     * valued at cost.
     */
    long marketValue(final long mark)
    {
        return mark == Ledger.NULL_PRICE ? costBasis : Math.multiplyExact(quantity, mark);
    }

    long unrealisedPnl(final long mark)
    {
        return Math.subtractExact(marketValue(mark), costBasis);
    }

    /** End of a trading day at the official close {@code mark}: the day's statement line; resets the day. */
    DailyPosition closeDay(final int tradingDate, final long mark, final String markSource)
    {
        final long marketValue = marketValue(mark);
        final DailyPosition d = new DailyPosition(tradingDate, firmId, symbolId, shardId, dayOpenQty, dayBoughtQty,
            daySoldQty, quantity, dayBuyNotional, daySellNotional, costBasis, mark, markSource, marketValue,
            Math.subtractExact(marketValue, costBasis), dayRealisedPnl, realisedPnl,
            Math.addExact(Math.subtractExact(daySellNotional, dayBuyNotional),
                Math.subtractExact(marketValue, dayOpenMarketValue)),
            Math.subtractExact(daySellNotional, dayBuyNotional), cash, dayTradeCount);
        dayOpenQty = quantity;
        dayOpenMarketValue = marketValue;
        dayBoughtQty = 0;
        daySoldQty = 0;
        dayBuyNotional = 0;
        daySellNotional = 0;
        dayRealisedPnl = 0;
        dayTradeCount = 0;
        dirty = true;
        return d;
    }

    /** a * b / c truncated toward zero, exact for any long inputs whose result fits a long. */
    static long mulDiv(final long a, final long b, final long c)
    {
        final long hi = Math.multiplyHigh(a, b);
        final long lo = a * b;
        if ((hi == 0 && lo >= 0) || (hi == -1 && lo < 0))
        {
            return lo / c;
        }
        return BigInteger.valueOf(a).multiply(BigInteger.valueOf(b)).divide(BigInteger.valueOf(c)).longValueExact();
    }

    /** One position's line of a daily statement (a row of {@code ledger_daily_position}). */
    record DailyPosition(
        int tradingDate, int firmId, int symbolId, int shardId,
        long openQty, long boughtQty, long soldQty, long closeQty, long buyNotional, long sellNotional,
        long costBasis, long closePrice, String closeSource, long marketValue, long unrealisedPnl,
        long realisedPnlDay, long realisedPnlTotal, long pnlDay, long cashFlowDay, long cashTotal, int tradeCount)
    {
    }
}
