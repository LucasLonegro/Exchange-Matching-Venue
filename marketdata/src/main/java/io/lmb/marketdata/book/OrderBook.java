package io.lmb.marketdata.book;

/**
 * Price-level book for one symbol: per side a sorted array of (price, quantity, orderCount), best first
 * (bids descending, asks ascending). Allocation-free once the arrays have grown to the book's depth.
 * Updates are O(levels) in the worst case (array shift), O(log levels) to find a level; books are a few
 * hundred levels at most, so this beats a tree on cache behaviour. Not thread-safe.
 */
public final class OrderBook
{
    public static final int BID = 0;
    public static final int ASK = 1;

    private final Ladder[] sides = {new Ladder(true), new Ladder(false)};

    /** Side index from the wire value (1 = BUY, 2 = SELL). */
    public static int sideIndex(final int wireSide)
    {
        return wireSide == 1 ? BID : ASK;
    }

    public static int wireSide(final int sideIndex)
    {
        return sideIndex == BID ? 1 : 2;
    }

    /** Sets the level total; quantity <= 0 removes it. */
    public void set(final int side, final long price, final long quantity, final int orderCount)
    {
        sides[side].set(price, quantity, orderCount);
    }

    public void clear()
    {
        sides[BID].size = 0;
        sides[ASK].size = 0;
    }

    public int levels(final int side)
    {
        return sides[side].size;
    }

    public long price(final int side, final int index)
    {
        return sides[side].prices[index];
    }

    public long quantity(final int side, final int index)
    {
        return sides[side].quantities[index];
    }

    public int orderCount(final int side, final int index)
    {
        return sides[side].counts[index];
    }

    /** Quantity at the price, 0 if absent. */
    public long quantityAt(final int side, final long price)
    {
        final Ladder l = sides[side];
        final int i = l.find(price);
        return i >= 0 ? l.quantities[i] : 0;
    }

    public boolean isEmpty()
    {
        return sides[BID].size == 0 && sides[ASK].size == 0;
    }

    public void copyFrom(final OrderBook other)
    {
        for (int s = 0; s < 2; s++)
        {
            final Ladder src = other.sides[s];
            final Ladder dst = sides[s];
            dst.ensure(src.size);
            System.arraycopy(src.prices, 0, dst.prices, 0, src.size);
            System.arraycopy(src.quantities, 0, dst.quantities, 0, src.size);
            System.arraycopy(src.counts, 0, dst.counts, 0, src.size);
            dst.size = src.size;
        }
    }

    public boolean sameLevels(final OrderBook other)
    {
        for (int s = 0; s < 2; s++)
        {
            final Ladder a = sides[s];
            final Ladder b = other.sides[s];
            if (a.size != b.size)
            {
                return false;
            }
            for (int i = 0; i < a.size; i++)
            {
                if (a.prices[i] != b.prices[i] || a.quantities[i] != b.quantities[i] || a.counts[i] != b.counts[i])
                {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("bids[");
        append(sb, sides[BID]);
        sb.append("] asks[");
        append(sb, sides[ASK]);
        return sb.append(']').toString();
    }

    private static void append(final StringBuilder sb, final Ladder l)
    {
        for (int i = 0; i < l.size; i++)
        {
            if (i > 0)
            {
                sb.append(' ');
            }
            sb.append(l.quantities[i]).append('@').append(l.prices[i]).append('/').append(l.counts[i]);
        }
    }

    private static final class Ladder
    {
        private final boolean descending;
        long[] prices = new long[16];
        long[] quantities = new long[16];
        int[] counts = new int[16];
        int size;

        Ladder(final boolean descending)
        {
            this.descending = descending;
        }

        /** Index of the price, or -(insertionPoint) - 1. */
        int find(final long price)
        {
            int lo = 0;
            int hi = size - 1;
            while (lo <= hi)
            {
                final int mid = (lo + hi) >>> 1;
                final long p = prices[mid];
                if (p == price)
                {
                    return mid;
                }
                final boolean before = descending ? p > price : p < price;
                if (before)
                {
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }
            return -(lo + 1);
        }

        void set(final long price, final long quantity, final int orderCount)
        {
            final int i = find(price);
            if (i >= 0)
            {
                if (quantity <= 0)
                {
                    final int tail = size - i - 1;
                    if (tail > 0)
                    {
                        System.arraycopy(prices, i + 1, prices, i, tail);
                        System.arraycopy(quantities, i + 1, quantities, i, tail);
                        System.arraycopy(counts, i + 1, counts, i, tail);
                    }
                    size--;
                }
                else
                {
                    quantities[i] = quantity;
                    counts[i] = orderCount;
                }
            }
            else if (quantity > 0)
            {
                final int at = -(i + 1);
                ensure(size + 1);
                final int tail = size - at;
                if (tail > 0)
                {
                    System.arraycopy(prices, at, prices, at + 1, tail);
                    System.arraycopy(quantities, at, quantities, at + 1, tail);
                    System.arraycopy(counts, at, counts, at + 1, tail);
                }
                prices[at] = price;
                quantities[at] = quantity;
                counts[at] = orderCount;
                size++;
            }
        }

        void ensure(final int capacity)
        {
            if (capacity > prices.length)
            {
                final int n = Math.max(capacity, prices.length * 2);
                prices = java.util.Arrays.copyOf(prices, n);
                quantities = java.util.Arrays.copyOf(quantities, n);
                counts = java.util.Arrays.copyOf(counts, n);
            }
        }
    }
}
