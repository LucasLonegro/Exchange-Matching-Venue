package io.lmb.matcher;

import org.agrona.collections.Long2ObjectHashMap;

import java.util.Arrays;

/**
 * One side of one symbol's book.
 *
 * <p>Two structures over the same levels:
 * <ul>
 * <li>A tick-indexed lookup: a direct-mapped window of {@code windowSize} ticks centred on the
 * reference price, plus an overflow hash map for prices outside the window. Lookup by price is O(1)
 * wherever the price is; the window is re-centred when the reference price drifts out of its middle
 * half, so it keeps working however far the price moves.</li>
 * <li>A sorted array of the non-empty ("attached") levels, worst first and best last, so the best level
 * is O(1), removing it is O(1), and inserting near the top of the book shifts only a few entries.</li>
 * </ul>
 * Levels that became empty during the current step stay in the lookup (detached from the sorted array)
 * until the step's BookDeltas are flushed, so a level emptied and refilled in one step is one level.
 */
final class BookSide
{
    final Symbol symbol;
    final boolean buy;
    final byte sideCode;

    PriceLevel[] sorted = new PriceLevel[16];
    int size;

    private final PriceLevel[] window;
    private final int windowSize;
    private long base;
    private boolean centred;
    long indexTick = 1;
    private final Long2ObjectHashMap<PriceLevel> overflow = new Long2ObjectHashMap<>();

    BookSide(final Symbol symbol, final boolean buy, final int windowSize)
    {
        this.symbol = symbol;
        this.buy = buy;
        this.sideCode = buy ? Codes.BUY : Codes.SELL;
        this.windowSize = windowSize;
        this.window = new PriceLevel[windowSize];
    }

    PriceLevel best()
    {
        return size == 0 ? null : sorted[size - 1];
    }

    /** True if price a has strictly higher priority than price b on this side. */
    boolean better(final long a, final long b)
    {
        return buy ? a > b : a < b;
    }

    PriceLevel find(final long price)
    {
        final long idx = price / indexTick;
        if (centred)
        {
            final long off = idx - base;
            if (off >= 0 && off < windowSize)
            {
                return window[(int)off];
            }
        }
        return overflow.get(idx);
    }

    void indexPut(final PriceLevel l)
    {
        final long idx = l.price / indexTick;
        if (!centred)
        {
            centre(idx);
        }
        final long off = idx - base;
        if (off >= 0 && off < windowSize)
        {
            window[(int)off] = l;
        }
        else
        {
            overflow.put(idx, l);
        }
    }

    void indexRemove(final PriceLevel l)
    {
        final long idx = l.price / indexTick;
        final long off = idx - base;
        if (centred && off >= 0 && off < windowSize)
        {
            window[(int)off] = null;
        }
        else
        {
            overflow.remove(idx);
        }
    }

    void attach(final PriceLevel l)
    {
        int lo = 0;
        int hi = size;
        final long p = l.price;
        while (lo < hi)
        {
            final int mid = (lo + hi) >>> 1;
            if (better(p, sorted[mid].price))
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }
        if (size == sorted.length)
        {
            sorted = Arrays.copyOf(sorted, size * 2);
        }
        System.arraycopy(sorted, lo, sorted, lo + 1, size - lo);
        sorted[lo] = l;
        size++;
        l.attached = true;
    }

    void detach(final PriceLevel l)
    {
        int pos;
        if (sorted[size - 1] == l)
        {
            pos = size - 1;
        }
        else
        {
            int lo = 0;
            int hi = size - 1;
            pos = -1;
            final long p = l.price;
            while (lo <= hi)
            {
                final int mid = (lo + hi) >>> 1;
                final long mp = sorted[mid].price;
                if (mp == p)
                {
                    pos = mid;
                    break;
                }
                if (better(p, mp))
                {
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }
            if (pos < 0)
            {
                throw new IllegalStateException("level not in sorted array: " + p);
            }
        }
        System.arraycopy(sorted, pos + 1, sorted, pos, size - pos - 1);
        sorted[--size] = null;
        l.attached = false;
    }

    /** Re-centre the window if the reference price left its middle half. Only between steps. */
    void maybeRecentre(final long referencePrice)
    {
        final long idx = referencePrice / indexTick;
        final long off = idx - base;
        if (!centred || off < windowSize / 4 || off >= windowSize - windowSize / 4)
        {
            centre(idx);
        }
    }

    /** Changes the index granularity (tick size change) and rebuilds. Only between steps. */
    void reindex(final long newIndexTick, final long centrePrice)
    {
        if (newIndexTick == indexTick)
        {
            return;
        }
        indexTick = newIndexTick;
        centre(centrePrice == Codes.NULL_PRICE ? (size > 0 ? sorted[size - 1].price / indexTick : 0) : centrePrice / indexTick);
    }

    private void centre(final long centreIdx)
    {
        base = centreIdx - windowSize / 2;
        centred = true;
        Arrays.fill(window, null);
        overflow.clear();
        for (int i = 0; i < size; i++)
        {
            final PriceLevel l = sorted[i];
            final long off = l.price / indexTick - base;
            if (off >= 0 && off < windowSize)
            {
                window[(int)off] = l;
            }
            else
            {
                overflow.put(l.price / indexTick, l);
            }
        }
    }

    long totalQty()
    {
        long t = 0;
        for (int i = 0; i < size; i++)
        {
            t += sorted[i].totalQty;
        }
        return t;
    }

    void clearAll()
    {
        Arrays.fill(sorted, 0, size, null);
        size = 0;
        Arrays.fill(window, null);
        overflow.clear();
        centred = false;
    }
}
