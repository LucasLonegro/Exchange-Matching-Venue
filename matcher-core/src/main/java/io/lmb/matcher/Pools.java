package io.lmb.matcher;

import java.util.Arrays;

/** Preallocated order objects; grows (allocates) only when exhausted. */
final class OrderPool
{
    private Order[] byIndex;
    private int created;
    private Order free;
    private int inUse;

    OrderPool(final int initial)
    {
        byIndex = new Order[initial];
        grow(initial);
    }

    private void grow(final int n)
    {
        if (created + n > byIndex.length)
        {
            byIndex = Arrays.copyOf(byIndex, created + n);
        }
        for (int i = 0; i < n; i++)
        {
            final Order o = new Order(created);
            byIndex[created++] = o;
            o.nextFree = free;
            free = o;
        }
    }

    Order acquire()
    {
        if (free == null)
        {
            grow(Math.max(1024, created));
        }
        final Order o = free;
        free = o.nextFree;
        o.nextFree = null;
        inUse++;
        return o;
    }

    void release(final Order o)
    {
        o.firm = null;
        o.symbol = null;
        o.fs = null;
        o.session = null;
        o.level = null;
        o.linked = false;
        o.nextFree = free;
        free = o;
        inUse--;
    }

    Order byIndex(final int i)
    {
        return byIndex[i];
    }

    int inUse()
    {
        return inUse;
    }
}

/** Preallocated price levels; grows only when exhausted. */
final class LevelPool
{
    private PriceLevel free;
    private int inUse;

    LevelPool(final int initial)
    {
        for (int i = 0; i < initial; i++)
        {
            final PriceLevel l = new PriceLevel();
            l.nextFree = free;
            free = l;
        }
    }

    PriceLevel acquire(final BookSide side, final long price)
    {
        PriceLevel l = free;
        if (l == null)
        {
            l = new PriceLevel();
        }
        else
        {
            free = l.nextFree;
            l.nextFree = null;
        }
        l.side = side;
        l.price = price;
        l.totalQty = 0;
        l.attached = false;
        l.dirty = false;
        l.orders.clear();
        inUse++;
        return l;
    }

    void release(final PriceLevel l)
    {
        l.side = null;
        l.nextFree = free;
        free = l;
        inUse--;
    }

    int inUse()
    {
        return inUse;
    }
}
