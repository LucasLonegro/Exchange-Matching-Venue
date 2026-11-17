package io.lmb.matcher;

/** Head/tail of one intrusive list threaded through {@link Order#prev}/{@link Order#next} slot k. */
final class OrderList
{
    final int k;
    Order head;
    Order tail;
    int size;

    OrderList(final int k)
    {
        this.k = k;
    }

    void append(final Order o)
    {
        o.prev[k] = tail;
        o.next[k] = null;
        if (tail == null)
        {
            head = o;
        }
        else
        {
            tail.next[k] = o;
        }
        tail = o;
        size++;
    }

    /** Inserts keeping the list sorted by orderId (scans from the tail; used only on session transfer). */
    void insertByOrderId(final Order o)
    {
        Order c = tail;
        while (c != null && c.orderId > o.orderId)
        {
            c = c.prev[k];
        }
        if (c == null)
        {
            o.prev[k] = null;
            o.next[k] = head;
            if (head != null)
            {
                head.prev[k] = o;
            }
            else
            {
                tail = o;
            }
            head = o;
        }
        else
        {
            final Order n = c.next[k];
            o.prev[k] = c;
            o.next[k] = n;
            c.next[k] = o;
            if (n == null)
            {
                tail = o;
            }
            else
            {
                n.prev[k] = o;
            }
        }
        size++;
    }

    void remove(final Order o)
    {
        final Order p = o.prev[k];
        final Order n = o.next[k];
        if (p == null)
        {
            head = n;
        }
        else
        {
            p.next[k] = n;
        }
        if (n == null)
        {
            tail = p;
        }
        else
        {
            n.prev[k] = p;
        }
        o.prev[k] = null;
        o.next[k] = null;
        size--;
    }

    void clear()
    {
        head = null;
        tail = null;
        size = 0;
    }
}
