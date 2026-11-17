package io.lmb.matcher;

import java.util.Arrays;

/**
 * Open-addressing hash map from a (long, long) key to an int, with entries stored densely in insertion
 * order. Entries are never removed individually (only {@link #clear()}), so the entry index is a stable
 * handle and iteration order is the insertion order, independent of capacity. That makes snapshots of
 * it canonical. Allocation-free except when growing.
 */
final class LongPairIntMap
{
    static final int MISSING = Integer.MIN_VALUE;

    private long[] keyA;
    private long[] keyB;
    private int[] values;
    private int size;
    private int[] slots;
    private int mask;

    LongPairIntMap(final int initialCapacity)
    {
        final int cap = Math.max(16, Integer.highestOneBit(Math.max(16, initialCapacity) - 1) << 1);
        keyA = new long[cap / 2];
        keyB = new long[cap / 2];
        values = new int[cap / 2];
        slots = new int[cap];
        mask = cap - 1;
    }

    private static int hash(final long a, final long b)
    {
        long h = a * 0x9E37_79B9_7F4A_7C15L + b;
        h ^= h >>> 33;
        h *= 0xFF51_AFD7_ED55_8CCDL;
        h ^= h >>> 33;
        h *= 0xC4CE_B9FE_1A85_EC53L;
        h ^= h >>> 33;
        return (int)h;
    }

    /** Entry index, or -1. */
    int find(final long a, final long b)
    {
        int i = hash(a, b) & mask;
        while (true)
        {
            final int e = slots[i];
            if (e == 0)
            {
                return -1;
            }
            if (keyA[e - 1] == a && keyB[e - 1] == b)
            {
                return e - 1;
            }
            i = (i + 1) & mask;
        }
    }

    int get(final long a, final long b)
    {
        final int e = find(a, b);
        return e < 0 ? MISSING : values[e];
    }

    /** Inserts a key that is known to be absent. Returns its entry index. */
    int insert(final long a, final long b, final int value)
    {
        if ((size + 1) * 2 > slots.length)
        {
            grow();
        }
        if (size == keyA.length)
        {
            final int n = keyA.length * 2;
            keyA = Arrays.copyOf(keyA, n);
            keyB = Arrays.copyOf(keyB, n);
            values = Arrays.copyOf(values, n);
        }
        final int e = size++;
        keyA[e] = a;
        keyB[e] = b;
        values[e] = value;
        place(e);
        return e;
    }

    private void place(final int e)
    {
        int i = hash(keyA[e], keyB[e]) & mask;
        while (slots[i] != 0)
        {
            i = (i + 1) & mask;
        }
        slots[i] = e + 1;
    }

    private void grow()
    {
        slots = new int[slots.length * 2];
        mask = slots.length - 1;
        for (int e = 0; e < size; e++)
        {
            place(e);
        }
    }

    int value(final int entry)
    {
        return values[entry];
    }

    void setValue(final int entry, final int value)
    {
        values[entry] = value;
    }

    long keyA(final int entry)
    {
        return keyA[entry];
    }

    long keyB(final int entry)
    {
        return keyB[entry];
    }

    int size()
    {
        return size;
    }

    void clear()
    {
        Arrays.fill(slots, 0);
        size = 0;
    }
}
