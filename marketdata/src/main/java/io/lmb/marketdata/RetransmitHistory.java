package io.lmb.marketdata;

import io.lmb.marketdata.wire.MdWire;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Bounded in-memory history of sequenced messages per symbol, for TCP retransmission.
 * Written by the publisher thread, read by retransmission connections; each symbol has its own lock,
 * held only for a memory copy.
 *
 * <p>Each symbol holds its last {@code depth} messages in a ring of fixed slots (2-byte length plus the
 * message). Rings start small and double up to {@code depth}, so idle symbols cost little.
 * Worst case memory: symbols x depth x {@link #SLOT_SIZE} bytes.
 */
public final class RetransmitHistory
{
    public static final int SLOT_SIZE = (MdWire.MAX_SEQUENCED_LENGTH + 2 + 7) & ~7;
    private static final int INITIAL_CAPACITY = 64;

    private final int depth;
    private final ConcurrentHashMap<Integer, SymbolHistory> bySymbol = new ConcurrentHashMap<>();

    public RetransmitHistory(final int depth)
    {
        if (depth < 1)
        {
            throw new IllegalArgumentException("depth must be >= 1");
        }
        this.depth = depth;
    }

    public int depth()
    {
        return depth;
    }

    /** The symbol's ring, created on first use. Callers on the hot path should cache the result. */
    public SymbolHistory forSymbol(final int symbolId)
    {
        return bySymbol.computeIfAbsent(symbolId, id -> new SymbolHistory(id, depth));
    }

    /** null if the symbol has never published a message. */
    public SymbolHistory get(final int symbolId)
    {
        return bySymbol.get(symbolId);
    }

    /** Result of {@link SymbolHistory#copyRange}. Reused per caller; not thread-safe. */
    public static final class Range
    {
        public long oldestSeq;
        public long lastSeq;
        public long fromSeq;
        public long toSeq;
        public int count;
        public int bytes;
    }

    public static final class SymbolHistory
    {
        private final int symbolId;
        private final int maxCapacity;
        private byte[] slots;
        private UnsafeBuffer view;
        private int capacity;
        private long oldestSeq; // 0 = empty
        private long lastSeq;

        SymbolHistory(final int symbolId, final int maxCapacity)
        {
            this.symbolId = symbolId;
            this.maxCapacity = maxCapacity;
            this.capacity = Math.min(INITIAL_CAPACITY, maxCapacity);
            this.slots = new byte[capacity * SLOT_SIZE];
            this.view = new UnsafeBuffer(slots);
        }

        public int symbolId()
        {
            return symbolId;
        }

        /** Appends the message with sequence seq. Sequences must arrive in order; a jump restarts the ring. */
        public synchronized void append(final long seq, final DirectBuffer buffer, final int offset, final int length)
        {
            if (length > SLOT_SIZE - 2)
            {
                throw new IllegalArgumentException("message too large for history slot: " + length);
            }
            if (oldestSeq == 0 || seq != lastSeq + 1)
            {
                oldestSeq = seq;
            }
            else if (seq - oldestSeq + 1 > capacity)
            {
                if (capacity < maxCapacity)
                {
                    grow();
                }
                else
                {
                    oldestSeq++;
                }
            }
            lastSeq = seq;
            final int slot = slotOffset(seq);
            view.putShort(slot, (short)length, MdWire.ORDER);
            view.putBytes(slot + 2, buffer, offset, length);
        }

        public synchronized long oldestSeq()
        {
            return oldestSeq;
        }

        public synchronized long lastSeq()
        {
            return lastSeq;
        }

        /**
         * Copies messages fromSeq..min(toSeq, lastSeq, fromSeq + maxCount - 1) into out at outOffset as
         * frames (uint16 length + message). The caller has checked that fromSeq >= oldestSeq; if not, or if
         * nothing is available, count is 0. out must have room for maxCount * SLOT_SIZE bytes.
         */
        public synchronized void copyRange(
            final long fromSeq, final long toSeq, final int maxCount, final MutableDirectBuffer out, final int outOffset,
            final Range range)
        {
            range.oldestSeq = oldestSeq;
            range.lastSeq = lastSeq;
            range.fromSeq = fromSeq;
            range.count = 0;
            range.bytes = 0;
            range.toSeq = fromSeq - 1;
            if (oldestSeq == 0 || fromSeq < oldestSeq || fromSeq > lastSeq)
            {
                return;
            }
            final long last = Math.min(Math.min(toSeq, lastSeq), fromSeq + maxCount - 1);
            int pos = outOffset;
            for (long s = fromSeq; s <= last; s++)
            {
                final int slot = slotOffset(s);
                final int len = view.getShort(slot, MdWire.ORDER) & 0xFFFF;
                out.putShort(pos, (short)len, MdWire.ORDER);
                out.putBytes(pos + 2, view, slot + 2, len);
                pos += 2 + len;
            }
            range.toSeq = last;
            range.count = (int)(last - fromSeq + 1);
            range.bytes = pos - outOffset;
        }

        private int slotOffset(final long seq)
        {
            return (int)(seq % capacity) * SLOT_SIZE;
        }

        private void grow()
        {
            final int newCapacity = Math.min(maxCapacity, capacity * 2);
            final byte[] newSlots = new byte[newCapacity * SLOT_SIZE];
            for (long s = oldestSeq; s <= lastSeq; s++)
            {
                System.arraycopy(slots, (int)(s % capacity) * SLOT_SIZE, newSlots, (int)(s % newCapacity) * SLOT_SIZE, SLOT_SIZE);
            }
            slots = newSlots;
            view = new UnsafeBuffer(newSlots);
            capacity = newCapacity;
        }
    }
}
