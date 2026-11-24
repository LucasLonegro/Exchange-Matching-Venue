package io.lmb.replayer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * FIFO of event messages copied out of an {@link EventSource} (whose buffers are only valid during the
 * callback). Each message is stored as int32 length + bytes; storage is compacted when drained, so steady
 * state does not allocate.
 */
final class EventQueue
{
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64 * 1024);
    private int head;
    private int tail;
    private int count;

    void add(final DirectBuffer src, final int offset, final int length)
    {
        if (head == tail)
        {
            head = 0;
            tail = 0;
        }
        else if (head > 1024 * 1024 && head > tail / 2)
        {
            buffer.putBytes(0, buffer, head, tail - head);
            tail -= head;
            head = 0;
        }
        buffer.putInt(tail, length);
        buffer.putBytes(tail + 4, src, offset, length);
        tail += 4 + length;
        count++;
    }

    boolean isEmpty()
    {
        return count == 0;
    }

    int size()
    {
        return count;
    }

    MutableDirectBuffer buffer()
    {
        return buffer;
    }

    /** Offset of the head message's bytes (only valid while not empty). */
    int headOffset()
    {
        return head + 4;
    }

    int headLength()
    {
        return buffer.getInt(head);
    }

    void removeHead()
    {
        head += 4 + buffer.getInt(head);
        count--;
    }

    void clear()
    {
        head = 0;
        tail = 0;
        count = 0;
    }
}
