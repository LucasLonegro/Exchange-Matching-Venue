package io.lmb.feeds;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/**
 * Wire format of the simulated external venues (one UDP datagram = one packet, little endian).
 * Modelled on MoldUDP64: a packet carries {@code count} consecutive messages starting at sequence
 * {@code firstSeq}; a packet with {@code count = 0} is a heartbeat whose {@code firstSeq} is the next
 * sequence number the venue will assign (so a heartbeat also reveals loss at the tail of the stream).
 *
 * <pre>
 * Packet header (32 bytes)
 *   0  u16  magic      0x4C46
 *   2  u8   version    1
 *   3  u8   line       0 = A, 1 = B (informational; the receiving socket identifies the line)
 *   4  u16  venueId
 *   6  u16  count      messages in this packet, 0..28
 *   8  i64  session    feed session id (venue start time, epoch ns); a larger value means the venue restarted
 *  16  i64  firstSeq   sequence of the first message (sequences start at 1 per session)
 *  24  i64  sendTsNs   venue clock when the packet was emitted
 * Message (48 bytes), repeated count times
 *   0  u32  symbolId
 *   4  u16  type       1 = QUOTE (others are skipped by receivers)
 *   6  u16  flags      0
 *   8  i64  venueTsNs  venue clock when the quote was made
 *  16  i64  bidPrice   cents, Long.MIN_VALUE = no bid
 *  24  i64  bidSize    shares
 *  32  i64  askPrice   cents, Long.MIN_VALUE = no ask
 *  40  i64  askSize    shares
 * </pre>
 */
public final class SimFeedProtocol
{
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    public static final int MAGIC = 0x4C46;
    public static final int VERSION = 1;

    public static final int HEADER_LENGTH = 32;
    public static final int MESSAGE_LENGTH = 48;
    public static final int MAX_MESSAGES_PER_PACKET = 28;
    public static final int MAX_PACKET_LENGTH = HEADER_LENGTH + MAX_MESSAGES_PER_PACKET * MESSAGE_LENGTH;

    public static final int MSG_QUOTE = 1;

    public static final int LINE_A = 0;
    public static final int LINE_B = 1;

    // header offsets
    static final int MAGIC_OFFSET = 0;
    static final int VERSION_OFFSET = 2;
    static final int LINE_OFFSET = 3;
    static final int VENUE_OFFSET = 4;
    static final int COUNT_OFFSET = 6;
    static final int SESSION_OFFSET = 8;
    static final int FIRST_SEQ_OFFSET = 16;
    static final int SEND_TS_OFFSET = 24;

    // message offsets
    static final int M_SYMBOL = 0;
    static final int M_TYPE = 4;
    static final int M_FLAGS = 6;
    static final int M_VENUE_TS = 8;
    static final int M_BID_PX = 16;
    static final int M_BID_SZ = 24;
    static final int M_ASK_PX = 32;
    static final int M_ASK_SZ = 40;

    private SimFeedProtocol()
    {
    }

    public static void writeHeader(
        final MutableDirectBuffer buffer,
        final int offset,
        final int line,
        final int venueId,
        final int count,
        final long session,
        final long firstSeq,
        final long sendTsNs)
    {
        buffer.putShort(offset + MAGIC_OFFSET, (short)MAGIC, ORDER);
        buffer.putByte(offset + VERSION_OFFSET, (byte)VERSION);
        buffer.putByte(offset + LINE_OFFSET, (byte)line);
        buffer.putShort(offset + VENUE_OFFSET, (short)venueId, ORDER);
        buffer.putShort(offset + COUNT_OFFSET, (short)count, ORDER);
        buffer.putLong(offset + SESSION_OFFSET, session, ORDER);
        buffer.putLong(offset + FIRST_SEQ_OFFSET, firstSeq, ORDER);
        buffer.putLong(offset + SEND_TS_OFFSET, sendTsNs, ORDER);
    }

    /** Writes message {@code index} of the packet at {@code packetOffset}. */
    public static void writeQuote(
        final MutableDirectBuffer buffer,
        final int packetOffset,
        final int index,
        final int symbolId,
        final long venueTsNs,
        final long bidPrice,
        final long bidSize,
        final long askPrice,
        final long askSize)
    {
        final int o = packetOffset + HEADER_LENGTH + index * MESSAGE_LENGTH;
        buffer.putInt(o + M_SYMBOL, symbolId, ORDER);
        buffer.putShort(o + M_TYPE, (short)MSG_QUOTE, ORDER);
        buffer.putShort(o + M_FLAGS, (short)0, ORDER);
        buffer.putLong(o + M_VENUE_TS, venueTsNs, ORDER);
        buffer.putLong(o + M_BID_PX, bidPrice, ORDER);
        buffer.putLong(o + M_BID_SZ, bidSize, ORDER);
        buffer.putLong(o + M_ASK_PX, askPrice, ORDER);
        buffer.putLong(o + M_ASK_SZ, askSize, ORDER);
    }

    public static int packetLength(final int count)
    {
        return HEADER_LENGTH + count * MESSAGE_LENGTH;
    }

    /** Structural check: magic, version, and a length that matches the message count. */
    public static boolean isValid(final DirectBuffer buffer, final int offset, final int length)
    {
        if (length < HEADER_LENGTH || length > MAX_PACKET_LENGTH)
        {
            return false;
        }
        if ((buffer.getShort(offset + MAGIC_OFFSET, ORDER) & 0xFFFF) != MAGIC ||
            buffer.getByte(offset + VERSION_OFFSET) != VERSION)
        {
            return false;
        }
        final int count = count(buffer, offset);
        return count <= MAX_MESSAGES_PER_PACKET && length == packetLength(count) && firstSeq(buffer, offset) > 0;
    }

    public static int line(final DirectBuffer buffer, final int offset)
    {
        return buffer.getByte(offset + LINE_OFFSET) & 0xFF;
    }

    public static int venueId(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(offset + VENUE_OFFSET, ORDER) & 0xFFFF;
    }

    public static int count(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(offset + COUNT_OFFSET, ORDER) & 0xFFFF;
    }

    public static long session(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + SESSION_OFFSET, ORDER);
    }

    public static long firstSeq(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + FIRST_SEQ_OFFSET, ORDER);
    }

    public static long sendTsNs(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + SEND_TS_OFFSET, ORDER);
    }

    /** Offset of message {@code index} in the packet at {@code packetOffset}. */
    public static int messageOffset(final int packetOffset, final int index)
    {
        return packetOffset + HEADER_LENGTH + index * MESSAGE_LENGTH;
    }

    public static int msgSymbolId(final DirectBuffer buffer, final int messageOffset)
    {
        return buffer.getInt(messageOffset + M_SYMBOL, ORDER);
    }

    public static int msgType(final DirectBuffer buffer, final int messageOffset)
    {
        return buffer.getShort(messageOffset + M_TYPE, ORDER) & 0xFFFF;
    }

    public static long msgVenueTsNs(final DirectBuffer buffer, final int messageOffset)
    {
        return buffer.getLong(messageOffset + M_VENUE_TS, ORDER);
    }

    public static long msgBidPrice(final DirectBuffer buffer, final int messageOffset)
    {
        return buffer.getLong(messageOffset + M_BID_PX, ORDER);
    }

    public static long msgBidSize(final DirectBuffer buffer, final int messageOffset)
    {
        return buffer.getLong(messageOffset + M_BID_SZ, ORDER);
    }

    public static long msgAskPrice(final DirectBuffer buffer, final int messageOffset)
    {
        return buffer.getLong(messageOffset + M_ASK_PX, ORDER);
    }

    public static long msgAskSize(final DirectBuffer buffer, final int messageOffset)
    {
        return buffer.getLong(messageOffset + M_ASK_SZ, ORDER);
    }
}
