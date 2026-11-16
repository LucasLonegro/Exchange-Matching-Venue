package io.lmb.marketdata.wire;

import io.lmb.marketdata.sbe.InstrumentDefinitionEncoder;
import io.lmb.marketdata.sbe.LevelUpdateEncoder;
import io.lmb.marketdata.sbe.MessageHeaderDecoder;
import io.lmb.marketdata.sbe.SeqHeaderDecoder;
import io.lmb.marketdata.sbe.SnapshotBeginEncoder;
import io.lmb.marketdata.sbe.SymbolStatusEncoder;
import io.lmb.marketdata.sbe.TradePrintEncoder;
import io.lmb.marketdata.sbe.AuctionIndicativeEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/**
 * Framing constants and allocation-free accessors for the market data wire protocol
 * ({@code src/main/resources/sbe/marketdata.xml}).
 *
 * <pre>
 * datagram = PacketHeader(24) frame*
 * frame    = uint16le frameLength, SBE message (messageHeader + body) of frameLength bytes
 * </pre>
 *
 * PacketHeader layout (hand-coded, matches the PacketHeader composite in the schema):
 * sessionId u32 @0, feedId u8 @4, channel u8 @5, messageCount u16 @6, packetSeq i64 @8, sendTimeNs i64 @16.
 */
public final class MdWire
{
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    public static final int SCHEMA_ID = 2;

    public static final int PACKET_HEADER_LENGTH = 24;
    public static final int SESSION_ID_OFFSET = 0;
    public static final int FEED_ID_OFFSET = 4;
    public static final int CHANNEL_OFFSET = 5;
    public static final int MESSAGE_COUNT_OFFSET = 6;
    public static final int PACKET_SEQ_OFFSET = 8;
    public static final int SEND_TIME_OFFSET = 16;

    public static final int FRAME_PREFIX_LENGTH = 2;
    public static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;

    public static final int CHANNEL_INCREMENTAL = 1;
    public static final int CHANNEL_SNAPSHOT = 2;

    public static final int FEED_A = 0;
    public static final int FEED_B = 1;

    /** SeqHeader.flags bit 0: last message of this symbol for one log entry. */
    public static final int FLAG_END_OF_BATCH = 1;

    /** Template ids 1..9 are sequenced messages starting with SeqHeader. */
    public static final int MAX_SEQUENCED_TEMPLATE_ID = 9;

    /** Largest sequenced message (messageHeader + body). Used to size retransmission slots. */
    public static final int MAX_SEQUENCED_LENGTH = HEADER_LENGTH + Math.max(
        Math.max(LevelUpdateEncoder.BLOCK_LENGTH, TradePrintEncoder.BLOCK_LENGTH),
        Math.max(Math.max(SymbolStatusEncoder.BLOCK_LENGTH, AuctionIndicativeEncoder.BLOCK_LENGTH),
            InstrumentDefinitionEncoder.BLOCK_LENGTH));

    public static final int SNAPSHOT_BEGIN_LENGTH = HEADER_LENGTH + SnapshotBeginEncoder.BLOCK_LENGTH;

    private static final int SEQ_OFFSET = HEADER_LENGTH + SeqHeaderDecoder.seqEncodingOffset();
    private static final int TIME_OFFSET = HEADER_LENGTH + SeqHeaderDecoder.transactTimeNsEncodingOffset();
    private static final int SYMBOL_OFFSET = HEADER_LENGTH + SeqHeaderDecoder.symbolIdEncodingOffset();
    private static final int FLAGS_OFFSET = HEADER_LENGTH + SeqHeaderDecoder.flagsEncodingOffset();

    private MdWire()
    {
    }

    // ---- packet header ----

    public static void writePacketHeader(
        final MutableDirectBuffer b, final int o, final long sessionId, final int feedId, final int channel,
        final int messageCount, final long packetSeq, final long sendTimeNs)
    {
        b.putInt(o + SESSION_ID_OFFSET, (int)sessionId, ORDER);
        b.putByte(o + FEED_ID_OFFSET, (byte)feedId);
        b.putByte(o + CHANNEL_OFFSET, (byte)channel);
        b.putShort(o + MESSAGE_COUNT_OFFSET, (short)messageCount, ORDER);
        b.putLong(o + PACKET_SEQ_OFFSET, packetSeq, ORDER);
        b.putLong(o + SEND_TIME_OFFSET, sendTimeNs, ORDER);
    }

    public static long sessionId(final DirectBuffer b, final int packetOffset)
    {
        return b.getInt(packetOffset + SESSION_ID_OFFSET, ORDER) & 0xFFFF_FFFFL;
    }

    public static int feedId(final DirectBuffer b, final int packetOffset)
    {
        return b.getByte(packetOffset + FEED_ID_OFFSET) & 0xFF;
    }

    public static int channel(final DirectBuffer b, final int packetOffset)
    {
        return b.getByte(packetOffset + CHANNEL_OFFSET) & 0xFF;
    }

    public static int messageCount(final DirectBuffer b, final int packetOffset)
    {
        return b.getShort(packetOffset + MESSAGE_COUNT_OFFSET, ORDER) & 0xFFFF;
    }

    public static long packetSeq(final DirectBuffer b, final int packetOffset)
    {
        return b.getLong(packetOffset + PACKET_SEQ_OFFSET, ORDER);
    }

    public static long sendTimeNs(final DirectBuffer b, final int packetOffset)
    {
        return b.getLong(packetOffset + SEND_TIME_OFFSET, ORDER);
    }

    // ---- frames ----

    public static int frameLength(final DirectBuffer b, final int frameOffset)
    {
        return b.getShort(frameOffset, ORDER) & 0xFFFF;
    }

    public static void putFrameLength(final MutableDirectBuffer b, final int frameOffset, final int length)
    {
        b.putShort(frameOffset, (short)length, ORDER);
    }

    // ---- message accessors; offset points at the SBE messageHeader ----

    public static int templateId(final DirectBuffer b, final int msgOffset)
    {
        return b.getShort(msgOffset + MessageHeaderDecoder.templateIdEncodingOffset(), ORDER) & 0xFFFF;
    }

    public static int blockLength(final DirectBuffer b, final int msgOffset)
    {
        return b.getShort(msgOffset + MessageHeaderDecoder.blockLengthEncodingOffset(), ORDER) & 0xFFFF;
    }

    public static int version(final DirectBuffer b, final int msgOffset)
    {
        return b.getShort(msgOffset + MessageHeaderDecoder.versionEncodingOffset(), ORDER) & 0xFFFF;
    }

    public static int schemaId(final DirectBuffer b, final int msgOffset)
    {
        return b.getShort(msgOffset + MessageHeaderDecoder.schemaIdEncodingOffset(), ORDER) & 0xFFFF;
    }

    public static boolean isSequenced(final int templateId)
    {
        return templateId >= 1 && templateId <= MAX_SEQUENCED_TEMPLATE_ID;
    }

    public static long seq(final DirectBuffer b, final int msgOffset)
    {
        return b.getLong(msgOffset + SEQ_OFFSET, ORDER);
    }

    public static long transactTimeNs(final DirectBuffer b, final int msgOffset)
    {
        return b.getLong(msgOffset + TIME_OFFSET, ORDER);
    }

    public static int symbolId(final DirectBuffer b, final int msgOffset)
    {
        return b.getInt(msgOffset + SYMBOL_OFFSET, ORDER);
    }

    public static int flags(final DirectBuffer b, final int msgOffset)
    {
        return b.getByte(msgOffset + FLAGS_OFFSET) & 0xFF;
    }

    public static void setFlags(final MutableDirectBuffer b, final int msgOffset, final int flags)
    {
        b.putByte(msgOffset + FLAGS_OFFSET, (byte)flags);
    }

    /** Fixed tickers are NUL padded; returns the trimmed string. Allocates. */
    public static String ticker(final byte[] raw)
    {
        int n = raw.length;
        while (n > 0 && (raw[n - 1] == 0 || raw[n - 1] == ' '))
        {
            n--;
        }
        return new String(raw, 0, n, java.nio.charset.StandardCharsets.US_ASCII);
    }
}
