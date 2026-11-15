package io.lmb.common;

import io.lmb.sbe.EventHeaderDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;

/**
 * Allocation-free accessors for the fields common to every event message. The offset argument
 * always points at the SBE messageHeader. Every event body starts with the EventHeader.
 */
public final class Events
{
    public static final int FIRST_EVENT_TEMPLATE_ID = 100;
    private static final int BODY = MessageHeaderDecoder.ENCODED_LENGTH;

    private Events()
    {
    }

    public static int templateId(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(offset + MessageHeaderDecoder.templateIdEncodingOffset(), ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
    }

    public static boolean isEvent(final DirectBuffer buffer, final int offset)
    {
        return templateId(buffer, offset) >= FIRST_EVENT_TEMPLATE_ID;
    }

    public static long eventSeq(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + BODY + EventHeaderDecoder.eventSeqEncodingOffset(), ByteOrder.LITTLE_ENDIAN);
    }

    public static long logPosition(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + BODY + EventHeaderDecoder.logPositionEncodingOffset(), ByteOrder.LITTLE_ENDIAN);
    }

    public static long timestampNs(final DirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + BODY + EventHeaderDecoder.timestampNsEncodingOffset(), ByteOrder.LITTLE_ENDIAN);
    }

    public static long eventIdx(final DirectBuffer buffer, final int offset)
    {
        return buffer.getInt(offset + BODY + EventHeaderDecoder.eventIdxEncodingOffset(), ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL;
    }

    public static int shardId(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(offset + BODY + EventHeaderDecoder.shardIdEncodingOffset(), ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
    }

    /** Execution id as shown to clients: "shard-logPosition-eventIdx". Allocates; not for the hot path. */
    public static String execId(final DirectBuffer buffer, final int offset)
    {
        return shardId(buffer, offset) + "-" + logPosition(buffer, offset) + "-" + eventIdx(buffer, offset);
    }
}
