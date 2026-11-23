package io.lmb.feeds;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static io.lmb.feeds.SimFeedProtocol.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimFeedProtocolTest
{
    @Test
    void roundTripsHeaderAndQuotes()
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[8 + MAX_PACKET_LENGTH]);
        final int off = 8;
        writeHeader(b, off, LINE_B, 9, 2, 123L, 77L, 999L);
        writeQuote(b, off, 0, 5, 1000L, 18_999L, 300, NormalizedQuote.NO_PRICE, 0);
        writeQuote(b, off, 1, 0xFFFF_FFF0, 1001L, 1L, 2, 3L, 4);

        assertTrue(isValid(b, off, packetLength(2)));
        assertEquals(LINE_B, line(b, off));
        assertEquals(9, venueId(b, off));
        assertEquals(2, count(b, off));
        assertEquals(123L, session(b, off));
        assertEquals(77L, firstSeq(b, off));
        assertEquals(999L, sendTsNs(b, off));

        final int m0 = messageOffset(off, 0);
        assertEquals(5, msgSymbolId(b, m0));
        assertEquals(MSG_QUOTE, msgType(b, m0));
        assertEquals(1000L, msgVenueTsNs(b, m0));
        assertEquals(18_999L, msgBidPrice(b, m0));
        assertEquals(300L, msgBidSize(b, m0));
        assertEquals(NormalizedQuote.NO_PRICE, msgAskPrice(b, m0));
        assertEquals(0L, msgAskSize(b, m0));
        final int m1 = messageOffset(off, 1);
        assertEquals(0xFFFF_FFF0, msgSymbolId(b, m1));
        assertEquals(4L, msgAskSize(b, m1));
    }

    @Test
    void layoutIsLittleEndianAtTheDocumentedOffsets()
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[packetLength(1)]);
        writeHeader(b, 0, LINE_A, 3, 1, 0x0102030405060708L, 1, 0);
        assertEquals(0x46, b.getByte(0));
        assertEquals(0x4C, b.getByte(1));
        assertEquals(1, b.getByte(2));
        assertEquals(3, b.getByte(4));
        assertEquals(0x08, b.getByte(8));
        assertEquals(80, packetLength(1));
    }

    @Test
    void largestPacketFitsOneEthernetFrame()
    {
        assertEquals(32 + 28 * 48, MAX_PACKET_LENGTH);
        assertTrue(MAX_PACKET_LENGTH <= 1472);
    }

    @Test
    void rejectsStructurallyInvalidPackets()
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[MAX_PACKET_LENGTH + 48]);
        writeHeader(b, 0, LINE_A, 0, 1, 1, 1, 1);
        assertTrue(isValid(b, 0, packetLength(1)));
        assertFalse(isValid(b, 0, packetLength(1) + 1), "length does not match count");
        assertFalse(isValid(b, 0, HEADER_LENGTH - 1), "shorter than a header");

        writeHeader(b, 0, LINE_A, 0, 0, 1, 0, 1);
        assertFalse(isValid(b, 0, HEADER_LENGTH), "firstSeq must be positive");

        writeHeader(b, 0, LINE_A, 0, 29, 1, 1, 1);
        assertFalse(isValid(b, 0, packetLength(29)), "too many messages");

        writeHeader(b, 0, LINE_A, 0, 0, 1, 1, 1);
        b.putByte(2, (byte)2);
        assertFalse(isValid(b, 0, HEADER_LENGTH), "version");
        writeHeader(b, 0, LINE_A, 0, 0, 1, 1, 1);
        b.putByte(0, (byte)0);
        assertFalse(isValid(b, 0, HEADER_LENGTH), "magic");
    }
}
