package io.lmb.cluster.client;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventProtocolTest
{
    private final UnsafeBuffer b = new UnsafeBuffer(new byte[EventProtocol.MAX_MESSAGE_LENGTH]);

    @Test
    void requestRoundTrip()
    {
        final int len = EventProtocol.encodeRequest(b, 0, 11, 22, EventProtocol.LATEST, 7002, 3,
            "10.0.0.1:40001", "10.0.0.1:40002");
        assertTrue(len < EventProtocol.MAX_MESSAGE_LENGTH);
        assertEquals(EventProtocol.REQUEST, EventProtocol.type(b, 0));
        assertEquals(11, EventProtocol.requestCorrelationId(b, 0));
        assertEquals(22, EventProtocol.requestClientId(b, 0));
        assertEquals(-1, EventProtocol.requestAfterEventSeq(b, 0));
        assertEquals(7002, EventProtocol.requestDataStreamId(b, 0));
        assertEquals(3, EventProtocol.requestShardId(b, 0));
        assertEquals("10.0.0.1:40001", EventProtocol.requestResponseEndpoint(b, 0));
        assertEquals("10.0.0.1:40002", EventProtocol.requestDataEndpoint(b, 0));
    }

    @Test
    void responsesRoundTrip()
    {
        EventProtocol.encodeReplayStarted(b, 0, 5, 0x1_0000_0007L, 99, -1, 2, 1, 1, 120);
        assertEquals(EventProtocol.REPLAY_STARTED, EventProtocol.type(b, 0));
        assertEquals(5, EventProtocol.rsCorrelationId(b, 0));
        assertEquals(7, (int)EventProtocol.rsReplaySessionId(b, 0));
        assertEquals(99, EventProtocol.rsResolvedAfterSeq(b, 0));
        assertEquals(-1, EventProtocol.rsEndEventSeq(b, 0));
        assertEquals(2, EventProtocol.rsMemberId(b, 0));
        assertEquals(1, EventProtocol.rsLeaderMemberId(b, 0));
        assertEquals(120, EventProtocol.rsLastSeq(b, 0));

        EventProtocol.encodeUnavailable(b, 0, 6, EventProtocol.REASON_TOO_OLD, 2, 0, 501, 900);
        assertEquals(EventProtocol.UNAVAILABLE, EventProtocol.type(b, 0));
        assertEquals(6, EventProtocol.uaCorrelationId(b, 0));
        assertEquals(EventProtocol.REASON_TOO_OLD, EventProtocol.uaReason(b, 0));
        assertEquals(501, EventProtocol.uaFirstSeq(b, 0));
        assertEquals(900, EventProtocol.uaLastSeq(b, 0));

        EventProtocol.encodeHeartbeat(b, 0, 8, 1, 2, 1, 77);
        assertEquals(EventProtocol.HEARTBEAT, EventProtocol.type(b, 0));
        assertEquals(8, EventProtocol.hbCorrelationId(b, 0));
        assertEquals(2, EventProtocol.hbLeaderMemberId(b, 0));
        assertEquals(77, EventProtocol.hbLastSeq(b, 0));
    }

    @Test
    void sizesParse()
    {
        assertEquals(65536, ClientConfig.parseSize("64k"));
        assertEquals(1 << 20, ClientConfig.parseSize("1m"));
        assertEquals(4096, ClientConfig.parseSize("4096"));
    }
}
