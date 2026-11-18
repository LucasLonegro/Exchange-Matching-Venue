package io.lmb.cluster.client;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.lmb.api.EventStreamClient;
import io.lmb.common.Events;
import io.lmb.common.Topology;
import io.lmb.sbe.EventHeaderDecoder;
import io.lmb.sbe.MessageHeaderEncoder;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The event stream client against scripted fake members (no cluster): dedupe across members, gap
 * detection, TOO_OLD handling, heartbeat loss, LATEST resolution.
 */
@Timeout(60)
class EventStreamClientTest
{
    private static final File DIR = new File("target/client-tests");
    private ClusterVenueClients clients;
    private Topology topology;
    private final List<FakeMember> members = new ArrayList<>();

    @BeforeEach
    void setUp()
    {
        IoUtil.delete(DIR, true);
        final Properties p = new Properties();
        p.setProperty("lmb.shards", "1");
        p.setProperty("lmb.members", "2");
        p.setProperty("lmb.shard.0.member.0.basePort", "32000");
        p.setProperty("lmb.shard.0.member.1.basePort", "32010");
        topology = Topology.of(p);
        final ClientConfig c = new ClientConfig();
        c.embeddedDirBase = DIR.getAbsolutePath();
        c.termLength = 64 * 1024;
        c.eventConnectTimeoutNs = TimeUnit.MILLISECONDS.toNanos(500);
        c.eventHeartbeatTimeoutNs = TimeUnit.MILLISECONDS.toNanos(500);
        c.eventMemberCooldownNs = TimeUnit.MILLISECONDS.toNanos(200);
        c.eventPreferredMember = 0;
        clients = new ClusterVenueClients(topology, "client-test", c);
        for (int m = 0; m < 2; m++)
        {
            members.add(new FakeMember(clients.aeron(), topology, m));
        }
    }

    @AfterEach
    void tearDown()
    {
        members.forEach(FakeMember::close);
        clients.close();
        IoUtil.delete(DIR, true);
    }

    @Test
    void gapOnOneMemberFailsOverAndDuplicatesAreDropped()
    {
        members.get(0).script = after -> new long[]{ 1, 2, 3, 4, 5, 8, 9 };   // hole at 6..7
        members.get(1).script = after -> new long[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        final List<Long> got = new ArrayList<>();
        try (EventStreamClient s = clients.eventStream(0, 0))
        {
            pollUntil(s, got, () -> s.lastEventSeq() == 10);
        }
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), got);
        assertTrue(members.get(1).requests.get(0) == 5L, "second member asked for events after 5");
    }

    @Test
    void tooOldMemberIsSkippedAndResumeStartsAfterRequestedSeq()
    {
        members.get(0).tooOldBelow = 100;
        members.get(1).script = after -> new long[]{ 3, 4, 5, 6, 7 };   // replay may start before the requested point
        final List<Long> got = new ArrayList<>();
        try (ClusterEventStreamClient s = (ClusterEventStreamClient)clients.eventStream(0, 4))
        {
            pollUntil(s, got, () -> s.lastEventSeq() == 7);
            assertEquals(EventProtocol.REASON_TOO_OLD, s.lastUnavailableReason());
        }
        assertEquals(List.of(5L, 6L, 7L), got);
    }

    @Test
    void latestResolvesToMembersLastSeqAndHeartbeatLossTriggersFailover()
    {
        members.get(0).lastSeq = 41;
        members.get(0).script = after -> new long[]{ 41, 42 };
        members.get(0).heartbeats = false;     // will be abandoned after the heartbeat timeout
        members.get(1).lastSeq = 43;
        members.get(1).script = after -> new long[]{ 42, 43 };
        final List<Long> got = new ArrayList<>();
        try (ClusterEventStreamClient s = (ClusterEventStreamClient)clients.eventStream(0, -1))
        {
            pollUntil(s, got, () -> s.lastEventSeq() == 43);
            assertEquals(1, s.connectedMemberId());
        }
        assertEquals(List.of(42L, 43L), got);
    }

    private static void pollUntil(final EventStreamClient s, final List<Long> got, final BooleanSupplier done)
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!done.getAsBoolean())
        {
            assertTrue(System.nanoTime() < deadline, "timed out at " + s.lastEventSeq() + " got " + got);
            s.poll((b, o, l) -> got.add(Events.eventSeq(b, o)), 10);
            for (final FakeMember m : List.copyOf(FakeMember.ALL))
            {
                m.poll();
            }
            Thread.yield();
        }
    }

    interface Script
    {
        long[] seqs(long after);
    }

    /** Speaks the member side of {@link EventProtocol}, "replaying" scripted eventSeqs. */
    static final class FakeMember
    {
        static final List<FakeMember> ALL = new ArrayList<>();
        final Aeron aeron;
        final int memberId;
        final Subscription control;
        final Map<String, Publication> responses = new HashMap<>();
        final List<Long> requests = new ArrayList<>();
        final List<ExclusivePublication> replays = new ArrayList<>();
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[1024]);
        Script script = after -> new long[0];
        long tooOldBelow = -1;
        long lastSeq = 0;
        boolean heartbeats = true;
        long correlation;
        Publication current;
        long nextHeartbeat;
        int nextSession = 1000;

        FakeMember(final Aeron aeron, final Topology t, final int memberId)
        {
            this.aeron = aeron;
            this.memberId = memberId;
            this.control = aeron.addSubscription(
                "aeron:udp?endpoint=" + t.memberEndpoint(0, memberId, Topology.EVENT_CONTROL_OFFSET),
                EventProtocol.CONTROL_STREAM_ID);
            this.nextSession += memberId * 1000;
            ALL.add(this);
        }

        void poll()
        {
            control.poll((b, o, l, h) ->
            {
                if (EventProtocol.type(b, o) != EventProtocol.REQUEST)
                {
                    return;
                }
                final long after = EventProtocol.requestAfterEventSeq(b, o);
                correlation = EventProtocol.requestCorrelationId(b, o);
                requests.add(after);
                current = responses.computeIfAbsent(EventProtocol.requestResponseEndpoint(b, o),
                    e -> aeron.addPublication("aeron:udp?endpoint=" + e, EventProtocol.RESPONSE_STREAM_ID));
                awaitConnected(current);
                if (tooOldBelow >= 0 && after + 1 < tooOldBelow)
                {
                    final int len = EventProtocol.encodeUnavailable(buf, 0, correlation, EventProtocol.REASON_TOO_OLD,
                        memberId, -1, tooOldBelow, tooOldBelow + 10);
                    offer(current, len);
                    return;
                }
                final int session = nextSession++;
                final ExclusivePublication data = aeron.addExclusivePublication(
                    "aeron:udp?endpoint=" + EventProtocol.requestDataEndpoint(b, o) + "|session-id=" + session,
                    EventProtocol.requestDataStreamId(b, o));
                replays.add(data);
                final long resolved = after == -1 ? lastSeq : after;
                final int len = EventProtocol.encodeReplayStarted(buf, 0, correlation, session, resolved, -1,
                    memberId, memberId, 1, lastSeq);
                offer(current, len);
                awaitConnected(data);
                final UnsafeBuffer ev = new UnsafeBuffer(new byte[64]);
                for (final long seq : script.seqs(resolved))
                {
                    ev.putShort(2, (short)101, ByteOrder.LITTLE_ENDIAN);   // templateId: an event
                    ev.putLong(MessageHeaderEncoder.ENCODED_LENGTH + EventHeaderDecoder.eventSeqEncodingOffset(),
                        seq, ByteOrder.LITTLE_ENDIAN);
                    while (data.offer(ev, 0, 48) < 0)
                    {
                        Thread.yield();
                    }
                }
            }, 10);
            final long now = System.nanoTime();
            if (heartbeats && current != null && now > nextHeartbeat)
            {
                nextHeartbeat = now + TimeUnit.MILLISECONDS.toNanos(100);
                final int len = EventProtocol.encodeHeartbeat(buf, 0, correlation, memberId, memberId, 1, lastSeq);
                current.offer(buf, 0, len);
            }
        }

        private void offer(final Publication p, final int len)
        {
            while (p.offer(buf, 0, len) < 0)
            {
                Thread.yield();
            }
        }

        private static void awaitConnected(final Publication p)
        {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!p.isConnected() && System.nanoTime() < deadline)
            {
                Thread.yield();
            }
        }

        void close()
        {
            ALL.remove(this);
            replays.forEach(Publication::close);
            responses.values().forEach(Publication::close);
            control.close();
        }
    }
}
