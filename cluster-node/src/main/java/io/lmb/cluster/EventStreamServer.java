package io.lmb.cluster;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.lmb.cluster.client.EventProtocol;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Serves the event stream protocol ({@link EventProtocol}) on the member's EVENT_CONTROL endpoint.
 * For each consumer it starts an Archive replay of the right recording segment from the indexed
 * position at or before the requested eventSeq: following the live recording for the current segment,
 * bounded for older ones (the consumer then asks again for the rest). Runs on its own thread; the
 * replays themselves are performed by the Archive, so a slow consumer only slows its own replay.
 */
public final class EventStreamServer implements Agent
{
    private static final Logger LOG = LoggerFactory.getLogger(EventStreamServer.class);

    private final Aeron aeron;
    private final AeronArchive.Context archiveContext;
    private final EventIndex index;
    private final NodeConfig config;
    private final String controlChannel;
    private final Long2ObjectHashMap<Session> sessions = new Long2ObjectHashMap<>();
    private final List<Session> expired = new ArrayList<>();
    private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(EventProtocol.MAX_MESSAGE_LENGTH));
    private final FragmentHandler handler = this::onMessage;

    private Subscription controlSub;
    private AeronArchive archive;
    private long nextHeartbeatNs;
    private long requestsServed;

    private static final class Session
    {
        final long clientId;
        String responseEndpoint;
        Publication responsePub;
        long correlationId;
        long replaySessionId = Aeron.NULL_VALUE;
        long lastSeenNs;
        final UnsafeBuffer pending = new UnsafeBuffer(new byte[EventProtocol.MAX_MESSAGE_LENGTH]);
        int pendingLength;

        Session(final long clientId)
        {
            this.clientId = clientId;
        }
    }

    public EventStreamServer(
        final Aeron aeron, final AeronArchive.Context archiveContext, final EventIndex index, final NodeConfig config)
    {
        this.aeron = aeron;
        this.archiveContext = archiveContext;
        this.index = index;
        this.config = config;
        this.controlChannel = "aeron:udp?endpoint=" + config.endpoint(io.lmb.common.Topology.EVENT_CONTROL_OFFSET);
    }

    @Override
    public String roleName()
    {
        return "lmb-event-server-" + config.shardId + "-" + config.memberId;
    }

    @Override
    public void onStart()
    {
        controlSub = aeron.addSubscription(controlChannel, EventProtocol.CONTROL_STREAM_ID);
        archive = AeronArchive.connect(archiveContext.clone());
        LOG.info("event stream server listening on {}", controlChannel);
    }

    public long requestsServed()
    {
        return requestsServed;
    }

    public int sessionCount()
    {
        return sessions.size();
    }

    @Override
    public int doWork()
    {
        int work = controlSub.poll(handler, 16);
        final long now = System.nanoTime();
        if (now - nextHeartbeatNs >= 0)
        {
            nextHeartbeatNs = now + config.eventHeartbeatIntervalNs;
            work += heartbeat(now);
        }
        return work;
    }

    private int heartbeat(final long now)
    {
        int work = 0;
        final long first = index.firstSeq();
        final long last = index.lastSeq();
        final int leader = index.leaderMemberId();
        for (final Session s : sessions.values())
        {
            if (now - s.lastSeenNs > config.eventSessionTimeoutNs)
            {
                expired.add(s);
                continue;
            }
            if (s.pendingLength > 0)
            {
                if (s.responsePub.offer(s.pending, 0, s.pendingLength) > 0)
                {
                    s.pendingLength = 0;
                    work++;
                }
                continue;
            }
            if (s.replaySessionId != Aeron.NULL_VALUE)
            {
                final int len = EventProtocol.encodeHeartbeat(
                    buffer, 0, s.correlationId, config.memberId, leader, first, last);
                s.responsePub.offer(buffer, 0, len);
                work++;
            }
        }
        for (final Session s : expired)
        {
            LOG.debug("consumer {} timed out", s.clientId);
            endSession(s);
        }
        expired.clear();
        return work;
    }

    private void onMessage(final DirectBuffer b, final int o, final int length, final Header header)
    {
        if (length < EventProtocol.BODY)
        {
            return;
        }
        final long now = System.nanoTime();
        switch (EventProtocol.type(b, o))
        {
            case EventProtocol.REQUEST -> onRequest(b, o, now);
            case EventProtocol.KEEPALIVE ->
            {
                final Session s = sessions.get(EventProtocol.keepAliveClientId(b, o));
                if (s != null)
                {
                    s.lastSeenNs = now;
                }
            }
            case EventProtocol.CLOSE ->
            {
                final Session s = sessions.get(EventProtocol.keepAliveClientId(b, o));
                if (s != null && s.correlationId == EventProtocol.keepAliveCorrelationId(b, o))
                {
                    endSession(s);
                }
            }
            default ->
            {
            }
        }
    }

    private void onRequest(final DirectBuffer b, final int o, final long now)
    {
        final long clientId = EventProtocol.requestClientId(b, o);
        final long correlationId = EventProtocol.requestCorrelationId(b, o);
        final long requestedAfter = EventProtocol.requestAfterEventSeq(b, o);
        final int dataStreamId = EventProtocol.requestDataStreamId(b, o);
        final int shard = EventProtocol.requestShardId(b, o);
        final String responseEndpoint = EventProtocol.requestResponseEndpoint(b, o);
        final String dataEndpoint = EventProtocol.requestDataEndpoint(b, o);

        Session s = sessions.get(clientId);
        if (s == null)
        {
            s = new Session(clientId);
            sessions.put(clientId, s);
        }
        if (!responseEndpoint.equals(s.responseEndpoint))
        {
            CloseHelper.quietClose(s.responsePub);
            s.responseEndpoint = responseEndpoint;
            s.responsePub = aeron.addPublication(
                "aeron:udp?endpoint=" + responseEndpoint + "|term-length=64k", EventProtocol.RESPONSE_STREAM_ID);
        }
        stopReplay(s);
        s.correlationId = correlationId;
        s.lastSeenNs = now;
        requestsServed++;

        final int leader = index.leaderMemberId();
        if (shard != config.shardId)
        {
            unavailable(s, EventProtocol.REASON_WRONG_SHARD, leader);
            return;
        }
        if (!index.isReady())
        {
            unavailable(s, EventProtocol.REASON_NOT_READY, leader);
            return;
        }
        if (sessions.size() > config.eventMaxSessions)
        {
            unavailable(s, EventProtocol.REASON_BUSY, leader);
            return;
        }

        final long lastSeq = index.lastSeq();
        final long after = requestedAfter == EventProtocol.LATEST ? lastSeq : requestedAfter;
        final long wanted = after + 1;
        final EventIndex.Segment live = index.liveSegment();
        EventIndex.Segment segment = index.segmentFor(wanted);
        if (segment == null && live != null && wanted >= live.firstSeq())
        {
            segment = live; // at or beyond the live edge (this member may also be lagging): follow live
        }
        if (segment == null)
        {
            final long first = index.firstSeq();
            unavailable(s, wanted < first || first == 0 ? EventProtocol.REASON_TOO_OLD : EventProtocol.REASON_GAP, leader);
            return;
        }

        try
        {
            final boolean following = segment.isLive();
            final long maxPosition = following ? archive.getRecordingPosition(segment.recordingId()) : Long.MAX_VALUE;
            final long position = segment.floorPosition(wanted, maxPosition);
            final String replayChannel = "aeron:udp?endpoint=" + dataEndpoint;
            final long replaySessionId = archive.startReplay(
                segment.recordingId(), position,
                following ? AeronArchive.REPLAY_ALL_AND_FOLLOW : AeronArchive.REPLAY_ALL_AND_STOP,
                replayChannel, dataStreamId);
            s.replaySessionId = replaySessionId;
            final long end = following ? -1 : segment.lastSeq();
            respond(s, EventProtocol.encodeReplayStarted(s.pending, 0, correlationId, replaySessionId, after, end,
                config.memberId, leader, index.firstSeq(), lastSeq));
            LOG.debug("consumer {}: replay {} of recording {} from position {} (after eventSeq {}, end {})",
                clientId, replaySessionId, segment.recordingId(), position, after, end);
        }
        catch (final RuntimeException e)
        {
            LOG.warn("consumer {}: cannot start replay after eventSeq {}: {}", clientId, after, e.toString());
            unavailable(s, EventProtocol.REASON_ERROR, leader);
        }
    }

    private void unavailable(final Session s, final int reason, final int leader)
    {
        respond(s, EventProtocol.encodeUnavailable(s.pending, 0, s.correlationId, reason, config.memberId, leader,
            index.firstSeq(), index.lastSeq()));
    }

    private void respond(final Session s, final int length)
    {
        s.pendingLength = length;
        if (s.responsePub.offer(s.pending, 0, length) > 0)
        {
            s.pendingLength = 0;
        }
    }

    private void stopReplay(final Session s)
    {
        if (s.replaySessionId != Aeron.NULL_VALUE)
        {
            try
            {
                archive.stopReplay(s.replaySessionId);
            }
            catch (final RuntimeException e)
            {
                // the replay already ended (bounded replay done or consumer gone)
            }
            s.replaySessionId = Aeron.NULL_VALUE;
        }
    }

    private void endSession(final Session s)
    {
        stopReplay(s);
        CloseHelper.quietClose(s.responsePub);
        sessions.remove(s.clientId);
    }

    @Override
    public void onClose()
    {
        for (final Session s : new ArrayList<>(sessions.values()))
        {
            endSession(s);
        }
        CloseHelper.quietClose(controlSub);
        CloseHelper.quietClose(archive);
    }
}
