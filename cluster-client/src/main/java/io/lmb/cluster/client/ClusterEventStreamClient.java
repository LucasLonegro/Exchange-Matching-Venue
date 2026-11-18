package io.lmb.cluster.client;

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.common.Events;
import io.lmb.common.Topology;
import io.lmb.sbe.EventHeaderDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Event stream reader for one shard. Asks a member (the leader when known) for "events after
 * {@link #lastEventSeq()}" over the member's EVENT_CONTROL endpoint and reads the Archive replay the
 * member starts towards this client. Delivers each eventSeq exactly once and in order: anything at or
 * below the last delivered eventSeq is dropped, a jump forward is a gap and triggers a reconnect.
 * The current member is abandoned (and another one tried) when it stops heartbeating, when it reports
 * that it cannot serve the requested range, or when no progress is made for the stall timeout while
 * the member says it has newer events.
 *
 * <p>Single-threaded. Must be polled regularly: the member drops the session (and its replay) when
 * keep-alives stop, after which the client simply reconnects.
 */
public final class ClusterEventStreamClient implements EventStreamClient
{
    private static final Logger LOG = LoggerFactory.getLogger(ClusterEventStreamClient.class);
    private static final int MIN_EVENT_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH + EventHeaderDecoder.ENCODED_LENGTH;
    private static final long RESOLVE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    private enum State
    {
        IDLE, REQUESTING, STREAMING, CLOSED
    }

    private final Aeron aeron;
    private final int shardId;
    private final int memberCount;
    private final ClientConfig config;
    private final long clientId;
    private final Publication[] controlPubs;
    private final long[] memberBadUntilNs;
    private final Subscription responseSub;
    private final Subscription dataSub;
    private final String responseEndpoint;
    private final String dataEndpoint;
    private final UnsafeBuffer sendBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(EventProtocol.MAX_MESSAGE_LENGTH));
    private final FragmentHandler responseHandler = this::onResponse;
    private final ControlledFragmentAssembler dataAssembler = new ControlledFragmentAssembler(this::onData);

    private State state = State.IDLE;
    private long lastEventSeq;
    private long nextAttemptNs;
    private int currentMember = -1;
    private int lastTriedMember = -1;
    private long correlationId;
    private int requestLength;
    private boolean requestSent;
    private long requestDeadlineNs;
    private int replaySessionId;
    private long endEventSeq;
    private Image image;
    private long streamingSinceNs;
    private long lastHeartbeatNs;
    private long lastProgressNs;
    private long lastKeepAliveNs;
    private long lastPollNs;
    private long memberLastSeq;
    private int knownLeader = -1;
    private long nowNs;

    private EventHandler handler;
    private int delivered;
    private int limit;
    private boolean gapDetected;
    private Throwable handlerError;

    private long reconnects;
    private long unavailableResponses;
    private long lastUnavailableReason;

    ClusterEventStreamClient(
        final Aeron aeron, final Topology topology, final int shardId, final long fromEventSeq, final ClientConfig config)
    {
        this.aeron = aeron;
        this.shardId = shardId;
        this.config = config;
        this.memberCount = topology.membersPerShard();
        this.lastEventSeq = fromEventSeq;
        this.clientId = ThreadLocalRandom.current().nextLong() & Long.MAX_VALUE;
        this.controlPubs = new Publication[memberCount];
        this.memberBadUntilNs = new long[memberCount];
        Subscription rs = null;
        Subscription ds = null;
        try
        {
            rs = aeron.addSubscription(
                "aeron:udp?endpoint=" + config.clientHost + ":0", EventProtocol.RESPONSE_STREAM_ID);
            ds = aeron.addSubscription(
                "aeron:udp?endpoint=" + config.clientHost + ":0", EventProtocol.DATA_STREAM_ID);
            for (int m = 0; m < memberCount; m++)
            {
                controlPubs[m] = aeron.addPublication(
                    "aeron:udp?endpoint=" + topology.memberEndpoint(shardId, m, Topology.EVENT_CONTROL_OFFSET) +
                    "|term-length=64k", EventProtocol.CONTROL_STREAM_ID);
            }
            this.responseSub = rs;
            this.dataSub = ds;
            this.responseEndpoint = resolve(rs);
            this.dataEndpoint = resolve(ds);
        }
        catch (final RuntimeException e)
        {
            CloseHelper.quietCloseAll(rs, ds);
            CloseHelper.quietCloseAll(controlPubs);
            throw e;
        }
        this.lastPollNs = now();
    }

    private static String resolve(final Subscription sub)
    {
        final long deadline = System.nanoTime() + RESOLVE_TIMEOUT_NS;
        String endpoint;
        while ((endpoint = sub.resolvedEndpoint()) == null)
        {
            if (System.nanoTime() > deadline)
            {
                throw new IllegalStateException("could not resolve endpoint of " + sub.channel());
            }
            Thread.onSpinWait();
            Thread.yield();
        }
        return endpoint;
    }

    private long now()
    {
        return aeron.context().nanoClock().nanoTime();
    }

    @Override
    public int shardId()
    {
        return shardId;
    }

    @Override
    public long lastEventSeq()
    {
        return lastEventSeq;
    }

    @Override
    public boolean isConnected()
    {
        return state == State.STREAMING && nowNs - lastHeartbeatNs < config.eventHeartbeatTimeoutNs;
    }

    /** Member currently streaming to this client, or -1. */
    public int connectedMemberId()
    {
        return state == State.STREAMING ? currentMember : -1;
    }

    /** Number of times a stream was (re)established. */
    public long reconnects()
    {
        return reconnects;
    }

    /** Number of UNAVAILABLE answers received (TOO_OLD, NOT_READY, ...). */
    public long unavailableResponses()
    {
        return unavailableResponses;
    }

    /** Reason code of the last UNAVAILABLE answer, 0 if none. */
    public long lastUnavailableReason()
    {
        return lastUnavailableReason;
    }

    @Override
    public int poll(final EventHandler handler, final int limit)
    {
        if (state == State.CLOSED)
        {
            return 0;
        }
        final long now = now();
        nowNs = now;
        if (now - lastPollNs > config.eventStallTimeoutNs)
        {
            // The application did not poll for a while: our timers say nothing about the member.
            lastProgressNs = now;
            lastHeartbeatNs = Math.max(lastHeartbeatNs, now - config.eventHeartbeatTimeoutNs / 2);
        }
        lastPollNs = now;

        responseSub.poll(responseHandler, 16);

        int count = 0;
        switch (state)
        {
            case IDLE:
                if (now - nextAttemptNs >= 0)
                {
                    final int member = chooseMember(now);
                    if (member >= 0)
                    {
                        startRequest(member, now);
                    }
                }
                break;

            case REQUESTING:
                if (!requestSent)
                {
                    sendRequest();
                }
                if (now - requestDeadlineNs > 0)
                {
                    failMember("no answer to request", config.eventMemberCooldownNs);
                }
                break;

            case STREAMING:
                count = pollData(handler, limit, now);
                break;

            default:
                break;
        }
        return count;
    }

    private int pollData(final EventHandler handler, final int limit, final long now)
    {
        if (image == null)
        {
            image = dataSub.imageBySessionId(replaySessionId);
            if (image == null)
            {
                if (now - streamingSinceNs > config.eventConnectTimeoutNs)
                {
                    failMember("replay image did not arrive", config.eventMemberCooldownNs);
                }
                sendKeepAliveIfDue(now);
                return 0;
            }
        }

        this.handler = handler;
        this.limit = Math.max(1, limit);
        this.delivered = 0;
        image.controlledPoll(dataAssembler, Math.max(limit, 1) + 64);
        this.handler = null;
        final int count = delivered;

        if (handlerError != null)
        {
            final Throwable t = handlerError;
            handlerError = null;
            LangUtil.rethrowUnchecked(t);
        }

        if (gapDetected)
        {
            gapDetected = false;
            failMember("gap in replay", config.eventMemberCooldownNs);
            return count;
        }

        if (count > 0 || memberLastSeq == lastEventSeq)
        {
            lastProgressNs = now;
        }

        if (image.isEndOfStream() || image.isClosed())
        {
            if (count == 0)
            {
                if (endEventSeq >= 0 && lastEventSeq >= endEventSeq)
                {
                    // bounded replay of an older recording segment finished: ask for the rest
                    startRequest(currentMember, now);
                }
                else
                {
                    failMember("replay ended", 0);
                }
            }
            return count;
        }

        if (now - lastHeartbeatNs > config.eventHeartbeatTimeoutNs)
        {
            failMember("heartbeat timeout", config.eventMemberCooldownNs);
            return count;
        }
        if (now - lastProgressNs > config.eventStallTimeoutNs)
        {
            failMember("stalled (member lastSeq=" + memberLastSeq + ", ours=" + lastEventSeq + ")",
                config.eventMemberCooldownNs);
            return count;
        }
        sendKeepAliveIfDue(now);

        if (config.eventRehomeAfterNs > 0 && config.eventPreferredMember < 0 && knownLeader >= 0 &&
            knownLeader != currentMember && knownLeader < memberCount &&
            now - streamingSinceNs > config.eventRehomeAfterNs && lastEventSeq >= memberLastSeq &&
            now - memberBadUntilNs[knownLeader] >= 0)
        {
            LOG.info("shard {}: moving event stream from member {} to leader {} at eventSeq {}",
                shardId, currentMember, knownLeader, lastEventSeq);
            sendClose(currentMember);
            startRequest(knownLeader, now);
        }
        return count;
    }

    private ControlledFragmentHandler.Action onData(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (length < MIN_EVENT_LENGTH || !Events.isEvent(buffer, offset))
        {
            return ControlledFragmentHandler.Action.CONTINUE;
        }
        final long seq = Events.eventSeq(buffer, offset);
        if (seq <= lastEventSeq)
        {
            return ControlledFragmentHandler.Action.CONTINUE;
        }
        if (delivered >= limit)
        {
            return ControlledFragmentHandler.Action.ABORT;
        }
        if (seq != lastEventSeq + 1)
        {
            LOG.warn("shard {}: gap from member {}: expected eventSeq {} got {}",
                shardId, currentMember, lastEventSeq + 1, seq);
            gapDetected = true;
            return ControlledFragmentHandler.Action.ABORT;
        }
        try
        {
            handler.onEvent(buffer, offset, length);
        }
        catch (final Throwable t)
        {
            handlerError = t;
            return ControlledFragmentHandler.Action.ABORT;
        }
        lastEventSeq = seq;
        delivered++;
        return delivered >= limit ? ControlledFragmentHandler.Action.BREAK : ControlledFragmentHandler.Action.CONTINUE;
    }

    private void onResponse(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (length < EventProtocol.BODY)
        {
            return;
        }
        switch (EventProtocol.type(buffer, offset))
        {
            case EventProtocol.REPLAY_STARTED:
                if (state == State.REQUESTING && EventProtocol.rsCorrelationId(buffer, offset) == correlationId)
                {
                    if (lastEventSeq == VenueClients.LATEST)
                    {
                        lastEventSeq = EventProtocol.rsResolvedAfterSeq(buffer, offset);
                    }
                    replaySessionId = (int)EventProtocol.rsReplaySessionId(buffer, offset);
                    endEventSeq = EventProtocol.rsEndEventSeq(buffer, offset);
                    memberLastSeq = EventProtocol.rsLastSeq(buffer, offset);
                    noteLeader(EventProtocol.rsLeaderMemberId(buffer, offset));
                    image = null;
                    dataAssembler.clear();
                    state = State.STREAMING;
                    streamingSinceNs = nowNs;
                    lastHeartbeatNs = nowNs;
                    lastProgressNs = nowNs;
                    lastKeepAliveNs = nowNs;
                    reconnects++;
                    LOG.debug("shard {}: streaming from member {} after eventSeq {} (replay {}, end {})",
                        shardId, currentMember, lastEventSeq, replaySessionId, endEventSeq);
                }
                break;

            case EventProtocol.UNAVAILABLE:
                if (state == State.REQUESTING && EventProtocol.uaCorrelationId(buffer, offset) == correlationId)
                {
                    final int reason = EventProtocol.uaReason(buffer, offset);
                    unavailableResponses++;
                    lastUnavailableReason = reason;
                    noteLeader(EventProtocol.uaLeaderMemberId(buffer, offset));
                    final long cooldown = reason == EventProtocol.REASON_TOO_OLD || reason == EventProtocol.REASON_GAP ?
                        config.eventMemberCooldownNs * 10 : config.eventMemberCooldownNs;
                    failMember("unavailable: " + EventProtocol.reasonName(reason) + " (member holds " +
                        EventProtocol.uaFirstSeq(buffer, offset) + ".." + EventProtocol.uaLastSeq(buffer, offset) +
                        ", asked after " + lastEventSeq + ")", cooldown);
                }
                break;

            case EventProtocol.HEARTBEAT:
                if (state == State.STREAMING && EventProtocol.hbCorrelationId(buffer, offset) == correlationId)
                {
                    lastHeartbeatNs = nowNs;
                    memberLastSeq = EventProtocol.hbLastSeq(buffer, offset);
                    noteLeader(EventProtocol.hbLeaderMemberId(buffer, offset));
                }
                break;

            default:
                break;
        }
    }

    private void noteLeader(final int leader)
    {
        if (leader >= 0 && leader < memberCount)
        {
            knownLeader = leader;
        }
    }

    private int chooseMember(final long now)
    {
        final int first = config.eventPreferredMember >= 0 && config.eventPreferredMember < memberCount ?
            config.eventPreferredMember : knownLeader;
        if (first >= 0 && now - memberBadUntilNs[first] >= 0)
        {
            return first;
        }
        for (int i = 1; i <= memberCount; i++)
        {
            final int m = (Math.max(lastTriedMember, 0) + i) % memberCount;
            if (now - memberBadUntilNs[m] >= 0)
            {
                return m;
            }
        }
        long earliest = Long.MAX_VALUE;
        for (int m = 0; m < memberCount; m++)
        {
            earliest = Math.min(earliest, memberBadUntilNs[m] - now);
        }
        nextAttemptNs = now + Math.max(earliest, 0);
        return -1;
    }

    private void startRequest(final int member, final long now)
    {
        currentMember = member;
        lastTriedMember = member;
        correlationId = aeron.nextCorrelationId();
        requestLength = EventProtocol.encodeRequest(sendBuffer, 0, correlationId, clientId, lastEventSeq,
            EventProtocol.DATA_STREAM_ID, shardId, responseEndpoint, dataEndpoint);
        requestSent = false;
        requestDeadlineNs = now + config.eventConnectTimeoutNs;
        image = null;
        state = State.REQUESTING;
        sendRequest();
    }

    private void sendRequest()
    {
        requestSent = controlPubs[currentMember].offer(sendBuffer, 0, requestLength) > 0;
    }

    private void sendKeepAliveIfDue(final long now)
    {
        if (now - lastKeepAliveNs >= config.eventKeepAliveIntervalNs)
        {
            final int len = EventProtocol.encodeKeepAlive(
                sendBuffer, 0, EventProtocol.KEEPALIVE, correlationId, clientId);
            if (controlPubs[currentMember].offer(sendBuffer, 0, len) > 0)
            {
                lastKeepAliveNs = now;
            }
        }
    }

    private void sendClose(final int member)
    {
        if (member >= 0 && !controlPubs[member].isClosed())
        {
            final int len = EventProtocol.encodeKeepAlive(sendBuffer, 0, EventProtocol.CLOSE, correlationId, clientId);
            controlPubs[member].offer(sendBuffer, 0, len);
        }
    }

    private void failMember(final String why, final long cooldownNs)
    {
        LOG.info("shard {}: event stream from member {} abandoned at eventSeq {}: {}",
            shardId, currentMember, lastEventSeq, why);
        if (currentMember >= 0)
        {
            sendClose(currentMember);
            memberBadUntilNs[currentMember] = nowNs + cooldownNs;
            if (knownLeader == currentMember)
            {
                knownLeader = -1;
            }
        }
        image = null;
        state = State.IDLE;
        nextAttemptNs = nowNs;
    }

    @Override
    public void close()
    {
        if (state != State.CLOSED)
        {
            if (!aeron.isClosed())
            {
                sendClose(currentMember);
                CloseHelper.quietCloseAll(responseSub, dataSub);
                CloseHelper.quietCloseAll(controlPubs);
            }
            state = State.CLOSED;
        }
    }
}
