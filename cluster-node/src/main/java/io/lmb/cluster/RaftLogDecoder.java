package io.lmb.cluster;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.TimerEventDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Decodes complete Aeron Cluster log messages (cluster codecs, schema 111) into
 * {@link RaftLogReader.Handler} callbacks: session messages become commands (the bytes after the
 * 32-byte session header are the raw LMB command), timer events become timers, anything else is passed
 * to {@link RaftLogReader.Handler#onClusterMessage}.
 *
 * <p>Used by {@link RaftLogReader} for the files of a member's Archive, and usable directly on an Archive
 * replay of the log recording: {@code subscription.poll(new FragmentAssembler(new RaftLogDecoder(h)), n)}.
 * There {@code header.position()} is the log position after the entry, exactly the {@code logPosition}
 * the live state machine received. Allocation-free; not thread safe.
 */
public final class RaftLogDecoder implements FragmentHandler
{
    /** Cluster codec template ids that are neither commands nor timers. */
    public static final int SESSION_OPEN_TEMPLATE_ID = io.aeron.cluster.codecs.SessionOpenEventDecoder.TEMPLATE_ID;
    public static final int SESSION_CLOSE_TEMPLATE_ID = io.aeron.cluster.codecs.SessionCloseEventDecoder.TEMPLATE_ID;
    public static final int CLUSTER_ACTION_TEMPLATE_ID = io.aeron.cluster.codecs.ClusterActionRequestDecoder.TEMPLATE_ID;
    public static final int NEW_LEADERSHIP_TERM_TEMPLATE_ID =
        io.aeron.cluster.codecs.NewLeadershipTermEventDecoder.TEMPLATE_ID;

    private final RaftLogReader.Handler handler;
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder session = new SessionMessageHeaderDecoder();
    private final TimerEventDecoder timer = new TimerEventDecoder();

    public RaftLogDecoder(final RaftLogReader.Handler handler)
    {
        this.handler = handler;
    }

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        decode(buffer, offset, length, header.position());
    }

    /**
     * Decodes one complete log message.
     *
     * @param logPosition log position after the message
     * @throws IllegalStateException if the message is not a cluster codec message
     */
    public void decode(final DirectBuffer buffer, final int offset, final int length, final long logPosition)
    {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH)
        {
            throw new IllegalStateException("log message of " + length + " bytes at position " + logPosition);
        }
        header.wrap(buffer, offset);
        if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new IllegalStateException("log message with schemaId " + header.schemaId() + " at position " +
                logPosition + " (expected " + MessageHeaderDecoder.SCHEMA_ID + ")");
        }
        final int templateId = header.templateId();
        if (templateId == SessionMessageHeaderDecoder.TEMPLATE_ID)
        {
            session.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
            handler.onCommand(logPosition, session.timestamp(), session.clusterSessionId(), buffer,
                offset + AeronCluster.SESSION_HEADER_LENGTH, length - AeronCluster.SESSION_HEADER_LENGTH);
        }
        else if (templateId == TimerEventDecoder.TEMPLATE_ID)
        {
            timer.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
            handler.onTimer(logPosition, timer.timestamp(), timer.correlationId());
        }
        else
        {
            handler.onClusterMessage(templateId, logPosition, buffer, offset, length);
        }
    }
}
