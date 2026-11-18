package io.lmb.cluster.client;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * The event stream request/response protocol spoken on a member's EVENT_CONTROL port.
 *
 * <p>All messages are little endian and start with {@code int16 type, int16 version}. A consumer sends
 * {@link #REQUEST} ("events after eventSeq N, replay them to this endpoint") to the member's control
 * endpoint; the member answers on the consumer's response endpoint with {@link #REPLAY_STARTED} (the
 * Archive replay session id to read from) or {@link #UNAVAILABLE} (with a reason), then sends
 * {@link #HEARTBEAT}s for as long as the consumer keeps sending {@link #KEEPALIVE}s. Consumers never
 * see Aeron positions.
 *
 * <pre>
 * REQUEST        correlationId:i64 clientId:i64 afterEventSeq:i64 dataStreamId:i32 shardId:i32
 *                responseEndpoint:(len:u16 ascii) dataEndpoint:(len:u16 ascii)
 * KEEPALIVE      correlationId:i64 clientId:i64
 * CLOSE          correlationId:i64 clientId:i64
 * REPLAY_STARTED correlationId:i64 replaySessionId:i64 resolvedAfterSeq:i64 endEventSeq:i64 (-1 = follows live)
 *                memberId:i32 leaderMemberId:i32 firstSeq:i64 lastSeq:i64
 * UNAVAILABLE    correlationId:i64 reason:i32 memberId:i32 leaderMemberId:i32 pad:i32 firstSeq:i64 lastSeq:i64
 * HEARTBEAT      correlationId:i64 memberId:i32 leaderMemberId:i32 firstSeq:i64 lastSeq:i64
 * </pre>
 *
 * firstSeq is the first eventSeq the member can replay, lastSeq the last one it has recorded.
 */
public final class EventProtocol
{
    public static final int VERSION = 1;

    /** Stream id of the member's control subscription. */
    public static final int CONTROL_STREAM_ID = 7000;
    /** Stream id of the consumer's response subscription. */
    public static final int RESPONSE_STREAM_ID = 7001;
    /** Stream id the replayed events are delivered on. */
    public static final int DATA_STREAM_ID = 7002;

    public static final short REQUEST = 1;
    public static final short KEEPALIVE = 2;
    public static final short CLOSE = 3;
    public static final short REPLAY_STARTED = 10;
    public static final short UNAVAILABLE = 11;
    public static final short HEARTBEAT = 12;

    /** The member's event stream is not initialised yet (service starting or replaying its log). */
    public static final int REASON_NOT_READY = 1;
    /** The requested events precede the first event this member holds (for example it was rebuilt from a snapshot). */
    public static final int REASON_TOO_OLD = 2;
    /** The requested events fall in a hole of this member's recordings. */
    public static final int REASON_GAP = 3;
    /** The member serves too many consumers. */
    public static final int REASON_BUSY = 4;
    /** The member could not start the replay. */
    public static final int REASON_ERROR = 5;
    /** The request was for another shard. */
    public static final int REASON_WRONG_SHARD = 6;

    public static final long LATEST = -1L;

    public static final int TYPE_OFFSET = 0;
    public static final int VERSION_OFFSET = 2;
    public static final int BODY = 4;

    public static final int MAX_MESSAGE_LENGTH = 1024;

    // REQUEST
    private static final int REQ_CORRELATION = BODY;
    private static final int REQ_CLIENT = BODY + 8;
    private static final int REQ_AFTER = BODY + 16;
    private static final int REQ_DATA_STREAM = BODY + 24;
    private static final int REQ_SHARD = BODY + 28;
    private static final int REQ_STRINGS = BODY + 32;

    // KEEPALIVE / CLOSE
    private static final int KA_CORRELATION = BODY;
    private static final int KA_CLIENT = BODY + 8;
    public static final int KEEPALIVE_LENGTH = BODY + 16;

    // REPLAY_STARTED
    private static final int RS_CORRELATION = BODY;
    private static final int RS_REPLAY_SESSION = BODY + 8;
    private static final int RS_RESOLVED_AFTER = BODY + 16;
    private static final int RS_END = BODY + 24;
    private static final int RS_MEMBER = BODY + 32;
    private static final int RS_LEADER = BODY + 36;
    private static final int RS_FIRST = BODY + 40;
    private static final int RS_LAST = BODY + 48;
    public static final int REPLAY_STARTED_LENGTH = BODY + 56;

    // UNAVAILABLE
    private static final int UA_CORRELATION = BODY;
    private static final int UA_REASON = BODY + 8;
    private static final int UA_MEMBER = BODY + 12;
    private static final int UA_LEADER = BODY + 16;
    private static final int UA_FIRST = BODY + 24;
    private static final int UA_LAST = BODY + 32;
    public static final int UNAVAILABLE_LENGTH = BODY + 40;

    // HEARTBEAT
    private static final int HB_CORRELATION = BODY;
    private static final int HB_MEMBER = BODY + 8;
    private static final int HB_LEADER = BODY + 12;
    private static final int HB_FIRST = BODY + 16;
    private static final int HB_LAST = BODY + 24;
    public static final int HEARTBEAT_LENGTH = BODY + 32;

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    private EventProtocol()
    {
    }

    public static short type(final DirectBuffer b, final int o)
    {
        return b.getShort(o + TYPE_OFFSET, LE);
    }

    private static void header(final MutableDirectBuffer b, final int o, final short type)
    {
        b.putShort(o + TYPE_OFFSET, type, LE);
        b.putShort(o + VERSION_OFFSET, (short)VERSION, LE);
    }

    // ---------------- REQUEST ----------------

    public static int encodeRequest(
        final MutableDirectBuffer b, final int o, final long correlationId, final long clientId,
        final long afterEventSeq, final int dataStreamId, final int shardId,
        final String responseEndpoint, final String dataEndpoint)
    {
        header(b, o, REQUEST);
        b.putLong(o + REQ_CORRELATION, correlationId, LE);
        b.putLong(o + REQ_CLIENT, clientId, LE);
        b.putLong(o + REQ_AFTER, afterEventSeq, LE);
        b.putInt(o + REQ_DATA_STREAM, dataStreamId, LE);
        b.putInt(o + REQ_SHARD, shardId, LE);
        int p = o + REQ_STRINGS;
        p = putString(b, p, responseEndpoint);
        p = putString(b, p, dataEndpoint);
        return p - o;
    }

    public static long requestCorrelationId(final DirectBuffer b, final int o)
    {
        return b.getLong(o + REQ_CORRELATION, LE);
    }

    public static long requestClientId(final DirectBuffer b, final int o)
    {
        return b.getLong(o + REQ_CLIENT, LE);
    }

    public static long requestAfterEventSeq(final DirectBuffer b, final int o)
    {
        return b.getLong(o + REQ_AFTER, LE);
    }

    public static int requestDataStreamId(final DirectBuffer b, final int o)
    {
        return b.getInt(o + REQ_DATA_STREAM, LE);
    }

    public static int requestShardId(final DirectBuffer b, final int o)
    {
        return b.getInt(o + REQ_SHARD, LE);
    }

    public static String requestResponseEndpoint(final DirectBuffer b, final int o)
    {
        return getString(b, o + REQ_STRINGS);
    }

    public static String requestDataEndpoint(final DirectBuffer b, final int o)
    {
        final int first = o + REQ_STRINGS;
        final int len = b.getShort(first, LE) & 0xFFFF;
        return getString(b, first + 2 + len);
    }

    private static int putString(final MutableDirectBuffer b, final int p, final String s)
    {
        final byte[] bytes = s.getBytes(StandardCharsets.US_ASCII);
        b.putShort(p, (short)bytes.length, LE);
        b.putBytes(p + 2, bytes);
        return p + 2 + bytes.length;
    }

    private static String getString(final DirectBuffer b, final int p)
    {
        final int len = b.getShort(p, LE) & 0xFFFF;
        final byte[] bytes = new byte[len];
        b.getBytes(p + 2, bytes);
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    // ---------------- KEEPALIVE / CLOSE ----------------

    public static int encodeKeepAlive(
        final MutableDirectBuffer b, final int o, final short type, final long correlationId, final long clientId)
    {
        header(b, o, type);
        b.putLong(o + KA_CORRELATION, correlationId, LE);
        b.putLong(o + KA_CLIENT, clientId, LE);
        return KEEPALIVE_LENGTH;
    }

    public static long keepAliveCorrelationId(final DirectBuffer b, final int o)
    {
        return b.getLong(o + KA_CORRELATION, LE);
    }

    public static long keepAliveClientId(final DirectBuffer b, final int o)
    {
        return b.getLong(o + KA_CLIENT, LE);
    }

    // ---------------- REPLAY_STARTED ----------------

    public static int encodeReplayStarted(
        final MutableDirectBuffer b, final int o, final long correlationId, final long replaySessionId,
        final long resolvedAfterSeq, final long endEventSeq, final int memberId, final int leaderMemberId,
        final long firstSeq, final long lastSeq)
    {
        header(b, o, REPLAY_STARTED);
        b.putLong(o + RS_CORRELATION, correlationId, LE);
        b.putLong(o + RS_REPLAY_SESSION, replaySessionId, LE);
        b.putLong(o + RS_RESOLVED_AFTER, resolvedAfterSeq, LE);
        b.putLong(o + RS_END, endEventSeq, LE);
        b.putInt(o + RS_MEMBER, memberId, LE);
        b.putInt(o + RS_LEADER, leaderMemberId, LE);
        b.putLong(o + RS_FIRST, firstSeq, LE);
        b.putLong(o + RS_LAST, lastSeq, LE);
        return REPLAY_STARTED_LENGTH;
    }

    public static long rsCorrelationId(final DirectBuffer b, final int o)
    {
        return b.getLong(o + RS_CORRELATION, LE);
    }

    public static long rsReplaySessionId(final DirectBuffer b, final int o)
    {
        return b.getLong(o + RS_REPLAY_SESSION, LE);
    }

    public static long rsResolvedAfterSeq(final DirectBuffer b, final int o)
    {
        return b.getLong(o + RS_RESOLVED_AFTER, LE);
    }

    public static long rsEndEventSeq(final DirectBuffer b, final int o)
    {
        return b.getLong(o + RS_END, LE);
    }

    public static int rsMemberId(final DirectBuffer b, final int o)
    {
        return b.getInt(o + RS_MEMBER, LE);
    }

    public static int rsLeaderMemberId(final DirectBuffer b, final int o)
    {
        return b.getInt(o + RS_LEADER, LE);
    }

    public static long rsFirstSeq(final DirectBuffer b, final int o)
    {
        return b.getLong(o + RS_FIRST, LE);
    }

    public static long rsLastSeq(final DirectBuffer b, final int o)
    {
        return b.getLong(o + RS_LAST, LE);
    }

    // ---------------- UNAVAILABLE ----------------

    public static int encodeUnavailable(
        final MutableDirectBuffer b, final int o, final long correlationId, final int reason, final int memberId,
        final int leaderMemberId, final long firstSeq, final long lastSeq)
    {
        header(b, o, UNAVAILABLE);
        b.putLong(o + UA_CORRELATION, correlationId, LE);
        b.putInt(o + UA_REASON, reason, LE);
        b.putInt(o + UA_MEMBER, memberId, LE);
        b.putInt(o + UA_LEADER, leaderMemberId, LE);
        b.putInt(o + UA_LEADER + 4, 0, LE);
        b.putLong(o + UA_FIRST, firstSeq, LE);
        b.putLong(o + UA_LAST, lastSeq, LE);
        return UNAVAILABLE_LENGTH;
    }

    public static long uaCorrelationId(final DirectBuffer b, final int o)
    {
        return b.getLong(o + UA_CORRELATION, LE);
    }

    public static int uaReason(final DirectBuffer b, final int o)
    {
        return b.getInt(o + UA_REASON, LE);
    }

    public static int uaMemberId(final DirectBuffer b, final int o)
    {
        return b.getInt(o + UA_MEMBER, LE);
    }

    public static int uaLeaderMemberId(final DirectBuffer b, final int o)
    {
        return b.getInt(o + UA_LEADER, LE);
    }

    public static long uaFirstSeq(final DirectBuffer b, final int o)
    {
        return b.getLong(o + UA_FIRST, LE);
    }

    public static long uaLastSeq(final DirectBuffer b, final int o)
    {
        return b.getLong(o + UA_LAST, LE);
    }

    // ---------------- HEARTBEAT ----------------

    public static int encodeHeartbeat(
        final MutableDirectBuffer b, final int o, final long correlationId, final int memberId,
        final int leaderMemberId, final long firstSeq, final long lastSeq)
    {
        header(b, o, HEARTBEAT);
        b.putLong(o + HB_CORRELATION, correlationId, LE);
        b.putInt(o + HB_MEMBER, memberId, LE);
        b.putInt(o + HB_LEADER, leaderMemberId, LE);
        b.putLong(o + HB_FIRST, firstSeq, LE);
        b.putLong(o + HB_LAST, lastSeq, LE);
        return HEARTBEAT_LENGTH;
    }

    public static long hbCorrelationId(final DirectBuffer b, final int o)
    {
        return b.getLong(o + HB_CORRELATION, LE);
    }

    public static int hbMemberId(final DirectBuffer b, final int o)
    {
        return b.getInt(o + HB_MEMBER, LE);
    }

    public static int hbLeaderMemberId(final DirectBuffer b, final int o)
    {
        return b.getInt(o + HB_LEADER, LE);
    }

    public static long hbFirstSeq(final DirectBuffer b, final int o)
    {
        return b.getLong(o + HB_FIRST, LE);
    }

    public static long hbLastSeq(final DirectBuffer b, final int o)
    {
        return b.getLong(o + HB_LAST, LE);
    }

    public static String reasonName(final int reason)
    {
        return switch (reason)
        {
            case REASON_NOT_READY -> "NOT_READY";
            case REASON_TOO_OLD -> "TOO_OLD";
            case REASON_GAP -> "GAP";
            case REASON_BUSY -> "BUSY";
            case REASON_ERROR -> "ERROR";
            case REASON_WRONG_SHARD -> "WRONG_SHARD";
            default -> "UNKNOWN(" + reason + ")";
        };
    }
}
