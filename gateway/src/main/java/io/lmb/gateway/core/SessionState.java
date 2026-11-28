package io.lmb.gateway.core;

import io.lmb.api.EventStreamClient;
import io.lmb.common.access.AccessDirectory;

import java.util.HashMap;
import java.util.Map;

/**
 * The gateway's record of one participant session. Created at the first Logon on this gateway and
 * kept after disconnect (sequence numbers and delivery cursors survive a reconnect to the same
 * gateway). Core thread only.
 *
 * <p>Delivery per shard is either LIVE (reports are pushed from the shared live reader; {@code cursor}
 * is a guard: only events above it are delivered) or CATCH-UP (a private event stream reader
 * re-reads from {@code cursor}, the last event handled for this session, until it reaches the live
 * reader and hands over).
 *
 * <p>What the session may do is fixed at logon: the {@link AccessDirectory.Grant} (scopes, throttle tier) is kept
 * here for the life of the connection and never looked up again per message. A newer access snapshot only matters
 * if it revokes the grant, which ends the connection.
 */
public final class SessionState
{
    static final class MassCancelAggregate
    {
        final long requestId;
        int pending;
        int total;

        MassCancelAggregate(final long requestId, final int pending)
        {
            this.requestId = requestId;
            this.pending = pending;
        }
    }

    final long sessionId;
    final int firmId;
    SessionOutlet outlet;
    long nextExpectedClientSeq;
    AccessDirectory.Grant grant;
    /** TRADE scope: new orders and replaces. Without it only cancels and mass cancels. */
    boolean canTrade;
    /** Cancel-on-disconnect: on a lost connection, on a logout, and how long to wait before cancelling. */
    boolean codOnDisconnect;
    boolean codOnLogout;
    long codWindowNs;
    /** New orders and replaces. */
    TokenBucket orderBucket;
    /** Every request, cancels included. */
    TokenBucket messageBucket;
    double orderRatePerSec;
    double messageRatePerSec;

    final long[] cursor;
    final long[] possDupUpTo;
    final boolean[] catchingUp;
    final EventStreamClient[] catchupReader;
    final GatewayCore.CatchupHandler[] catchupHandler;

    long lastInboundNs;
    long lastOutboundNs;
    long notWritableSinceNs;
    long connectedAtNs;
    final Map<Long, MassCancelAggregate> massCancels = new HashMap<>();

    SessionState(final long sessionId, final int firmId, final int shardCount)
    {
        this.sessionId = sessionId;
        this.firmId = firmId;
        this.cursor = new long[shardCount];
        this.possDupUpTo = new long[shardCount];
        this.catchingUp = new boolean[shardCount];
        this.catchupReader = new EventStreamClient[shardCount];
        this.catchupHandler = new GatewayCore.CatchupHandler[shardCount];
    }

    public long sessionId()
    {
        return sessionId;
    }

    public int firmId()
    {
        return firmId;
    }

    public boolean isConnected()
    {
        return outlet != null;
    }

    public long nextExpectedClientSeq()
    {
        return nextExpectedClientSeq;
    }

    public AccessDirectory.Grant grant()
    {
        return grant;
    }
}
