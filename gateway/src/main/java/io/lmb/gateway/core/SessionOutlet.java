package io.lmb.gateway.core;

/**
 * The transport side of a logged-on session (one gRPC stream or one FIX session). Every method is
 * called on the core thread only.
 */
public interface SessionOutlet
{
    enum Transport
    {
        GRPC, FIX
    }

    Transport transport();

    /** Logon accepted: the gRPC outlet sends LogonAck and opens its inbound window. */
    void onLogonAccepted(SessionState session, long nextExpectedClientSeq, int heartbeatIntervalMs);

    /**
     * Whether a report can be written now without growing transport buffers. When false the core
     * stops pushing and re-reads the event stream later (the stream is the buffer).
     */
    boolean isWritable();

    /** @return false if the transport refused it (back pressure); the core will retry from the stream. */
    boolean sendExecutionReport(ReportView report, boolean possDup);

    void sendMassCancelAck(long requestId, int shardId, int canceledCount, int shardsPending, int totalCanceled);

    void sendReject(Reject reason, long refSeq, long refClOrdId, String text);

    /** Core-managed heartbeat (gRPC). FIX heartbeats are Artio's. */
    void sendHeartbeat();

    /** Whether the core runs heartbeats and liveness checks for this transport. */
    boolean coreManagedHeartbeats();

    /** Last server sequence number used on this transport (for status queries). */
    long lastServerSeq();

    /** Core-initiated close. The core detaches the session itself; the outlet must not call back. */
    void disconnect(Reject reason, String text);
}
