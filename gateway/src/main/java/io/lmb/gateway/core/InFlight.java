package io.lmb.gateway.core;

import org.agrona.concurrent.UnsafeBuffer;

/**
 * One command on its way to a shard: queued in a lane (not yet offered) or sent and waiting for its
 * effect in the event stream. Holds the encoded SBE bytes so it can be resubmitted verbatim with
 * possResend flipped to TRUE.
 */
final class InFlight
{
    OrderRequest.Kind kind;
    int firmId;
    long clOrdId;
    long sessionId;
    long requestId;
    long clientSeq;
    /** Gateway-generated (cancel-on-disconnect): never expires and is never abandoned. */
    boolean internal;
    boolean urgent;
    boolean sent;
    boolean done;
    int attempts;
    long enqueuedNs;
    long deadlineNs;
    SessionState session;

    final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
    int length;
    /** Absolute offset of the possResend byte, -1 when the command has none (MassCancel). */
    int possResendOffset = -1;

    void markPossResend()
    {
        if (possResendOffset >= 0)
        {
            buffer.putByte(possResendOffset, (byte)1);
        }
    }
}
