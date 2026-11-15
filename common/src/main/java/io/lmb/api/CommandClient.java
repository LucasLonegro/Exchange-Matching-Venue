package io.lmb.api;

import org.agrona.DirectBuffer;

/**
 * Submits commands to one shard's Raft log. Single-threaded: use each instance from one thread.
 *
 * <p>Delivery is at-most-once per offer: a command accepted by {@link #offer} can still be lost if
 * the leader fails before committing it. Callers that need a command applied must watch the event
 * stream for its effect and resubmit (orders: with possResend = TRUE; the state machine dedupes).
 */
public interface CommandClient extends AutoCloseable
{
    int shardId();

    /**
     * Non-blocking. The buffer holds one SBE command (messageHeader + body).
     *
     * @return true if handed to the cluster; false on back pressure or while disconnected (retry later)
     */
    boolean offer(DirectBuffer buffer, int offset, int length);

    /** Duty cycle: keep-alives, leader changes, reconnects. Call frequently. Returns work done. */
    int poll();

    boolean isConnected();

    @Override
    void close();
}
