package io.lmb.api;

import org.agrona.DirectBuffer;

/**
 * Reads one shard's event stream. Delivers every event with eventSeq greater than the starting
 * point exactly once and in eventSeq order, across node failovers and reconnects.
 * Single-threaded: use each instance from one thread.
 */
public interface EventStreamClient extends AutoCloseable
{
    @FunctionalInterface
    interface EventHandler
    {
        /** One SBE event message (messageHeader + body). The buffer is only valid during the call. */
        void onEvent(DirectBuffer buffer, int offset, int length);
    }

    int shardId();

    /** Non-blocking. Returns the number of events delivered (0 if none or while reconnecting). */
    int poll(EventHandler handler, int limit);

    /** eventSeq of the last event delivered, or the starting point if none yet. */
    long lastEventSeq();

    boolean isConnected();

    @Override
    void close();
}
