package io.lmb.posttrade;

import org.agrona.DirectBuffer;

/**
 * One post-trade consumer of the shards' event streams. All methods are called on the sink's own
 * {@link SinkRunner} thread. The runner guarantees that {@link #onEvent} sees, per shard, exactly the
 * events after the cursors returned by {@link #open}, once each and in eventSeq order (redeliveries are
 * dropped, a gap fails the sink).
 *
 * <p>If any method throws, the runner calls {@link #close(boolean) close(false)}, waits, and calls
 * {@link #open} again, so a sink only needs to be correct from its durable cursor.
 */
public interface PostTradeSink
{
    String name();

    /**
     * Acquires resources and returns, per shard, the eventSeq of the last event already processed durably
     * (0 = none: start at the beginning of the retained stream).
     */
    long[] open(int shardCount) throws Exception;

    void onEvent(int shardId, long eventSeq, DirectBuffer buffer, int offset, int length) throws Exception;

    /** Periodic duty: commits, flushes, checkpoints, protocol work. Returns the amount of work done. */
    int doWork(long nowMs) throws Exception;

    /**
     * Releases resources. {@code clean} = normal stop (persist the final cursor); false = after a failure
     * (do not persist anything derived from the failed state).
     */
    void close(boolean clean);
}
