package io.lmb.api;

import org.agrona.DirectBuffer;

/**
 * The deterministic state machine of one shard: risk checks, order books, matching, symbol states.
 *
 * <p>Determinism contract. Given the same sequence of {@link #onCommand} and {@link #onTimer} calls
 * (same bytes, same logPosition, same timestampNs), every instance must emit byte-identical events
 * in the same order and produce a byte-identical snapshot. Implementations therefore must not read
 * the wall clock, use randomness, iterate hash-ordered collections whose order depends on identity
 * hash codes, start threads, or perform I/O. The only notion of time is timestampNs.
 *
 * <p>All methods are called from a single thread.
 */
public interface ShardStateMachine
{
    int shardId();

    /** Called once, before any other method. */
    void bind(EventSink events, TimerScheduler timers);

    /**
     * Apply one committed command. The buffer holds an SBE message starting with its messageHeader
     * (schema io.lmb.sbe, templateId 1..99). Must end by emitting a CommandApplied event.
     *
     * @param logPosition Raft log position of this entry; strictly increasing
     * @param timestampNs cluster time of this entry, epoch nanoseconds; non-decreasing
     */
    void onCommand(DirectBuffer buffer, int offset, int length, long logPosition, long timestampNs);

    /** A timer scheduled through {@link TimerScheduler} fired. Must end by emitting a CommandApplied event. */
    void onTimer(long correlationId, long logPosition, long timestampNs);

    /**
     * Write the full state as one or more chunks. Chunks must not exceed {@link SnapshotSink#MAX_CHUNK_LENGTH}.
     * Emits no events and does not change state.
     */
    void takeSnapshot(SnapshotSink sink);

    /** Restore: chunks arrive in the order they were written, then {@link #onSnapshotLoaded()}. */
    void onSnapshotChunk(DirectBuffer buffer, int offset, int length);

    void onSnapshotLoaded();

    /** eventSeq of the last event emitted (0 if none). Part of the snapshot. */
    long lastEventSeq();
}
