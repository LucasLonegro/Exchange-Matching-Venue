package io.lmb.api;

import org.agrona.DirectBuffer;

@FunctionalInterface
public interface SnapshotSink
{
    /** Chunks larger than this cannot be written to a cluster snapshot in one message. */
    int MAX_CHUNK_LENGTH = 32 * 1024;

    void write(DirectBuffer buffer, int offset, int length);
}
