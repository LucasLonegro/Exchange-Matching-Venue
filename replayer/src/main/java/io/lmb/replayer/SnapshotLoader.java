package io.lmb.replayer;

import io.lmb.api.ShardStateMachine;
import io.lmb.cluster.RaftLogReader;
import org.agrona.DirectBuffer;

import java.nio.ByteOrder;
import java.util.List;

/**
 * Restores a machine from a cluster member's service snapshot (SNAPSHOT entry with serviceId 0 in
 * {@code recording.log}), read offline from the member's archive. The service writes, one message each,
 * HEADER(int type=1, int version=1, int shardId, long lastEventSeq, long logPosition), CHUNK(int type=2, machine
 * bytes)* and END(int type=3, int chunkCount, long lastEventSeq) (docs/modules/cluster.md section 7); the
 * container's own messages (markers, client sessions) around them are skipped.
 */
public final class SnapshotLoader
{
    static final int TYPE_HEADER = 1;
    static final int TYPE_CHUNK = 2;
    static final int TYPE_END = 3;
    static final int VERSION = 1;

    /** What was restored. */
    public record Loaded(long logPosition, long lastEventSeq, int chunks)
    {
    }

    private SnapshotLoader()
    {
    }

    /** The latest service snapshot at or before {@code maxPosition}, or null. */
    public static RaftLogReader.Snapshot latestServiceSnapshot(final List<RaftLogReader.Snapshot> snapshots, final long maxPosition)
    {
        RaftLogReader.Snapshot best = null;
        for (final RaftLogReader.Snapshot s : snapshots)
        {
            if (s.serviceId() == 0 && s.logPosition() <= maxPosition && (best == null || s.logPosition() > best.logPosition()))
            {
                best = s;
            }
        }
        return best;
    }

    public static Loaded load(final ArchiveFiles archive, final RaftLogReader.Snapshot snapshot, final ShardStateMachine machine)
    {
        final State state = new State(machine);
        try (ArchiveFiles.Cursor cursor = archive.cursor(snapshot.recordingId(), 0))
        {
            while (!state.ended && cursor.next(state))
            {
                // state consumes the messages
            }
        }
        if (!state.ended)
        {
            throw new IllegalStateException("snapshot recording " + snapshot.recordingId() + " in " + archive.dir() +
                (state.headerSeen ? " ended before its END message" : " has no LMB snapshot HEADER"));
        }
        machine.onSnapshotLoaded();
        if (machine.lastEventSeq() != state.lastEventSeq)
        {
            throw new IllegalStateException("machine restored lastEventSeq " + machine.lastEventSeq() +
                " but the snapshot recorded " + state.lastEventSeq);
        }
        if (state.logPosition != snapshot.logPosition())
        {
            throw new IllegalStateException("snapshot HEADER logPosition " + state.logPosition +
                " differs from recording.log " + snapshot.logPosition());
        }
        return new Loaded(state.logPosition, state.lastEventSeq, state.chunks);
    }

    private static final class State implements ArchiveFiles.MessageHandler
    {
        final ShardStateMachine machine;
        boolean headerSeen;
        boolean ended;
        long lastEventSeq;
        long logPosition;
        int chunks;

        State(final ShardStateMachine machine)
        {
            this.machine = machine;
        }

        @Override
        public void onMessage(final DirectBuffer buffer, final int offset, final int length, final long position)
        {
            if (length < 4)
            {
                return;
            }
            final int type = buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
            if (!headerSeen)
            {
                // container messages start with an SBE header whose first int is never 1 (templateId != 0)
                if (type == TYPE_HEADER && length >= 28)
                {
                    final int version = buffer.getInt(offset + 4, ByteOrder.LITTLE_ENDIAN);
                    final int shard = buffer.getInt(offset + 8, ByteOrder.LITTLE_ENDIAN);
                    if (version != VERSION || shard != machine.shardId())
                    {
                        throw new IllegalStateException("snapshot version " + version + " shard " + shard +
                            " (expected version " + VERSION + " shard " + machine.shardId() + ")");
                    }
                    lastEventSeq = buffer.getLong(offset + 12, ByteOrder.LITTLE_ENDIAN);
                    logPosition = buffer.getLong(offset + 20, ByteOrder.LITTLE_ENDIAN);
                    headerSeen = true;
                }
                return;
            }
            if (ended)
            {
                return;
            }
            if (type == TYPE_CHUNK)
            {
                machine.onSnapshotChunk(buffer, offset + 4, length - 4);
                chunks++;
            }
            else if (type == TYPE_END)
            {
                final int count = buffer.getInt(offset + 4, ByteOrder.LITTLE_ENDIAN);
                if (count != chunks)
                {
                    throw new IllegalStateException("snapshot has " + chunks + " chunks, END says " + count);
                }
                ended = true;
            }
            else
            {
                throw new IllegalStateException("unexpected snapshot message type " + type);
            }
        }
    }
}
