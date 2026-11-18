package io.lmb.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.lmb.api.EventSink;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.SnapshotSink;
import io.lmb.api.TimerScheduler;
import org.HdrHistogram.Recorder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

/**
 * The clustered service of a shard member: feeds committed ingress messages and timer events to the
 * {@link ShardStateMachine} with the entry's log position and cluster timestamp (ns), implements
 * {@link TimerScheduler} on cluster timers, takes and loads snapshots, and hands every event to the
 * {@link EventStore}.
 *
 * <p>Snapshot layout (one message each): HEADER(type=1, version, shardId, lastEventSeq, logPosition),
 * CHUNK(type=2, machine bytes)*, END(type=3, chunkCount, lastEventSeq).
 */
public final class ShardClusteredService implements ClusteredService, TimerScheduler, EventSink
{
    private static final Logger LOG = LoggerFactory.getLogger(ShardClusteredService.class);
    static final int SNAPSHOT_VERSION = 1;
    static final int TYPE_HEADER = 1;
    static final int TYPE_CHUNK = 2;
    static final int TYPE_END = 3;

    private final ShardStateMachine machine;
    private final EventStore store;
    private final EventIndex index;
    private final int memberId;
    private final UnsafeBuffer snapshotBuffer = new UnsafeBuffer(
        ByteBuffer.allocateDirect(SnapshotSink.MAX_CHUNK_LENGTH + 64).order(ByteOrder.LITTLE_ENDIAN));
    private final Recorder serviceTimeNs = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);

    private Cluster cluster;
    private IdleStrategy idle;
    private volatile long lastEventSeq;
    private volatile long lastLogPosition;
    private volatile long commandsApplied;
    private volatile long snapshotsTaken;
    private volatile boolean snapshotLoaded;

    // snapshot load state
    private boolean loadEnd;
    private int loadChunks;
    private long loadLastEventSeq;
    private ExclusivePublication snapshotPublication;
    private int snapshotChunks;

    public ShardClusteredService(
        final ShardStateMachine machine, final EventStore store, final EventIndex index, final int memberId)
    {
        this.machine = machine;
        this.store = store;
        this.index = index;
        this.memberId = memberId;
    }

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        this.idle = cluster.idleStrategy();
        machine.bind(this, this);
        if (snapshotImage != null)
        {
            loadSnapshot(snapshotImage);
        }
        lastEventSeq = machine.lastEventSeq();
        store.open(machine.lastEventSeq());
    }

    private void loadSnapshot(final Image image)
    {
        final ImageFragmentAssembler assembler = new ImageFragmentAssembler(this::onSnapshotFragment);
        idle.reset();
        while (!loadEnd)
        {
            final int n = image.poll(assembler, 16);
            if (n == 0)
            {
                if (image.isEndOfStream() || image.isClosed())
                {
                    throw new IllegalStateException("snapshot ended before its END marker");
                }
                idle.idle();
            }
        }
        machine.onSnapshotLoaded();
        snapshotLoaded = true;
        if (machine.lastEventSeq() != loadLastEventSeq)
        {
            LOG.error("machine restored lastEventSeq {} but snapshot recorded {}", machine.lastEventSeq(), loadLastEventSeq);
        }
        LOG.info("snapshot loaded: {} chunks, lastEventSeq {}", loadChunks, machine.lastEventSeq());
    }

    private void onSnapshotFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final int type = buffer.getInt(offset, ByteOrder.LITTLE_ENDIAN);
        switch (type)
        {
            case TYPE_HEADER ->
            {
                final int version = buffer.getInt(offset + 4, ByteOrder.LITTLE_ENDIAN);
                final int shard = buffer.getInt(offset + 8, ByteOrder.LITTLE_ENDIAN);
                if (version != SNAPSHOT_VERSION || shard != machine.shardId())
                {
                    throw new IllegalStateException("snapshot version " + version + " shard " + shard +
                        " cannot be loaded by shard " + machine.shardId());
                }
                loadLastEventSeq = buffer.getLong(offset + 12, ByteOrder.LITTLE_ENDIAN);
            }
            case TYPE_CHUNK ->
            {
                machine.onSnapshotChunk(buffer, offset + 4, length - 4);
                loadChunks++;
            }
            case TYPE_END ->
            {
                final int chunks = buffer.getInt(offset + 4, ByteOrder.LITTLE_ENDIAN);
                if (chunks != loadChunks)
                {
                    throw new IllegalStateException("snapshot has " + loadChunks + " chunks, END says " + chunks);
                }
                loadEnd = true;
            }
            default -> throw new IllegalStateException("unknown snapshot message type " + type);
        }
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
    }

    @Override
    public void onSessionMessage(
        final ClientSession session, final long timestamp, final DirectBuffer buffer, final int offset,
        final int length, final Header header)
    {
        final long start = System.nanoTime();
        final long logPosition = cluster.logPosition();
        machine.onCommand(buffer, offset, length, logPosition, timestamp);
        lastLogPosition = logPosition;
        commandsApplied++;
        serviceTimeNs.recordValue(Math.min(System.nanoTime() - start, TimeUnit.SECONDS.toNanos(10)));
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp)
    {
        final long logPosition = cluster.logPosition();
        machine.onTimer(correlationId, logPosition, timestamp);
        lastLogPosition = logPosition;
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication publication)
    {
        snapshotPublication = publication;
        snapshotChunks = 0;
        snapshotBuffer.putInt(0, TYPE_HEADER, ByteOrder.LITTLE_ENDIAN);
        snapshotBuffer.putInt(4, SNAPSHOT_VERSION, ByteOrder.LITTLE_ENDIAN);
        snapshotBuffer.putInt(8, machine.shardId(), ByteOrder.LITTLE_ENDIAN);
        snapshotBuffer.putLong(12, machine.lastEventSeq(), ByteOrder.LITTLE_ENDIAN);
        snapshotBuffer.putLong(20, cluster.logPosition(), ByteOrder.LITTLE_ENDIAN);
        offerSnapshot(28);
        machine.takeSnapshot(this::writeChunk);
        snapshotBuffer.putInt(0, TYPE_END, ByteOrder.LITTLE_ENDIAN);
        snapshotBuffer.putInt(4, snapshotChunks, ByteOrder.LITTLE_ENDIAN);
        snapshotBuffer.putLong(8, machine.lastEventSeq(), ByteOrder.LITTLE_ENDIAN);
        offerSnapshot(16);
        snapshotPublication = null;
        snapshotsTaken++;
        LOG.info("snapshot taken at log position {}: {} chunks, lastEventSeq {}",
            cluster.logPosition(), snapshotChunks, machine.lastEventSeq());
    }

    private void writeChunk(final DirectBuffer buffer, final int offset, final int length)
    {
        if (length > SnapshotSink.MAX_CHUNK_LENGTH)
        {
            throw new IllegalArgumentException("snapshot chunk of " + length + " bytes exceeds " + SnapshotSink.MAX_CHUNK_LENGTH);
        }
        snapshotBuffer.putInt(0, TYPE_CHUNK, ByteOrder.LITTLE_ENDIAN);
        snapshotBuffer.putBytes(4, buffer, offset, length);
        offerSnapshot(4 + length);
        snapshotChunks++;
    }

    private void offerSnapshot(final int length)
    {
        idle.reset();
        long result;
        while ((result = snapshotPublication.offer(snapshotBuffer, 0, length)) < 0)
        {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED ||
                result == Publication.NOT_CONNECTED)
            {
                throw new IllegalStateException("snapshot publication failed: " + result);
            }
            idle.idle();
        }
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole)
    {
        index.isLeader(newRole == Cluster.Role.LEADER);
        if (newRole == Cluster.Role.LEADER)
        {
            index.leaderMemberId(memberId);
        }
    }

    @Override
    public void onNewLeadershipTermEvent(
        final long leadershipTermId, final long logPosition, final long timestamp, final long termBaseLogPosition,
        final int leaderMemberId, final int logSessionId, final TimeUnit timeUnit, final int appVersion)
    {
        if (timeUnit != TimeUnit.NANOSECONDS)
        {
            throw new IllegalStateException("cluster time unit must be NANOSECONDS, got " + timeUnit);
        }
        index.leaderMemberId(leaderMemberId);
    }

    @Override
    public void onTerminate(final Cluster cluster)
    {
        store.close();
    }

    // ---------------- EventSink ----------------

    @Override
    public void onEvent(final DirectBuffer buffer, final int offset, final int length)
    {
        store.append(buffer, offset, length);
        lastEventSeq = io.lmb.common.Events.eventSeq(buffer, offset);
    }

    // ---------------- TimerScheduler ----------------

    @Override
    public void schedule(final long correlationId, final long deadlineNs)
    {
        idle.reset();
        while (!cluster.scheduleTimer(correlationId, deadlineNs))
        {
            idle.idle();
        }
    }

    @Override
    public void cancel(final long correlationId)
    {
        cluster.cancelTimer(correlationId);
    }

    // ---------------- observability (any thread) ----------------

    public long lastEventSeq()
    {
        return lastEventSeq;
    }

    public long lastLogPosition()
    {
        return lastLogPosition;
    }

    public long commandsApplied()
    {
        return commandsApplied;
    }

    public long snapshotsTaken()
    {
        return snapshotsTaken;
    }

    public boolean snapshotLoaded()
    {
        return snapshotLoaded;
    }

    public Recorder serviceTimeRecorder()
    {
        return serviceTimeNs;
    }

    EventStore store()
    {
        return store;
    }
}
