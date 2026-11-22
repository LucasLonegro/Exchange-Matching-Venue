package io.lmb.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackupMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.lmb.cluster.client.SharedSecret;
import io.lmb.common.Topology;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Aeron {@link ClusterBackup} for one shard, with its own embedded driver and Archive. It periodically
 * queries the cluster, copies the latest snapshots and records the live log into its local Archive,
 * producing a cluster directory + archive from which a replacement cluster can be started in another
 * region. It is asynchronous (RPO of seconds on region loss) and does not run the state machine or
 * materialise the event stream.
 */
public final class ClusterBackupNode implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(ClusterBackupNode.class);

    private final ClusterBackupMediaDriver backup;
    private final int shardId;

    private ClusterBackupNode(final ClusterBackupMediaDriver backup, final int shardId)
    {
        this.backup = backup;
        this.shardId = shardId;
    }

    /** Backup ports: base = {@code lmb.backup.basePort} (default 20000 + 1000*shard + 900). */
    public static int basePort(final Topology t, final int shardId)
    {
        return t.getInt("lmb.backup." + shardId + ".basePort", 20000 + 1000 * shardId + 900);
    }

    public static ClusterBackupNode launch(final NodeConfig config, final String host, final int basePort)
    {
        return launch(config, host, basePort, ctx -> {});
    }

    /**
     * Launches with {@code customizer} applied to the ClusterBackup context last (tests shorten timings).
     * Keys: {@code lmb.backup.coolDownMs} (default 5000; Aeron's 30 s would idle that long after every
     * reset, e.g. each time the leader answers a FOLLOWER-sourced query), {@code lmb.backup.queryIntervalSec}
     * (default 3600; how often new snapshots are fetched, the live log is followed continuously),
     * {@code lmb.backup.progressTimeoutMs} (default 10000).
     */
    public static ClusterBackupNode launch(
        final NodeConfig config, final String host, final int basePort,
        final java.util.function.Consumer<ClusterBackup.Context> customizer)
    {
        final Topology t = config.topology;
        final int shard = config.shardId;
        final File dir = new File(new File(config.dataDir, "cluster-backup"), "shard-" + shard);
        final ErrorHandler errorHandler = e -> LOG.error("backup shard {}: Aeron error", shard, e);
        // never the member's own driver directory: the embedded driver deletes its directory on start
        final String aeronDir = t.get("lmb.backup.aeron.dir",
            new File(new File(config.resolvedAeronDir()).getAbsoluteFile().getParentFile(), "lmb-backup-" + shard).getPath());

        final StringBuilder consensus = new StringBuilder();
        for (int m = 0; m < t.membersPerShard(); m++)
        {
            consensus.append(m == 0 ? "" : ",").append(t.memberEndpoint(shard, m, Topology.CONSENSUS_OFFSET));
        }
        final String localControl = "aeron:ipc?term-length=64k";
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .publicationTermBufferLength(config.driverTermLength)
            .ipcTermBufferLength(config.driverTermLength)
            .sharedIdleStrategy(Idle.of(config.idleStrategy))
            .errorHandler(errorHandler);
        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(dir, "archive"))
            .controlChannel("aeron:udp?endpoint=" + host + ":" + (basePort + Topology.ARCHIVE_CONTROL_OFFSET))
            .localControlChannel(localControl)
            .replicationChannel("aeron:udp?endpoint=" + host + ":0")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .idleStrategySupplier(Idle.supplier(config.idleStrategy))
            .segmentFileLength(Math.max(config.archiveSegmentLength, config.logTermLength))
            .fileSyncLevel(config.archiveFileSyncLevel)
            .catalogFileSyncLevel(config.archiveFileSyncLevel)
            .errorHandler(errorHandler);
        final ClusterBackup.Context backupCtx = new ClusterBackup.Context()
            .aeronDirectoryName(aeronDir)
            .clusterDir(new File(dir, "cluster"))
            .clusterConsensusEndpoints(consensus.toString())
            .consensusChannel("aeron:udp?endpoint=" + host + ":" + (basePort + Topology.CONSENSUS_OFFSET))
            .catchupEndpoint(host + ":" + (basePort + Topology.BACKUP_OFFSET))
            .clusterArchiveContext(new AeronArchive.Context()
                .controlRequestChannel("aeron:udp?term-length=64k")
                .controlResponseChannel("aeron:udp?endpoint=" + host + ":0"))
            .archiveContext(new AeronArchive.Context()
                .controlRequestChannel(localControl)
                .controlRequestStreamId(archiveCtx.localControlStreamId())
                .controlResponseChannel(localControl))
            .credentialsSupplier(SharedSecret.credentials(config.clusterSecret))
            .sourceType(ClusterBackup.SourceType.valueOf(t.get("lmb.backup.sourceType", "FOLLOWER").trim().toUpperCase()))
            .eventsListener(new LoggingListener(shard))
            .clusterBackupCoolDownIntervalNs(TimeUnit.MILLISECONDS.toNanos(t.getLong("lmb.backup.coolDownMs", 5000)))
            .clusterBackupIntervalNs(TimeUnit.SECONDS.toNanos(t.getLong("lmb.backup.queryIntervalSec", 3600)))
            .clusterBackupProgressTimeoutNs(TimeUnit.MILLISECONDS.toNanos(t.getLong("lmb.backup.progressTimeoutMs", 10000)))
            .errorHandler(errorHandler);
        customizer.accept(backupCtx);
        final ClusterBackupMediaDriver backup = ClusterBackupMediaDriver.launch(driverCtx, archiveCtx, backupCtx);
        LOG.info("cluster backup for shard {} started: consensus={} dir={}", shard, consensus, dir);
        return new ClusterBackupNode(backup, shard);
    }

    /** Logs the backup's progress; failures at WARN (the agent resets and retries after them). */
    private record LoggingListener(int shard) implements io.aeron.cluster.ClusterBackupEventsListener
    {
        @Override
        public void onBackupQuery()
        {
            LOG.debug("backup shard {}: query", shard);
        }

        @Override
        public void onPossibleFailure(final Exception ex)
        {
            LOG.warn("backup shard {}: possible failure, resetting: {}", shard, ex.toString());
        }

        @Override
        public void onBackupResponse(
            final io.aeron.cluster.ClusterMember[] members, final io.aeron.cluster.ClusterMember logSource,
            final java.util.List<io.aeron.cluster.RecordingLog.Snapshot> snapshotsToRetrieve)
        {
            LOG.info("backup shard {}: response, log source member {} archive {}, {} snapshot(s) to retrieve",
                shard, logSource.id(), logSource.archiveEndpoint(), snapshotsToRetrieve.size());
        }

        @Override
        public void onUpdatedRecordingLog(
            final io.aeron.cluster.RecordingLog recordingLog,
            final java.util.List<io.aeron.cluster.RecordingLog.Snapshot> snapshotsRetrieved)
        {
            LOG.info("backup shard {}: recording log updated, {} snapshot(s) retrieved", shard, snapshotsRetrieved.size());
        }

        @Override
        public void onLiveLogProgress(final long recordingId, final long recordingPosCounterId, final long logPosition)
        {
            LOG.debug("backup shard {}: live log recording {} at {}", shard, recordingId, logPosition);
        }
    }

    public int shardId()
    {
        return shardId;
    }

    /** Current ClusterBackup state (BACKUP_QUERY, SNAPSHOT_RETRIEVE, ..., BACKING_UP), or null when closed. */
    public ClusterBackup.State state()
    {
        final var counter = backup.clusterBackup().context().stateCounter();
        return counter == null || counter.isClosed() ? null : ClusterBackup.State.get(counter);
    }

    /** Log position the backup has recorded from the live log, or -1. */
    public long liveLogPosition()
    {
        final var counter = backup.clusterBackup().context().liveLogPositionCounter();
        return counter == null || counter.isClosed() ? -1 : counter.get();
    }

    /** Snapshots retrieved from the cluster so far. */
    public long snapshotsRetrieved()
    {
        final var counter = backup.clusterBackup().context().snapshotRetrieveCounter();
        return counter == null || counter.isClosed() ? -1 : counter.get();
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(backup);
    }
}
