package io.lmb.cluster;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.ShardStateMachineFactory;
import io.lmb.common.Topology;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * One member of a shard's Aeron Cluster, fully assembled in this JVM: media driver (embedded unless
 * configured otherwise), Archive, ConsensusModule, ClusteredServiceContainer hosting a
 * {@link ShardClusteredService}, the event stream server and the periodic snapshot trigger.
 */
public final class ClusterNode implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(ClusterNode.class);

    private final NodeConfig config;
    private final EventIndex index = new EventIndex();
    private MediaDriver driver;
    private Archive archive;
    private ConsensusModule consensusModule;
    private ClusteredServiceContainer container;
    private Aeron aeron;
    private EventStore store;
    private ShardClusteredService service;
    private EventStreamServer server;
    private AgentRunner serverRunner;
    private ConsensusModule.Context cmCtx;
    private volatile boolean closed;

    private ClusterNode(final NodeConfig config)
    {
        this.config = config;
    }

    public static ClusterNode launch(final NodeConfig config, final ShardStateMachineFactory factory)
    {
        final ClusterNode node = new ClusterNode(config);
        try
        {
            node.start(factory.create(config.shardId));
            return node;
        }
        catch (final RuntimeException e)
        {
            node.close();
            throw e;
        }
    }

    private void start(final ShardStateMachine machine)
    {
        if (machine.shardId() != config.shardId)
        {
            throw new IllegalArgumentException("machine reports shard " + machine.shardId() + ", node is " + config.shardId);
        }
        final File memberDir = config.memberDir();
        final File archiveDir = new File(memberDir, "archive");
        final File clusterDir = new File(memberDir, "cluster");
        final File eventsDir = new File(memberDir, "events");
        if (!memberDir.isDirectory() && !memberDir.mkdirs())
        {
            throw new IllegalStateException("cannot create " + memberDir);
        }
        final String host = config.host();
        final String name = "shard-" + config.shardId + "-member-" + config.memberId;
        final ErrorHandler errorHandler = t -> LOG.error("{}: Aeron error", name, t);

        final String aeronDir;
        if (config.embeddedDriver)
        {
            aeronDir = config.resolvedAeronDir();
            final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.valueOf(config.driverThreading.trim().toUpperCase()))
                .termBufferSparseFile(true)
                .publicationTermBufferLength(config.driverTermLength)
                .ipcTermBufferLength(config.driverTermLength)
                .sharedIdleStrategy(Idle.of(config.idleStrategy))
                .sharedNetworkIdleStrategy(Idle.of(config.idleStrategy))
                .conductorIdleStrategy(Idle.of(config.idleStrategy))
                .senderIdleStrategy(Idle.of(config.idleStrategy))
                .receiverIdleStrategy(Idle.of(config.idleStrategy))
                .errorHandler(errorHandler);
            driver = MediaDriver.launch(driverCtx);
        }
        else
        {
            aeronDir = config.aeronDir;
            if (aeronDir == null)
            {
                throw new IllegalStateException("lmb.driver.embedded=false requires lmb.aeron.dir");
            }
        }

        final String localControl = "aeron:ipc?term-length=64k";
        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(archiveDir)
            .controlChannel("aeron:udp?endpoint=" + config.endpoint(Topology.ARCHIVE_CONTROL_OFFSET) + "|term-length=64k")
            .localControlChannel(localControl)
            .replicationChannel("aeron:udp?endpoint=" + host + ":0")
            .archiveClientContext(new AeronArchive.Context()
                .controlResponseChannel("aeron:udp?endpoint=" + host + ":0"))
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.valueOf(config.archiveThreading.trim().toUpperCase()))
            .idleStrategySupplier(Idle.supplier(config.idleStrategy))
            .segmentFileLength(Math.max(config.archiveSegmentLength,
                Math.max(config.logTermLength, Math.max(config.eventsTermLength, config.snapshotTermLength))))
            .fileSyncLevel(config.archiveFileSyncLevel)
            .catalogFileSyncLevel(config.archiveFileSyncLevel)
            .deleteArchiveOnStart(false)
            .errorHandler(errorHandler);
        archive = Archive.launch(archiveCtx);

        final AeronArchive.Context localArchive = new AeronArchive.Context()
            .aeronDirectoryName(aeronDir)
            .controlRequestChannel(localControl)
            .controlRequestStreamId(archiveCtx.localControlStreamId())
            .controlResponseChannel(localControl);

        final String snapshotChannel = "aeron:ipc?alias=snapshot|term-length=" + config.snapshotTermLength;
        cmCtx = new ConsensusModule.Context()
            .aeronDirectoryName(aeronDir)
            .clusterId(config.clusterId())
            .clusterMemberId(config.memberId)
            .clusterMembers(config.topology.clusterMembers(config.shardId))
            .clusterDir(clusterDir)
            .clusterClock(new NanosecondClusterClock())
            .ingressChannel("aeron:udp?term-length=" + config.driverTermLength)
            .logChannel("aeron:udp?term-length=" + config.logTermLength)
            .consensusChannel("aeron:udp?term-length=64k")
            .replicationChannel("aeron:udp?endpoint=" + host + ":0")
            .snapshotChannel(snapshotChannel)
            .archiveContext(localArchive.clone())
            .leaderHeartbeatIntervalNs(config.leaderHeartbeatIntervalNs)
            .leaderHeartbeatTimeoutNs(config.leaderHeartbeatTimeoutNs)
            .electionTimeoutNs(config.electionTimeoutNs)
            .startupCanvassTimeoutNs(config.startupCanvassTimeoutNs)
            .sessionTimeoutNs(config.sessionTimeoutNs)
            .maxConcurrentSessions(config.maxClusterSessions)
            .fileSyncLevel(config.clusterFileSyncLevel)
            .idleStrategySupplier(Idle.supplier(config.idleStrategy))
            .deleteDirOnStart(false)
            .errorHandler(errorHandler);
        if (config.clusterSecret != null && !config.clusterSecret.isEmpty())
        {
            final String secret = config.clusterSecret;
            cmCtx.authenticatorSupplier(() -> new SharedSecretAuthenticator(secret));
        }
        consensusModule = ConsensusModule.launch(cmCtx);

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir).clientName("lmb-node-" + name)
            .errorHandler(errorHandler));
        store = new EventStore(aeron, localArchive.clone().aeron(aeron).ownsAeronClient(false), eventsDir.toPath(),
            config.eventsTermLength, config.eventIndexInterval, index, Idle.of(config.idleStrategy));
        service = new ShardClusteredService(machine, store, index, config.memberId);

        container = ClusteredServiceContainer.launch(new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .clusterId(config.clusterId())
            .clusterDir(clusterDir)
            .archiveContext(localArchive.clone())
            .snapshotChannel(snapshotChannel)
            .clusteredService(service)
            .idleStrategySupplier(Idle.supplier(config.idleStrategy))
            .errorHandler(errorHandler));

        server = new EventStreamServer(aeron, localArchive.clone().aeron(aeron).ownsAeronClient(false), index, config);
        final Agent agent = config.snapshotIntervalNs > 0 ?
            new CompositeAgent(server, new SnapshotTrigger(config.snapshotIntervalNs)) : server;
        serverRunner = new AgentRunner(Idle.of(config.idleStrategy), errorHandler, null, agent);
        AgentRunner.startOnThread(serverRunner);
        LOG.info("{} started: members={} dir={} aeronDir={} embeddedDriver={}", name,
            config.topology.membersPerShard(), memberDir, aeronDir, config.embeddedDriver);
    }

    /** Leader-only periodic snapshot through the cluster control toggle. */
    private final class SnapshotTrigger implements Agent
    {
        private final long intervalNs;
        private long nextNs;

        SnapshotTrigger(final long intervalNs)
        {
            this.intervalNs = intervalNs;
            this.nextNs = System.nanoTime() + intervalNs;
        }

        @Override
        public int doWork()
        {
            final long now = System.nanoTime();
            if (now - nextNs < 0)
            {
                return 0;
            }
            nextNs = now + intervalNs;
            if (role() == Cluster.Role.LEADER && triggerSnapshot())
            {
                LOG.info("periodic snapshot requested");
                return 1;
            }
            return 0;
        }

        @Override
        public String roleName()
        {
            return "lmb-snapshot-trigger";
        }
    }

    public NodeConfig config()
    {
        return config;
    }

    public EventIndex eventIndex()
    {
        return index;
    }

    public ShardClusteredService service()
    {
        return service;
    }

    public EventStreamServer eventServer()
    {
        return server;
    }

    public Cluster.Role role()
    {
        if (closed || cmCtx == null || cmCtx.clusterNodeRoleCounter() == null || cmCtx.clusterNodeRoleCounter().isClosed())
        {
            return Cluster.Role.FOLLOWER;
        }
        return Cluster.Role.get(cmCtx.clusterNodeRoleCounter().get());
    }

    public boolean isLeader()
    {
        return role() == Cluster.Role.LEADER;
    }

    public long commitPosition()
    {
        return counter(cmCtx == null ? null : cmCtx.commitPositionCounter());
    }

    public long electionCount()
    {
        return counter(cmCtx == null ? null : cmCtx.electionCounter());
    }

    public long snapshotCount()
    {
        return counter(cmCtx == null ? null : cmCtx.snapshotCounter());
    }

    /** Consensus module state code (see ConsensusModule.State). */
    public long moduleState()
    {
        return counter(cmCtx == null ? null : cmCtx.moduleStateCounter());
    }

    private long counter(final io.aeron.Counter c)
    {
        return c == null || c.isClosed() || closed ? -1 : c.get();
    }

    /** Requests a snapshot. Only has an effect on the leader. Returns false if another action is pending. */
    public boolean triggerSnapshot()
    {
        return !closed && ClusterControl.ToggleState.SNAPSHOT.toggle(cmCtx.controlToggleCounter());
    }

    public boolean isClosed()
    {
        return closed;
    }

    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        CloseHelper.quietClose(serverRunner);
        CloseHelper.quietClose(container);
        if (store != null)
        {
            try
            {
                store.close();
            }
            catch (final RuntimeException e)
            {
                LOG.warn("closing event store: {}", e.toString());
            }
        }
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(consensusModule);
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(driver);
        LOG.info("shard-{}-member-{} closed", config.shardId, config.memberId);
    }
}
