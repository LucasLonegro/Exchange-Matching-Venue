package io.lmb.cluster;

import io.lmb.cluster.client.ClientConfig;
import io.lmb.common.Topology;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Configuration of one cluster member. {@link #from(Topology)} reads every key from the topology,
 * system properties or environment (docs/modules/cluster.md lists them); the defaults are the local
 * profile (small buffers, backoff idling, no fsync). Fields are public so tests can adjust them.
 */
public final class NodeConfig
{
    public Topology topology;
    public int shardId;
    public int memberId;

    /** Root of persistent data; this member uses {@code <dataDir>/cluster-node/shard-S-member-M}. */
    public File dataDir = new File("./data");
    /** Aeron directory. For an embedded driver null means a per-member default. */
    public String aeronDir;
    public boolean embeddedDriver = true;
    /** SHARED, SHARED_NETWORK or DEDICATED. */
    public String driverThreading = "SHARED";
    /** Idle strategy for driver, consensus module, service and event server: backoff, sleep, yield, busy. */
    public String idleStrategy = "backoff";
    /** SHARED or DEDICATED. */
    public String archiveThreading = "SHARED";

    /** Default term length of the driver's publications (control and consensus channels). */
    public int driverTermLength = 256 * 1024;
    public int logTermLength = 1024 * 1024;
    public int eventsTermLength = 1024 * 1024;
    public int snapshotTermLength = 1024 * 1024;
    public int archiveSegmentLength = 64 * 1024 * 1024;
    /** 0 = no fsync, 1 = fsync data, 2 = fsync data and metadata. */
    public int archiveFileSyncLevel = 0;
    public int clusterFileSyncLevel = 0;

    public long leaderHeartbeatIntervalNs = TimeUnit.MILLISECONDS.toNanos(200);
    public long leaderHeartbeatTimeoutNs = TimeUnit.SECONDS.toNanos(3);
    public long electionTimeoutNs = TimeUnit.SECONDS.toNanos(1);
    public long startupCanvassTimeoutNs = TimeUnit.SECONDS.toNanos(10);
    public long sessionTimeoutNs = TimeUnit.SECONDS.toNanos(10);
    public int maxClusterSessions = 128;
    /** Leader takes a snapshot this often; 0 disables. */
    public long snapshotIntervalNs = 0;
    /** Ingress shared secret; null disables authentication. */
    public String clusterSecret;

    /** One index entry every this many events. */
    public int eventIndexInterval = 64;
    public long eventHeartbeatIntervalNs = TimeUnit.MILLISECONDS.toNanos(250);
    public long eventSessionTimeoutNs = TimeUnit.SECONDS.toNanos(10);
    public int eventMaxSessions = 256;

    /** Event segment retention (closed segments only); see {@link EventRetention}. */
    public EventRetention eventRetention = EventRetention.NONE;

    /** Prometheus endpoint; false disables it. */
    public boolean metricsEnabled = true;

    public static NodeConfig from(final Topology t)
    {
        final NodeConfig c = new NodeConfig();
        c.topology = t;
        c.shardId = Integer.parseInt(t.get("lmb.shard.id", "0").trim());
        c.memberId = Integer.parseInt(t.get("lmb.member.id", "0").trim());
        c.dataDir = new File(t.get("lmb.data.dir", "./data"));
        c.aeronDir = t.get("lmb.aeron.dir", null);
        c.embeddedDriver = t.getBool("lmb.driver.embedded", true);
        c.driverThreading = t.get("lmb.driver.threading", c.driverThreading);
        c.idleStrategy = t.get("lmb.cluster.idle", c.idleStrategy);
        c.archiveThreading = t.get("lmb.archive.threading", c.archiveThreading);
        c.driverTermLength = ClientConfig.parseSize(t.get("lmb.driver.termLength", "256k"));
        c.logTermLength = ClientConfig.parseSize(t.get("lmb.cluster.logTermLength", "1m"));
        c.eventsTermLength = ClientConfig.parseSize(t.get("lmb.events.termLength", "1m"));
        c.snapshotTermLength = ClientConfig.parseSize(t.get("lmb.cluster.snapshotTermLength", "1m"));
        c.archiveSegmentLength = ClientConfig.parseSize(t.get("lmb.archive.segmentLength", "64m"));
        c.archiveFileSyncLevel = t.getInt("lmb.archive.fileSyncLevel", 0);
        c.clusterFileSyncLevel = t.getInt("lmb.cluster.fileSyncLevel", 0);
        c.leaderHeartbeatIntervalNs = ms(t.getLong("lmb.cluster.leaderHeartbeatIntervalMs", 200));
        c.leaderHeartbeatTimeoutNs = ms(t.getLong("lmb.cluster.leaderHeartbeatTimeoutMs", 3000));
        c.electionTimeoutNs = ms(t.getLong("lmb.cluster.electionTimeoutMs", 1000));
        c.startupCanvassTimeoutNs = ms(t.getLong("lmb.cluster.startupCanvassTimeoutMs", 10000));
        c.sessionTimeoutNs = ms(t.getLong("lmb.cluster.sessionTimeoutMs", 10000));
        c.maxClusterSessions = t.getInt("lmb.cluster.maxSessions", 128);
        c.snapshotIntervalNs = TimeUnit.SECONDS.toNanos(t.getLong("lmb.cluster.snapshotIntervalSec", 0));
        c.clusterSecret = t.get("lmb.cluster.secret", null);
        c.eventIndexInterval = t.getInt("lmb.events.indexInterval", 64);
        c.eventHeartbeatIntervalNs = ms(t.getLong("lmb.events.serverHeartbeatMs", 250));
        c.eventSessionTimeoutNs = ms(t.getLong("lmb.events.sessionTimeoutMs", 10000));
        c.eventMaxSessions = t.getInt("lmb.events.maxSessions", 256);
        c.eventRetention = new EventRetention(
            t.getInt("lmb.events.retention.maxSegments", 0),
            TimeUnit.SECONDS.toMillis(t.getLong("lmb.events.retention.maxAgeSec", 0)),
            TimeUnit.SECONDS.toNanos(t.getLong("lmb.events.retention.intervalSec", 60)));
        c.metricsEnabled = t.getBool("lmb.cluster.metrics.enabled", true);
        return c;
    }

    public static NodeConfig from(final Topology t, final int shardId, final int memberId)
    {
        final NodeConfig c = from(t);
        c.shardId = shardId;
        c.memberId = memberId;
        return c;
    }

    private static long ms(final long v)
    {
        return TimeUnit.MILLISECONDS.toNanos(v);
    }

    public File memberDir()
    {
        return new File(new File(dataDir, "cluster-node"), "shard-" + shardId + "-member-" + memberId);
    }

    public String host()
    {
        return topology.memberHost(shardId, memberId);
    }

    public int port(final int offset)
    {
        return topology.memberPort(shardId, memberId, offset);
    }

    public String endpoint(final int offset)
    {
        return topology.memberEndpoint(shardId, memberId, offset);
    }

    /** Aeron counters namespace inside the driver: unique per (shard, member) so members may share a driver. */
    public int clusterId()
    {
        return shardId * 16 + memberId;
    }

    public String resolvedAeronDir()
    {
        if (aeronDir != null)
        {
            return aeronDir;
        }
        final File shm = new File("/dev/shm");
        final String base = shm.isDirectory() ? shm.getPath() : System.getProperty("java.io.tmpdir");
        return new File(base, "lmb-node-" + shardId + "-" + memberId).getPath();
    }
}
