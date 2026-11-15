package io.lmb.common;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Where everything runs. Loaded from a properties file (-Dlmb.topology / LMB_TOPOLOGY); every key
 * can be overridden by a system property or environment variable (see {@link Config}), and every
 * key has a default that yields a working single-host layout.
 *
 * <p>Each cluster node (shard s, member m) has a base port, default 20000 + 1000*s + 100*m, and
 * uses fixed offsets from it (see the *_OFFSET constants).
 */
public final class Topology
{
    public static final int ARCHIVE_CONTROL_OFFSET = 0;
    public static final int ARCHIVE_REPLICATION_OFFSET = 1;
    public static final int INGRESS_OFFSET = 2;
    public static final int CONSENSUS_OFFSET = 3;
    public static final int LOG_OFFSET = 4;
    public static final int CATCHUP_OFFSET = 5;
    public static final int EVENT_CONTROL_OFFSET = 6;
    public static final int ARCHIVE_RESPONSE_OFFSET = 7;
    public static final int BACKUP_OFFSET = 8;
    public static final int METRICS_OFFSET = 9;

    private final Properties file;

    private Topology(final Properties file)
    {
        this.file = file;
    }

    /** Loads from -Dlmb.topology / LMB_TOPOLOGY if set, otherwise pure defaults. */
    public static Topology load()
    {
        final String path = Config.get("lmb.topology", null);
        return path == null ? new Topology(new Properties()) : load(Path.of(path));
    }

    public static Topology load(final Path path)
    {
        final Properties p = new Properties();
        try (Reader r = Files.newBufferedReader(path))
        {
            p.load(r);
        }
        catch (final IOException e)
        {
            throw new IllegalStateException("cannot read topology " + path, e);
        }
        return new Topology(p);
    }

    public static Topology of(final Properties properties)
    {
        final Properties copy = new Properties();
        copy.putAll(properties);
        return new Topology(copy);
    }

    // ---- generic lookup: system property / env, then file, then default ----

    public String get(final String key, final String defaultValue)
    {
        final String override = Config.get(key, null);
        return override != null ? override : file.getProperty(key, defaultValue);
    }

    public int getInt(final String key, final int defaultValue)
    {
        final String v = get(key, null);
        return v == null ? defaultValue : Integer.parseInt(v.trim());
    }

    public long getLong(final String key, final long defaultValue)
    {
        final String v = get(key, null);
        return v == null ? defaultValue : Long.parseLong(v.trim());
    }

    public boolean getBool(final String key, final boolean defaultValue)
    {
        final String v = get(key, null);
        return v == null ? defaultValue : Boolean.parseBoolean(v.trim());
    }

    // ---- shards and cluster members ----

    public int shardCount()
    {
        return getInt("lmb.shards", 1);
    }

    /** Raft members per shard: 2 locally (no fault tolerance, both must be up), 3 in production. */
    public int membersPerShard()
    {
        return getInt("lmb.members", 2);
    }

    public String memberHost(final int shard, final int member)
    {
        return get("lmb.shard." + shard + ".member." + member + ".host", "127.0.0.1");
    }

    public int memberBasePort(final int shard, final int member)
    {
        return getInt("lmb.shard." + shard + ".member." + member + ".basePort", 20000 + 1000 * shard + 100 * member);
    }

    public int memberPort(final int shard, final int member, final int offset)
    {
        return memberBasePort(shard, member) + offset;
    }

    public String memberEndpoint(final int shard, final int member, final int offset)
    {
        return memberHost(shard, member) + ":" + memberPort(shard, member, offset);
    }

    /** Aeron Cluster members string: id,ingress,consensus,log,catchup,archive|... */
    public String clusterMembers(final int shard)
    {
        final StringBuilder sb = new StringBuilder();
        for (int m = 0; m < membersPerShard(); m++)
        {
            if (m > 0)
            {
                sb.append('|');
            }
            sb.append(m)
                .append(',').append(memberEndpoint(shard, m, INGRESS_OFFSET))
                .append(',').append(memberEndpoint(shard, m, CONSENSUS_OFFSET))
                .append(',').append(memberEndpoint(shard, m, LOG_OFFSET))
                .append(',').append(memberEndpoint(shard, m, CATCHUP_OFFSET))
                .append(',').append(memberEndpoint(shard, m, ARCHIVE_CONTROL_OFFSET));
        }
        return sb.toString();
    }

    /** Aeron Cluster client ingress endpoints: 0=host:port,1=host:port */
    public String ingressEndpoints(final int shard)
    {
        final StringBuilder sb = new StringBuilder();
        for (int m = 0; m < membersPerShard(); m++)
        {
            if (m > 0)
            {
                sb.append(',');
            }
            sb.append(m).append('=').append(memberEndpoint(shard, m, INGRESS_OFFSET));
        }
        return sb.toString();
    }

    // ---- gateways ----

    public int gatewayCount()
    {
        return getInt("lmb.gateways", 2);
    }

    public String gatewayHost(final int gateway)
    {
        return get("lmb.gateway." + gateway + ".host", "127.0.0.1");
    }

    public int gatewayGrpcPort(final int gateway)
    {
        return getInt("lmb.gateway." + gateway + ".grpcPort", 9100 + gateway);
    }

    public int gatewayFixPort(final int gateway)
    {
        return getInt("lmb.gateway." + gateway + ".fixPort", 9200 + gateway);
    }

    public int gatewayMetricsPort(final int gateway)
    {
        return getInt("lmb.gateway." + gateway + ".metricsPort", 9300 + gateway);
    }

    // ---- market data publishers (instance 0 = feed A, 1 = feed B) ----

    public int marketDataInstances()
    {
        return getInt("lmb.md.instances", 2);
    }

    public String marketDataHost(final int instance)
    {
        return get("lmb.md." + instance + ".host", "127.0.0.1");
    }

    public int marketDataRetransmitPort(final int instance)
    {
        return getInt("lmb.md." + instance + ".retransmitPort", 9500 + instance);
    }

    public int marketDataGrpcPort(final int instance)
    {
        return getInt("lmb.md." + instance + ".grpcPort", 9510 + instance);
    }

    public int marketDataMetricsPort(final int instance)
    {
        return getInt("lmb.md." + instance + ".metricsPort", 9520 + instance);
    }

    // ---- feeds / NBBO consolidators ----

    public int consolidatorInstances()
    {
        return getInt("lmb.consolidators", 2);
    }

    public int consolidatorMetricsPort(final int instance)
    {
        return getInt("lmb.consolidator." + instance + ".metricsPort", 9600 + instance);
    }

    public int feedSimulatorMetricsPort()
    {
        return getInt("lmb.feedsim.metricsPort", 9650);
    }

    // ---- control plane ----

    public int controlPlaneInstances()
    {
        return getInt("lmb.controlplane.instances", 2);
    }

    public String controlPlaneHost(final int instance)
    {
        return get("lmb.controlplane." + instance + ".host", "127.0.0.1");
    }

    public int controlPlaneRestPort(final int instance)
    {
        return getInt("lmb.controlplane." + instance + ".restPort", 9700 + instance);
    }

    public int controlPlaneMetricsPort(final int instance)
    {
        return getInt("lmb.controlplane." + instance + ".metricsPort", 9710 + instance);
    }

    // ---- post-trade, replayer ----

    public int dropCopyFixPort()
    {
        return getInt("lmb.posttrade.dropCopyFixPort", 9800);
    }

    public int postTradeMetricsPort()
    {
        return getInt("lmb.posttrade.metricsPort", 9810);
    }

    public int replayerMetricsPort()
    {
        return getInt("lmb.replayer.metricsPort", 9900);
    }

    // ---- static reference data files ----

    public Path symbolsFile()
    {
        return Path.of(get("lmb.symbols", "deploy/local/symbols.csv"));
    }

    public Path firmsFile()
    {
        return Path.of(get("lmb.firms", "deploy/local/firms.csv"));
    }
}
