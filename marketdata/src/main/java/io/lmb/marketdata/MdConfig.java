package io.lmb.marketdata;

import io.lmb.common.Topology;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration of one publisher instance. Every key is read through {@link Topology#get} (system property,
 * then environment variable, then topology file, then the default) so it can live in the topology file or
 * be overridden per process. Keys and defaults are listed in docs/modules/marketdata.md.
 */
public final class MdConfig
{
    public int instance;
    public int shards;
    public long sessionId = 1;

    public String bindHost = "0.0.0.0";
    public int retransmitPort;
    public int grpcPort;
    public int metricsPort;

    /** UDP destinations (unicast or multicast); empty disables that channel. */
    public List<InetSocketAddress> incrementalDestinations = new ArrayList<>();
    public List<InetSocketAddress> snapshotDestinations = new ArrayList<>();
    public String multicastInterface = "";
    public int multicastTtl = 1;
    public int maxDatagram = 1400;
    public long heartbeatIntervalMs = 1000;
    public long snapshotIntervalMs = 1000;
    public int snapshotSymbolsPerPoll = 64;

    public int historyDepth = 8192;
    public int retransmitMaxPerRequest = 1000;
    public int retransmitMaxConnections = 64;
    public int retransmitIdleTimeoutMs = 60_000;

    public int grpcRingBytes = 1 << 20;
    public int grpcMaxQueued = 20_000;
    public int grpcMaxSubscribers = 1000;

    public int pollLimit = 256;
    public boolean suppressCatchUp = true;
    public Path dataDir;
    public long checkpointIntervalMs = 5000;

    public static MdConfig load(final Topology t, final int instance)
    {
        final MdConfig c = new MdConfig();
        final String p = "lmb.md.";
        final String pi = "lmb.md." + instance + ".";
        c.instance = instance;
        c.shards = t.shardCount();
        c.sessionId = t.getLong(p + "sessionId", 1);
        c.bindHost = t.get(pi + "bindHost", "0.0.0.0");
        c.retransmitPort = t.marketDataRetransmitPort(instance);
        c.grpcPort = t.marketDataGrpcPort(instance);
        c.metricsPort = t.marketDataMetricsPort(instance);
        c.incrementalDestinations = parse(t.get(pi + "incremental", "127.0.0.1:" + (9540 + instance)));
        c.snapshotDestinations = parse(t.get(pi + "snapshot", "127.0.0.1:" + (9550 + instance)));
        c.multicastInterface = t.get(p + "multicastInterface", "");
        c.multicastTtl = t.getInt(p + "multicastTtl", 1);
        c.maxDatagram = t.getInt(p + "maxDatagram", 1400);
        c.heartbeatIntervalMs = t.getLong(p + "heartbeatIntervalMs", 1000);
        c.snapshotIntervalMs = t.getLong(p + "snapshotIntervalMs", 1000);
        c.snapshotSymbolsPerPoll = t.getInt(p + "snapshotSymbolsPerPoll", 64);
        c.historyDepth = t.getInt(p + "historyDepth", 8192);
        c.retransmitMaxPerRequest = t.getInt(p + "retransmitMaxPerRequest", 1000);
        c.retransmitMaxConnections = t.getInt(p + "retransmitMaxConnections", 64);
        c.retransmitIdleTimeoutMs = t.getInt(p + "retransmitIdleTimeoutMs", 60_000);
        c.grpcRingBytes = t.getInt(p + "grpcRingBytes", 1 << 20);
        c.grpcMaxQueued = t.getInt(p + "grpcMaxQueued", 20_000);
        c.grpcMaxSubscribers = t.getInt(p + "grpcMaxSubscribers", 1000);
        c.pollLimit = t.getInt(p + "pollLimit", 256);
        c.suppressCatchUp = t.getBool(p + "suppressCatchUp", true);
        c.dataDir = Path.of(t.get("lmb.data.dir", "./data")).resolve("marketdata").resolve(Integer.toString(instance));
        c.checkpointIntervalMs = t.getLong(p + "checkpointIntervalMs", 5000);
        if (c.maxDatagram < 512 || c.maxDatagram > 65000)
        {
            throw new IllegalArgumentException("lmb.md.maxDatagram must be in [512, 65000]");
        }
        return c;
    }

    /** "host:port,host:port"; blank or "none" = no destination. */
    public static List<InetSocketAddress> parse(final String spec)
    {
        final List<InetSocketAddress> out = new ArrayList<>();
        if (spec == null || spec.isBlank() || spec.trim().equalsIgnoreCase("none"))
        {
            return out;
        }
        for (final String part : spec.split(","))
        {
            final String s = part.trim();
            final int colon = s.lastIndexOf(':');
            if (colon <= 0)
            {
                throw new IllegalArgumentException("expected host:port, got '" + s + "'");
            }
            out.add(new InetSocketAddress(s.substring(0, colon), Integer.parseInt(s.substring(colon + 1))));
        }
        return out;
    }
}
