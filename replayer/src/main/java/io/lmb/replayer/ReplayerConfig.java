package io.lmb.replayer;

import io.lmb.common.Config;
import io.lmb.common.Topology;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration of a {@link ShadowReplayer}. Keys (system property or environment, see {@link Config}):
 * <ul>
 *   <li>{@code lmb.replayer.shard} - the shard to verify (default {@code lmb.instance.id}, default 0)</li>
 *   <li>{@code lmb.replayer.memberDirs} - comma-separated member directories to read the Raft log from (default:
 *       {@code <lmb.data.dir>/cluster-node/shard-S-member-M} for every member of the topology). The one with
 *       the most recorded log is used; all must be on this host (files are read directly).</li>
 *   <li>{@code lmb.replayer.metricsPort} - Prometheus port (default {@code Topology.replayerMetricsPort() + shard});
 *       negative disables the HTTP server</li>
 *   <li>{@code lmb.replayer.fromSnapshot} - start from the latest service snapshot instead of log position 0
 *       (default false; forced when the log was purged)</li>
 *   <li>{@code lmb.replayer.reopenIntervalMs} - minimum interval between re-reads of the member files when the
 *       event stream is ahead of the recorded log (default 5)</li>
 *   <li>{@code lmb.replayer.maxPendingEvents} - stop pulling the stream while this many events wait for the log
 *       (default 1000000)</li>
 *   <li>{@code lmb.replayer.readyQuietMs} - READY once connected, verified up to the stream, and no event arrived
 *       for this long (default 300)</li>
 * </ul>
 */
public final class ReplayerConfig
{
    public int shardId;
    public List<File> memberDirs = new ArrayList<>();
    public int metricsPort = -1;
    public boolean fromSnapshot;
    public long reopenIntervalMs = 5;
    public int maxPendingEvents = 1_000_000;
    public long readyQuietMs = 300;

    public static ReplayerConfig load(final Topology topology)
    {
        final ReplayerConfig c = new ReplayerConfig();
        c.shardId = Config.getInt("lmb.replayer.shard", Config.getInt("lmb.instance.id", 0));
        final String dirs = Config.get("lmb.replayer.memberDirs", "");
        if (dirs.isBlank())
        {
            final File base = new File(Config.get("lmb.data.dir", "./data"), "cluster-node");
            for (int m = 0; m < topology.membersPerShard(); m++)
            {
                c.memberDirs.add(new File(base, "shard-" + c.shardId + "-member-" + m));
            }
        }
        else
        {
            for (final String d : dirs.split(","))
            {
                if (!d.isBlank())
                {
                    c.memberDirs.add(new File(d.trim()));
                }
            }
        }
        c.metricsPort = Config.getInt("lmb.replayer.metricsPort", topology.replayerMetricsPort() + c.shardId);
        c.fromSnapshot = Config.getBool("lmb.replayer.fromSnapshot", false);
        c.reopenIntervalMs = Config.getLong("lmb.replayer.reopenIntervalMs", 5);
        c.maxPendingEvents = Config.getInt("lmb.replayer.maxPendingEvents", 1_000_000);
        c.readyQuietMs = Config.getLong("lmb.replayer.readyQuietMs", 300);
        return c;
    }

    @Override
    public String toString()
    {
        return "shard " + shardId + ", memberDirs " + memberDirs + ", metricsPort " + metricsPort +
            ", fromSnapshot " + fromSnapshot;
    }
}
