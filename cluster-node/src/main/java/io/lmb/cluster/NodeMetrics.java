package io.lmb.cluster;

import io.aeron.cluster.service.Cluster;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Prometheus endpoint of a member (port = member base port + METRICS_OFFSET). Gauges:
 * lmb_cluster_role (0 follower, 1 candidate, 2 leader), lmb_cluster_commit_position,
 * lmb_cluster_last_event_seq, lmb_cluster_first_event_seq, lmb_cluster_election_count,
 * lmb_cluster_snapshot_count, lmb_cluster_commands_applied, lmb_cluster_event_consumers and
 * lmb_cluster_service_time_ns{quantile=p50|p99|p999|max} (per scrape interval).
 */
public final class NodeMetrics implements AutoCloseable
{
    private final HTTPServer server;

    private NodeMetrics(final HTTPServer server)
    {
        this.server = server;
    }

    public static NodeMetrics start(final ClusterNode node, final int port)
    {
        final PrometheusRegistry registry = new PrometheusRegistry();
        final String shard = Integer.toString(node.config().shardId);
        final String member = Integer.toString(node.config().memberId);
        gauge(registry, "lmb_cluster_role", "Raft role: 0 follower, 1 candidate, 2 leader",
            () -> node.role().code(), shard, member);
        gauge(registry, "lmb_cluster_commit_position", "Raft commit position", node::commitPosition, shard, member);
        gauge(registry, "lmb_cluster_last_event_seq", "Last eventSeq recorded on this member",
            () -> node.eventIndex().lastSeq(), shard, member);
        gauge(registry, "lmb_cluster_first_event_seq", "First eventSeq this member can replay",
            () -> node.eventIndex().firstSeq(), shard, member);
        gauge(registry, "lmb_cluster_election_count", "Elections since start", node::electionCount, shard, member);
        gauge(registry, "lmb_cluster_snapshot_count", "Snapshots taken since start", node::snapshotCount, shard, member);
        gauge(registry, "lmb_cluster_commands_applied", "Commands applied by the service since start",
            () -> node.service() == null ? 0 : node.service().commandsApplied(), shard, member);
        gauge(registry, "lmb_cluster_event_consumers", "Event stream consumer sessions",
            () -> node.eventServer() == null ? 0 : node.eventServer().sessionCount(), shard, member);

        GaugeWithCallback.builder()
            .name("lmb_cluster_service_time_ns")
            .help("State machine time per command, quantiles over the last scrape interval")
            .labelNames("shard", "member", "quantile")
            .callback(cb ->
            {
                final ShardClusteredService service = node.service();
                if (service == null)
                {
                    return;
                }
                final Histogram h = service.serviceTimeRecorder().getIntervalHistogram();
                cb.call(h.getValueAtPercentile(50), shard, member, "p50");
                cb.call(h.getValueAtPercentile(99), shard, member, "p99");
                cb.call(h.getValueAtPercentile(99.9), shard, member, "p999");
                cb.call(h.getMaxValue(), shard, member, "max");
            })
            .register(registry);
        try
        {
            return new NodeMetrics(HTTPServer.builder().port(port).registry(registry).buildAndStart());
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException("cannot start metrics endpoint on port " + port, e);
        }
    }

    private interface LongSource
    {
        long get();
    }

    private static void gauge(
        final PrometheusRegistry registry, final String name, final String help, final LongSource source,
        final String shard, final String member)
    {
        GaugeWithCallback.builder()
            .name(name)
            .help(help)
            .labelNames("shard", "member")
            .callback(cb -> cb.call(source.get(), shard, member))
            .register(registry);
    }

    public int port()
    {
        return server.getPort();
    }

    @Override
    public void close()
    {
        server.close();
    }

    static int roleCode(final Cluster.Role role)
    {
        return role.code();
    }
}
