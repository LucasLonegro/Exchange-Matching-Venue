package io.lmb.controlplane;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.HdrHistogram.Histogram;

import java.io.IOException;

/** Prometheus metrics, names lmb_controlplane_*. One registry per instance (tests run two in one JVM). */
public final class Metrics implements AutoCloseable
{
    private final PrometheusRegistry registry = new PrometheusRegistry();
    private final Gauge leader = Gauge.builder().name("lmb_controlplane_is_leader").help("1 while this instance holds the lease")
        .register(registry);
    private final Gauge shardConnected = Gauge.builder().name("lmb_controlplane_shard_connected").help("command client connected")
        .labelNames("shard").register(registry);
    private final Gauge shardEventSeq = Gauge.builder().name("lmb_controlplane_shard_event_seq").help("last event consumed")
        .labelNames("shard").register(registry);
    private final Gauge pending = Gauge.builder().name("lmb_controlplane_unresolved_commands").help("commands awaiting effect")
        .register(registry);
    private final Counter issued = Counter.builder().name("lmb_controlplane_commands_issued").help("commands issued")
        .labelNames("kind").register(registry);
    private final Counter retries = Counter.builder().name("lmb_controlplane_command_retries").help("command resends")
        .register(registry);
    private final Counter rejected = Counter.builder().name("lmb_controlplane_commands_rejected").help("CommandRejected seen")
        .register(registry);
    private final Counter admin = Counter.builder().name("lmb_controlplane_admin_requests").help("admin requests accepted")
        .labelNames("action").register(registry);
    private final Gauge tickLatency = Gauge.builder().name("lmb_controlplane_tick_latency_ns").help("leader tick duration")
        .labelNames("quantile").register(registry);
    private final Gauge confirmLatency = Gauge.builder().name("lmb_controlplane_confirm_latency_ns")
        .help("offer to observed effect").labelNames("quantile").register(registry);
    private final Histogram tickHist = new Histogram(3_600_000_000_000L, 2);
    private final Histogram confirmHist = new Histogram(3_600_000_000_000L, 2);
    private long lastPublishNs = System.nanoTime();
    private HTTPServer server;

    public void start(final int port) throws IOException
    {
        if (port >= 0)
        {
            server = HTTPServer.builder().port(port).registry(registry).buildAndStart();
        }
    }

    public int port()
    {
        return server == null ? -1 : server.getPort();
    }

    public void leader(final boolean isLeader)
    {
        leader.set(isLeader ? 1 : 0);
    }

    public void shard(final int shardId, final boolean connected, final long eventSeq)
    {
        shardConnected.labelValues(Integer.toString(shardId)).set(connected ? 1 : 0);
        shardEventSeq.labelValues(Integer.toString(shardId)).set(eventSeq);
    }

    public void pending(final int n)
    {
        pending.set(n);
    }

    public void commandIssued(final String kind)
    {
        issued.labelValues(kind).inc();
    }

    public void retry()
    {
        retries.inc();
    }

    public void commandRejected()
    {
        rejected.inc();
    }

    public void adminRequest(final String action)
    {
        admin.labelValues(action).inc();
    }

    public synchronized void confirmLatency(final long ns)
    {
        confirmHist.recordValue(Math.min(ns, confirmHist.getHighestTrackableValue()));
    }

    public synchronized void tickLatency(final long ns)
    {
        tickHist.recordValue(Math.min(Math.max(0, ns), tickHist.getHighestTrackableValue()));
        final long now = System.nanoTime();
        if (now - lastPublishNs >= 1_000_000_000L)
        {
            lastPublishNs = now;
            publish(tickHist, tickLatency);
            publish(confirmHist, confirmLatency);
        }
    }

    private static void publish(final Histogram h, final Gauge g)
    {
        if (h.getTotalCount() == 0)
        {
            return;
        }
        g.labelValues("p50").set(h.getValueAtPercentile(50));
        g.labelValues("p99").set(h.getValueAtPercentile(99));
        g.labelValues("p999").set(h.getValueAtPercentile(99.9));
        g.labelValues("max").set(h.getMaxValue());
        h.reset();
    }

    @Override
    public void close()
    {
        if (server != null)
        {
            server.close();
        }
    }
}
