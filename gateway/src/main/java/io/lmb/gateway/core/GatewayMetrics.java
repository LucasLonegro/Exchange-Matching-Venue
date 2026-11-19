package io.lmb.gateway.core;

import io.prometheus.metrics.core.datapoints.CounterDataPoint;
import io.prometheus.metrics.core.datapoints.GaugeDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.HdrHistogram.Histogram;

/**
 * Prometheus metrics of one gateway (own registry, so several gateways can live in one JVM).
 * Counters are thread-safe; the latency histogram is recorded and published on the core thread.
 */
public final class GatewayMetrics
{
    private final PrometheusRegistry registry = new PrometheusRegistry();
    private final CounterDataPoint[] requestsByKind = new CounterDataPoint[OrderRequest.Kind.values().length];
    private final CounterDataPoint[] rejectsByReason = new CounterDataPoint[Reject.VALUES.length];
    private final GaugeDataPoint[] inflightDepth;
    private final CounterDataPoint resubmitTimeout;
    private final CounterDataPoint resubmitReconnect;
    private final CounterDataPoint abandoned;
    private final CounterDataPoint reportsLive;
    private final CounterDataPoint reportsPossDup;
    private final CounterDataPoint slowConsumerDisconnects;
    private final CounterDataPoint cancelOnDisconnect;
    private final CounterDataPoint resendsAnsweredFromCache;
    private final GaugeDataPoint sessionsConnected;
    private final GaugeDataPoint catchupStreams;
    private final GaugeDataPoint latencyP50;
    private final GaugeDataPoint latencyP99;
    private final GaugeDataPoint latencyP999;
    private final GaugeDataPoint latencyMax;
    private final Histogram latency = new Histogram(60_000_000_000L, 3);
    private final Counter rejects;

    public GatewayMetrics(final int shardCount)
    {
        final Counter requests = Counter.builder().name("lmb_gateway_requests_total")
            .help("Client requests received, by type").labelNames("type").register(registry);
        for (final OrderRequest.Kind k : OrderRequest.Kind.values())
        {
            requestsByKind[k.ordinal()] = requests.labelValues(k.name());
        }
        rejects = Counter.builder().name("lmb_gateway_rejects_total")
            .help("Requests refused by the gateway before reaching a shard, by reason").labelNames("reason").register(registry);
        for (final Reject r : Reject.VALUES)
        {
            rejectsByReason[r.ordinal()] = rejects.labelValues(r.name());
        }
        final Gauge depth = Gauge.builder().name("lmb_gateway_inflight_depth")
            .help("Commands queued or in flight per shard").labelNames("shard").register(registry);
        inflightDepth = new GaugeDataPoint[shardCount];
        for (int s = 0; s < shardCount; s++)
        {
            inflightDepth[s] = depth.labelValues(Integer.toString(s));
        }
        final Counter resubmits = Counter.builder().name("lmb_gateway_resubmits_total")
            .help("Commands resubmitted with possResend=TRUE").labelNames("cause").register(registry);
        resubmitTimeout = resubmits.labelValues("timeout");
        resubmitReconnect = resubmits.labelValues("reconnect");
        abandoned = Counter.builder().name("lmb_gateway_inflight_abandoned_total")
            .help("In-flight commands given up after the maximum number of attempts").register(registry);
        final Counter reports = Counter.builder().name("lmb_gateway_reports_out_total")
            .help("Execution reports delivered to clients").labelNames("poss_dup").register(registry);
        reportsLive = reports.labelValues("false");
        reportsPossDup = reports.labelValues("true");
        slowConsumerDisconnects = Counter.builder().name("lmb_gateway_slow_consumer_disconnects_total")
            .help("Sessions disconnected because they did not read their reports").register(registry);
        cancelOnDisconnect = Counter.builder().name("lmb_gateway_cancel_on_disconnect_total")
            .help("Cancel-on-disconnect mass cancels issued (one per shard)").register(registry);
        resendsAnsweredFromCache = Counter.builder().name("lmb_gateway_resends_from_cache_total")
            .help("Possible resends whose outcome was already known and not resubmitted").register(registry);
        sessionsConnected = Gauge.builder().name("lmb_gateway_sessions_connected")
            .help("Logged-on sessions").register(registry);
        catchupStreams = Gauge.builder().name("lmb_gateway_catchup_streams")
            .help("Per-session event stream readers replaying for resume or slow consumers").register(registry);
        final Gauge lat = Gauge.builder().name("lmb_gateway_ack_latency_ns")
            .help("Gateway-in to report-out latency over the last interval (from the echoed gatewayInTsNs)")
            .labelNames("quantile").register(registry);
        latencyP50 = lat.labelValues("p50");
        latencyP99 = lat.labelValues("p99");
        latencyP999 = lat.labelValues("p999");
        latencyMax = lat.labelValues("max");
    }

    public PrometheusRegistry registry()
    {
        return registry;
    }

    void request(final OrderRequest.Kind kind)
    {
        requestsByKind[kind.ordinal()].inc();
    }

    void reject(final Reject reason)
    {
        rejectsByReason[reason.ordinal()].inc();
    }

    public long rejects(final Reject reason)
    {
        return (long)rejectsByReason[reason.ordinal()].get();
    }

    void inflightDepth(final int shard, final int depth)
    {
        inflightDepth[shard].set(depth);
    }

    void resubmit(final boolean reconnect)
    {
        (reconnect ? resubmitReconnect : resubmitTimeout).inc();
    }

    public long resubmits()
    {
        return (long)(resubmitTimeout.get() + resubmitReconnect.get());
    }

    void abandoned()
    {
        abandoned.inc();
    }

    void reportOut(final boolean possDup)
    {
        (possDup ? reportsPossDup : reportsLive).inc();
    }

    void slowConsumerDisconnect()
    {
        slowConsumerDisconnects.inc();
    }

    public long slowConsumerDisconnects()
    {
        return (long)slowConsumerDisconnects.get();
    }

    void cancelOnDisconnect()
    {
        cancelOnDisconnect.inc();
    }

    void resendFromCache()
    {
        resendsAnsweredFromCache.inc();
    }

    void sessions(final int n)
    {
        sessionsConnected.set(n);
    }

    void catchups(final int n)
    {
        catchupStreams.set(n);
    }

    public long catchupStreamsNow()
    {
        return (long)catchupStreams.get();
    }

    void recordLatency(final long ns)
    {
        if (ns > 0)
        {
            latency.recordValue(Math.min(ns, latency.getHighestTrackableValue()));
        }
    }

    /** Publishes the quantiles of the interval just ended and starts a new interval. */
    void publishLatency()
    {
        if (latency.getTotalCount() == 0)
        {
            return;
        }
        latencyP50.set(latency.getValueAtPercentile(50));
        latencyP99.set(latency.getValueAtPercentile(99));
        latencyP999.set(latency.getValueAtPercentile(99.9));
        latencyMax.set(latency.getMaxValue());
        latency.reset();
    }
}
