package io.lmb.replayer;

import io.prometheus.metrics.core.datapoints.GaugeDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

/** Prometheus metrics of one shadow replayer ({@code lmb_replayer_*}, label {@code shard}). */
final class ReplayerMetrics
{
    private final PrometheusRegistry registry = new PrometheusRegistry();
    final GaugeDataPoint divergence;
    final GaugeDataPoint verifiedSeq;
    final GaugeDataPoint verifiedPosition;
    final GaugeDataPoint appliedPosition;
    final GaugeDataPoint streamSeq;
    final GaugeDataPoint lagEvents;
    final GaugeDataPoint lagBytes;
    final GaugeDataPoint entries;
    final GaugeDataPoint eventsCompared;
    final io.prometheus.metrics.core.datapoints.CounterDataPoint logReopens;

    ReplayerMetrics(final int shardId)
    {
        final String shard = Integer.toString(shardId);
        divergence = gauge("lmb_replayer_divergence", "1 once the replay diverged from the live event stream (page)", shard);
        verifiedSeq = gauge("lmb_replayer_verified_event_seq", "eventSeq of the last event that matched", shard);
        verifiedPosition = gauge("lmb_replayer_verified_log_position", "Log position of the last entry whose events all matched", shard);
        appliedPosition = gauge("lmb_replayer_applied_log_position", "Log position of the last entry replayed", shard);
        streamSeq = gauge("lmb_replayer_stream_event_seq", "eventSeq of the last event received from the live stream", shard);
        lagEvents = gauge("lmb_replayer_lag_events", "Events received from the stream but not yet verified", shard);
        lagBytes = gauge("lmb_replayer_lag_bytes", "Committed log bytes (as confirmed by the stream) not yet replayed", shard);
        entries = gauge("lmb_replayer_entries", "Log entries (commands and timers) replayed since start", shard);
        eventsCompared = gauge("lmb_replayer_events_compared", "Events compared byte for byte since start", shard);
        logReopens = Counter.builder().name("lmb_replayer_log_reopens_total")
            .help("Re-reads of the member files to find newly recorded log").labelNames("shard").register(registry)
            .labelValues(shard);
    }

    private GaugeDataPoint gauge(final String name, final String help, final String shard)
    {
        return Gauge.builder().name(name).help(help).labelNames("shard").register(registry).labelValues(shard);
    }

    PrometheusRegistry registry()
    {
        return registry;
    }
}
