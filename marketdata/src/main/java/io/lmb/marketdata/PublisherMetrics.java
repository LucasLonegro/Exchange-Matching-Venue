package io.lmb.marketdata;

import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Counters and latency recorders of one publisher. Written from the publisher, retransmission and gRPC
 * threads; exported through Prometheus callbacks so the hot path never touches the Prometheus client.
 */
public final class PublisherMetrics
{
    public final AtomicLong events = new AtomicLong();
    public final AtomicLong messages = new AtomicLong();
    public final AtomicLong incrementalPackets = new AtomicLong();
    public final AtomicLong snapshotPackets = new AtomicLong();
    public final AtomicLong bytesSent = new AtomicLong();
    public final AtomicLong sendErrors = new AtomicLong();
    public final AtomicLong snapshotsSent = new AtomicLong();
    public final AtomicLong heartbeats = new AtomicLong();
    public final AtomicLong suppressedMessages = new AtomicLong();
    public final AtomicLong retransmitOk = new AtomicLong();
    public final AtomicLong retransmitTooOld = new AtomicLong();
    public final AtomicLong retransmitUnknown = new AtomicLong();
    public final AtomicLong retransmitInvalid = new AtomicLong();
    public final AtomicLong retransmitMessages = new AtomicLong();
    public final AtomicLong retransmitRejectedConnections = new AtomicLong();
    public final AtomicLong grpcSubscribers = new AtomicLong();
    public final AtomicLong grpcSlowConsumerDisconnects = new AtomicLong();
    public final AtomicLong grpcUpdates = new AtomicLong();
    public final AtomicLong grpcBackpressureSpins = new AtomicLong();

    /** Internal: first event of a batch read from the event stream to the datagram carrying its last message sent. */
    public final LatencyRecorder receiveToSend = new LatencyRecorder();
    /** Audit only: event timestampNs (cluster time) to send, wall clock. Includes clock skew between hosts. */
    public final LatencyRecorder eventToPublish = new LatencyRecorder();

    private volatile LongSupplier symbolCount = () -> 0;
    private volatile java.util.function.IntToLongFunction lastEventSeq = shard -> 0;
    private volatile int shardCount;

    void bindEngine(final LongSupplier symbolCount, final java.util.function.IntToLongFunction lastEventSeq, final int shards)
    {
        this.symbolCount = symbolCount;
        this.lastEventSeq = lastEventSeq;
        this.shardCount = shards;
    }

    public void register(final PrometheusRegistry registry)
    {
        counter(registry, "lmb_marketdata_events", "Events consumed from the shards' event streams", events);
        counter(registry, "lmb_marketdata_messages", "Sequenced messages produced", messages);
        CounterWithCallback.builder().name("lmb_marketdata_packets").help("Datagrams sent").labelNames("channel")
            .callback(cb ->
            {
                cb.call(incrementalPackets.get(), "incremental");
                cb.call(snapshotPackets.get(), "snapshot");
            }).register(registry);
        counter(registry, "lmb_marketdata_sent_bytes", "Datagram bytes sent (per destination)", bytesSent);
        counter(registry, "lmb_marketdata_send_errors", "Datagram send failures", sendErrors);
        counter(registry, "lmb_marketdata_snapshots", "Symbol snapshots sent on the snapshot channel", snapshotsSent);
        counter(registry, "lmb_marketdata_heartbeats", "Heartbeats sent", heartbeats);
        counter(registry, "lmb_marketdata_suppressed_messages",
            "Messages not multicast because the shard stream was still catching up after start", suppressedMessages);
        CounterWithCallback.builder().name("lmb_marketdata_retransmit_requests").help("Retransmission requests by status")
            .labelNames("status")
            .callback(cb ->
            {
                cb.call(retransmitOk.get(), "OK");
                cb.call(retransmitTooOld.get(), "TOO_OLD");
                cb.call(retransmitUnknown.get(), "UNKNOWN_SYMBOL");
                cb.call(retransmitInvalid.get(), "INVALID_RANGE");
            }).register(registry);
        counter(registry, "lmb_marketdata_retransmit_messages", "Messages served by retransmission", retransmitMessages);
        counter(registry, "lmb_marketdata_retransmit_rejected_connections", "Connections refused (limit)", retransmitRejectedConnections);
        gauge(registry, "lmb_marketdata_grpc_subscribers", "Active gRPC subscriptions", grpcSubscribers::get);
        counter(registry, "lmb_marketdata_grpc_slow_consumer_disconnects", "gRPC subscriptions closed as slow consumers",
            grpcSlowConsumerDisconnects);
        counter(registry, "lmb_marketdata_grpc_updates", "gRPC updates sent", grpcUpdates);
        counter(registry, "lmb_marketdata_grpc_backpressure_spins", "Publisher waits on a full gRPC hand-off ring", grpcBackpressureSpins);
        gauge(registry, "lmb_marketdata_symbols", "Symbols known", () -> symbolCount.getAsLong());
        GaugeWithCallback.builder().name("lmb_marketdata_last_event_seq").help("Last eventSeq consumed per shard")
            .labelNames("shard")
            .callback(cb ->
            {
                for (int s = 0; s < shardCount; s++)
                {
                    cb.call(lastEventSeq.applyAsLong(s), Integer.toString(s));
                }
            }).register(registry);
        quantiles(registry, "lmb_marketdata_receive_to_send_ns",
            "Internal latency: event read from the stream to datagram sent (interval since last scrape)", receiveToSend);
        quantiles(registry, "lmb_marketdata_event_to_publish_ns",
            "AUDIT ONLY: cluster event timestamp to datagram sent, wall clock, includes clock skew", eventToPublish);
    }

    private static void counter(final PrometheusRegistry r, final String name, final String help, final AtomicLong v)
    {
        CounterWithCallback.builder().name(name).help(help).callback(cb -> cb.call(v.get())).register(r);
    }

    private static void gauge(final PrometheusRegistry r, final String name, final String help, final LongSupplier v)
    {
        GaugeWithCallback.builder().name(name).help(help).callback(cb -> cb.call(v.getAsLong())).register(r);
    }

    private static void quantiles(final PrometheusRegistry r, final String name, final String help, final LatencyRecorder rec)
    {
        GaugeWithCallback.builder().name(name).help(help).labelNames("quantile")
            .callback(cb ->
            {
                final Histogram h = rec.sample();
                cb.call(h.getValueAtPercentile(50), "0.5");
                cb.call(h.getValueAtPercentile(99), "0.99");
                cb.call(h.getValueAtPercentile(99.9), "0.999");
                cb.call(h.getMaxValue(), "max");
            }).register(r);
    }

    /** Wait-free recording from one thread; quantiles over the interval since the previous sample. */
    public static final class LatencyRecorder
    {
        private static final long HIGHEST = 60_000_000_000L;
        private final Recorder recorder = new Recorder(HIGHEST, 3);
        private Histogram interval;
        private Histogram last = new Histogram(HIGHEST, 3);

        public void record(final long valueNs)
        {
            recorder.recordValue(Math.max(0, Math.min(HIGHEST, valueNs)));
        }

        /** Histogram of values recorded since the previous call (the previous interval if none). */
        public synchronized Histogram sample()
        {
            interval = recorder.getIntervalHistogram(interval);
            if (interval.getTotalCount() > 0)
            {
                last = interval.copy();
            }
            return last;
        }
    }
}
