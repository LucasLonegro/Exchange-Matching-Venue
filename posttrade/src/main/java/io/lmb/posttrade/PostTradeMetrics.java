package io.lmb.posttrade;

import io.lmb.api.SinkRunner;
import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Counters of one post-trade process, written by the sink threads and exported through Prometheus callbacks
 * (the hot path never touches the Prometheus client). Prefix {@code lmb_posttrade_}.
 */
public final class PostTradeMetrics
{
    public final AtomicLong tradesStored = new AtomicLong();
    public final AtomicLong fillsStored = new AtomicLong();
    public final AtomicLong storeCommits = new AtomicLong();
    public final LatencyRecorder storeCommitNs = new LatencyRecorder();
    public final AtomicLong dropCopyMessages = new AtomicLong();
    public final AtomicLong dropCopyReplayed = new AtomicLong();
    public final AtomicLong dropCopyBackPressure = new AtomicLong();
    public final AtomicLong dropCopyLogonsRejected = new AtomicLong();
    public final AtomicLong dropCopySessions = new AtomicLong();
    public final AtomicLong surveillanceRecords = new AtomicLong();
    public final AtomicLong surveillanceErrors = new AtomicLong();

    private final List<SinkRunner> runners = new CopyOnWriteArrayList<>();

    public void bind(final SinkRunner runner)
    {
        runners.add(runner);
    }

    public void register(final PrometheusRegistry r)
    {
        perSink(r, "lmb_posttrade_events", "Events consumed per sink", true, x -> x.events.get());
        perSink(r, "lmb_posttrade_duplicate_events", "Redelivered events dropped by the eventSeq dedupe", true,
            x -> x.duplicates.get());
        perSink(r, "lmb_posttrade_sink_restarts", "Sink failures followed by a restart from the durable cursor", true,
            x -> x.restarts.get());
        perSink(r, "lmb_posttrade_sink_up", "1 while the sink is open", false, x -> x.isUp() ? 1 : 0);
        GaugeWithCallback.builder().name("lmb_posttrade_last_event_seq").help("Last eventSeq applied per sink and shard")
            .labelNames("sink", "shard")
            .callback(cb ->
            {
                for (final SinkRunner x : runners)
                {
                    for (int s = 0; s < x.shardCount(); s++)
                    {
                        cb.call(x.lastEventSeq(s), x.name(), Integer.toString(s));
                    }
                }
            }).register(r);
        counter(r, "lmb_posttrade_trades_stored", "Trade rows written to the trade store", tradesStored);
        counter(r, "lmb_posttrade_fills_stored", "Fill rows written to the trade store", fillsStored);
        counter(r, "lmb_posttrade_store_commits", "Trade store transactions committed", storeCommits);
        quantiles(r, "lmb_posttrade_store_commit_ns", "Trade store commit duration (interval since last scrape)",
            storeCommitNs);
        counter(r, "lmb_posttrade_dropcopy_messages", "Drop copy ExecutionReports handed to Artio", dropCopyMessages);
        counter(r, "lmb_posttrade_dropcopy_replayed", "Drop copy ExecutionReports sent while a session caught up",
            dropCopyReplayed);
        counter(r, "lmb_posttrade_dropcopy_back_pressure", "Drop copy sends refused by Artio (session re-reads)",
            dropCopyBackPressure);
        counter(r, "lmb_posttrade_dropcopy_logons_rejected", "Drop copy logons refused", dropCopyLogonsRejected);
        gauge(r, "lmb_posttrade_dropcopy_sessions", "Drop copy sessions logged on", dropCopySessions::get);
        counter(r, "lmb_posttrade_surveillance_records", "Records written to the surveillance sink", surveillanceRecords);
        counter(r, "lmb_posttrade_surveillance_errors", "Surveillance write/produce failures", surveillanceErrors);
    }

    private interface RunnerValue
    {
        long get(SinkRunner runner);
    }

    private void perSink(final PrometheusRegistry r, final String name, final String help, final boolean counter,
        final RunnerValue v)
    {
        if (counter)
        {
            CounterWithCallback.builder().name(name).help(help).labelNames("sink")
                .callback(cb -> runners.forEach(x -> cb.call(v.get(x), x.name()))).register(r);
        }
        else
        {
            GaugeWithCallback.builder().name(name).help(help).labelNames("sink")
                .callback(cb -> runners.forEach(x -> cb.call(v.get(x), x.name()))).register(r);
        }
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
