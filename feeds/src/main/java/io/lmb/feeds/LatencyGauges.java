package io.lmb.feeds;

import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Latency recorded wait-free on the duty thread (HdrHistogram {@link Recorder}) and exposed as
 * quantile gauges (label {@code q} = p50, p99, p999, max) computed over the interval since the
 * previous scrape. If nothing was recorded in an interval, the previous interval's values are kept.
 */
public final class LatencyGauges
{
    private final Recorder recorder = new Recorder(3);
    private Histogram interval;
    private Histogram lastNonEmpty;

    public void record(final long valueNs)
    {
        recorder.recordValue(Math.max(0, valueNs));
    }

    public synchronized double[] snapshot()
    {
        interval = recorder.getIntervalHistogram(interval);
        if (interval.getTotalCount() > 0)
        {
            lastNonEmpty = interval.copy();
        }
        if (lastNonEmpty == null)
        {
            return new double[]{Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0};
        }
        return new double[]{
            lastNonEmpty.getValueAtPercentile(50),
            lastNonEmpty.getValueAtPercentile(99),
            lastNonEmpty.getValueAtPercentile(99.9),
            lastNonEmpty.getMaxValue(),
            lastNonEmpty.getTotalCount()};
    }

    public void register(final PrometheusRegistry registry, final String name, final String help)
    {
        GaugeWithCallback.builder()
            .name(name)
            .help(help)
            .labelNames("q")
            .callback(cb ->
            {
                final double[] s = snapshot();
                cb.call(s[0], "p50");
                cb.call(s[1], "p99");
                cb.call(s[2], "p999");
                cb.call(s[3], "max");
            })
            .register(registry);
    }
}
