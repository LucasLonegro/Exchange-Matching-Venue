package io.lmb.feeds;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LatencyGaugesTest
{
    @Test
    void exposesQuantileGaugesPerIntervalAndKeepsTheLastNonEmptyInterval()
    {
        final LatencyGauges g = new LatencyGauges();
        final PrometheusRegistry registry = new PrometheusRegistry();
        g.register(registry, "lmb_feeds_test_latency_ns", "test");

        assertTrue(Double.isNaN(g.snapshot()[0]), "nothing recorded yet");
        for (int i = 1; i <= 1000; i++)
        {
            g.record(i * 1000L);
        }
        g.record(-5); // clamped, not an exception
        final String text = Scrape.text(registry);
        final double p50 = Scrape.value(text, "lmb_feeds_test_latency_ns{quantile=\"p50\"}");
        final double max = Scrape.value(text, "lmb_feeds_test_latency_ns{quantile=\"max\"}");
        assertEquals(500_000, p50, 500_000 * 0.01);
        assertEquals(1_000_000, max, 1_000_000 * 0.01);
        assertTrue(text.contains("quantile=\"p99\"") && text.contains("quantile=\"p999\""));

        final double[] idle = g.snapshot(); // empty interval: previous values kept
        assertEquals(max, idle[3]);
        g.record(7);
        assertEquals(7, g.snapshot()[3], 0.01);
    }
}
