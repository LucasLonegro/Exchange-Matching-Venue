package io.lmb.feeds;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** Prometheus text exposition of a registry, for assertions on metric names and labels. */
final class Scrape
{
    private Scrape()
    {
    }

    static String text(final PrometheusRegistry registry)
    {
        try
        {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrometheusTextFormatWriter.create().write(out, registry.scrape());
            return out.toString(StandardCharsets.UTF_8);
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /** Value of the sample line starting with {@code nameAndLabels + " "}; NaN if absent. */
    static double value(final String text, final String nameAndLabels)
    {
        for (final String line : text.split("\n"))
        {
            if (line.startsWith(nameAndLabels + " "))
            {
                return Double.parseDouble(line.substring(nameAndLabels.length() + 1).trim());
            }
        }
        return Double.NaN;
    }
}
