package io.lmb.ledger;

import io.lmb.api.SinkRunner;
import io.lmb.api.VenueClients;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * One ledger instance, usable in-process: the {@link LedgerSink} on its {@link SinkRunner} and the Prometheus
 * endpoint. The venue connection belongs to the caller. Run exactly one instance per database: the ledger is the
 * only writer of its tables.
 */
public final class Ledger implements AutoCloseable
{
    /** The venue's null Price (no price). */
    public static final long NULL_PRICE = Long.MIN_VALUE;

    private static final Logger LOG = LoggerFactory.getLogger(Ledger.class);

    private final LedgerConfig config;
    private final VenueClients venue;
    private final LedgerMetrics metrics = new LedgerMetrics();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private SinkRunner runner;
    private HTTPServer metricsServer;
    private boolean closed;

    public Ledger(final LedgerConfig config, final VenueClients venue)
    {
        this.config = config;
        this.venue = venue;
    }

    public synchronized Ledger start() throws IOException
    {
        try
        {
            Files.createDirectories(config.dataDir);
            Files.createDirectories(config.statementsDir);
            runner = new SinkRunner(new LedgerSink(config, metrics), venue, config.pollLimit, config.restartBackoffMs);
            metrics.bind(runner);
            if (config.metricsPort > 0)
            {
                final PrometheusRegistry registry = new PrometheusRegistry();
                metrics.register(registry);
                metricsServer = HTTPServer.builder().hostname(config.bindHost).port(config.metricsPort).registry(registry)
                    .buildAndStart();
            }
            runner.start();
        }
        catch (final IOException | RuntimeException e)
        {
            close();
            throw e;
        }
        LOG.info("ledger {}: database {}, statements in {}, metrics port {}", config.instance, config.dbUrl,
            config.statementsDir, config.metricsPort);
        return this;
    }

    public LedgerConfig config()
    {
        return config;
    }

    public LedgerMetrics metrics()
    {
        return metrics;
    }

    public SinkRunner runner()
    {
        return runner;
    }

    /** True once the ledger has caught up with every shard's live edge at least once. */
    public boolean isCaughtUp()
    {
        return runner != null && runner.isCaughtUp();
    }

    /** Waits for {@link #isCaughtUp()}; throws IllegalStateException if the instance is closed first. */
    public boolean awaitReady(final long timeoutMs) throws InterruptedException
    {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!isCaughtUp())
        {
            if (stopped.getCount() == 0)
            {
                throw new IllegalStateException("ledger closed");
            }
            if (System.currentTimeMillis() >= deadline)
            {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    public boolean awaitStopped(final long timeout, final TimeUnit unit) throws InterruptedException
    {
        return stopped.await(timeout, unit);
    }

    /** Stops the runner (the sink commits what it has applied), then the metrics endpoint. */
    @Override
    public synchronized void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        if (runner != null)
        {
            runner.close();
        }
        if (metricsServer != null)
        {
            metricsServer.close();
            metricsServer = null;
        }
        stopped.countDown();
        LOG.info("ledger {} stopped", config.instance);
    }
}
