package io.lmb.posttrade;

import io.lmb.api.VenueClients;
import io.lmb.common.FirmDirectory;
import io.lmb.common.SymbolDirectory;
import io.lmb.posttrade.dropcopy.DropCopyEngine;
import io.lmb.posttrade.dropcopy.DropCopySink;
import io.lmb.posttrade.store.TradeStore;
import io.lmb.posttrade.surveillance.JsonLinesOutput;
import io.lmb.posttrade.surveillance.KafkaOutput;
import io.lmb.posttrade.surveillance.SurveillanceOutput;
import io.lmb.posttrade.surveillance.SurveillanceSink;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * One post-trade instance, usable in-process: the enabled sinks (trade store, FIX drop copy, surveillance), each
 * on its own {@link SinkRunner}, plus the drop copy's FIX engine and the Prometheus endpoint. The venue connection
 * belongs to the caller.
 */
public final class PostTrade implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(PostTrade.class);

    private final PostTradeConfig config;
    private final VenueClients venue;
    private final FirmDirectory firms;
    private final IntFunction<String> tickers;
    private final PostTradeMetrics metrics = new PostTradeMetrics();
    private final List<SinkRunner> runners = new ArrayList<>();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private DropCopyEngine dropCopyEngine;
    private HTTPServer metricsServer;
    private boolean closed;

    public PostTrade(final PostTradeConfig config, final VenueClients venue, final FirmDirectory firms,
        final SymbolDirectory symbols)
    {
        this.config = config;
        this.venue = venue;
        this.firms = firms;
        this.tickers = id ->
        {
            final SymbolDirectory.Symbol s = symbols == null ? null : symbols.byId(id);
            return s == null ? Integer.toString(id) : s.ticker();
        };
    }

    public synchronized PostTrade start() throws IOException
    {
        try
        {
            Files.createDirectories(config.dataDir);
            if (config.tradeStoreEnabled)
            {
                add(new TradeStore(config.dbUrl, config.dbUser, config.dbPassword, "tradestore-" + config.instance,
                    config.commitIntervalMs, config.commitBatch, metrics));
            }
            if (config.dropCopyEnabled)
            {
                dropCopyEngine = new DropCopyEngine(config.dataDir, config.bindHost, config.dropCopyPort,
                    config.dropCopyCompId, config.dropCopyAdminKey, config.dropCopyTermBufferLength,
                    config.dropCopyDeleteLogsOnStart, firms, metrics).start();
                add(new DropCopySink(dropCopyEngine, venue, tickers, config.dropCopyExecTypes,
                    config.dropCopyCheckpointIntervalMs, metrics));
            }
            if (config.surveillanceEnabled)
            {
                final SurveillanceOutput out = config.kafkaBootstrap == null || config.kafkaBootstrap.isBlank() ?
                    new JsonLinesOutput(config.surveillanceFile) :
                    new KafkaOutput(config.kafkaBootstrap, config.surveillanceTopic,
                        config.dataDir.resolve("surveillance-kafka.ckpt"));
                add(new SurveillanceSink(out, config.surveillanceCheckpointIntervalMs, metrics));
            }
            if (config.metricsPort > 0)
            {
                final PrometheusRegistry registry = new PrometheusRegistry();
                metrics.register(registry);
                metricsServer = HTTPServer.builder().hostname(config.bindHost).port(config.metricsPort).registry(registry)
                    .buildAndStart();
            }
            for (final SinkRunner r : runners)
            {
                r.start();
            }
        }
        catch (final IOException | RuntimeException e)
        {
            close();
            throw e;
        }
        LOG.info("posttrade {}: sinks {}, drop copy port {}, metrics port {}", config.instance, sinkNames(),
            dropCopyEngine == null ? "-" : config.dropCopyPort, config.metricsPort);
        return this;
    }

    private void add(final PostTradeSink sink)
    {
        final SinkRunner r = new SinkRunner(sink, venue, config.pollLimit, config.restartBackoffMs);
        runners.add(r);
        metrics.bind(r);
    }

    public List<String> sinkNames()
    {
        return runners.stream().map(SinkRunner::name).toList();
    }

    public List<SinkRunner> runners()
    {
        return List.copyOf(runners);
    }

    public PostTradeMetrics metrics()
    {
        return metrics;
    }

    public PostTradeConfig config()
    {
        return config;
    }

    /** True once every sink has caught up with every shard's live edge at least once. */
    public boolean isCaughtUp()
    {
        for (final SinkRunner r : runners)
        {
            if (!r.isCaughtUp())
            {
                return false;
            }
        }
        return true;
    }

    /** Waits for {@link #isCaughtUp()}; throws IllegalStateException if the instance is closed first. */
    public boolean awaitReady(final long timeoutMs) throws InterruptedException
    {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!isCaughtUp())
        {
            if (stopped.getCount() == 0)
            {
                throw new IllegalStateException("posttrade closed");
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

    /** Stops the runners (each closes its sink cleanly: final commit/checkpoint), then the FIX engine and metrics. */
    @Override
    public synchronized void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        for (final SinkRunner r : runners)
        {
            r.close();
        }
        if (dropCopyEngine != null)
        {
            dropCopyEngine.close();
            dropCopyEngine = null;
        }
        if (metricsServer != null)
        {
            metricsServer.close();
            metricsServer = null;
        }
        stopped.countDown();
        LOG.info("posttrade {} stopped", config.instance);
    }
}
