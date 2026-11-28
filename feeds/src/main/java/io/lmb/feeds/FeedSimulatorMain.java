package io.lmb.feeds;

import io.lmb.common.Config;
import io.lmb.common.SymbolDirectory;
import io.lmb.common.Topology;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Feed simulator process: the simulated external venues ({@link FeedSimulator}) sending to every
 * consolidator ({@link FeedsConfig#simulatorTargets}). Instance from {@code lmb.instance.id}. Prints
 * {@code LMB_READY feedsim <instance>} once the socket and the metrics endpoint are up; stops on
 * SIGTERM; exits with status 1 if the simulator thread fails.
 */
public final class FeedSimulatorMain implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(FeedSimulatorMain.class);

    private final int instance;
    private final FeedSimulator simulator;
    private final HTTPServer metricsServer;
    private final DutyLoop loop;

    private FeedSimulatorMain(
        final int instance, final FeedSimulator simulator, final HTTPServer metricsServer, final DutyLoop loop)
    {
        this.instance = instance;
        this.simulator = simulator;
        this.metricsServer = metricsServer;
        this.loop = loop;
    }

    /** Opens the socket and the metrics endpoint and starts the simulator thread; {@code symbols} is fixed. */
    public static FeedSimulatorMain start(final Topology topology, final int instance, final SymbolDirectory symbols,
        final EpochNanoClock clock)
    {
        final FeedSimulator.Settings settings = FeedSimulator.Settings.from(topology);
        final List<InetSocketAddress> targets = FeedsConfig.simulatorTargets(topology);
        final UdpFeedSink sink = new UdpFeedSink(targets, settings.venues());
        final FeedSimulator simulator = new FeedSimulator(settings, symbols, sink, clock.nanoTime());
        final PrometheusRegistry registry = new PrometheusRegistry();
        simulator.registerMetrics(registry);
        final HTTPServer server;
        final int metricsPort = topology.feedSimulatorMetricsPort();
        try
        {
            server = HTTPServer.builder().port(metricsPort).registry(registry).buildAndStart();
        }
        catch (final IOException e)
        {
            sink.close();
            throw new UncheckedIOException("cannot start metrics endpoint on port " + metricsPort, e);
        }
        final DutyLoop loop = new DutyLoop("feedsim-" + instance, () -> simulator.doWork(clock.nanoTime()), sink::close);
        loop.start();
        LOG.info("feedsim {}: {} venues, {} symbols, {} quotes/s per venue, targets {}, metrics {}", instance,
            settings.venues(), symbols.all().size(), settings.quotesPerSecond(), targets, metricsPort);
        return new FeedSimulatorMain(instance, simulator, server, loop);
    }

    FeedSimulator simulator()
    {
        return simulator;
    }

    DutyLoop loop()
    {
        return loop;
    }

    @Override
    public void close()
    {
        loop.close();
        metricsServer.close();
    }

    public static void main(final String[] args) throws Exception
    {
        final int instance = Config.getInt("lmb.instance.id", 0);
        final Topology topology = Topology.load();
        final SymbolDirectory symbols = ConsolidatorMain.symbols(topology, "feedsim-" + instance);
        if (symbols == null)
        {
            return;
        }
        final FeedSimulatorMain sim = start(topology, instance, symbols, new SystemEpochNanoClock());
        final Thread hook = new Thread(() ->
        {
            LOG.info("feedsim {} shutting down", instance);
            sim.close();
        }, "feedsim-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        System.out.println("LMB_READY feedsim " + instance);
        System.out.flush();
        sim.loop.awaitStopped(Long.MAX_VALUE, TimeUnit.DAYS);
        if (sim.loop.failure() != null)
        {
            Runtime.getRuntime().removeShutdownHook(hook);
            sim.close();
            System.exit(1);
        }
    }
}
