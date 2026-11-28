package io.lmb.feeds;

import io.lmb.api.CommandClient;
import io.lmb.api.VenueClients;
import io.lmb.common.Config;
import io.lmb.common.SymbolDirectory;
import io.lmb.common.SymbolDirectorySource;
import io.lmb.common.Topology;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

/**
 * NBBO consolidator process. Instance from {@code lmb.instance.id}; feed handler from
 * {@code lmb.feeds.handler} (default {@code udp-sim}); parameters from {@link ConsolidatorConfig}.
 * Prints {@code LMB_READY consolidator <instance>} once the feed is open, the metrics endpoint is up and
 * every shard's command client is connected; stops cleanly on SIGTERM; exits with status 1 if the
 * duty thread fails.
 */
public final class ConsolidatorMain implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(ConsolidatorMain.class);

    private final int instance;
    private final NbboConsolidator consolidator;
    private final HTTPServer metricsServer;
    private final DutyLoop loop;

    private ConsolidatorMain(
        final int instance, final NbboConsolidator consolidator, final HTTPServer metricsServer, final DutyLoop loop)
    {
        this.instance = instance;
        this.consolidator = consolidator;
        this.metricsServer = metricsServer;
        this.loop = loop;
    }

    /**
     * Opens the feed, the command clients and the metrics endpoint, and starts the duty thread. The caller
     * keeps ownership of {@code venue} and closes it after {@link #close()}. {@code symbols} is fixed for the life
     * of the process: a symbol listed later gets an NBBO after a restart.
     */
    public static ConsolidatorMain start(final Topology topology, final int instance, final SymbolDirectory symbols,
        final VenueClients venue, final EpochNanoClock clock)
    {
        final ConsolidatorConfig config = ConsolidatorConfig.from(topology, instance);
        final String handlerName = topology.get(FeedsConfig.HANDLER, FeedsConfig.DEFAULT_HANDLER);
        final CommandClient[] clients = new CommandClient[venue.shardCount()];
        VenueFeedHandler handler = null;
        HTTPServer server = null;
        try
        {
            for (int shard = 0; shard < clients.length; shard++)
            {
                clients[shard] = venue.commandClient(shard);
            }
            final NbboConsolidator consolidator = new NbboConsolidator(config, symbols, clients, clock);
            handler = VenueFeedHandler.load(handlerName,
                new VenueFeedHandler.Context(topology, instance, symbols, config.venueCount(), clock));
            final PrometheusRegistry registry = new PrometheusRegistry();
            consolidator.stats().register(registry);
            handler.registerMetrics(registry);
            final int metricsPort = topology.consolidatorMetricsPort(instance);
            server = HTTPServer.builder().port(metricsPort).registry(registry).buildAndStart();
            handler.start(consolidator);

            final VenueFeedHandler h = handler;
            final DutyLoop loop = new DutyLoop("consolidator-" + instance,
                () -> h.poll() + consolidator.doWork(),
                () -> closeAll(h, clients));
            loop.start();
            LOG.info("consolidator {}: handler {}, {} venues, {} symbols on {} shards, metrics {}, {}", instance,
                handlerName, config.venueCount(), symbols.all().size(), clients.length, metricsPort, config);
            return new ConsolidatorMain(instance, consolidator, server, loop);
        }
        catch (final IOException e)
        {
            closeAll(handler, clients);
            throw new UncheckedIOException("cannot start metrics endpoint", e);
        }
        catch (final RuntimeException e)
        {
            if (server != null)
            {
                server.close();
            }
            closeAll(handler, clients);
            throw e;
        }
    }

    private static void closeAll(final VenueFeedHandler handler, final CommandClient[] clients)
    {
        if (handler != null)
        {
            handler.close();
        }
        for (final CommandClient c : clients)
        {
            if (c != null)
            {
                c.close();
            }
        }
    }

    NbboConsolidator consolidator()
    {
        return consolidator;
    }

    DutyLoop loop()
    {
        return loop;
    }

    /**
     * Waits until every shard's command client is connected, logging every 5 s, then prints the READY line.
     * Returns false if the duty thread stopped first.
     */
    boolean awaitReadyAndAnnounce(final PrintStream out) throws InterruptedException
    {
        long nextLogNs = System.nanoTime() + 5_000_000_000L;
        while (!consolidator.allShardsConnected())
        {
            if (loop.awaitStopped(10, TimeUnit.MILLISECONDS))
            {
                return false;
            }
            if (System.nanoTime() - nextLogNs > 0)
            {
                LOG.info("consolidator {} waiting for the shards' command clients to connect", instance);
                nextLogNs += 5_000_000_000L;
            }
        }
        out.println("LMB_READY consolidator " + instance);
        out.flush();
        return true;
    }

    @Override
    public void close()
    {
        loop.close();
        metricsServer.close();
    }

    /**
     * The control plane's symbol snapshot at start (or the cached one while it is unreachable), waiting for a first
     * one; null if interrupted.
     */
    static SymbolDirectory symbols(final Topology topology, final String name)
    {
        final Path cache = Path.of(topology.get("lmb.data.dir", "./data"), name, "symbol-directory.json");
        try (SymbolDirectorySource source = SymbolDirectorySource.fromTopology(topology, cache, name).start())
        {
            return source.awaitFirst(name) ? source.current() : null;
        }
    }

    public static void main(final String[] args) throws Exception
    {
        final int instance = Config.getInt("lmb.instance.id", 0);
        final Topology topology = Topology.load();
        final SymbolDirectory symbols = symbols(topology, "consolidator-" + instance);
        if (symbols == null)
        {
            return;
        }
        final VenueClients venue = VenueClients.connect(topology, "consolidator-" + instance);
        final ConsolidatorMain main;
        try
        {
            main = start(topology, instance, symbols, venue, new SystemEpochNanoClock());
        }
        catch (final RuntimeException e)
        {
            venue.close();
            throw e;
        }
        final Thread hook = new Thread(() ->
        {
            LOG.info("consolidator {} shutting down", instance);
            main.close();
            venue.close();
        }, "consolidator-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);

        if (main.awaitReadyAndAnnounce(System.out))
        {
            main.loop.awaitStopped(Long.MAX_VALUE, TimeUnit.DAYS);
        }
        if (main.loop.failure() != null)
        {
            Runtime.getRuntime().removeShutdownHook(hook);
            main.close();
            venue.close();
            System.exit(1);
        }
    }
}
