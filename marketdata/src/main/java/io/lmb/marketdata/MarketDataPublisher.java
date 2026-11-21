package io.lmb.marketdata;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.lmb.api.VenueClients;
import io.lmb.marketdata.grpc.MarketDataFanout;
import io.lmb.marketdata.grpc.MarketDataGrpcService;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * One publisher instance with all its parts: engine (publisher thread), UDP feed, retransmission server,
 * gRPC server and fan-out, metrics endpoint. Usable in-process by tests.
 */
public final class MarketDataPublisher implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(MarketDataPublisher.class);

    private final MdConfig config;
    private final PublisherMetrics metrics = new PublisherMetrics();
    private final RetransmitHistory history;
    private final FeedPublisher feed;
    private final MarketDataFanout fanout;
    private final MarketDataEngine engine;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private RetransmitServer retransmit;
    private Server grpc;
    private HTTPServer metricsServer;
    private Thread publisherThread;
    private volatile boolean running;
    private volatile Throwable failure;

    public MarketDataPublisher(final MdConfig config, final VenueClients venue) throws IOException
    {
        this.config = config;
        this.history = new RetransmitHistory(config.historyDepth);
        this.feed = config.incrementalDestinations.isEmpty() && config.snapshotDestinations.isEmpty() ? null
            : new FeedPublisher(config.sessionId, config.instance, config.incrementalDestinations, config.snapshotDestinations,
                config.multicastInterface, config.multicastTtl, config.maxDatagram, config.heartbeatIntervalMs, metrics);
        this.fanout = new MarketDataFanout(config.grpcRingBytes, config.grpcMaxQueued, config.grpcMaxSubscribers, metrics);
        this.engine = new MarketDataEngine(config, venue, history, feed, fanout, metrics);
    }

    public PublisherMetrics metrics()
    {
        return metrics;
    }

    public MarketDataEngine engine()
    {
        return engine;
    }

    FeedPublisher feed()
    {
        return feed;
    }

    public int retransmitPort()
    {
        return retransmit.port();
    }

    public int grpcPort()
    {
        return grpc.getPort();
    }

    /** Starts every part; returns once the instance is serving. */
    public void start() throws IOException
    {
        engine.start();
        retransmit = new RetransmitServer(new InetSocketAddress(config.bindHost, config.retransmitPort), history,
            config.retransmitMaxPerRequest, config.retransmitMaxConnections, config.retransmitIdleTimeoutMs, metrics,
            engine::requestSnapshot);
        grpc = NettyServerBuilder.forAddress(new InetSocketAddress(config.bindHost, config.grpcPort))
            .addService(new MarketDataGrpcService(fanout, config.grpcMaxQueued, metrics))
            .build().start();
        if (config.metricsPort > 0)
        {
            final PrometheusRegistry registry = new PrometheusRegistry();
            metrics.register(registry);
            metricsServer = HTTPServer.builder().hostname(config.bindHost).port(config.metricsPort).registry(registry)
                .buildAndStart();
        }
        fanout.start();
        running = true;
        publisherThread = Thread.ofPlatform().name("md-publisher-" + config.instance).start(this::run);
    }

    private void run()
    {
        final BackoffIdleStrategy idle = new BackoffIdleStrategy(100, 50, 1_000, 200_000);
        try
        {
            while (running)
            {
                idle.idle(engine.doWork(System.nanoTime()));
            }
        }
        catch (final Throwable t)
        {
            failure = t;
            LOG.error("publisher thread failed; instance stops publishing", t);
        }
        finally
        {
            engine.close(); // final checkpoint, on the publisher thread
            stopped.countDown();
        }
    }

    /** Null while healthy. */
    public Throwable failure()
    {
        return failure;
    }

    /** Waits until the publisher thread stops (failure or close). */
    public boolean awaitStopped(final long timeout, final TimeUnit unit) throws InterruptedException
    {
        return stopped.await(timeout, unit);
    }

    @Override
    public void close()
    {
        running = false;
        if (publisherThread != null)
        {
            try
            {
                if (!stopped.await(10, TimeUnit.SECONDS))
                {
                    LOG.warn("publisher thread did not stop in 10 s");
                }
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
        else
        {
            engine.close();
        }
        if (grpc != null)
        {
            grpc.shutdown();
        }
        fanout.close();
        if (grpc != null)
        {
            try
            {
                if (!grpc.awaitTermination(5, TimeUnit.SECONDS))
                {
                    grpc.shutdownNow();
                }
            }
            catch (final InterruptedException e)
            {
                grpc.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (retransmit != null)
        {
            retransmit.close();
        }
        if (feed != null)
        {
            feed.close();
        }
        if (metricsServer != null)
        {
            metricsServer.close();
        }
    }
}
