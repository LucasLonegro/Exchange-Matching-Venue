package io.lmb.gateway;

import io.lmb.api.VenueClients;
import io.lmb.common.FirmDirectory;
import io.lmb.common.SymbolDirectory;
import io.lmb.gateway.core.GatewayCore;
import io.lmb.gateway.core.GatewayMetrics;
import io.lmb.gateway.grpc.GrpcFrontEnd;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * One gateway instance: the core thread, the gRPC server, optionally the FIX acceptor, and the
 * metrics endpoint. The caller owns the {@link VenueClients}.
 */
public final class Gateway implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);

    private final GatewayConfig cfg;
    private final GatewayMetrics metrics;
    private final GatewayCore core;
    private AgentRunner runner;
    private GrpcFrontEnd grpc;
    private AutoCloseable fix;
    private HTTPServer http;
    private boolean closed;

    public Gateway(final GatewayConfig cfg, final VenueClients venue, final SymbolDirectory symbols, final FirmDirectory firms)
    {
        this.cfg = cfg;
        this.metrics = new GatewayMetrics(venue.shardCount());
        this.core = new GatewayCore(cfg, venue, symbols, firms, metrics);
    }

    public Gateway start() throws Exception
    {
        try
        {
            if (cfg.fixEnabled)
            {
                fix = io.lmb.gateway.fix.FixFrontEnd.create(cfg, core);
            }
            runner = new AgentRunner(
                new BackoffIdleStrategy(100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(cfg.idleMaxParkUs)),
                t -> LOG.error("gateway core error", t), null, core);
            AgentRunner.startOnThread(runner);
            core.awaitStarted(60_000);
            grpc = new GrpcFrontEnd(cfg, core);
            if (cfg.metricsPort > 0)
            {
                http = HTTPServer.builder().port(cfg.metricsPort).registry(metrics.registry()).buildAndStart();
            }
            LOG.info("gateway {} started: gRPC {}, FIX {}, metrics {}", cfg.gatewayId, grpc.port(),
                cfg.fixEnabled ? String.valueOf(cfg.fixPort) : "disabled", cfg.metricsPort);
            return this;
        }
        catch (final Exception | Error e)
        {
            close();
            throw e;
        }
    }

    public boolean awaitReady(final long timeoutMs) throws InterruptedException
    {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!core.isReady())
        {
            if (System.currentTimeMillis() > deadline)
            {
                return false;
            }
            Thread.sleep(5);
        }
        return true;
    }

    public GatewayCore core()
    {
        return core;
    }

    public GatewayMetrics metrics()
    {
        return metrics;
    }

    public int grpcPort()
    {
        return grpc.port();
    }

    @Override
    public synchronized void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        if (grpc != null)
        {
            grpc.shutdown();
        }
        if (runner != null && !runner.isClosed())
        {
            core.beginShutdown();
            try
            {
                core.awaitShutdownFlushed(cfg.shutdownFlushMs + 2_000);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            runner.close();
        }
        if (grpc != null)
        {
            grpc.close();
        }
        if (fix != null)
        {
            try
            {
                fix.close();
            }
            catch (final Exception e)
            {
                LOG.warn("FIX front end close failed", e);
            }
        }
        if (http != null)
        {
            http.close();
        }
    }
}
