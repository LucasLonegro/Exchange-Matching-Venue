package io.lmb.gateway;

import io.lmb.common.Config;
import io.lmb.common.Topology;

import java.nio.file.Path;

/**
 * Every tunable of the gateway. {@link #fromTopology} reads the keys documented in
 * docs/modules/gateway.md (system property, environment variable, topology file, default);
 * tests build one directly and change fields.
 */
public final class GatewayConfig
{
    public int gatewayId;
    public String bindHost = "0.0.0.0";
    public int grpcPort;
    public int fixPort;
    /** 0 disables the Prometheus HTTP endpoint. */
    public int metricsPort;
    public Path dataDir = Path.of("data");

    // session layer
    public int heartbeatIntervalMs = 5_000;
    public int heartbeatTimeoutMultiplier = 3;
    public int grpcInboundWindow = 64;
    public int maxInboundMessageBytes = 64 * 1024;
    public long slowConsumerTimeoutMs = 30_000;

    // throttling (NewOrder and Replace only)
    public double throttleRatePerSec = 500;
    public int throttleBurst = 100;

    // per-shard in-flight buffer
    public int inflightMax = 4_096;
    public int cancelHeadroom = 1_024;
    public long inflightTimeoutMs = 1_000;
    public int inflightMaxAttempts = 10;
    public long sendTimeoutMs = 500;

    // event streams
    public boolean streamFromStart = true;
    public long bootstrapTimeoutMs = 30_000;
    public int orderCacheMax = 1_000_000;

    // query API
    public boolean queryRequireApiKey = true;

    // TLS (plaintext when certChain is null)
    public String tlsCertChain;
    public String tlsPrivateKey;
    public String tlsClientCa;

    // FIX
    public boolean fixEnabled = true;
    public String fixCompId = "LMB";
    public int fixTermBufferLength = 256 * 1024;
    public boolean fixDeleteLogsOnStart = false;

    // core thread
    public long idleMaxParkUs = 1_000;
    public long metricsIntervalMs = 5_000;
    public long shutdownFlushMs = 1_000;

    public static GatewayConfig fromTopology(final Topology t, final int gatewayId)
    {
        final GatewayConfig c = new GatewayConfig();
        c.gatewayId = gatewayId;
        c.bindHost = t.get("lmb.gateway.bindHost", c.bindHost);
        c.grpcPort = t.gatewayGrpcPort(gatewayId);
        c.fixPort = t.gatewayFixPort(gatewayId);
        c.metricsPort = t.gatewayMetricsPort(gatewayId);
        c.dataDir = Path.of(Config.get("lmb.data.dir", "./data")).resolve("gateway-" + gatewayId);

        c.heartbeatIntervalMs = t.getInt("lmb.gateway.heartbeatIntervalMs", c.heartbeatIntervalMs);
        c.heartbeatTimeoutMultiplier = t.getInt("lmb.gateway.heartbeatTimeoutMultiplier", c.heartbeatTimeoutMultiplier);
        c.grpcInboundWindow = t.getInt("lmb.gateway.grpc.inboundWindow", c.grpcInboundWindow);
        c.maxInboundMessageBytes = t.getInt("lmb.gateway.grpc.maxInboundMessageBytes", c.maxInboundMessageBytes);
        c.slowConsumerTimeoutMs = t.getLong("lmb.gateway.slowConsumerTimeoutMs", c.slowConsumerTimeoutMs);

        c.throttleRatePerSec = Double.parseDouble(t.get("lmb.gateway.throttle.ratePerSec", String.valueOf(c.throttleRatePerSec)));
        c.throttleBurst = t.getInt("lmb.gateway.throttle.burst", c.throttleBurst);

        c.inflightMax = t.getInt("lmb.gateway.inflight.max", c.inflightMax);
        c.cancelHeadroom = t.getInt("lmb.gateway.inflight.cancelHeadroom", c.cancelHeadroom);
        c.inflightTimeoutMs = t.getLong("lmb.gateway.inflight.timeoutMs", c.inflightTimeoutMs);
        c.inflightMaxAttempts = t.getInt("lmb.gateway.inflight.maxAttempts", c.inflightMaxAttempts);
        c.sendTimeoutMs = t.getLong("lmb.gateway.sendTimeoutMs", c.sendTimeoutMs);

        c.streamFromStart = t.getBool("lmb.gateway.stream.fromStart", c.streamFromStart);
        c.bootstrapTimeoutMs = t.getLong("lmb.gateway.bootstrapTimeoutMs", c.bootstrapTimeoutMs);
        c.orderCacheMax = t.getInt("lmb.gateway.orderCache.max", c.orderCacheMax);

        c.queryRequireApiKey = t.getBool("lmb.gateway.query.requireApiKey", c.queryRequireApiKey);

        c.tlsCertChain = t.get("lmb.gateway.tls.certChain", null);
        c.tlsPrivateKey = t.get("lmb.gateway.tls.privateKey", null);
        c.tlsClientCa = t.get("lmb.gateway.tls.clientCa", null);

        c.fixEnabled = t.getBool("lmb.gateway.fix.enabled", c.fixEnabled);
        c.fixCompId = t.get("lmb.gateway.fix.compId", c.fixCompId);
        c.fixTermBufferLength = t.getInt("lmb.gateway.fix.termBufferLength", c.fixTermBufferLength);
        c.fixDeleteLogsOnStart = t.getBool("lmb.gateway.fix.deleteLogsOnStart", c.fixDeleteLogsOnStart);

        c.idleMaxParkUs = t.getLong("lmb.gateway.idle.maxParkUs", c.idleMaxParkUs);
        c.metricsIntervalMs = t.getLong("lmb.gateway.metrics.intervalMs", c.metricsIntervalMs);
        c.shutdownFlushMs = t.getLong("lmb.gateway.shutdownFlushMs", c.shutdownFlushMs);
        return c;
    }
}
