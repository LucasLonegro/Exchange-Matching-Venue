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

    // throttling, per session, for sessions whose tier the access directory does not define
    /** New orders and replaces. */
    public double throttleRatePerSec = 500;
    public int throttleBurst = 100;
    /** Every request, cancels and mass cancels included. */
    public double messageRatePerSec = 2_000;
    public int messageBurst = 400;

    // access (credentials): the access service, polled; without a URL, the firms CSV file of the topology
    /** Base URL of the access service's directory (required by GatewayMain). */
    public String accessUrl;
    /** Bearer token of the access service's directory; the default is the local one (lmb.access.serviceToken). */
    public String accessToken = "lmb-local-access";
    public long accessPollMs = 2_000;
    /** How often connected sessions are checked against the latest snapshot (revoked or expired keys). */
    public long accessRevalidateMs = 1_000;

    /** Upper bound of a cancel-on-disconnect time window (FIX 35003, gRPC cancel_on_disconnect_window_ms). */
    public long codMaxWindowMs = 60_000;

    // gRPC transport limits (Internet-facing)
    public long grpcHandshakeTimeoutMs = 10_000;
    public long grpcPermitKeepAliveMs = 10_000;
    public int grpcMaxConcurrentCallsPerConnection = 8;
    public int grpcMaxInboundMetadataBytes = 8 * 1024;

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

    // FIX (no TLS in Artio: on the Internet the FIX port sits behind a TLS-terminating proxy, see docs)
    public boolean fixEnabled = true;
    /** null: {@link #bindHost}. 127.0.0.1 when a local TLS proxy is the only way in. */
    public String fixBindHost;
    /** The port Artio listens on; 0: {@link #fixPort}. Behind a TLS proxy on the same host, the proxy owns fixPort. */
    public int fixListenPort;
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
        c.messageRatePerSec = Double.parseDouble(t.get("lmb.gateway.throttle.messageRatePerSec", String.valueOf(c.messageRatePerSec)));
        c.messageBurst = t.getInt("lmb.gateway.throttle.messageBurst", c.messageBurst);

        c.accessUrl = t.get("lmb.gateway.access.url", null);
        c.accessToken = t.get("lmb.gateway.access.token", c.accessToken);
        c.accessPollMs = t.getLong("lmb.gateway.access.pollMs", c.accessPollMs);
        c.accessRevalidateMs = t.getLong("lmb.gateway.access.revalidateMs", c.accessRevalidateMs);
        c.codMaxWindowMs = t.getLong("lmb.gateway.cod.maxWindowMs", c.codMaxWindowMs);

        c.grpcHandshakeTimeoutMs = t.getLong("lmb.gateway.grpc.handshakeTimeoutMs", c.grpcHandshakeTimeoutMs);
        c.grpcPermitKeepAliveMs = t.getLong("lmb.gateway.grpc.permitKeepAliveMs", c.grpcPermitKeepAliveMs);
        c.grpcMaxConcurrentCallsPerConnection = t.getInt("lmb.gateway.grpc.maxConcurrentCallsPerConnection",
            c.grpcMaxConcurrentCallsPerConnection);
        c.grpcMaxInboundMetadataBytes = t.getInt("lmb.gateway.grpc.maxInboundMetadataBytes", c.grpcMaxInboundMetadataBytes);

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
        c.fixBindHost = t.get("lmb.gateway.fix.bindHost", null);
        c.fixListenPort = t.getInt("lmb.gateway.fix.listenPort", 0);
        c.fixTermBufferLength = t.getInt("lmb.gateway.fix.termBufferLength", c.fixTermBufferLength);
        c.fixDeleteLogsOnStart = t.getBool("lmb.gateway.fix.deleteLogsOnStart", c.fixDeleteLogsOnStart);

        c.idleMaxParkUs = t.getLong("lmb.gateway.idle.maxParkUs", c.idleMaxParkUs);
        c.metricsIntervalMs = t.getLong("lmb.gateway.metrics.intervalMs", c.metricsIntervalMs);
        c.shutdownFlushMs = t.getLong("lmb.gateway.shutdownFlushMs", c.shutdownFlushMs);
        return c;
    }
}
