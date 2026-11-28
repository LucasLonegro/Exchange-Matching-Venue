package io.lmb.gateway.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerCredentials;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.TlsServerCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.lmb.common.access.AccessDirectory;
import io.lmb.common.access.Scope;
import io.lmb.gateway.GatewayConfig;
import io.lmb.gateway.core.GatewayCore;
import io.lmb.proto.orderentry.ClientMessage;
import io.lmb.proto.orderentry.GetOrderRequest;
import io.lmb.proto.orderentry.ListSymbolsRequest;
import io.lmb.proto.orderentry.ListSymbolsResponse;
import io.lmb.proto.orderentry.OrderEntryGrpc;
import io.lmb.proto.orderentry.OrderQueryGrpc;
import io.lmb.proto.orderentry.OrderState;
import io.lmb.proto.orderentry.ServerMessage;
import io.lmb.proto.orderentry.SessionStatus;
import io.lmb.proto.orderentry.SessionStatusRequest;
import io.lmb.proto.orderentry.ShardCursor;
import io.lmb.proto.orderentry.SymbolInfo;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * gRPC server: {@code OrderEntry.Session} streams, {@code OrderQuery} unary calls (Envoy transcodes
 * these to REST) and server reflection. Runs on Netty with a direct executor: handlers only enqueue
 * work for the core thread, which answers.
 */
public final class GrpcFrontEnd implements AutoCloseable
{
    /** Metadata header carrying the firm's API key for OrderQuery calls. */
    public static final Metadata.Key<String> API_KEY_HEADER = Metadata.Key.of("x-lmb-api-key", Metadata.ASCII_STRING_MARSHALLER);
    static final Context.Key<String> API_KEY = Context.key("lmb-api-key");

    private final Server server;

    public GrpcFrontEnd(final GatewayConfig cfg, final GatewayCore core) throws IOException
    {
        final ServerCredentials creds;
        if (cfg.tlsCertChain != null && cfg.tlsPrivateKey != null)
        {
            final TlsServerCredentials.Builder tls = TlsServerCredentials.newBuilder()
                .keyManager(new File(cfg.tlsCertChain), new File(cfg.tlsPrivateKey));
            if (cfg.tlsClientCa != null)
            {
                tls.trustManager(new File(cfg.tlsClientCa)).clientAuth(TlsServerCredentials.ClientAuth.REQUIRE);
            }
            creds = tls.build();
        }
        else
        {
            creds = InsecureServerCredentials.create();
        }
        final ServerInterceptor apiKeyInterceptor = new ServerInterceptor()
        {
            public <Q, R> ServerCall.Listener<Q> interceptCall(
                final ServerCall<Q, R> call, final Metadata headers, final ServerCallHandler<Q, R> next)
            {
                final String key = headers.get(API_KEY_HEADER);
                return Contexts.interceptCall(Context.current().withValue(API_KEY, key), call, headers, next);
            }
        };
        server = NettyServerBuilder.forAddress(new InetSocketAddress(cfg.bindHost, cfg.grpcPort), creds)
            .directExecutor()
            .maxInboundMessageSize(cfg.maxInboundMessageBytes)
            .maxInboundMetadataSize(cfg.grpcMaxInboundMetadataBytes)
            .maxConcurrentCallsPerConnection(cfg.grpcMaxConcurrentCallsPerConnection)
            .handshakeTimeout(cfg.grpcHandshakeTimeoutMs, TimeUnit.MILLISECONDS)
            .permitKeepAliveTime(cfg.grpcPermitKeepAliveMs, TimeUnit.MILLISECONDS)
            .permitKeepAliveWithoutCalls(true)
            .addService(new OrderEntryService(core))
            .addService(ServerInterceptors.intercept(new OrderQueryService(core, cfg.queryRequireApiKey), apiKeyInterceptor))
            .addService(ProtoReflectionServiceV1.newInstance())
            .build()
            .start();
    }

    public int port()
    {
        return server.getPort();
    }

    /** Stops accepting new calls. */
    public void shutdown()
    {
        server.shutdown();
    }

    @Override
    public void close()
    {
        server.shutdown();
        try
        {
            if (!server.awaitTermination(2, TimeUnit.SECONDS))
            {
                server.shutdownNow();
                server.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }

    static final class OrderEntryService extends OrderEntryGrpc.OrderEntryImplBase
    {
        private final GatewayCore core;

        OrderEntryService(final GatewayCore core)
        {
            this.core = core;
        }

        @Override
        public StreamObserver<ClientMessage> session(final StreamObserver<ServerMessage> responseObserver)
        {
            final ServerCallStreamObserver<ServerMessage> out = (ServerCallStreamObserver<ServerMessage>)responseObserver;
            out.disableAutoRequest();
            final GrpcConnection conn = new GrpcConnection(core, out);
            out.setOnCancelHandler(() -> core.submit(() -> conn.onTransportGone(false)));
            out.request(1); // the Logon
            return new StreamObserver<>()
            {
                public void onNext(final ClientMessage msg)
                {
                    final long ts = core.epochNs();
                    core.submit(() -> conn.onMessage(msg, ts));
                }

                public void onError(final Throwable t)
                {
                    core.submit(() -> conn.onTransportGone(false));
                }

                public void onCompleted()
                {
                    core.submit(() -> conn.onTransportGone(true));
                }
            };
        }
    }

    static final class OrderQueryService extends OrderQueryGrpc.OrderQueryImplBase
    {
        private final GatewayCore core;
        private final boolean requireApiKey;

        OrderQueryService(final GatewayCore core, final boolean requireApiKey)
        {
            this.core = core;
            this.requireApiKey = requireApiKey;
        }

        /**
         * The x-lmb-api-key header must be a valid key with the QUERY scope of {@code firmId} (and, for a session's
         * status, a firm-wide key or the session's own). {@code firmId} -1: unknown session.
         */
        private boolean authorised(final int firmId, final long sessionId, final StreamObserver<?> obs)
        {
            final AccessDirectory directory = core.access().current();
            if (directory == null)
            {
                obs.onError(Status.UNAVAILABLE.withDescription("gateway not ready").asRuntimeException());
                return false;
            }
            if (requireApiKey)
            {
                final AccessDirectory.KeyEntry k = directory.authenticateFirm(API_KEY.get(), Scope.QUERY, System.currentTimeMillis());
                if (k == null)
                {
                    core.metrics().authFailureFromFrontEnd();
                    obs.onError(Status.UNAUTHENTICATED.withDescription("missing, wrong, expired or revoked x-lmb-api-key, " +
                        "or a key without the QUERY scope").asRuntimeException());
                    return false;
                }
                if (k.firmId() != firmId || (sessionId != 0 && k.sessionId() != 0 && k.sessionId() != sessionId))
                {
                    core.metrics().authFailureFromFrontEnd();
                    obs.onError(Status.PERMISSION_DENIED.withDescription("this key is not for that firm or session").asRuntimeException());
                    return false;
                }
            }
            else if (firmId < 0 || !directory.firms().containsKey(firmId))
            {
                obs.onError(Status.NOT_FOUND.withDescription("unknown firm or session").asRuntimeException());
                return false;
            }
            return true;
        }

        @Override
        public void getOrder(final GetOrderRequest request, final StreamObserver<OrderState> obs)
        {
            if (!authorised(request.getFirmId(), 0, obs))
            {
                return;
            }
            core.submit(() ->
            {
                final byte[] report = core.lastReport(request.getFirmId(), request.getClOrdId());
                final OrderState.Builder b = OrderState.newBuilder().setFound(report != null);
                if (report != null)
                {
                    b.setLastReport(ProtoMapping.toProto(core.viewOf(report), false));
                }
                obs.onNext(b.build());
                obs.onCompleted();
            });
        }

        @Override
        public void listSymbols(final ListSymbolsRequest request, final StreamObserver<ListSymbolsResponse> obs)
        {
            core.submit(() ->
            {
                final ListSymbolsResponse.Builder b = ListSymbolsResponse.newBuilder();
                for (final GatewayCore.SymbolSnapshot s : core.symbolSnapshots())
                {
                    b.addSymbols(SymbolInfo.newBuilder().setSymbolId(s.symbol().symbolId()).setTicker(s.symbol().ticker())
                        .setShardId(s.symbol().shard()).setTickSizeCents(s.symbol().tickSize())
                        .setLotSize(s.symbol().lotSize()).setState(s.state())
                        .setReferencePriceCents(s.symbol().referencePrice()));
                }
                obs.onNext(b.build());
                obs.onCompleted();
            });
        }

        @Override
        public void getSessionStatus(final SessionStatusRequest request, final StreamObserver<SessionStatus> obs)
        {
            final AccessDirectory directory = core.access().current();
            final AccessDirectory.SessionEntry session = directory == null ? null : directory.session(request.getSessionId());
            if (!authorised(session == null ? -1 : session.firmId(), request.getSessionId(), obs))
            {
                return;
            }
            core.submit(() ->
            {
                final GatewayCore.SessionStatusSnapshot st = core.sessionStatus(request.getSessionId(), 0);
                final SessionStatus.Builder b = SessionStatus.newBuilder().setConnected(st.connected())
                    .setGatewayId(st.gatewayId()).setLastClientSeq(Math.max(0, st.lastClientSeq()))
                    .setLastServerSeq(st.lastServerSeq());
                for (int sh = 0; sh < st.cursors().length; sh++)
                {
                    b.addCursors(ShardCursor.newBuilder().setShardId(sh).setEventSeq(st.cursors()[sh]));
                }
                obs.onNext(b.build());
                obs.onCompleted();
            });
        }
    }
}
