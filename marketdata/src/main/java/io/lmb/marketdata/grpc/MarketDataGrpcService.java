package io.lmb.marketdata.grpc;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.lmb.marketdata.PublisherMetrics;
import io.lmb.proto.marketdata.MarketDataGrpc;
import io.lmb.proto.marketdata.MarketDataUpdate;
import io.lmb.proto.marketdata.SubscribeRequest;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * {@code MarketData.Subscribe}: a snapshot of every matching symbol (md_seq = the last sequenced message it
 * includes), then trades, status and auction updates as they happen and book incrementals, either per log
 * entry (conflation_ms = 0) or at most one per symbol per conflation interval. A subscriber that does not
 * keep up (more than {@code maxQueued} updates waiting for the transport) is closed with RESOURCE_EXHAUSTED.
 */
public final class MarketDataGrpcService extends MarketDataGrpc.MarketDataImplBase
{
    public static final int MAX_DEPTH = 10_000;
    public static final int MAX_CONFLATION_MS = 60_000;

    private final MarketDataFanout fanout;
    private final int maxQueued;
    private final PublisherMetrics metrics;

    public MarketDataGrpcService(final MarketDataFanout fanout, final int maxQueued, final PublisherMetrics metrics)
    {
        this.fanout = fanout;
        this.maxQueued = maxQueued;
        this.metrics = metrics;
    }

    @Override
    public void subscribe(final SubscribeRequest request, final StreamObserver<MarketDataUpdate> responseObserver)
    {
        final ServerCallStreamObserver<MarketDataUpdate> observer = (ServerCallStreamObserver<MarketDataUpdate>)responseObserver;
        if (request.getDepth() > MAX_DEPTH || request.getConflationMs() > MAX_CONFLATION_MS)
        {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(
                "depth must be <= " + MAX_DEPTH + " and conflation_ms <= " + MAX_CONFLATION_MS).asRuntimeException());
            return;
        }
        final Set<String> tickers = Set.copyOf(request.getSymbolsList());
        final Subscriber s = new Subscriber(fanout.nextId(), observer, tickers, request.getDepth(),
            TimeUnit.MILLISECONDS.toNanos(request.getConflationMs()), maxQueued, metrics);
        observer.setOnCancelHandler(s::cancel);
        if (!fanout.register(s))
        {
            observer.onError(Status.UNAVAILABLE.withDescription(fanout.isBroken()
                ? "gRPC market data unavailable" : "too many subscribers").asRuntimeException());
        }
    }
}
