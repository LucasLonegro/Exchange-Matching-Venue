package io.lmb.tools.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.lmb.proto.orderentry.CancelOrder;
import io.lmb.proto.orderentry.ClientMessage;
import io.lmb.proto.orderentry.ExecutionReport;
import io.lmb.proto.orderentry.Heartbeat;
import io.lmb.proto.orderentry.Logon;
import io.lmb.proto.orderentry.LogonAck;
import io.lmb.proto.orderentry.NewOrder;
import io.lmb.proto.orderentry.OrdType;
import io.lmb.proto.orderentry.OrderEntryGrpc;
import io.lmb.proto.orderentry.ServerMessage;
import io.lmb.proto.orderentry.SessionReject;
import io.lmb.proto.orderentry.Side;
import io.lmb.proto.orderentry.TimeInForce;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** gRPC {@code OrderEntry.Session} participant. */
public final class GrpcOrderSession implements OrderSession
{
    private final String host;
    private final int port;
    private final long firmId;
    private final long sessionId;
    private final String apiKey;
    private final CompletableFuture<LogonAck> logonAck = new CompletableFuture<>();
    private final Object sendLock = new Object();
    private volatile Consumer<Report> listener = r -> { };
    private volatile boolean connected;
    private ManagedChannel channel;
    private StreamObserver<ClientMessage> out;
    private ScheduledExecutorService heartbeats;
    private long nextSeq;

    public GrpcOrderSession(final String host, final int port, final long firmId, final long sessionId, final String apiKey)
    {
        this.host = host;
        this.port = port;
        this.firmId = firmId;
        this.sessionId = sessionId;
        this.apiKey = apiKey;
    }

    @Override
    public void logon(final long timeoutMs) throws Exception
    {
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        out = OrderEntryGrpc.newStub(channel).session(new StreamObserver<>()
        {
            @Override
            public void onNext(final ServerMessage m)
            {
                onMessage(m);
            }

            @Override
            public void onError(final Throwable t)
            {
                connected = false;
                logonAck.completeExceptionally(t);
                listener.accept(sessionReject(0, "stream error: " + t));
            }

            @Override
            public void onCompleted()
            {
                connected = false;
                logonAck.completeExceptionally(new IllegalStateException("stream completed by gateway"));
            }
        });
        synchronized (sendLock)
        {
            out.onNext(ClientMessage.newBuilder().setSeq(0).setLogon(Logon.newBuilder()
                .setFirmId((int)firmId).setSessionId(sessionId).setApiKey(apiKey).setNextSeq(1)).build());
        }
        final LogonAck ack = logonAck.get(timeoutMs, TimeUnit.MILLISECONDS);
        synchronized (sendLock)
        {
            nextSeq = Math.max(1, ack.getNextExpectedClientSeq());
        }
        connected = true;
        final long hbMs = Math.max(100, ack.getHeartbeatIntervalMs());
        heartbeats = Executors.newSingleThreadScheduledExecutor(r ->
        {
            final Thread t = new Thread(r, "grpc-hb-" + sessionId);
            t.setDaemon(true);
            return t;
        });
        heartbeats.scheduleAtFixedRate(this::heartbeat, hbMs, hbMs, TimeUnit.MILLISECONDS);
    }

    private void heartbeat()
    {
        synchronized (sendLock)
        {
            if (connected)
            {
                out.onNext(ClientMessage.newBuilder().setHeartbeat(Heartbeat.getDefaultInstance()).build());
            }
        }
    }

    private void onMessage(final ServerMessage m)
    {
        final long now = System.nanoTime();
        switch (m.getBodyCase())
        {
            case LOGON_ACK -> logonAck.complete(m.getLogonAck());
            case EXECUTION_REPORT -> listener.accept(toReport(m.getExecutionReport(), now));
            case REJECT ->
            {
                final SessionReject r = m.getReject();
                if (!logonAck.isDone())
                {
                    logonAck.completeExceptionally(new IllegalStateException("logon rejected: " + r.getReason() + " " + r.getText()));
                }
                listener.accept(new Report("SESSION_REJECT", r.getRefClOrdId(), 0, "", '?', 0, 0, 0, 0, -1, 0, false,
                    r.getReason() + " " + r.getText(), now));
            }
            default -> { }
        }
    }

    private static Report sessionReject(final long clOrdId, final String text)
    {
        return new Report("SESSION_REJECT", clOrdId, 0, "", '?', 0, 0, 0, 0, -1, 0, false, text, System.nanoTime());
    }

    private static Report toReport(final ExecutionReport er, final long now)
    {
        final String kind = switch (er.getExecType())
        {
            case EXEC_NEW -> "NEW";
            case EXEC_PARTIAL_FILL -> "PARTIAL_FILL";
            case EXEC_FILL -> "FILL";
            case EXEC_CANCELED -> "CANCELED";
            case EXEC_REPLACED -> "REPLACED";
            case EXEC_REJECTED -> "REJECTED";
            case EXEC_EXPIRED -> "EXPIRED";
            case EXEC_CANCEL_REJECTED -> "CANCEL_REJECTED";
            default -> er.getExecType().name();
        };
        final String text = er.getRejectReason().isEmpty() ? er.getCancelReason() : er.getRejectReason();
        return new Report(kind, er.getClOrdId(), er.getOrderId(), er.getSymbol(), er.getSide() == Side.BUY ? 'B' : 'S',
            er.getLastQty(), er.getLastPxCents(), er.getLeavesQty(), er.getCumQty(), er.getShardId(), er.getEventSeq(),
            er.getPossDup(), text, now);
    }

    @Override
    public long newOrder(final long clOrdId, final String symbol, final char side, final long qty, final long priceCents)
    {
        final NewOrder.Builder o = NewOrder.newBuilder().setClOrdId(clOrdId).setSymbol(symbol)
            .setSide(side == 'B' ? Side.BUY : Side.SELL).setType(OrdType.LIMIT).setTif(TimeInForce.DAY)
            .setQuantity(qty).setPriceCents(priceCents);
        synchronized (sendLock)
        {
            final long t = System.nanoTime();
            out.onNext(ClientMessage.newBuilder().setSeq(nextSeq++).setNewOrder(o).build());
            return t;
        }
    }

    @Override
    public void cancel(final long clOrdId, final long origClOrdId, final String symbol)
    {
        synchronized (sendLock)
        {
            out.onNext(ClientMessage.newBuilder().setSeq(nextSeq++).setCancelOrder(CancelOrder.newBuilder()
                .setClOrdId(clOrdId).setOrigClOrdId(origClOrdId).setSymbol(symbol)).build());
        }
    }

    @Override
    public void listener(final Consumer<Report> listener)
    {
        this.listener = listener;
    }

    @Override
    public boolean isConnected()
    {
        return connected;
    }

    @Override
    public String describe()
    {
        return "grpc " + host + ":" + port + " firm=" + firmId + " session=" + sessionId;
    }

    @Override
    public void close()
    {
        connected = false;
        if (heartbeats != null)
        {
            heartbeats.shutdownNow();
        }
        synchronized (sendLock)
        {
            if (out != null)
            {
                try
                {
                    out.onCompleted();
                }
                catch (final RuntimeException ignored)
                {
                    // stream already failed
                }
            }
        }
        if (channel != null)
        {
            channel.shutdown();
            try
            {
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            channel.shutdownNow();
        }
    }
}
