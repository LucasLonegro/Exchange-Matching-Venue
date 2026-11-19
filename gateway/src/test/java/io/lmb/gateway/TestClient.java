package io.lmb.gateway;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.lmb.proto.orderentry.CancelOrder;
import io.lmb.proto.orderentry.ClientMessage;
import io.lmb.proto.orderentry.ExecutionReport;
import io.lmb.proto.orderentry.Logon;
import io.lmb.proto.orderentry.LogonAck;
import io.lmb.proto.orderentry.MassCancel;
import io.lmb.proto.orderentry.MassCancelAck;
import io.lmb.proto.orderentry.NewOrder;
import io.lmb.proto.orderentry.OrdType;
import io.lmb.proto.orderentry.OrderEntryGrpc;
import io.lmb.proto.orderentry.ReplaceOrder;
import io.lmb.proto.orderentry.ServerMessage;
import io.lmb.proto.orderentry.SessionReject;
import io.lmb.proto.orderentry.ShardCursor;
import io.lmb.proto.orderentry.Side;
import io.lmb.proto.orderentry.TimeInForce;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** A participant on one OrderEntry.Session stream over a real loopback channel. */
final class TestClient implements AutoCloseable
{
    static final long TIMEOUT_MS = 5_000;

    private final ManagedChannel channel;
    private final BlockingQueue<ServerMessage> inbox = new LinkedBlockingQueue<>();
    final List<ExecutionReport> reports = new ArrayList<>();
    final List<SessionReject> rejects = new ArrayList<>();
    final List<MassCancelAck> massAcks = new ArrayList<>();
    private ClientCallStreamObserver<ClientMessage> requests;
    volatile Status closedWith;
    volatile boolean completed;
    long seq;

    TestClient(final int port)
    {
        this(port, false, 0);
    }

    /** manualFlow: the client reads nothing until {@link #request} (simulates a slow reader). */
    TestClient(final int port, final boolean manualFlow, final int flowWindow)
    {
        final NettyChannelBuilder b = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext();
        if (flowWindow > 0)
        {
            b.flowControlWindow(flowWindow);
        }
        channel = b.build();
        OrderEntryGrpc.newStub(channel).session(new ClientResponseObserver<ClientMessage, ServerMessage>()
        {
            public void beforeStart(final ClientCallStreamObserver<ClientMessage> r)
            {
                requests = r;
                if (manualFlow)
                {
                    r.disableAutoRequestWithInitial(1);
                }
            }

            public void onNext(final ServerMessage m)
            {
                inbox.add(m);
            }

            public void onError(final Throwable t)
            {
                closedWith = Status.fromThrowable(t);
            }

            public void onCompleted()
            {
                completed = true;
            }
        });
    }

    void request(final int n)
    {
        requests.request(n);
    }

    LogonAck logon(final int firm, final long session, final String key)
    {
        return logon(firm, session, key, false, 1);
    }

    LogonAck logon(final int firm, final long session, final String key, final boolean cod, final long nextSeq,
        final ShardCursor... resume)
    {
        sendLogon(firm, session, key, cod, nextSeq, resume);
        final ServerMessage m = next(TIMEOUT_MS, x -> x.hasLogonAck() || x.hasReject());
        assertTrue(m.hasLogonAck(), () -> "expected LogonAck, got " + m);
        seq = nextSeq - 1;
        return m.getLogonAck();
    }

    void sendLogon(final int firm, final long session, final String key, final boolean cod, final long nextSeq,
        final ShardCursor... resume)
    {
        final Logon.Builder l = Logon.newBuilder().setFirmId(firm).setSessionId(session).setApiKey(key)
            .setCancelOnDisconnect(cod).setNextSeq(nextSeq);
        for (final ShardCursor c : resume)
        {
            l.addResumeFrom(c);
        }
        requests.onNext(ClientMessage.newBuilder().setSeq(0).setLogon(l).build());
    }

    static ShardCursor cursor(final int shard, final long eventSeq)
    {
        return ShardCursor.newBuilder().setShardId(shard).setEventSeq(eventSeq).build();
    }

    long sendRaw(final long seqNo, final ClientMessage.Builder b)
    {
        requests.onNext(b.setSeq(seqNo).build());
        return seqNo;
    }

    long send(final ClientMessage.Builder b)
    {
        return sendRaw(++seq, b);
    }

    long buy(final long clOrdId, final String symbol, final long qty, final long price)
    {
        return send(newOrder(clOrdId, symbol, Side.BUY, qty, price));
    }

    long sell(final long clOrdId, final String symbol, final long qty, final long price)
    {
        return send(newOrder(clOrdId, symbol, Side.SELL, qty, price));
    }

    static ClientMessage.Builder newOrder(final long clOrdId, final String symbol, final Side side, final long qty, final long price)
    {
        return ClientMessage.newBuilder().setNewOrder(NewOrder.newBuilder().setClOrdId(clOrdId).setSymbol(symbol)
            .setSide(side).setType(OrdType.LIMIT).setTif(TimeInForce.DAY).setQuantity(qty).setPriceCents(price).setAccountId(7));
    }

    long cancel(final long clOrdId, final long orig, final String symbol)
    {
        return send(ClientMessage.newBuilder().setCancelOrder(
            CancelOrder.newBuilder().setClOrdId(clOrdId).setOrigClOrdId(orig).setSymbol(symbol)));
    }

    long replace(final long clOrdId, final long orig, final String symbol, final long qty, final long price)
    {
        return send(ClientMessage.newBuilder().setReplaceOrder(ReplaceOrder.newBuilder().setClOrdId(clOrdId)
            .setOrigClOrdId(orig).setSymbol(symbol).setNewQuantity(qty).setNewPriceCents(price)));
    }

    long massCancel(final long requestId, final String symbol)
    {
        return send(ClientMessage.newBuilder().setMassCancel(MassCancel.newBuilder().setRequestId(requestId).setSymbol(symbol)));
    }

    /** Next message matching the filter; every message read on the way is recorded. */
    ServerMessage next(final long timeoutMs, final Predicate<ServerMessage> filter)
    {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (true)
        {
            final long left = deadline - System.currentTimeMillis();
            final ServerMessage m;
            try
            {
                m = left <= 0 ? null : inbox.poll(left, TimeUnit.MILLISECONDS);
            }
            catch (final InterruptedException e)
            {
                throw new IllegalStateException(e);
            }
            if (m == null)
            {
                fail("timed out waiting for a message; reports=" + reports + " rejects=" + rejects + " closed=" + closedWith);
            }
            record(m);
            if (filter.test(m))
            {
                return m;
            }
        }
    }

    private void record(final ServerMessage m)
    {
        if (m.hasExecutionReport())
        {
            reports.add(m.getExecutionReport());
        }
        else if (m.hasReject())
        {
            rejects.add(m.getReject());
        }
        else if (m.hasMassCancelAck())
        {
            massAcks.add(m.getMassCancelAck());
        }
    }

    ExecutionReport awaitReport(final Predicate<ExecutionReport> p)
    {
        for (final ExecutionReport r : reports)
        {
            if (p.test(r))
            {
                return r;
            }
        }
        return next(TIMEOUT_MS, m -> m.hasExecutionReport() && p.test(m.getExecutionReport())).getExecutionReport();
    }

    ExecutionReport awaitReport(final long clOrdId, final io.lmb.proto.orderentry.ExecType type)
    {
        return awaitReport(r -> r.getClOrdId() == clOrdId && r.getExecType() == type);
    }

    SessionReject awaitReject(final SessionReject.Reason reason)
    {
        for (final SessionReject r : rejects)
        {
            if (r.getReason() == reason)
            {
                return r;
            }
        }
        return next(TIMEOUT_MS, m -> m.hasReject() && m.getReject().getReason() == reason).getReject();
    }

    MassCancelAck awaitMassAck(final int shardsPending)
    {
        for (final MassCancelAck a : massAcks)
        {
            if (a.getShardsPending() == shardsPending)
            {
                return a;
            }
        }
        return next(TIMEOUT_MS, m -> m.hasMassCancelAck() && m.getMassCancelAck().getShardsPending() == shardsPending)
            .getMassCancelAck();
    }

    /** Reads everything that arrives within the window. */
    void drain(final long windowMs)
    {
        final long deadline = System.currentTimeMillis() + windowMs;
        long left;
        while ((left = deadline - System.currentTimeMillis()) > 0)
        {
            try
            {
                final ServerMessage m = inbox.poll(left, TimeUnit.MILLISECONDS);
                if (m != null)
                {
                    record(m);
                }
            }
            catch (final InterruptedException e)
            {
                throw new IllegalStateException(e);
            }
        }
    }

    long countReports(final Predicate<ExecutionReport> p)
    {
        return reports.stream().filter(p).count();
    }

    void assertReportCount(final long expected, final Predicate<ExecutionReport> p, final String what)
    {
        assertEquals(expected, countReports(p), () -> what + "; reports=" + reports);
    }

    void awaitClosed()
    {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (closedWith == null && !completed)
        {
            if (System.currentTimeMillis() > deadline)
            {
                fail("stream not closed by the gateway");
            }
            drain(10);
        }
    }

    /** Client-side half close: the gateway treats it as a disconnect. */
    void closeStream()
    {
        requests.onCompleted();
    }

    void cancelStream()
    {
        requests.cancel("test disconnect", null);
    }

    @Override
    public void close()
    {
        try
        {
            requests.cancel("closed", null);
        }
        catch (final RuntimeException ignore)
        {
            // already closed
        }
        channel.shutdownNow();
        try
        {
            channel.awaitTermination(2, TimeUnit.SECONDS);
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
