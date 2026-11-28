package io.lmb.gateway.grpc;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.lmb.gateway.core.GatewayCore;
import io.lmb.gateway.core.OrderRequest;
import io.lmb.gateway.core.Reject;
import io.lmb.gateway.core.ReportView;
import io.lmb.gateway.core.SessionOutlet;
import io.lmb.gateway.core.SessionState;
import io.lmb.proto.orderentry.ClientMessage;
import io.lmb.proto.orderentry.Heartbeat;
import io.lmb.proto.orderentry.Logon;
import io.lmb.proto.orderentry.LogonAck;
import io.lmb.proto.orderentry.MassCancelAck;
import io.lmb.proto.orderentry.ServerMessage;
import io.lmb.proto.orderentry.SessionReject;
import io.lmb.proto.orderentry.ShardCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One OrderEntry.Session stream. Netty threads only enqueue its inbound messages to the core; every
 * method here runs on the core thread, which is therefore the only writer of the response observer.
 *
 * <p>Flow control. Inbound: automatic requests are disabled; one message is requested before logon
 * and {@code grpcInboundWindow} after it, then one more per message the core has processed, so a
 * fast client cannot queue more than a window of work. Outbound: reports are written only while
 * {@code isReady()}; otherwise the core switches the session to re-reading the event stream (see
 * {@link GatewayCore}), so a slow reader never grows gateway memory.
 */
final class GrpcConnection implements SessionOutlet
{
    private static final Logger LOG = LoggerFactory.getLogger(GrpcConnection.class);

    private final GatewayCore core;
    private final ServerCallStreamObserver<ServerMessage> out;
    private final OrderRequest request = new OrderRequest();
    private SessionState session;
    private long serverSeq;
    private boolean closed;

    GrpcConnection(final GatewayCore core, final ServerCallStreamObserver<ServerMessage> out)
    {
        this.core = core;
        this.out = out;
    }

    // ------------------------------------------------------------------ inbound (core thread)

    void onMessage(final ClientMessage msg, final long receivedEpochNs)
    {
        if (closed)
        {
            return;
        }
        switch (msg.getBodyCase())
        {
            case LOGON -> onLogon(msg);
            case HEARTBEAT ->
            {
                if (session != null)
                {
                    core.onClientActivity(session);
                }
                out.request(1);
            }
            case NEW_ORDER, CANCEL_ORDER, REPLACE_ORDER, MASS_CANCEL -> onRequest(msg, receivedEpochNs);
            default ->
            {
                core.countReject(Reject.INVALID_FIELD);
                sendReject(Reject.INVALID_FIELD, msg.getSeq(), 0, "empty message body");
                out.request(1);
            }
        }
    }

    private void onLogon(final ClientMessage msg)
    {
        if (session != null)
        {
            core.countReject(Reject.INVALID_FIELD);
            sendReject(Reject.INVALID_FIELD, msg.getSeq(), 0, "already logged on");
            out.request(1);
            return;
        }
        final Logon l = msg.getLogon();
        final GatewayCore.LogonRequest req = new GatewayCore.LogonRequest();
        req.firmId = l.getFirmId();
        req.sessionId = l.getSessionId();
        req.apiKey = l.getApiKey();
        req.cancelOnDisconnect = l.getCancelOnDisconnect();
        req.cancelOnLogout = l.getCancelOnDisconnect(); // a gRPC stream has no logout distinct from closing it
        req.cancelOnDisconnectWindowMs = l.getCancelOnDisconnectWindowMs();
        req.nextClientSeq = l.getNextSeq() == 0 ? 1 : l.getNextSeq();
        if (l.getResumeFromCount() > 0)
        {
            req.resumeShards = new int[l.getResumeFromCount()];
            req.resumeSeqs = new long[l.getResumeFromCount()];
            for (int i = 0; i < l.getResumeFromCount(); i++)
            {
                final ShardCursor c = l.getResumeFrom(i);
                req.resumeShards[i] = c.getShardId();
                req.resumeSeqs[i] = c.getEventSeq();
            }
        }
        final Reject r = core.logon(req, this);
        if (r != null)
        {
            core.countReject(r);
            LOG.info("logon refused for session {} firm {}: {}", req.sessionId, req.firmId, r);
            sendReject(r, msg.getSeq(), 0, switch (r)
            {
                case AUTH_FAILED -> "unknown or disabled firm or session, session not of this firm, or bad, expired or revoked api key";
                case NOT_PERMITTED -> "this api key does not allow order entry";
                case DUPLICATE_SESSION -> "session " + req.sessionId + " is already connected";
                default -> "gateway not ready";
            });
            close(r == Reject.AUTH_FAILED ? Status.UNAUTHENTICATED : r == Reject.NOT_PERMITTED ? Status.PERMISSION_DENIED :
                r == Reject.DUPLICATE_SESSION ? Status.ALREADY_EXISTS : Status.UNAVAILABLE, r.name());
        }
    }

    private void onRequest(final ClientMessage msg, final long receivedEpochNs)
    {
        if (session == null)
        {
            core.countReject(Reject.NOT_LOGGED_ON);
            sendReject(Reject.NOT_LOGGED_ON, msg.getSeq(), 0, "logon first");
            out.request(1);
            return;
        }
        final OrderRequest req = request.reset();
        req.clientSeq = msg.getSeq();
        req.gatewayInTsNs = receivedEpochNs;
        switch (msg.getBodyCase())
        {
            case NEW_ORDER ->
            {
                final var o = msg.getNewOrder();
                req.kind = OrderRequest.Kind.NEW;
                req.clOrdId = o.getClOrdId();
                req.symbol = o.getSymbol();
                req.side = ProtoMapping.side(o.getSide());
                req.ordType = ProtoMapping.ordType(o.getType());
                req.timeInForce = ProtoMapping.tif(o.getTif());
                req.quantity = o.getQuantity();
                req.priceCents = o.getPriceCents();
                req.accountId = o.getAccountId() & 0xFFFF_FFFFL;
            }
            case CANCEL_ORDER ->
            {
                final var c = msg.getCancelOrder();
                req.kind = OrderRequest.Kind.CANCEL;
                req.clOrdId = c.getClOrdId();
                req.origClOrdId = c.getOrigClOrdId();
                req.symbol = c.getSymbol();
            }
            case REPLACE_ORDER ->
            {
                final var r = msg.getReplaceOrder();
                req.kind = OrderRequest.Kind.REPLACE;
                req.clOrdId = r.getClOrdId();
                req.origClOrdId = r.getOrigClOrdId();
                req.symbol = r.getSymbol();
                req.quantity = r.getNewQuantity();
                req.priceCents = r.getNewPriceCents();
            }
            case MASS_CANCEL ->
            {
                final var m = msg.getMassCancel();
                req.kind = OrderRequest.Kind.MASS_CANCEL;
                req.requestId = m.getRequestId();
                req.symbol = m.getSymbol();
            }
            default -> throw new IllegalStateException();
        }
        final int seqCheck = core.checkClientSeq(session, msg.getSeq());
        if (seqCheck == GatewayCore.SEQ_GAP)
        {
            core.countReject(Reject.BAD_SEQUENCE);
            sendReject(Reject.BAD_SEQUENCE, msg.getSeq(),
                req.kind == OrderRequest.Kind.MASS_CANCEL ? req.requestId : req.clOrdId,
                "expected seq " + session.nextExpectedClientSeq());
        }
        else
        {
            core.handleRequest(session, req, seqCheck == GatewayCore.SEQ_POSSIBLE_RESEND);
        }
        if (!closed)
        {
            out.request(1);
        }
    }

    /** The client half-closed, cancelled or the transport failed. */
    void onTransportGone(final boolean completeOurSide)
    {
        if (session != null)
        {
            core.onTransportClosed(session, this);
        }
        if (!closed)
        {
            closed = true;
            if (completeOurSide)
            {
                try
                {
                    out.onCompleted();
                }
                catch (final RuntimeException ignore)
                {
                    // already cancelled
                }
            }
        }
    }

    // ------------------------------------------------------------------ outlet (core thread)

    @Override
    public Transport transport()
    {
        return Transport.GRPC;
    }

    @Override
    public void onLogonAccepted(final SessionState s, final long nextExpectedClientSeq, final int heartbeatIntervalMs)
    {
        this.session = s;
        send(ServerMessage.newBuilder().setSeq(++serverSeq).setLogonAck(LogonAck.newBuilder()
            .setSessionId(s.sessionId()).setNextExpectedClientSeq(nextExpectedClientSeq)
            .setHeartbeatIntervalMs(heartbeatIntervalMs).setGatewayId(core.gatewayId())).build());
        out.request(core.config().grpcInboundWindow);
    }

    @Override
    public boolean isWritable()
    {
        return !closed && out.isReady();
    }

    @Override
    public boolean sendExecutionReport(final ReportView report, final boolean possDup)
    {
        if (closed)
        {
            return false;
        }
        return send(ServerMessage.newBuilder().setSeq(++serverSeq)
            .setExecutionReport(ProtoMapping.toProto(report, possDup)).build());
    }

    @Override
    public void sendMassCancelAck(
        final long requestId, final int shardId, final int canceledCount, final int shardsPending, final int totalCanceled)
    {
        send(ServerMessage.newBuilder().setSeq(++serverSeq).setMassCancelAck(MassCancelAck.newBuilder()
            .setRequestId(requestId).setShardId(shardId).setCanceledCount(canceledCount)
            .setShardsPending(shardsPending)).build());
    }

    @Override
    public void sendReject(final Reject reason, final long refSeq, final long refClOrdId, final String text)
    {
        send(ServerMessage.newBuilder().setSeq(++serverSeq).setReject(SessionReject.newBuilder()
            .setRefSeq(refSeq).setRefClOrdId(refClOrdId).setReason(ProtoMapping.toProto(reason))
            .setText(text == null ? "" : text)).build());
    }

    @Override
    public void sendHeartbeat()
    {
        send(ServerMessage.newBuilder().setSeq(serverSeq).setHeartbeat(Heartbeat.getDefaultInstance()).build());
    }

    @Override
    public boolean coreManagedHeartbeats()
    {
        return true;
    }

    @Override
    public long lastServerSeq()
    {
        return serverSeq;
    }

    @Override
    public void disconnect(final Reject reason, final String text)
    {
        if (closed)
        {
            return;
        }
        sendReject(reason, 0, 0, text);
        close(Status.UNAVAILABLE, text);
    }

    private void close(final Status status, final String text)
    {
        closed = true;
        try
        {
            out.onError(status.withDescription(text).asRuntimeException());
        }
        catch (final RuntimeException ignore)
        {
            // stream already cancelled
        }
    }

    private boolean send(final ServerMessage m)
    {
        if (closed)
        {
            return false;
        }
        try
        {
            out.onNext(m);
            return true;
        }
        catch (final RuntimeException e)
        {
            LOG.debug("send failed on closed stream", e);
            return false;
        }
    }
}
