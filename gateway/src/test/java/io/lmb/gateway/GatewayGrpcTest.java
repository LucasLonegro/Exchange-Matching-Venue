package io.lmb.gateway;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.lmb.gateway.core.Reject;
import io.lmb.gateway.grpc.GrpcFrontEnd;
import io.lmb.proto.orderentry.ExecType;
import io.lmb.proto.orderentry.ExecutionReport;
import io.lmb.proto.orderentry.GetOrderRequest;
import io.lmb.proto.orderentry.ListSymbolsRequest;
import io.lmb.proto.orderentry.LogonAck;
import io.lmb.proto.orderentry.MassCancelAck;
import io.lmb.proto.orderentry.OrdStatus;
import io.lmb.proto.orderentry.OrderQueryGrpc;
import io.lmb.proto.orderentry.OrderState;
import io.lmb.proto.orderentry.ServerMessage;
import io.lmb.proto.orderentry.SessionReject;
import io.lmb.proto.orderentry.SessionStatus;
import io.lmb.proto.orderentry.SessionStatusRequest;
import io.lmb.proto.orderentry.Side;
import io.lmb.sbe.CancelOrderDecoder;
import io.lmb.sbe.CancelReason;
import io.lmb.sbe.MassCancelDecoder;
import io.lmb.sbe.MassCancelScope;
import io.lmb.sbe.NewOrderDecoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.lmb.gateway.TestClient.cursor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayGrpcTest
{
    private final Fixture fx = new Fixture();

    @AfterEach
    void tearDown()
    {
        fx.close();
    }

    @Test
    void logonSucceedsAndBadCredentialsAreRefused() throws Exception
    {
        final Gateway g = fx.gateway(3);
        final TestClient ok = fx.client(g);
        final LogonAck ack = ok.logon(100, 1001, "alpha-key", false, 1);
        assertEquals(1001, ack.getSessionId());
        assertEquals(1, ack.getNextExpectedClientSeq());
        assertEquals(3, ack.getGatewayId());
        assertEquals(5_000, ack.getHeartbeatIntervalMs());

        final TestClient badKey = fx.client(g);
        badKey.sendLogon(100, 1002, "wrong", false, 1);
        assertEquals(SessionReject.Reason.AUTH_FAILED, badKey.awaitReject(SessionReject.Reason.AUTH_FAILED).getReason());
        badKey.awaitClosed();
        assertEquals(Status.Code.UNAUTHENTICATED, badKey.closedWith.getCode());

        final TestClient otherFirmsSession = fx.client(g);
        otherFirmsSession.sendLogon(100, 2001, "alpha-key", false, 1);
        otherFirmsSession.awaitReject(SessionReject.Reason.AUTH_FAILED);
        otherFirmsSession.awaitClosed();

        final TestClient duplicate = fx.client(g);
        duplicate.sendLogon(100, 1001, "alpha-key", false, 1);
        duplicate.awaitReject(SessionReject.Reason.DUPLICATE_SESSION);
        duplicate.awaitClosed();

        final TestClient notLoggedOn = fx.client(g);
        notLoggedOn.buy(1, "AAPL", 10, 19000);
        notLoggedOn.awaitReject(SessionReject.Reason.NOT_LOGGED_ON);

        // the good session is unaffected by all of that
        ok.buy(10, "AAPL", 10, 19000);
        ok.awaitReport(10, ExecType.EXEC_NEW);
        assertEquals(1, g.metrics().rejects(Reject.DUPLICATE_SESSION));
    }

    @Test
    void newOrderIsAckedAndFilledAgainstAnotherFirm() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.alpha(g);
        final TestClient b = fx.beta(g);

        a.buy(1, "AAPL", 100, 19000);
        final ExecutionReport ackA = a.awaitReport(1, ExecType.EXEC_NEW);
        assertEquals("AAPL", ackA.getSymbol());
        assertEquals(Side.BUY, ackA.getSide());
        assertEquals(0, ackA.getShardId());
        assertEquals(100, ackA.getLeavesQty());
        assertEquals(7, ackA.getAccountId());
        assertFalse(ackA.getPossDup());
        assertTrue(ackA.getExecId().startsWith("0-"), ackA.getExecId());
        assertTrue(ackA.getOrderId() > 0);

        b.sell(1, "AAPL", 40, 18990);
        b.awaitReport(1, ExecType.EXEC_NEW);
        final ExecutionReport takerFill = b.awaitReport(1, ExecType.EXEC_FILL);
        assertEquals(40, takerFill.getLastQty());
        assertEquals(19000, takerFill.getLastPxCents());
        assertEquals("TAKER", takerFill.getLiquidity());

        final ExecutionReport makerFill = a.awaitReport(1, ExecType.EXEC_PARTIAL_FILL);
        assertEquals(OrdStatus.STATUS_PARTIALLY_FILLED, makerFill.getOrdStatus());
        assertEquals(60, makerFill.getLeavesQty());
        assertEquals("MAKER", makerFill.getLiquidity());
        assertEquals(takerFill.getMatchId(), makerFill.getMatchId());

        // a venue reject comes back as an execution report, not a session reject
        a.buy(2, "AAPL", 10, 1);
        final ExecutionReport rej = a.awaitReport(2, ExecType.EXEC_REJECTED);
        assertEquals("INVALID_PRICE", rej.getRejectReason());

        // each session only sees its own reports
        b.drain(100);
        assertEquals(0, b.countReports(r -> r.getClOrdId() == 2));
    }

    @Test
    void statelessValidationRefusesBeforeTheLog() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.alpha(g);
        a.buy(1, "NOPE", 10, 100);
        a.awaitReject(SessionReject.Reason.UNKNOWN_SYMBOL);
        a.buy(2, "AAPL", 100_001, 19000);
        a.awaitReject(SessionReject.Reason.MAX_ORDER_SIZE);
        a.buy(3, "AAPL", 0, 19000);
        a.awaitReject(SessionReject.Reason.INVALID_FIELD);
        a.send(TestClient.newOrder(4, "AAPL", Side.SIDE_UNSPECIFIED, 10, 19000));
        a.drain(100);
        assertEquals(2, a.rejects.stream().filter(r -> r.getReason() == SessionReject.Reason.INVALID_FIELD).count());
        assertEquals(0, fx.machines[0].countCommands(NewOrderDecoder.TEMPLATE_ID), "nothing reached the shard");
    }

    @Test
    void cancelAndReplaceRoundTrips() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.alpha(g);
        a.buy(1, "GOOG", 100, 17000);
        a.awaitReport(1, ExecType.EXEC_NEW);

        a.replace(2, 1, "GOOG", 60, 17010);
        final ExecutionReport replaced = a.awaitReport(2, ExecType.EXEC_REPLACED);
        assertEquals(1, replaced.getOrigClOrdId());
        assertEquals(60, replaced.getOrderQty());
        assertEquals(17010, replaced.getPriceCents());
        assertEquals(1, replaced.getShardId());

        a.cancel(3, 2, "GOOG");
        final ExecutionReport canceled = a.awaitReport(3, ExecType.EXEC_CANCELED);
        assertEquals(2, canceled.getOrigClOrdId());
        assertEquals("CLIENT", canceled.getCancelReason());

        a.cancel(4, 99, "GOOG");
        assertEquals("UNKNOWN_ORDER", a.awaitReport(4, ExecType.EXEC_CANCEL_REJECTED).getRejectReason());
    }

    @Test
    void massCancelFansOutToEveryShardWithAggregatedAcks() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.alpha(g);
        a.buy(1, "AAPL", 10, 19000);
        a.buy(2, "MSFT", 10, 42000);
        a.buy(3, "GOOG", 10, 17000);
        a.awaitReport(3, ExecType.EXEC_NEW);
        a.awaitReport(1, ExecType.EXEC_NEW);
        a.awaitReport(2, ExecType.EXEC_NEW);

        a.massCancel(77, "");
        final MassCancelAck last = a.awaitMassAck(0);
        assertEquals(77, last.getRequestId());
        assertEquals(2, a.massAcks.size(), "one ack per shard");
        assertEquals(1, a.massAcks.get(0).getShardsPending());
        assertEquals(3, a.massAcks.stream().mapToInt(MassCancelAck::getCanceledCount).sum());
        for (final long id : new long[] {1, 2, 3})
        {
            assertEquals("MASS_CANCEL", a.awaitReport(id, ExecType.EXEC_CANCELED).getCancelReason());
        }
        // symbol-scoped: only the owning shard
        a.buy(4, "GOOG", 10, 17000);
        a.awaitReport(4, ExecType.EXEC_NEW);
        a.massCancel(78, "GOOG");
        a.next(TestClient.TIMEOUT_MS, m -> m.hasMassCancelAck() && m.getMassCancelAck().getRequestId() == 78);
        final MassCancelAck single = a.massAcks.get(a.massAcks.size() - 1);
        assertEquals(1, single.getShardId());
        assertEquals(0, single.getShardsPending());
        assertEquals(1, single.getCanceledCount());
        final ScriptedShard.Command cmd = fx.machines[1].commands().stream()
            .filter(c -> c.templateId() == MassCancelDecoder.TEMPLATE_ID).reduce((x, y) -> y).orElseThrow();
        assertEquals(MassCancelScope.FIRM_SYMBOL, cmd.scope());
    }

    @Test
    void sequenceGapIsRejectedAndLowerSequenceIsTreatedAsResend() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.alpha(g);
        a.sendRaw(1, TestClient.newOrder(1, "AAPL", Side.BUY, 10, 19000));
        a.awaitReport(1, ExecType.EXEC_NEW);

        a.sendRaw(3, TestClient.newOrder(3, "AAPL", Side.BUY, 10, 19000));
        final SessionReject gap = a.awaitReject(SessionReject.Reason.BAD_SEQUENCE);
        assertEquals(3, gap.getRefSeq());
        assertTrue(gap.getText().contains("expected seq 2"), gap.getText());

        a.sendRaw(2, TestClient.newOrder(2, "AAPL", Side.BUY, 10, 19000));
        a.awaitReport(2, ExecType.EXEC_NEW);
        a.sendRaw(3, TestClient.newOrder(3, "AAPL", Side.BUY, 10, 19000));
        a.awaitReport(3, ExecType.EXEC_NEW);

        // seq 1 again: a possible resend. Its outcome is known, so the gateway repeats the report
        // with poss_dup and does not submit it again.
        a.sendRaw(1, TestClient.newOrder(1, "AAPL", Side.BUY, 10, 19000));
        final ExecutionReport dup = a.awaitReport(r -> r.getClOrdId() == 1 && r.getPossDup());
        assertEquals(ExecType.EXEC_NEW, dup.getExecType());
        a.drain(100);
        assertEquals(3, fx.machines[0].countCommands(NewOrderDecoder.TEMPLATE_ID));
        assertEquals(1, a.countReports(r -> r.getClOrdId() == 1 && !r.getPossDup()));

        // a possible resend with an unseen clOrdId is applied once, submitted with possResend=TRUE
        a.sendRaw(2, TestClient.newOrder(9, "AAPL", Side.BUY, 10, 19000));
        a.awaitReport(9, ExecType.EXEC_NEW);
        assertTrue(fx.machines[0].commands().stream().anyMatch(c -> c.clOrdId() == 9 && c.possResend()));
    }

    @Test
    void throttleRefusesExcessOrdersButNotCancels() throws Exception
    {
        final GatewayConfig cfg = Fixture.config(1);
        cfg.throttleRatePerSec = 0.5;
        cfg.throttleBurst = 3;
        final Gateway g = fx.gateway(cfg);
        final TestClient a = fx.alpha(g);
        for (int i = 1; i <= 6; i++)
        {
            a.buy(i, "AAPL", 10, 19000);
        }
        a.awaitReport(3, ExecType.EXEC_NEW);
        a.drain(200);
        assertEquals(3, a.countReports(r -> r.getExecType() == ExecType.EXEC_NEW));
        assertEquals(3, a.rejects.stream().filter(r -> r.getReason() == SessionReject.Reason.THROTTLED).count());
        assertEquals(Set.of(4L, 5L, 6L), a.rejects.stream().map(SessionReject::getRefClOrdId).collect(java.util.stream.Collectors.toSet()));

        a.cancel(10, 1, "AAPL");
        a.awaitReport(10, ExecType.EXEC_CANCELED);
        a.massCancel(11, "");
        a.awaitMassAck(0);
        assertEquals(3, g.metrics().rejects(Reject.THROTTLED));
    }

    @Test
    void unavailableShardIsRefusedExplicitlyAndRecovers() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.alpha(g);
        fx.inMemory.setAvailable(0, false);
        Thread.sleep(50);
        a.buy(1, "AAPL", 10, 19000);
        final SessionReject r = a.awaitReject(SessionReject.Reason.UNAVAILABLE);
        assertEquals(1, r.getRefClOrdId());

        a.buy(2, "GOOG", 10, 17000); // the other shard is fine
        a.awaitReport(2, ExecType.EXEC_NEW);

        fx.inMemory.setAvailable(0, true);
        Thread.sleep(50);
        a.buy(3, "AAPL", 10, 19000);
        a.awaitReport(3, ExecType.EXEC_NEW);
        assertEquals(0, a.countReports(x -> x.getClOrdId() == 1));
    }

    @Test
    void droppedOfferIsResubmittedAndReportedExactlyOnce() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.alpha(g);
        fx.inMemory.dropNextOffers(0, 1);
        a.buy(1, "AAPL", 10, 19000);
        a.awaitReport(1, ExecType.EXEC_NEW);
        a.drain(500);
        a.assertReportCount(1, r -> r.getClOrdId() == 1, "exactly one report for the dropped order");
        final List<ScriptedShard.Command> applied = fx.machines[0].commands();
        assertEquals(1, applied.size(), "the dropped offer never reached the machine");
        assertTrue(applied.get(0).possResend(), "the resubmission carries possResend=TRUE");
        assertTrue(g.metrics().resubmits() >= 1);
    }

    @Test
    void resubmissionOfAnAlreadyAppliedCommandIsDroppedByDedupe() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.alpha(g);
        fx.venue.pause(0, true); // the command applies but the gateway cannot see its effect yet
        a.buy(1, "AAPL", 10, 19000);
        Fixture.await(() -> fx.machines[0].countCommands(NewOrderDecoder.TEMPLATE_ID) >= 3, "two resubmissions");
        fx.venue.pause(0, false);
        a.awaitReport(1, ExecType.EXEC_NEW);
        a.drain(400);
        a.assertReportCount(1, r -> r.getClOrdId() == 1, "one report despite several submissions");
        final long resent = fx.machines[0].commands().stream().filter(ScriptedShard.Command::possResend).count();
        assertTrue(resent >= 2, "resubmissions use possResend=TRUE");
        final long settled = fx.machines[0].countCommands(NewOrderDecoder.TEMPLATE_ID);
        Thread.sleep(400);
        assertEquals(settled, fx.machines[0].countCommands(NewOrderDecoder.TEMPLATE_ID), "no resubmission after the effect was seen");
    }

    @Test
    void reconnectWithResumeDeliversMissedReportsOnceWithPossDup() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a1 = fx.alpha(g);
        final TestClient b = fx.beta(g);
        a1.buy(1, "AAPL", 100, 19000);
        final ExecutionReport ack = a1.awaitReport(1, ExecType.EXEC_NEW);
        a1.cancelStream();
        Fixture.await(() -> !sessionConnected(g, 1001), "disconnect seen by the gateway");

        b.sell(1, "AAPL", 30, 19000); // a fill A misses while away
        b.awaitReport(1, ExecType.EXEC_FILL);

        final TestClient a2 = fx.client(g);
        final LogonAck ack2 = a2.logon(100, 1001, "alpha-key", false, 2, cursor(0, ack.getEventSeq()));
        assertEquals(2, ack2.getNextExpectedClientSeq(), "sequence continues on the same gateway");
        final ExecutionReport missed = a2.awaitReport(1, ExecType.EXEC_PARTIAL_FILL);
        assertTrue(missed.getPossDup());
        assertEquals(30, missed.getLastQty());

        b.sell(2, "AAPL", 20, 19000); // after the resume: live, not poss dup
        final ExecutionReport live = a2.awaitReport(r -> r.getClOrdId() == 1 && r.getCumQty() == 50);
        assertFalse(live.getPossDup());
        a2.drain(300);
        a2.assertReportCount(1, r -> r.getClOrdId() == 1 && r.getCumQty() == 30, "missed report once");
        a2.assertReportCount(0, r -> r.getExecType() == ExecType.EXEC_NEW, "nothing before the cursor");
        final Set<Long> seen = new HashSet<>();
        for (final ExecutionReport r : a2.reports)
        {
            assertTrue(seen.add(r.getEventSeq()), "duplicate event " + r.getEventSeq());
        }
    }

    @Test
    void cancelOnDisconnectCancelsTheSessionsOrdersOnEveryShard() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.client(g);
        a.logon(100, 1001, "alpha-key", true, 1);
        final TestClient keep = fx.client(g);
        keep.logon(100, 1002, "alpha-key", false, 1); // same firm, other session: unaffected
        a.buy(1, "AAPL", 10, 19000);
        a.buy(2, "GOOG", 10, 17000);
        keep.buy(3, "AAPL", 10, 19000);
        a.awaitReport(1, ExecType.EXEC_NEW);
        a.awaitReport(2, ExecType.EXEC_NEW);
        keep.awaitReport(3, ExecType.EXEC_NEW);

        a.closeStream();
        Fixture.await(() -> fx.machines[0].openOrders(1001) == 0 && fx.machines[1].openOrders(1001) == 0, "orders canceled");
        assertEquals(1, fx.machines[0].openOrders(1002));
        for (final ScriptedShard m : fx.machines)
        {
            final ScriptedShard.Command c = m.commands().stream()
                .filter(x -> x.templateId() == MassCancelDecoder.TEMPLATE_ID).findFirst().orElseThrow();
            assertEquals(MassCancelScope.SESSION, c.scope());
            assertEquals(CancelReason.DISCONNECT, c.reason());
        }

        final TestClient back = fx.client(g);
        back.logon(100, 1001, "alpha-key", false, 3, cursor(0, 0), cursor(1, 0));
        assertEquals("DISCONNECT", back.awaitReport(1, ExecType.EXEC_CANCELED).getCancelReason());
        assertEquals("DISCONNECT", back.awaitReport(2, ExecType.EXEC_CANCELED).getCancelReason());
    }

    @Test
    void twoGatewaysShareASessionsReports() throws Exception
    {
        final Gateway g1 = fx.gateway(1);
        final Gateway g2 = fx.gateway(2);
        final TestClient a1 = fx.alpha(g1);
        a1.buy(1, "GOOG", 100, 17000);
        final ExecutionReport ack = a1.awaitReport(1, ExecType.EXEC_NEW);
        a1.cancelStream();
        Fixture.await(() -> !sessionConnected(g1, 1001), "disconnect on gateway 1");

        // fail over to gateway 2, replaying from the start: the earlier ack comes back as poss_dup
        final TestClient a2 = fx.client(g2);
        final LogonAck ack2 = a2.logon(100, 1001, "alpha-key", false, 2, cursor(1, 0));
        assertEquals(2, ack2.getGatewayId());
        final ExecutionReport replayed = a2.awaitReport(1, ExecType.EXEC_NEW);
        assertTrue(replayed.getPossDup());
        assertEquals(ack.getExecId(), replayed.getExecId());

        // an order entered through gateway 1 fills A's order: A sees it live on gateway 2
        final TestClient b = fx.beta(g1);
        b.sell(5, "GOOG", 100, 17000);
        final ExecutionReport fill = a2.awaitReport(1, ExecType.EXEC_FILL);
        assertFalse(fill.getPossDup());

        // A's new orders through gateway 2 work too, and gateway 1 knows the order's state
        a2.buy(2, "GOOG", 5, 16000);
        a2.awaitReport(2, ExecType.EXEC_NEW);
        final ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", g1.grpcPort()).usePlaintext().build();
        try
        {
            final OrderState st = query(ch, "alpha-key").getOrder(GetOrderRequest.newBuilder().setFirmId(100).setClOrdId(1).build());
            assertTrue(st.getFound());
            assertEquals(ExecType.EXEC_FILL, st.getLastReport().getExecType());
        }
        finally
        {
            ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void slowReaderIsServedFromTheEventStreamWithoutLossOrDuplicates() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.client(g, true, 16 * 1024);
        a.request(1);
        a.sendLogon(100, 1001, "alpha-key", false, 1);
        final int n = 1500;
        for (int i = 1; i <= n; i++)
        {
            a.sendRaw(i, TestClient.newOrder(i, i % 2 == 0 ? "AAPL" : "GOOG", Side.BUY, 1, 100));
        }
        Fixture.await(() -> fx.machines[0].countCommands(NewOrderDecoder.TEMPLATE_ID) +
            fx.machines[1].countCommands(NewOrderDecoder.TEMPLATE_ID) == n, "all orders applied");
        Fixture.await(() -> g.metrics().catchupStreamsNow() > 0, "gateway switched the slow reader to catch-up");
        a.request(Integer.MAX_VALUE);
        final long deadline = System.currentTimeMillis() + 10_000;
        while (a.countReports(r -> r.getExecType() == ExecType.EXEC_NEW) < n && System.currentTimeMillis() < deadline)
        {
            a.drain(50);
        }
        a.drain(200);
        assertEquals(n, a.reports.size(), "every report exactly once");
        final long[] lastSeq = new long[2];
        for (final ExecutionReport r : a.reports)
        {
            assertTrue(r.getEventSeq() > lastSeq[r.getShardId()], "per-shard order");
            lastSeq[r.getShardId()] = r.getEventSeq();
            assertFalse(r.getPossDup());
        }
        Fixture.await(() -> g.metrics().catchupStreamsNow() == 0, "back to live delivery");
    }

    @Test
    void heartbeatTimeoutDisconnectsAndCancels() throws Exception
    {
        final GatewayConfig cfg = Fixture.config(1);
        cfg.heartbeatIntervalMs = 100;
        final Gateway g = fx.gateway(cfg);
        final TestClient a = fx.client(g);
        final LogonAck ack = a.logon(100, 1001, "alpha-key", true, 1);
        assertEquals(100, ack.getHeartbeatIntervalMs());
        a.buy(1, "AAPL", 10, 19000);
        a.awaitReport(1, ExecType.EXEC_NEW);
        a.next(TestClient.TIMEOUT_MS, ServerMessage::hasHeartbeat);
        a.awaitClosed(); // silent client: closed after 3 intervals
        assertEquals(Status.Code.UNAVAILABLE, a.closedWith.getCode());
        Fixture.await(() -> fx.machines[0].openOrders(1001) == 0, "cancel on disconnect after heartbeat timeout");
    }

    @Test
    void orderQueryServesOrdersSymbolsAndSessionStatus() throws Exception
    {
        final Gateway g = fx.gateway(4);
        final TestClient a = fx.alpha(g);
        a.buy(1, "GOOG", 10, 17000);
        final ExecutionReport ack = a.awaitReport(1, ExecType.EXEC_NEW);
        final ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", g.grpcPort()).usePlaintext().build();
        try
        {
            final OrderState st = query(ch, "alpha-key").getOrder(GetOrderRequest.newBuilder().setFirmId(100).setClOrdId(1).build());
            assertTrue(st.getFound());
            assertEquals(ack.getOrderId(), st.getLastReport().getOrderId());
            assertFalse(query(ch, "alpha-key").getOrder(GetOrderRequest.newBuilder().setFirmId(100).setClOrdId(2).build()).getFound());

            final StatusRuntimeException denied = assertThrows(StatusRuntimeException.class,
                () -> query(ch, "beta-key").getOrder(GetOrderRequest.newBuilder().setFirmId(100).setClOrdId(1).build()));
            assertEquals(Status.Code.UNAUTHENTICATED, denied.getStatus().getCode());

            assertEquals(3, query(ch, null).listSymbols(ListSymbolsRequest.getDefaultInstance()).getSymbolsCount());

            final SessionStatus ss = query(ch, "alpha-key").getSessionStatus(SessionStatusRequest.newBuilder().setSessionId(1001).build());
            assertTrue(ss.getConnected());
            assertEquals(4, ss.getGatewayId());
            assertEquals(1, ss.getLastClientSeq());
            assertEquals(2, ss.getCursorsCount());
            assertTrue(ss.getCursors(1).getEventSeq() >= ack.getEventSeq());
        }
        finally
        {
            ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void cancelOnlyQueuedBehindItsOrderAndCancelCommandsReachTheShard() throws Exception
    {
        final Gateway g = fx.gateway(1);
        final TestClient a = fx.alpha(g);
        // order and its cancel in one burst: the cancel must not overtake its own order
        a.buy(1, "AAPL", 10, 19000);
        a.cancel(2, 1, "AAPL");
        a.awaitReport(2, ExecType.EXEC_CANCELED);
        assertEquals(1, fx.machines[0].countCommands(CancelOrderDecoder.TEMPLATE_ID));
    }

    private static boolean sessionConnected(final Gateway g, final long sessionId)
    {
        final java.util.concurrent.CompletableFuture<Boolean> f = new java.util.concurrent.CompletableFuture<>();
        g.core().submit(() -> f.complete(g.core().sessionStatus(sessionId, 0).connected()));
        try
        {
            return f.get(2, TimeUnit.SECONDS);
        }
        catch (final Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    private static OrderQueryGrpc.OrderQueryBlockingStub query(final ManagedChannel ch, final String apiKey)
    {
        final Metadata md = new Metadata();
        if (apiKey != null)
        {
            md.put(GrpcFrontEnd.API_KEY_HEADER, apiKey);
        }
        return OrderQueryGrpc.newBlockingStub(ch).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
            .withDeadlineAfter(5, TimeUnit.SECONDS);
    }
}
