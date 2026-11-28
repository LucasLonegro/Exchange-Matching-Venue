package io.lmb.gateway;

import io.grpc.Status;
import io.lmb.common.access.AccessDirectory;
import io.lmb.common.access.AccessDirectorySource;
import io.lmb.common.access.Scope;
import io.lmb.gateway.core.Reject;
import io.lmb.proto.orderentry.ExecType;
import io.lmb.proto.orderentry.SessionReject;
import io.lmb.sbe.MassCancelDecoder;
import io.lmb.sbe.MassCancelScope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Grants from the access directory: scopes, tiers, revocation; and the cancel-on-disconnect window. */
class GatewayAccessTest
{
    private static final String TRADER = "lmb_000000000001_trader";
    private static final String OTHER = "lmb_000000000002_other";
    private static final String CANCEL_ONLY = "lmb_000000000003_cancel";
    private static final String QUERY_ONLY = "lmb_000000000004_query";

    private final Fixture fx = new Fixture();

    @AfterEach
    void tearDown()
    {
        fx.close();
    }

    private static AccessDirectory.KeyEntry key(final String keyId, final String apiKey, final long sessionId, final Set<Scope> scopes)
    {
        return new AccessDirectory.KeyEntry(keyId, AccessDirectory.sha256Hex(apiKey), 100, sessionId, scopes, 0);
    }

    /** Firm 100 with sessions 1001 (tier {@code tier1001}) and 1002; {@code drop} keys left out. */
    private static AccessDirectory directory(final long version, final String tier1001, final String... drop)
    {
        final List<AccessDirectory.KeyEntry> keys = new ArrayList<>(List.of(
            key("k1", TRADER, 1001, EnumSet.of(Scope.TRADE, Scope.QUERY)),
            key("k2", OTHER, 1002, EnumSet.of(Scope.TRADE)),
            key("k3", CANCEL_ONLY, 0, EnumSet.of(Scope.CANCEL)),
            key("k4", QUERY_ONLY, 0, EnumSet.of(Scope.QUERY))));
        keys.removeIf(k -> List.of(drop).contains(k.keyId()));
        return new AccessDirectory(version, System.currentTimeMillis(),
            List.of(new AccessDirectory.Tier("standard", 100_000, 10_000, 100_000, 10_000),
                new AccessDirectory.Tier("tiny", 0.5, 2, 0.5, 4)),
            List.of(new AccessDirectory.FirmEntry(100, "Alpha", "standard", true)),
            List.of(new AccessDirectory.SessionEntry(1001, 100, "alice", tier1001, true),
                new AccessDirectory.SessionEntry(1002, 100, "bob", null, true)),
            keys);
    }

    @Test
    void scopesAndRevocation() throws Exception
    {
        final GatewayConfig cfg = Fixture.config(1);
        cfg.accessRevalidateMs = 50;
        final AccessDirectorySource access = AccessDirectorySource.fixed(directory(1, null));
        final Gateway g = fx.gateway(cfg, access);

        final TestClient trader = fx.client(g);
        trader.logon(100, 1001, TRADER);
        trader.buy(1, "AAPL", 10, 19000);
        trader.awaitReport(1, ExecType.EXEC_NEW);

        // a member's key is bound to their session
        final TestClient stolen = fx.client(g);
        stolen.sendLogon(100, 1002, TRADER, false, 1);
        stolen.awaitReject(SessionReject.Reason.AUTH_FAILED);
        stolen.awaitClosed();

        // a key without TRADE or CANCEL cannot enter orders at all
        final TestClient query = fx.client(g);
        query.sendLogon(100, 1002, QUERY_ONLY, false, 1);
        query.awaitReject(SessionReject.Reason.NOT_PERMITTED);
        query.awaitClosed();
        assertEquals(Status.Code.PERMISSION_DENIED, query.closedWith.getCode());

        // a cancel-only key: cancels yes, new orders no
        final TestClient panic = fx.client(g);
        panic.logon(100, 1002, CANCEL_ONLY);
        panic.buy(2, "AAPL", 10, 19000);
        assertEquals(2, panic.awaitReject(SessionReject.Reason.NOT_PERMITTED).getRefClOrdId());
        panic.massCancel(3, "");
        panic.awaitMassAck(0);
        assertEquals(2, g.metrics().rejects(Reject.NOT_PERMITTED), "the query-only logon and the order");

        // revoking the trader's key ends that connection, and only that one
        access.set(directory(2, null, "k1"));
        trader.awaitReject(SessionReject.Reason.AUTH_FAILED);
        trader.awaitClosed();
        assertEquals(1, g.metrics().accessRevocations());
        panic.massCancel(4, "");
        panic.awaitMassAck(0);
        final TestClient again = fx.client(g);
        again.sendLogon(100, 1001, TRADER, false, 1);
        again.awaitReject(SessionReject.Reason.AUTH_FAILED);
    }

    @Test
    void tierSetsTheSessionsOrderAndMessageRates() throws Exception
    {
        final Gateway g = fx.gateway(Fixture.config(1), AccessDirectorySource.fixed(directory(1, "tiny")));
        final TestClient a = fx.client(g);
        a.logon(100, 1001, TRADER);
        final TestClient b = fx.client(g);
        b.logon(100, 1002, OTHER); // the firm's tier: unaffected by its teammate's

        // tiny: 2 orders, 4 messages of burst
        a.buy(1, "AAPL", 10, 19000);
        a.buy(2, "AAPL", 10, 19000);
        a.buy(3, "AAPL", 10, 19000); // order bucket empty
        a.cancel(4, 1, "AAPL");      // the 4th message
        a.cancel(5, 2, "AAPL");      // message bucket empty
        a.awaitReport(4, ExecType.EXEC_CANCELED);
        a.drain(200);
        assertEquals(2, a.rejects.size(), a.rejects.toString());
        assertEquals(3, a.rejects.get(0).getRefClOrdId());
        assertEquals(SessionReject.Reason.THROTTLED, a.rejects.get(0).getReason());
        assertTrue(a.rejects.get(0).getText().contains("order rate"), a.rejects.get(0).getText());
        assertEquals(5, a.rejects.get(1).getRefClOrdId());
        assertTrue(a.rejects.get(1).getText().contains("message rate"), a.rejects.get(1).getText());

        for (int i = 1; i <= 20; i++)
        {
            b.buy(i, "MSFT", 1, 42000);
        }
        b.awaitReport(20, ExecType.EXEC_NEW);
        assertTrue(b.rejects.isEmpty());
    }

    @Test
    void cancelOnDisconnectWindowIsCappedAndAReconnectKeepsTheOrders() throws Exception
    {
        final GatewayConfig cfg = Fixture.config(1);
        cfg.codMaxWindowMs = 800;
        final Gateway g = fx.gateway(cfg);
        final TestClient a = fx.client(g);
        a.codWindowMs = 600_000; // capped to 800 ms
        a.logon(100, 1001, "alpha-key", true, 1);
        a.buy(1, "AAPL", 10, 19000);
        a.awaitReport(1, ExecType.EXEC_NEW);

        // lost, then back within the window: nothing is canceled
        a.cancelStream();
        Fixture.await(() -> g.metrics().cancelOnDisconnects() == 0 && reconnect(g, 2), "logon again");
        Thread.sleep(1_200);
        assertEquals(1, fx.machines[0].openOrders(1001), "a reconnect within the window keeps the orders");
        assertEquals(0, g.metrics().cancelOnDisconnects());

        // lost for good: canceled once the window is over, on every shard, only this gateway's orders
        fx.clients.get(fx.clients.size() - 1).cancelStream();
        final long lost = System.currentTimeMillis();
        Thread.sleep(300);
        assertEquals(1, fx.machines[0].openOrders(1001), "still inside the window");
        Fixture.await(() -> fx.machines[0].openOrders(1001) == 0, "canceled after the window");
        final long took = System.currentTimeMillis() - lost;
        assertTrue(took >= 700 && took < 4_000, "canceled after " + took + " ms");
        for (final ScriptedShard m : fx.machines)
        {
            final ScriptedShard.Command c = m.commands().stream()
                .filter(x -> x.templateId() == MassCancelDecoder.TEMPLATE_ID).findFirst().orElseThrow();
            assertEquals(MassCancelScope.SESSION_GATEWAY, c.scope());
        }
    }

    /** Logs session 1001 on again (with cancel-on-disconnect); false while the gateway still has it connected. */
    private boolean reconnect(final Gateway g, final long nextSeq)
    {
        final TestClient c = fx.client(g);
        c.codWindowMs = 600_000;
        c.sendLogon(100, 1001, "alpha-key", true, nextSeq);
        final var m = c.next(TestClient.TIMEOUT_MS, x -> x.hasLogonAck() || x.hasReject());
        if (m.hasLogonAck())
        {
            return true;
        }
        c.close();
        fx.clients.remove(c);
        return false;
    }
}
