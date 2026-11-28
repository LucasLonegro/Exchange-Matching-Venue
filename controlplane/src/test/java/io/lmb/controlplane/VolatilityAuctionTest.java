package io.lmb.controlplane;

import io.lmb.controlplane.db.Store;
import io.lmb.sbe.SymbolState;
import io.lmb.testkit.InMemoryVenue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static io.lmb.controlplane.TestSupport.MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Volatility auctions, venue-owned reference prices and closing prices (matcher contract v2) against the control plane. */
class VolatilityAuctionTest
{
    private static final ZoneId NY = ZoneId.of("America/New_York");
    private static final long VOL_NS = 120_000_000_000L;
    /** Symbol 1 has a dynamic collar and a 2 minute volatility auction; the others use the defaults. */
    private static final List<Store.SymbolRow> SYMBOLS = List.of(
        TestSupport.symbol(1, "AAPL", 0, 19000, 500, 120_000_000_000L),
        TestSupport.symbol(2, "MSFT", 0, 42000),
        TestSupport.symbol(3, "AMZN", 0, 18500, 0, 0),
        TestSupport.symbol(5, "GOOG", 1, 17000));

    private final ScriptedShard s0 = new ScriptedShard(0, null);
    private final ScriptedShard s1 = new ScriptedShard(1, null);
    private InMemoryVenue venue;
    private ControlPlane cp;
    private String url;
    private Path data;

    private static long at(final int h, final int m, final int s)
    {
        return ZonedDateTime.of(2026, 11, 16, h, m, s, 0, NY).toInstant().toEpochMilli() * MS;
    }

    private ControlPlane start(final long startNs, final boolean scheduler) throws Exception
    {
        venue = new InMemoryVenue(startNs, s0, s1);
        url = TestSupport.h2Url("vol");
        data = TestSupport.dataDir();
        TestSupport.seed(url, SYMBOLS, TestSupport.FIRMS);
        cp = open(scheduler);
        return cp;
    }

    private ControlPlane open(final boolean scheduler) throws Exception
    {
        final ControlPlaneConfig cfg = TestSupport.config(url, data, "a");
        cfg.schedulerEnabled = scheduler;
        cfg.enforce = scheduler;
        cfg.schedulerLeadMs = 60_000;
        cfg.schedulerGraceMs = 2_000;
        cfg.allocatorRefreshMs = 3_600_000;
        final ControlPlane c = new ControlPlane(cfg, venue, TestSupport.venueClock(venue));
        c.start(false);
        return c;
    }

    @AfterEach
    void stop()
    {
        if (cp != null)
        {
            cp.close();
        }
    }

    private void runTo(final long epochNs)
    {
        while (venue.nowNs() < epochNs)
        {
            venue.advanceTimeTo(Math.min(epochNs, venue.nowNs() + 1_000 * MS));
            cp.step();
        }
    }

    private void runFor(final long ms)
    {
        runTo(venue.nowNs() + ms * MS);
    }

    private ShardState.Symbol observed(final int symbolId)
    {
        return cp.engine().shard(0).symbols.get(symbolId);
    }

    private long audit(final String action, final String params)
    {
        final long id = cp.db().tx(c -> Store.insertAudit(c, "test", "a", action, params, venue.nowNs() / MS));
        runFor(3_000);
        return id;
    }

    private String lastStateCommand(final int symbolId)
    {
        return s0.received().stream().filter(x -> x.startsWith("STATE " + symbolId + " ")).reduce((a, b) -> b).orElse(null);
    }

    private Store.AuditRow auditRow(final long id)
    {
        return cp.db().read(c -> Store.audit(c, id));
    }

    @Test
    void hopRulesInAVolatilityAuction()
    {
        final SymbolState ac = SymbolState.AUCTION_CALL;
        assertNull(SymbolStates.nextHop(ac, true, SymbolState.CONTINUOUS), "the venue's timer ends it");
        assertEquals(SymbolState.CONTINUOUS, SymbolStates.nextHop(ac, false, SymbolState.CONTINUOUS));
        assertEquals(ac, SymbolStates.nextHop(ac, true, ac), "takeover");
        assertNull(SymbolStates.nextHop(ac, false, ac));
        assertEquals(SymbolState.HALTED, SymbolStates.nextHop(ac, true, SymbolState.HALTED));
        assertEquals(SymbolState.CLOSED, SymbolStates.nextHop(ac, true, SymbolState.CLOSED));
        assertEquals(SymbolState.CLOSED, SymbolStates.nextHop(ac, true, SymbolState.PRE_OPEN));
        assertNull(SymbolStates.desiredHop(ac, true, SymbolState.CONTINUOUS, null));
        assertEquals(ac, SymbolStates.desiredHop(ac, true, ac, null), "closing auction takes it over");
        assertEquals(SymbolState.CLOSED, SymbolStates.desiredHop(ac, true, SymbolState.CLOSED, null));
        assertEquals(SymbolState.HALTED, SymbolStates.desiredHop(ac, true, SymbolState.CONTINUOUS, "HALTED"));
        assertEquals(ac, SymbolStates.desiredHop(ac, true, SymbolState.CONTINUOUS, "AUCTION_CALL"));

        final ShardState.Symbol s = new ShardState.Symbol();
        s.state = ac;
        s.volatilityAuction = true;
        assertFalse(Engine.stateReached(s, ac), "a volatility auction does not satisfy an AUCTION_CALL command");
        s.volatilityAuction = false;
        assertTrue(Engine.stateReached(s, ac), "satisfied once the takeover arrived");
    }

    @Test
    void calendarDoesNotEndAVolatilityAuctionAndTheVenueReferenceDoesNotCauseRedefinitions() throws Exception
    {
        start(at(9, 29, 0), true);
        runTo(at(9, 31, 0));
        assertEquals(SymbolState.CONTINUOUS, s0.state(1));
        final long before = s0.count("STATE 1 "); // the opening auction repair (the control plane started after 09:25)
        s0.interrupt(1, venue.nowNs(), 19_100);
        runFor(VOL_NS / 1_000_000 - 10_000);
        assertTrue(s0.inVolatilityAuction(1), "not ended by enforcement");
        assertTrue(observed(1).volatilityAuction);
        assertTrue(observed(1).volatilityAuctionSinceNs > 0);
        assertEquals(before, s0.count("STATE 1 "), "no command for the symbol during its volatility auction");
        runFor(15_000);
        assertEquals(1, s0.count("VOLEND 1"));
        assertEquals(SymbolState.CONTINUOUS, observed(1).state);
        assertFalse(observed(1).volatilityAuction);
        assertEquals(19_100, observed(1).referencePrice);
        assertTrue(observed(1).venueReference);
        assertEquals(19_100 - 1_910, observed(1).bandLow);
        runFor(5_000); // several reconcile passes
        assertEquals(1, s0.count("DEF 1"), "the venue's reference is not fought with re-definitions");
        assertEquals(before, s0.count("STATE 1 "));
    }

    @Test
    void scheduledClosingAuctionTakesOverAVolatilityAuction() throws Exception
    {
        start(at(15, 50, 0), true);
        runTo(at(15, 54, 30));
        assertEquals(SymbolState.CONTINUOUS, s0.state(1));
        s0.interrupt(1, venue.nowNs(), Long.MIN_VALUE);
        runTo(at(15, 55, 1));
        assertEquals(SymbolState.AUCTION_CALL, s0.state(1));
        assertFalse(s0.inVolatilityAuction(1), "the 15:55 timer took it over");
        assertFalse(observed(1).volatilityAuction);
        runTo(at(16, 0, 5));
        assertEquals(0, s0.count("VOLEND 1"), "the volatility timer was cancelled");
        assertEquals(SymbolState.CLOSED, s0.state(1));
        assertEquals("NONE", observed(1).closeSource);
        assertTrue(cp.db().read(c -> Store.commands(c, "FAILED", null, 100)).isEmpty());
        assertTrue(cp.db().read(c -> Store.commands(c, "EXPIRED", null, 100)).isEmpty());
    }

    @Test
    void lostClosingAuctionTimerIsRepairedWithATakeover() throws Exception
    {
        start(at(15, 50, 0), true);
        runTo(at(15, 53, 50));
        venue.dropNextOffers(0, 1); // the 15:55 auction call for shard 0 (sent at 15:54) is lost
        runTo(at(15, 54, 10));
        s0.interrupt(1, venue.nowNs(), Long.MIN_VALUE);
        runTo(at(15, 55, 1));
        assertTrue(s0.inVolatilityAuction(1), "no repair inside the grace period");
        runTo(at(15, 55, 10));
        assertEquals(1, s0.count("STATE 1 AUCTION_CALL"), "one takeover");
        assertFalse(s0.inVolatilityAuction(1));
        assertEquals(SymbolState.AUCTION_CALL, s0.state(2));
        runTo(at(15, 57, 0));
        assertEquals(1, s0.count("STATE 1 AUCTION_CALL"), "the takeover is seen as done: not re-sent");
        assertEquals(0, s0.count("VOLEND 1"));
        assertEquals(SymbolState.AUCTION_CALL, s0.state(1));
        assertTrue(cp.db().read(c -> Store.commands(c, "FAILED", null, 100)).isEmpty());
    }

    @Test
    void adminCommandsRespectAVolatilityAuction() throws Exception
    {
        start(at(9, 29, 0), true);
        runTo(at(9, 31, 0));
        assertEquals("COMPLETED", auditRow(audit("HALT_SYMBOL", "{\"symbolId\":2}")).status());
        assertEquals(SymbolState.HALTED, s0.state(2));
        final long before = s0.count("STATE 1 ");
        s0.interrupt(1, venue.nowNs(), Long.MIN_VALUE);
        runFor(1_000);

        assertEquals("REJECTED", auditRow(audit("AUCTION_STOP", "{\"symbolId\":1}")).status());
        assertEquals("REJECTED", auditRow(audit("RESUME_SYMBOL", "{\"symbolId\":1}")).status());
        assertEquals(before, s0.count("STATE 1 "));

        final Store.AuditRow resume = auditRow(audit("RESUME_MARKET", "{\"shardId\":0}"));
        assertEquals("COMPLETED", resume.status());
        assertEquals(SymbolState.CONTINUOUS, s0.state(2), "the halted symbol resumes");
        assertFalse(s0.received().contains("STATE * CONTINUOUS"), "no shard-wide CONTINUOUS while a volatility auction runs");
        assertTrue(s0.inVolatilityAuction(1));

        assertEquals("COMPLETED", auditRow(audit("AUCTION_START", "{\"symbolId\":1}")).status());
        assertEquals(before + 1, s0.count("STATE 1 "));
        assertEquals("STATE 1 AUCTION_CALL", lastStateCommand(1), "the takeover");
        assertFalse(s0.inVolatilityAuction(1), "operator auction took it over");
        assertEquals("COMPLETED", auditRow(audit("AUCTION_STOP", "{\"symbolId\":1}")).status());
        assertEquals(SymbolState.CONTINUOUS, s0.state(1));

        s0.interrupt(1, venue.nowNs(), Long.MIN_VALUE);
        runFor(1_000);
        assertEquals("COMPLETED", auditRow(audit("AUCTION_STOP", "{\"symbolId\":1,\"force\":true}")).status());
        assertEquals(SymbolState.CONTINUOUS, s0.state(1), "force ends it now");
        assertFalse(s0.inVolatilityAuction(1));

        s0.interrupt(1, venue.nowNs(), Long.MIN_VALUE);
        runFor(1_000);
        assertEquals("COMPLETED", auditRow(audit("HALT_SYMBOL", "{\"symbolId\":1}")).status());
        assertEquals(SymbolState.HALTED, s0.state(1), "halt during a volatility auction");
        runFor(VOL_NS / 1_000_000 + 10_000);
        assertEquals(SymbolState.HALTED, s0.state(1), "stays halted: no timer, no calendar resume");
        assertEquals(0, s0.count("VOLEND 1"), "each auction was ended by a command (takeover, force, halt), never by its timer");
    }

    @Test
    void dynamicCollarRoundTripsAndClosingPriceSurvivesARestart() throws Exception
    {
        start(TestSupport.NOON, false);
        runFor(3_000);
        final Store.SymbolRow row = cp.engine().ref().symbolById().get(1);
        assertEquals(500, row.dynamicBandBps());
        assertEquals(VOL_NS, row.volatilityAuctionNs());
        assertEquals(0, cp.engine().ref().symbolById().get(2).dynamicBandBps());
        assertEquals(300_000_000_000L, cp.engine().ref().symbolById().get(2).volatilityAuctionNs(), "CSV default");
        assertEquals(500, s0.symbol(1).dyn, "SymbolDefinition carries the collar");
        assertEquals(VOL_NS, s0.symbol(1).vol);
        assertEquals(0, s0.symbol(3).vol);
        assertEquals(500, observed(1).dynamicBandBps);
        assertEquals(VOL_NS, observed(1).volatilityAuctionNs);

        s0.reference(1, venue.nowNs(), 19_500);
        s0.trade(1, 19_321);
        Reconciler.state(cp.engine(), 0, 1, SymbolState.CLOSED, 0, "TEST", null);
        runFor(3_000);
        assertEquals(SymbolState.CLOSED, s0.state(1));
        assertEquals(19_321, observed(1).closePrice);
        assertEquals("LAST_TRADE", observed(1).closeSource);
        assertEquals(19_500, observed(1).referencePrice);
        cp.close();

        cp = open(false);
        runFor(3_000);
        final ShardState.Symbol again = observed(1);
        assertEquals(19_321, again.closePrice);
        assertEquals(Long.MIN_VALUE, again.previousClose);
        assertEquals("LAST_TRADE", again.closeSource);
        assertEquals(19_500, again.referencePrice);
        assertTrue(again.venueReference);
        assertEquals(500, again.dynamicBandBps);
        final Store.ObservedSymbolRow persisted = cp.db().read(Store::observedSymbols).stream()
            .filter(r -> r.symbolId() == 1).findFirst().orElseThrow();
        assertEquals(19_321, persisted.closePrice());
        assertEquals(VOL_NS, persisted.volatilityAuctionNs());
        assertEquals(1, s0.count("DEF 1"), "no re-definition after a restart either");
    }
}
