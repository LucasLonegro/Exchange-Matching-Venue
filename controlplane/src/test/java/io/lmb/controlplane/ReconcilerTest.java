package io.lmb.controlplane;

import io.lmb.controlplane.db.Database;
import io.lmb.controlplane.db.Store;
import io.lmb.sbe.SymbolState;
import io.lmb.testkit.InMemoryVenue;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static io.lmb.controlplane.TestSupport.NOON;
import static io.lmb.controlplane.TestSupport.run;
import static io.lmb.controlplane.TestSupport.runUntil;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReconcilerTest
{
    private static final int[] SHARD0 = {1, 2, 3};
    private static final int[] SHARD1 = {5, 6, 7};

    private final ScriptedShard s0 = new ScriptedShard(0, null);
    private final ScriptedShard s1 = new ScriptedShard(1, null);
    private final InMemoryVenue venue = new InMemoryVenue(NOON, s0, s1);

    private ControlPlane start(final ControlPlaneConfig cfg) throws Exception
    {
        final ControlPlane cp = new ControlPlane(cfg, venue, TestSupport.venueClock(venue));
        cp.start(false);
        return cp;
    }

    private static boolean allDefined(final ScriptedShard shard, final int... ids)
    {
        for (final int id : ids)
        {
            if (shard.state(id) == null)
            {
                return false;
            }
        }
        return true;
    }

    private static boolean limitsApplied(final ScriptedShard shard)
    {
        final ScriptedShard.Limits a = shard.limits(100);
        final ScriptedShard.Limits b = shard.limits(200);
        return a != null && b != null && a.budget > 0 && b.budget > 0 && a.enabled && b.enabled &&
            a.maxPos == 5_000_000 && b.maxPos == 2_000_000;
    }

    @Test
    void bootstrapsDefinesSymbolsAppliesLimitsAndRestartsIdempotently() throws Exception
    {
        final String url = TestSupport.h2Url("reconcile");
        final Path data = TestSupport.dataDir();
        final ControlPlane cp = start(TestSupport.config(url, data, "a"));
        assertTrue(runUntil(venue, 10, 3_000, () -> allDefined(s0, SHARD0) && allDefined(s1, SHARD1) &&
            limitsApplied(s0) && limitsApplied(s1), cp));
        assertEquals(3, s0.symbolCount());
        assertEquals(SymbolState.PRE_OPEN, s0.state(1));
        assertEquals(1, s0.count("DEF 1"), "each symbol defined once");
        // allocator: per-firm budgets split the firm limit, never more
        assertTrue(s0.limits(100).budget + s1.limits(100).budget <= 100_000_000L);
        assertTrue(s0.limits(200).budget + s1.limits(200).budget <= 60_000_000L);
        run(venue, 10, 300, cp);
        final List<Store.SymbolRow> symbols = cp.db().read(Store::symbols);
        assertEquals(6, symbols.size());
        final long definitions = s0.count("DEF") + s1.count("DEF");
        cp.close();

        // a restart resumes from the database and the observed state: nothing is defined twice
        final ControlPlane again = start(TestSupport.config(url, data, "a"));
        run(venue, 10, 2_000, again);
        assertEquals(6, again.db().read(Store::symbols).size());
        assertEquals(definitions, s0.count("DEF") + s1.count("DEF"), "restart re-sends no definitions");
        assertNotNull(again.db().read(c -> Store.cursor(c, 0)));
        assertTrue(again.db().read(c -> Store.cursor(c, 0)).eventSeq() > 0);
        again.close();
    }

    @Test
    void lostCommandsAreRetriedUntilTheirEffectIsObserved() throws Exception
    {
        venue.dropNextOffers(0, 5);
        venue.dropNextOffers(1, 2);
        final ControlPlane cp = start(TestSupport.config(TestSupport.h2Url("lost"), TestSupport.dataDir(), "a"));
        assertTrue(runUntil(venue, 10, 5_000, () -> allDefined(s0, SHARD0) && allDefined(s1, SHARD1) &&
            limitsApplied(s0) && limitsApplied(s1), cp));
        run(venue, 10, 200, cp);
        final long retried = cp.db().read(c -> Database.queryLong(c, "SELECT COUNT(*) FROM command_log WHERE attempts > 1"));
        assertTrue(retried >= 1, "dropped commands were resent");
        assertEquals(0L, (long)cp.db().read(c -> Database.queryLong(c,
            "SELECT COUNT(*) FROM command_log WHERE status IN ('FAILED', 'REJECTED')")));
        cp.close();
    }

    @Test
    void unavailableShardIsReconciledWhenItReconnects() throws Exception
    {
        venue.setAvailable(1, false);
        final ControlPlane cp = start(TestSupport.config(TestSupport.h2Url("unavail"), TestSupport.dataDir(), "a"));
        assertTrue(runUntil(venue, 10, 3_000, () -> allDefined(s0, SHARD0) && limitsApplied(s0), cp));
        run(venue, 10, 1_000, cp);
        assertEquals(0, s1.symbolCount());
        final Store.CursorRow cursor = cp.db().read(c -> Store.cursor(c, 1));
        assertFalse(cursor.connected());

        venue.setAvailable(1, true);
        assertTrue(runUntil(venue, 10, 3_000, () -> allDefined(s1, SHARD1) && limitsApplied(s1), cp));
        assertEquals(1, s1.count("DEF 5"), "no duplicate definitions after reconnect");
        assertTrue(s0.limits(100).budget + s1.limits(100).budget <= 100_000_000L);
        cp.close();
    }

    @Test
    void autoOpenTakesEverySymbolToContinuous() throws Exception
    {
        final ControlPlaneConfig cfg = TestSupport.config(TestSupport.h2Url("autoopen"), TestSupport.dataDir(), "a");
        cfg.autoOpen = true;
        final ControlPlane cp = start(cfg);
        assertTrue(runUntil(venue, 10, 3_000, () ->
        {
            for (final int id : SHARD0)
            {
                if (s0.state(id) != SymbolState.CONTINUOUS)
                {
                    return false;
                }
            }
            for (final int id : SHARD1)
            {
                if (s1.state(id) != SymbolState.CONTINUOUS)
                {
                    return false;
                }
            }
            return true;
        }, cp));
        run(venue, 10, 1_000, cp);
        assertEquals(1, s0.count("STATE 1 CONTINUOUS"), "one transition per symbol, no flapping");
        cp.close();
    }

    @Test
    void transitionTableAndPathFinding()
    {
        assertTrue(SymbolStates.allowed(SymbolState.PRE_OPEN, SymbolState.CONTINUOUS));
        assertFalse(SymbolStates.allowed(SymbolState.PRE_OPEN, SymbolState.HALTED));
        assertFalse(SymbolStates.allowed(SymbolState.CONTINUOUS, SymbolState.CONTINUOUS));
        assertEquals(SymbolState.PRE_OPEN, SymbolStates.nextHop(SymbolState.CLOSED, SymbolState.CONTINUOUS));
        assertEquals(SymbolState.CLOSED, SymbolStates.nextHop(SymbolState.HALTED, SymbolState.PRE_OPEN));
        assertEquals(SymbolState.HALTED, SymbolStates.desiredHop(SymbolState.CONTINUOUS, SymbolState.CONTINUOUS, "HALTED"));
        assertEquals(null, SymbolStates.desiredHop(SymbolState.PRE_OPEN, SymbolState.CONTINUOUS, "HALTED"));
        assertEquals(SymbolState.CLOSED, SymbolStates.desiredHop(SymbolState.HALTED, SymbolState.CLOSED, "HALTED"));
        assertEquals(SymbolState.AUCTION_CALL, SymbolStates.desiredHop(SymbolState.CONTINUOUS, SymbolState.CONTINUOUS, "AUCTION_CALL"));
    }
}
