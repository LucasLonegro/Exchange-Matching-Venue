package io.lmb.controlplane;

import io.lmb.controlplane.TradingCalendar.Phase;
import io.lmb.controlplane.TradingCalendar.Transition;
import io.lmb.controlplane.db.Store;
import io.lmb.sbe.SymbolState;
import io.lmb.testkit.InMemoryVenue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static io.lmb.controlplane.TestSupport.MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerTest
{
    private static final ZoneId NY = ZoneId.of("America/New_York");

    private final ScriptedShard s0 = new ScriptedShard(0, null);
    private final ScriptedShard s1 = new ScriptedShard(1, null);
    private InMemoryVenue venue;
    private ControlPlane cp;

    private static long at(final int day, final int h, final int m)
    {
        return ZonedDateTime.of(2026, 11, day, h, m, 0, 0, NY).toInstant().toEpochMilli() * MS;
    }

    private void start(final long startNs) throws Exception
    {
        venue = new InMemoryVenue(startNs, s0, s1);
        final ControlPlaneConfig cfg = TestSupport.config(TestSupport.h2Url("sched"), TestSupport.dataDir(), "a");
        cfg.schedulerEnabled = true;
        cfg.enforce = true;
        cfg.schedulerLeadMs = 60_000;
        cfg.schedulerGraceMs = 2_000;
        cfg.allocatorRefreshMs = 3_600_000;
        cp = new ControlPlane(cfg, venue, TestSupport.venueClock(venue));
        cp.start(false);
    }

    @AfterEach
    void stop()
    {
        if (cp != null)
        {
            cp.close();
        }
    }

    private void runTo(final long epochNs, final long stepMs)
    {
        while (venue.nowNs() < epochNs)
        {
            venue.advanceTimeTo(Math.min(epochNs, venue.nowNs() + stepMs * MS));
            cp.step();
        }
    }

    private static List<String> schedule(final ScriptedShard s)
    {
        return s.received().stream().filter(x -> x.startsWith("STATE") || x.startsWith("FIRE") || x.startsWith("ROLL")).toList();
    }

    @Test
    void sendsTheTradingDayAheadOfTimeAsTimerCommands() throws Exception
    {
        start(at(16, 6, 0));
        runTo(at(16, 18, 30), 10_000);
        final List<String> expected = List.of(
            "ROLL 20261116",
            "STATE * AUCTION_CALL eff=" + at(16, 9, 25), "FIRE * AUCTION_CALL @" + at(16, 9, 25),
            "STATE * CONTINUOUS eff=" + at(16, 9, 30), "FIRE * CONTINUOUS @" + at(16, 9, 30),
            "STATE * AUCTION_CALL eff=" + at(16, 15, 55), "FIRE * AUCTION_CALL @" + at(16, 15, 55),
            "STATE * CLOSED eff=" + at(16, 16, 0), "FIRE * CLOSED @" + at(16, 16, 0),
            "ROLL 20261117");
        assertEquals(expected, schedule(s0));
        assertEquals(expected, schedule(s1));
        assertEquals(SymbolState.PRE_OPEN, s0.state(1));
        assertEquals(20261117, s1.tradingDate());
        assertEquals(4L * 2, (long)cp.db().read(c -> io.lmb.controlplane.db.Database.queryLong(c, "SELECT COUNT(*) FROM schedule_sent")));
    }

    @Test
    void lostTimerCommandIsRepairedByEnforcementAfterTheGrace() throws Exception
    {
        start(at(16, 9, 20));
        runTo(at(16, 9, 23), 1_000);
        assertEquals(SymbolState.PRE_OPEN, s0.state(1));
        venue.dropNextOffers(0, 1); // the 09:25 auction call for shard 0 is lost
        runTo(at(16, 9, 25) + 1_000 * MS, 1_000);
        assertEquals(SymbolState.PRE_OPEN, s0.state(1), "no repair inside the grace period");
        assertEquals(SymbolState.AUCTION_CALL, s1.state(5));
        runTo(at(16, 9, 26), 1_000);
        for (final int id : new int[] {1, 2, 3})
        {
            assertEquals(SymbolState.AUCTION_CALL, s0.state(id));
            assertEquals(1, s0.count("STATE " + id + " AUCTION_CALL"), "one immediate repair per symbol");
        }
        runTo(at(16, 9, 31), 1_000);
        assertEquals(SymbolState.CONTINUOUS, s0.state(1), "next timer applies normally");
        assertEquals(0, s0.count("STATE 1 CONTINUOUS"));
    }

    @Test
    void operatorHaltIsExcludedFromScheduledOpenings() throws Exception
    {
        start(at(16, 9, 20));
        runTo(at(16, 9, 21), 1_000);
        cp.db().tx(c -> Store.insertAudit(c, "test", "a", "HALT_SYMBOL", "{\"symbolId\":2}", venue.nowNs() / MS));
        runTo(at(16, 9, 31), 1_000);
        assertEquals(SymbolState.CONTINUOUS, s0.state(1));
        assertEquals(SymbolState.CONTINUOUS, s0.state(3));
        assertEquals(SymbolState.PRE_OPEN, s0.state(2), "halted symbol is not opened");
        assertTrue(s0.count("STATE 1 CONTINUOUS eff=") == 1 && s0.count("STATE * ") == 0, "per-symbol timers on shard 0");
        assertEquals(1, s1.count("STATE * CONTINUOUS eff="), "shard-wide timer on shard 1");
        assertEquals("COMPLETED", cp.db().read(c -> Store.audits(c, Long.MAX_VALUE, 1)).get(0).status());

        cp.db().tx(c -> Store.insertAudit(c, "test", "a", "RESUME_SYMBOL", "{\"symbolId\":2}", venue.nowNs() / MS));
        runTo(at(16, 9, 32), 1_000);
        assertEquals(SymbolState.CONTINUOUS, s0.state(2), "resume follows the calendar");
    }

    @Test
    void calendarHandlesWeekendsHolidaysAndHalfDays()
    {
        final ControlPlaneConfig cfg = new ControlPlaneConfig();
        final TradingCalendar cal = TradingCalendar.from(cfg, List.of(
            new Store.CalendarRow(20261126, false, null, null, null, null, null, null, "Thanksgiving"),
            new Store.CalendarRow(20261127, true, null, null, null, "12:55", "13:00", null, "half day")));
        final List<Transition> friday = cal.day(LocalDate.of(2026, 11, 20));
        assertEquals(6, friday.size());
        assertEquals(20261123, friday.get(5).rollToDate(), "Friday rolls to Monday");
        assertEquals(20261127, cal.day(LocalDate.of(2026, 11, 25)).get(5).rollToDate(), "skips the holiday");
        assertTrue(cal.day(LocalDate.of(2026, 11, 26)).isEmpty());
        final Transition close = cal.day(LocalDate.of(2026, 11, 27)).get(4);
        assertEquals(Phase.CLOSE, close.phase());
        assertEquals(at(27, 13, 0), close.epochNs());
        assertEquals(Phase.DAY_ROLL, cal.lastAtOrBefore(at(22, 12, 0)).phase(), "weekend: last is Friday's roll");
        assertEquals(SymbolState.CONTINUOUS, cal.lastAtOrBefore(at(16, 12, 0)).state());
        cfg.calOpenAuction = "-";
        cfg.calCloseAuction = "-";
        assertEquals(4, TradingCalendar.from(cfg, List.of()).day(LocalDate.of(2026, 11, 16)).size());
    }
}
