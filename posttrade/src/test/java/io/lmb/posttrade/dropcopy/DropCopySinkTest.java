package io.lmb.posttrade.dropcopy;

import io.lmb.common.Events;
import io.lmb.common.access.AccessDirectory;
import io.lmb.common.access.AccessDirectorySource;
import io.lmb.common.access.Scope;
import io.lmb.testkit.TestAccess;
import io.lmb.posttrade.PostTradeMetrics;
import io.lmb.api.SinkRunner;
import io.lmb.posttrade.TestVenue;
import io.lmb.sbe.ExecType;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.Side;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DropCopySinkTest
{
    private static final AtomicInteger PORTS = new AtomicInteger(37000 + (int)(ProcessHandle.current().pid() % 16) * 3); // 37000-37049
    private static final EnumSet<ExecType> DEFAULT_TYPES =
        EnumSet.of(ExecType.PARTIAL_FILL, ExecType.FILL, ExecType.CANCELED, ExecType.EXPIRED);
    private static final int ALL = DropCopyEngine.ALL_FIRMS;

    private final TestVenue venue = new TestVenue(2).standardSetup();
    private final PostTradeMetrics metrics = new PostTradeMetrics();
    private final Path dataDir = Path.of("target", "test-data", "dropcopy-" + System.nanoTime()).toAbsolutePath();
    private final int port = PORTS.getAndIncrement();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private DropCopyEngine engine;
    private SinkRunner runner;

    @AfterEach
    void tearDown() throws Exception
    {
        if (runner != null)
        {
            runner.close();
        }
        for (int i = closeables.size() - 1; i >= 0; i--)
        {
            closeables.get(i).close();
        }
    }

    private void startEngine()
    {
        final AccessDirectory firms = TestAccess.directory(
            TestAccess.firm(100, "alpha", "k100", 1000), TestAccess.firm(200, "beta", "k200", 2000));
        engine = new DropCopyEngine(dataDir, "127.0.0.1", port, "LMB", "admin", 256 * 1024, true,
            AccessDirectorySource.fixed(firms), metrics).start();
        closeables.add(engine);
    }

    @Test
    void firmLogonsNeedAKeyWithTheDropCopyScope()
    {
        final AccessDirectory d = new AccessDirectory(1, System.currentTimeMillis(), List.of(),
            List.of(new AccessDirectory.FirmEntry(100, "alpha", "standard", true), new AccessDirectory.FirmEntry(200, "beta", "standard", false)),
            List.of(),
            List.of(new AccessDirectory.KeyEntry("t", AccessDirectory.sha256Hex("trade-key"), 100, 1000, EnumSet.of(Scope.TRADE, Scope.QUERY), 0),
                new AccessDirectory.KeyEntry("d", AccessDirectory.sha256Hex("dc-key"), 100, 0, EnumSet.of(Scope.DROP_COPY), 0),
                new AccessDirectory.KeyEntry("x", AccessDirectory.sha256Hex("disabled-firm-key"), 200, 0, EnumSet.of(Scope.DROP_COPY), 0)));
        engine = new DropCopyEngine(dataDir, "127.0.0.1", port, "LMB", "admin", 256 * 1024, true, AccessDirectorySource.fixed(d), metrics)
            .start();
        closeables.add(engine);
        startRunner();
        final FixTestClient c = client("scope");
        assertNull(c.logon(port, "T100", "100", "trade-key"), "an order entry key is not a drop copy key");
        assertNull(c.logon(port, "X200", "200", "disabled-firm-key"), "disabled firm");
        assertNotNull(c.logon(port, "D100", "100", "dc-key"));
        assertEquals(2, metrics.dropCopyLogonsRejected.get());
    }

    private SinkRunner startRunner()
    {
        final DropCopySink sink = new DropCopySink(engine, venue, id -> "SYM" + (id - 1),
            "PARTIAL_FILL,FILL,CANCELED,EXPIRED", 50, metrics);
        runner = new SinkRunner(sink, venue, 64, 50).start();
        TestVenue.await(runner::isUp, 40_000, "drop copy sink to open");
        return runner;
    }

    private FixTestClient client(final String name)
    {
        final FixTestClient c = new FixTestClient(name);
        closeables.add(c);
        return c;
    }

    /** "shard:eventSeq" of every report a session of the firm (or ALL) must receive, from the venue's events. */
    private Set<String> expected(final int firmId)
    {
        final Set<String> out = new HashSet<>();
        final MessageHeaderDecoder h = new MessageHeaderDecoder();
        final ExecutionReportDecoder d = new ExecutionReportDecoder();
        for (int s = 0; s < venue.shardCount(); s++)
        {
            for (long seq = 1; seq <= venue.venue.eventCount(s); seq++)
            {
                final UnsafeBuffer b = new UnsafeBuffer(venue.venue.event(s, seq));
                h.wrap(b, 0);
                if (h.templateId() == ExecutionReportDecoder.TEMPLATE_ID)
                {
                    d.wrapAndApplyHeader(b, 0, h);
                    if (DEFAULT_TYPES.contains(d.execType()) && (firmId == ALL || d.firmId() == firmId))
                    {
                        assertEquals(seq, Events.eventSeq(b, 0));
                        out.add(s + ":" + seq);
                    }
                }
            }
        }
        return out;
    }

    private static List<String> keys(final List<Map<Integer, String>> reports)
    {
        final List<String> out = new ArrayList<>();
        for (final Map<Integer, String> m : reports)
        {
            out.add(m.get(20001) + ":" + m.get(20002));
        }
        return out;
    }

    /** Waits until the session has exactly the expected reports, then checks nothing else arrives. */
    private void assertReports(final FixTestClient c, final String compId, final Set<String> expected)
    {
        c.pollUntil(() -> c.reports(compId).size() >= expected.size(), 10_000,
            compId + " to receive " + expected.size() + " reports (has " + keys(c.reports(compId)) + ")");
        c.pollFor(200);
        final List<String> got = keys(c.reports(compId));
        assertEquals(expected, new HashSet<>(got), compId);
        assertEquals(got.size(), new HashSet<>(got).size(), compId + " received a report twice: " + got);
    }

    private void trade()
    {
        venue.cross(0, 10_000, 5);
        venue.cross(1, 10_100, 7);
        final long resting = venue.order(0, 100, Side.BUY, 9_000, 3);
        venue.cancel(0, 100, resting);
        final long resting2 = venue.order(1, 200, Side.SELL, 11_000, 4);
        venue.cancel(1, 200, resting2);
    }

    @Test
    void firmSessionsGetOnlyTheirOwnReportsAndAllGetsEveryFirms()
    {
        trade(); // history: delivered by catch-up
        startEngine();
        startRunner();
        final FixTestClient c = client("filter");
        assertNotNull(c.logon(port, "C100", "100", "k100"));
        assertNotNull(c.logon(port, "C200", "200", "k200"));
        assertNotNull(c.logon(port, "CALL", "ALL", "admin"));
        assertNull(c.logon(port, "BAD", "100", "k200"), "wrong key refused");
        assertNull(c.logon(port, "BAD2", "ALL", "k100"), "ALL needs the admin key");
        assertTrue(metrics.dropCopyLogonsRejected.get() >= 2);

        final Set<String> e100 = expected(100);
        final Set<String> e200 = expected(200);
        assertEquals(3, e100.size()); // a fill on each shard and one cancel
        assertReports(c, "C100", e100);
        assertReports(c, "C200", e200);
        assertReports(c, "CALL", expected(ALL));

        trade(); // live
        assertReports(c, "C100", expected(100));
        assertReports(c, "C200", expected(200));
        assertReports(c, "CALL", expected(ALL));
        assertTrue(expected(100).stream().noneMatch(expected(200)::contains), "firms' reports are disjoint");

        final Map<Integer, String> fill = c.reports("C100").stream().filter(m -> "F".equals(m.get(150))).findFirst()
            .orElseThrow();
        assertEquals("100", fill.get(20003), "LmbFirmID");
        assertEquals("1000", fill.get(20004), "LmbSessionID (firm 100 orders use session 1000)");
        assertTrue(fill.get(55).startsWith("SYM"));
        assertEquals("1", fill.get(1));
        assertNotNull(fill.get(31), "LastPx");
        assertNotNull(fill.get(880), "TrdMatchID");
        assertTrue(c.reports("C100").stream().anyMatch(m -> "4".equals(m.get(150))), "cancel delivered");
        assertTrue(c.reports("CALL").stream().noneMatch(m -> "Y".equals(m.get(97))), "no PossResend on a first delivery");
    }

    @Test
    void cleanRestartResumesAfterTheLastDeliveredReport()
    {
        trade();
        startEngine();
        startRunner();
        final FixTestClient c = client("restart");
        assertNotNull(c.logon(port, "C100", "100", "k100"));
        final Set<String> before = expected(100);
        assertReports(c, "C100", before);

        runner.close(); // clean stop: sessions logged out, final checkpoint
        runner = null;
        trade(); // happens while the drop copy is down
        startRunner();
        assertNotNull(c.logon(port, "C100", "100", "k100"));
        final Set<String> all = expected(100);
        assertReports(c, "C100", all); // every report exactly once across both FIX connections
        assertTrue(all.size() > before.size());
        assertTrue(c.reports("C100").stream().noneMatch(m -> "Y".equals(m.get(97))), "clean restart: no PossResend");

        trade(); // live after the restart
        assertReports(c, "C100", expected(100));
    }

    @Test
    void selfTradeCancelNamesItsReasonInText()
    {
        final long resting = venue.order(0, 100, Side.SELL, 10_000, 5);
        final long taker = venue.order(0, 100, Side.BUY, 10_000, 5); // crosses its own firm's resting sell
        startEngine();
        startRunner();
        final FixTestClient c = client("stp");
        assertNotNull(c.logon(port, "C100", "100", "k100"));
        final Set<String> e100 = expected(100);
        assertEquals(1, e100.size(), "only the taker's cancel; no fill");
        assertReports(c, "C100", e100);

        final Map<Integer, String> cancel = c.reports("C100").get(0);
        assertEquals("4", cancel.get(150), "ExecType CANCELED");
        assertEquals(Long.toString(taker), cancel.get(11), "the incoming order is the one canceled");
        assertEquals("SELF_TRADE", cancel.get(58), "Text names the cancel reason");
        assertEquals("0", cancel.get(151), "LeavesQty");
        assertNull(cancel.get(378), "no ExecRestatementReason");
        assertTrue(resting != taker);
    }

    @Test
    void resumeFromAndExecTypesParse()
    {
        final DropCopySink.Client probe = new DropCopySink.Client("X", 2);
        DropCopySink.applyResumeFrom(probe, "0:5; 1:7,bad,9:1,1:x");
        assertEquals(5, probe.cursor[0]);
        assertEquals(7, probe.cursor[1]);
        assertFalse(DropCopySink.parseExecTypes("FILL, canceled").contains(ExecType.NEW));
    }
}
