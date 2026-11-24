package io.lmb.replayer;

import io.lmb.api.ShardStateMachineFactory;
import io.lmb.common.Events;
import io.lmb.sbe.TradeDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BacktestTest
{
    private static final File ROOT = new File(TestShard.ROOT, "backtest");

    private static final String SETUP = """
        # reference data
        @1000 SymbolDefinition correlationId=1 symbolId=1 ticker=ACME tickSize=1 lotSize=1 referencePrice=10000 maxOrderQty=100000 priceBandBps=1000 initialState=CONTINUOUS
        RiskLimitUpdate correlationId=2 firmId=1 creditBudget=1000000000000 maxPositionPerSymbol=1000000 enabled=TRUE
        RiskLimitUpdate correlationId=3 firmId=2 creditBudget=1000000000000 maxPositionPerSymbol=1000000 enabled=TRUE
        """;

    private static String orders(final int first, final int count)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = first; i < first + count; i++)
        {
            final boolean buy = (i & 1) == 0;
            sb.append("NewOrder sessionId=1 clOrdId=").append(100 + i).append(" firmId=").append(buy ? 1 : 2)
                .append(" accountId=1 symbolId=1 side=").append(buy ? "BUY" : "SELL")
                .append(" ordType=LIMIT timeInForce=DAY price=").append(buy ? 10001 : 10000)
                .append(" quantity=100 possResend=FALSE\n");
        }
        return sb.toString();
    }

    private static File write(final String name, final String content) throws IOException
    {
        ROOT.mkdirs();
        final File f = new File(ROOT, name);
        Files.writeString(f.toPath(), content);
        return f;
    }

    @Test
    void commandFileRunIsDeterministicFiresTimersAndWritesFills() throws IOException
    {
        // the halt becomes effective at t=10000: orders before it trade, orders after it are rejected
        final File commands = write("commands.txt", SETUP +
            "@4000 SymbolStateCommand correlationId=4 symbolId=1 effectiveTimeNs=10000 targetState=HALTED\n" +
            orders(0, 6) + "@10500 " + orders(6, 2));
        final Backtest.Options a = new Backtest.Options(null, -1, false, commands, 0, 1000, false, new File(ROOT, "a"));
        final Backtest.Options b = new Backtest.Options(null, -1, false, commands, 0, 1000, false, new File(ROOT, "b"));
        final Backtest.Result ra = Backtest.run(ShardStateMachineFactory.load(), a);
        final Backtest.Result rb = Backtest.run(ShardStateMachineFactory.load(), b);

        assertEquals(ra, rb);
        assertArrayEquals(Files.readAllBytes(new File(a.outDir(), "events.bin").toPath()),
            Files.readAllBytes(new File(b.outDir(), "events.bin").toPath()));
        assertEquals(12, ra.injected());
        assertEquals(1, ra.timersFired());
        assertEquals(0, ra.timersPending());
        // orders at t=5000..9000 cross twice; the one at t=10000 comes after the timer halted the symbol
        assertEquals(2, ra.trades(), ra::toString);
        assertEquals(200, ra.volume());

        final List<byte[]> events = readEvents(new File(a.outDir(), "events.bin"));
        assertEquals(ra.events(), events.size());
        long seq = 0;
        for (final byte[] e : events)
        {
            assertEquals(++seq, Events.eventSeq(new UnsafeBuffer(e), 0), "eventSeq is contiguous");
        }
        final List<String> fills = Files.readAllLines(new File(a.outDir(), "fills.csv").toPath());
        assertEquals(1 + ra.trades(), fills.size());
        for (final String fill : fills.subList(1, fills.size()))
        {
            assertTrue(Long.parseLong(fill.split(",")[1]) < 10000, fill);
            assertTrue(fill.contains(",10000,100,") || fill.contains(",10001,100,"), fill);
        }
        final String summary = Files.readString(new File(a.outDir(), "summary.txt").toPath());
        assertTrue(summary.contains("timers fired 1, pending 0") && summary.contains("  Trade 2") &&
            summary.contains("  SymbolStateChanged 1"), summary);
    }

    @Test
    void logReplayReproducesTheRecordedStreamThenInjects() throws IOException
    {
        final List<byte[]> recorded = new ArrayList<>();
        final File memberDir;
        try (TestShard shard = new TestShard("backtest-shard", 1, 38200))
        {
            shard.startAll();
            shard.awaitLeader();
            for (final String line : (SETUP + orders(0, 8)).lines().toList())
            {
                final String l = line.strip();
                if (!l.isEmpty() && !l.startsWith("#"))
                {
                    shard.send(l.startsWith("@") ? l.substring(l.indexOf(' ') + 1) : l);
                }
            }
            shard.awaitEntries(11);
            shard.stopAll();
            memberDir = shard.memberDir(0);
        }
        try (EventFiles files = new EventFiles(memberDir, 0))
        {
            while (files.poll((buf, o, l) ->
            {
                final byte[] e = new byte[l];
                buf.getBytes(o, e);
                recorded.add(e);
            }, 64) > 0)
            {
                // drain
            }
        }

        final File out = new File(ROOT, "log");
        final Backtest.Result r = Backtest.run(ShardStateMachineFactory.load(),
            new Backtest.Options(memberDir, -1, false, null, -1, 1000, false, out));
        final List<byte[]> replayed = readEvents(new File(out, "events.bin"));
        assertEquals(11, r.logEntries());
        assertEquals(recorded.size(), replayed.size());
        for (int i = 0; i < recorded.size(); i++)
        {
            assertTrue(Arrays.equals(recorded.get(i), replayed.get(i)), "event " + i);
        }
        assertEquals(4, r.trades());

        // the same log plus two more crossing orders through the CLI
        final File more = write("more.txt", orders(8, 2));
        final File out2 = new File(ROOT, "log-plus");
        assertEquals(0, BacktestMain.run(new String[] { "--log", memberDir.getPath(), "--commands", more.getPath(), "--out", out2.getPath() }));
        final List<byte[]> extended = readEvents(new File(out2, "events.bin"));
        for (int i = 0; i < recorded.size(); i++)
        {
            assertTrue(Arrays.equals(recorded.get(i), extended.get(i)), "event " + i);
        }
        assertTrue(extended.size() > recorded.size());
        final UnsafeBuffer last = new UnsafeBuffer(extended.get(extended.size() - 1));
        assertEquals(extended.size(), Events.eventSeq(last, 0));
        assertTrue(Events.logPosition(last, 0) > r.endPosition(), "injected entries continue the log positions");
        long trades = 0;
        for (final byte[] e : extended)
        {
            trades += Events.templateId(new UnsafeBuffer(e), 0) == TradeDecoder.TEMPLATE_ID ? 1 : 0;
        }
        assertEquals(5, trades);
        assertEquals(1, BacktestMain.run(new String[] { "--out", out2.getPath() }));
    }

    static List<byte[]> readEvents(final File file) throws IOException
    {
        final ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(file.toPath())).order(ByteOrder.LITTLE_ENDIAN);
        final List<byte[]> result = new ArrayList<>();
        while (b.remaining() >= 4)
        {
            final byte[] e = new byte[b.getInt()];
            b.get(e);
            result.add(e);
        }
        return result;
    }
}
