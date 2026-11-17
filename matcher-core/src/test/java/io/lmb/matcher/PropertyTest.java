package io.lmb.matcher;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Randomised runs with every invariant checked after every log entry (Harness does that), the stream
 * contract checked per entry, plus determinism and snapshot round-trip properties.
 */
class PropertyTest
{
    static Harness run(final long seed, final int n)
    {
        final Harness h = new Harness();
        final RandomFlow flow = new RandomFlow(seed, true);
        flow.setup().forEach(h::send);
        RandomFlow.drive(h, flow, n);
        return h;
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8})
    void invariantsHoldAfterEveryStepOfRandomisedRuns(final long seed)
    {
        final Harness h = run(seed, 4_000);
        new StreamChecker().check(h.events);
        assertEquals(0, h.m.internalErrors());
        // the run actually exercised the interesting paths
        final String all = String.join("\n", Harness.texts(h.events));
        for (final String expected : new String[] {"TRADE", "auct=TRUE", "ER FILL", "ER PARTIAL_FILL", "ER CANCELED",
            "ER REPLACED", "ER EXPIRED", "rej=CREDIT_LIMIT", "rej=DUPLICATE_CLORDID", "rej=TOO_LATE_TO_CANCEL",
            "rej=UNKNOWN_ORDER", "rej=PRICE_BAND", "cxl=MARKET_COLLAR", "cxl=IOC_REMAINDER", "MASSDONE", "DAYROLLED",
            "AUCT", "r=MALFORMED", "r=INVALID_STATE_TRANSITION", "REF "})
        {
            assertTrue(all.contains(expected), "seed " + seed + " never produced " + expected);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {11, 12, 13})
    void twoInstancesFedTheSameLogEmitIdenticalBytesHashesAndSnapshots(final long seed)
    {
        final Harness a = run(seed, 3_000);
        // a second machine with different pool / window / table sizing replays the recorded log
        final Harness b = new Harness(new MatcherShard(0, new MatcherConfig(1 << 12, 1 << 10, 4096, 1 << 12)));
        b.checkInvariants = false;
        for (final Harness.Entry e : a.entries)
        {
            b.apply(e);
        }
        assertEquals(a.events.size(), b.events.size());
        for (int i = 0; i < a.events.size(); i++)
        {
            assertArrayEquals(a.events.get(i).bytes, b.events.get(i).bytes, "event " + i);
        }
        assertArrayEquals(a.m.chainHash(), b.m.chainHash());
        assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(b.m));
    }

    @ParameterizedTest
    @ValueSource(longs = {21, 22, 23, 24, 25})
    void snapshotAtARandomPointRestoresExactly(final long seed)
    {
        final Random rnd = new Random(seed);
        final Harness a = new Harness();
        final RandomFlow flow = new RandomFlow(seed, true);
        flow.setup().forEach(a::send);
        RandomFlow.drive(a, flow, 200 + rnd.nextInt(2_500));

        final byte[] snap = Harness.snapshot(a.m);
        final Harness b = a.restoreCopy();
        Invariants.check(b.m);
        assertArrayEquals(snap, Harness.snapshot(b.m), "restored machine re-snapshots identically");
        assertEquals(a.m.lastEventSeq(), b.m.lastEventSeq());

        // continue both with the same log (generated once against a, replayed into b)
        final int before = a.entries.size();
        RandomFlow.drive(a, flow, 1_500);
        for (final Harness.Entry e : a.entries.subList(before, a.entries.size()))
        {
            b.apply(e);
        }
        final List<Ev> tailA = a.events.subList(a.events.size() - (b.events.size() - 1), a.events.size());
        final List<Ev> tailB = b.events.subList(1, b.events.size());
        assertTrue(Harness.sameBytes(tailA, tailB), "outputs after restore identical");
        assertArrayEquals(a.m.chainHash(), b.m.chainHash());
        assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(b.m), "next snapshots identical");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 7, 33})
    void snapshotsAreChunkedAndLargeBooksRoundTrip(final int extra)
    {
        final Harness a = new Harness().standard();
        a.checkInvariants = false;
        for (int i = 0; i < 6_000 + extra; i++)
        {
            a.send(Cmd.limit(1001 + i % 3, i + 1, 100, 1, io.lmb.sbe.Side.BUY, 9_000 + i % 900, 1 + i % 7));
        }
        final int[] chunks = {0};
        a.m.takeSnapshot((buf, off, len) -> chunks[0]++);
        assertTrue(chunks[0] > 1, "large snapshot is split into chunks");
        final Harness b = a.restoreCopy();
        Invariants.check(b.m);
        assertArrayEquals(Harness.snapshot(a.m), Harness.snapshot(b.m));
        // corrupting a byte is detected
        final byte[] snap = Harness.snapshot(a.m);
        snap[snap.length / 2] ^= 1;
        final MatcherShard c = new MatcherShard(0);
        c.onSnapshotChunk(new UnsafeBuffer(snap), 0, snap.length);
        final IllegalStateException ex = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, c::onSnapshotLoaded);
        assertTrue(ex.getMessage().contains("checksum"));
        assertTrue(Arrays.equals(Harness.snapshot(a.m), Harness.snapshot(a.m)), "snapshot does not change state");
    }
}
