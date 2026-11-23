package io.lmb.feeds;

import io.lmb.common.SymbolDirectory;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeedSimulatorTest
{
    private static final long MS = 1_000_000L;
    private static final long T0 = 1_000_000_000_000L;
    static final SymbolDirectory SYMBOLS = new SymbolDirectory(List.of(
        new SymbolDirectory.Symbol(1, "AAA", 0, 1, 1, 10_000, 1000, 100_000),
        new SymbolDirectory.Symbol(2, "BBB", 1, 5, 1, 5_000, 1000, 100_000),
        new SymbolDirectory.Symbol(3, "CCC", 0, 1, 1, 12, 1000, 100_000)));

    record Sent(int venue, int line, byte[] bytes)
    {
    }

    private static List<Sent> run(final FeedSimulator.Settings settings, final long untilNs)
    {
        final List<Sent> sent = new ArrayList<>();
        final FeedSimulator sim = new FeedSimulator(settings, SYMBOLS, (v, line, p) ->
        {
            final byte[] b = new byte[p.remaining()];
            p.get(b);
            sent.add(new Sent(v, line, b));
            return true;
        }, T0);
        for (long t = T0; t <= untilNs; t += MS / 4)
        {
            sim.doWork(t);
        }
        return sent;
    }

    @Test
    void sendsEveryPacketOnBothLinesWithValidQuotesAndDenseSequences()
    {
        final List<Sent> sent = run(new FeedSimulator.Settings(2, 2_000, 10 * MS, 0, 0, 1), T0 + 100 * MS);
        final TestFeeds.Recorder rec = new TestFeeds.Recorder();
        final LineArbitrator[] arbs = {new LineArbitrator(0, rec, MS, 50 * MS), new LineArbitrator(1, rec, MS, 50 * MS)};
        int quotes = 0;
        int heartbeatsOnA = 0;
        for (final Sent s : sent)
        {
            final UnsafeBuffer b = new UnsafeBuffer(s.bytes);
            assertTrue(SimFeedProtocol.isValid(b, 0, s.bytes.length));
            assertEquals(s.line, SimFeedProtocol.line(b, 0));
            if (s.line == 0 && SimFeedProtocol.count(b, 0) == 0)
            {
                heartbeatsOnA++;
            }
            for (int i = 0; i < SimFeedProtocol.count(b, 0); i++)
            {
                final int m = SimFeedProtocol.messageOffset(0, i);
                assertTrue(SimFeedProtocol.msgBidPrice(b, m) > 0);
                assertTrue(SimFeedProtocol.msgBidPrice(b, m) < SimFeedProtocol.msgAskPrice(b, m));
                if (SimFeedProtocol.msgSymbolId(b, m) == 2)
                {
                    assertEquals(0, SimFeedProtocol.msgBidPrice(b, m) % 5, "on the symbol's tick");
                }
                if (s.line == 0)
                {
                    quotes++;
                }
            }
            arbs[s.venue].onPacket(s.line, b, 0, s.bytes.length, T0);
        }
        assertEquals(400, quotes, "2 venues x 2000/s x 100 ms");
        assertEquals(0, arbs[0].gaps() + arbs[1].gaps());
        assertEquals(quotes / 2, arbs[0].messagesDelivered());
        // the B copy of every data packet is a duplicate (heartbeat copies are idempotent, not counted)
        assertEquals(sent.size() / 2 - heartbeatsOnA, arbs[0].duplicates() + arbs[1].duplicates());
    }

    @Test
    void quietVenueSendsHeartbeatsCarryingTheNextSequence()
    {
        final List<Sent> sent = run(new FeedSimulator.Settings(1, 0, 10 * MS, 0, 0, 1), T0 + 35 * MS);
        assertEquals(8, sent.size(), "t = 0, 10, 20, 30 ms on A and B");
        for (final Sent s : sent)
        {
            final UnsafeBuffer b = new UnsafeBuffer(s.bytes);
            assertEquals(0, SimFeedProtocol.count(b, 0));
            assertEquals(1, SimFeedProtocol.firstSeq(b, 0));
        }
    }

    @Test
    void sameSeedSameBytesAndInjectedLossIsMaskedByTheOtherLine()
    {
        final FeedSimulator.Settings lossy = new FeedSimulator.Settings(1, 4_000, 10 * MS, 0.2, 0.2, 7);
        final List<Sent> a = run(lossy, T0 + 200 * MS);
        final List<Sent> b = run(lossy, T0 + 200 * MS);
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++)
        {
            assertArrayEquals(a.get(i).bytes, b.get(i).bytes);
        }

        final TestFeeds.Recorder rec = new TestFeeds.Recorder();
        final LineArbitrator arb = new LineArbitrator(0, rec, MS, 50 * MS);
        for (final Sent s : a)
        {
            arb.onPacket(s.line, new UnsafeBuffer(s.bytes), 0, s.bytes.length, T0);
        }
        arb.checkTimeouts(T0 + 300 * MS);
        assertTrue(arb.lineMissed(0) > 0 && arb.lineMissed(1) > 0, "both lines lost packets");
        assertTrue(arb.gaps() > 0, "with 20% loss per line about 4% of packets are lost on both");
        final long assigned = arb.nextExpectedSeq() - 1; // loss at the very tail stays undetected
        assertTrue(assigned > 780, "assigned " + assigned);
        assertEquals(assigned - arb.gapMessages(), arb.messagesDelivered());
        final long[] seqs = rec.quoteSeqs.stream().mapToLong(Long::longValue).toArray();
        final long[] sorted = seqs.clone();
        Arrays.sort(sorted);
        assertArrayEquals(sorted, seqs, "delivered in sequence order");
    }

    @Test
    void sinkBufferIsThePacketOnly()
    {
        final List<Integer> lengths = new ArrayList<>();
        final FeedSimulator sim = new FeedSimulator(new FeedSimulator.Settings(1, 1_000, 10 * MS, 0, 0, 1), SYMBOLS,
            (v, line, p) -> lengths.add(p.remaining()), T0);
        sim.doWork(T0 + 3 * MS);
        assertEquals(List.of(SimFeedProtocol.packetLength(3), SimFeedProtocol.packetLength(3)), lengths);
        assertEquals(4, sim.nextSequence(0));
    }
}
