package io.lmb.feeds;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.LongStream;

import static io.lmb.feeds.SimFeedProtocol.LINE_A;
import static io.lmb.feeds.SimFeedProtocol.LINE_B;
import static io.lmb.feeds.TestFeeds.packet;
import static io.lmb.feeds.TestFeeds.q;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineArbitratorTest
{
    private static final int VENUE = 2;
    private static final long SESSION = 1_000;
    private static final long GAP_TIMEOUT = 2_000;
    private static final long SILENCE = 50_000;

    private final TestFeeds.Recorder rec = new TestFeeds.Recorder();
    private final LineArbitrator arb = new LineArbitrator(VENUE, rec, GAP_TIMEOUT, SILENCE);

    private void rx(final int line, final long firstSeq, final long ts, final TestFeeds.Q... quotes)
    {
        rx(line, SESSION, firstSeq, ts, quotes);
    }

    private void rx(final int line, final long session, final long firstSeq, final long ts, final TestFeeds.Q... quotes)
    {
        final UnsafeBuffer b = packet(line, VENUE, session, firstSeq, ts, quotes);
        arb.onPacket(line, b, 0, b.capacity(), ts);
    }

    private static List<Long> seqs(final long from, final long toInclusive)
    {
        return LongStream.rangeClosed(from, toInclusive).boxed().toList();
    }

    @Test
    void deliversFirstCopyAndDropsTheOtherLine()
    {
        rx(LINE_A, 1, 10, q(1, 100, 101), q(2, 200, 201));
        rx(LINE_B, 1, 11, q(1, 100, 101), q(2, 200, 201));
        rx(LINE_B, 3, 12, q(1, 99, 101));
        rx(LINE_A, 3, 13, q(1, 99, 101));

        assertEquals(List.of(
            "ST 2 UP 0", "Q 2 1 1 100 101", "Q 2 2 2 200 201", "HB 2 10", "Q 2 1 3 99 101", "HB 2 12"), rec.events);
        assertEquals(2, arb.duplicates());
        assertEquals(3, arb.messagesDelivered());
        assertEquals(4, arb.nextExpectedSeq());
    }

    @Test
    void holeOnOneLineIsFilledByTheOtherWithoutGap()
    {
        rx(LINE_A, 1, 10, q(1, 1, 2));
        rx(LINE_A, 3, 12, q(1, 3, 4)); // seq 2 lost on A
        assertEquals(1, arb.pendingCount());
        rx(LINE_B, 1, 10, q(1, 1, 2));
        rx(LINE_B, 2, 11, q(1, 2, 3)); // B fills it, releases 3
        rx(LINE_B, 3, 12, q(1, 3, 4));
        arb.checkTimeouts(12 + GAP_TIMEOUT * 10);

        assertEquals(seqs(1, 3), rec.quoteSeqs);
        assertTrue(rec.statuses().stream().noneMatch(s -> s.contains("GAP")));
        assertEquals(0, arb.gaps());
        assertEquals(1, arb.lineMissed(LINE_A));
        assertEquals(0, arb.lineMissed(LINE_B));
    }

    @Test
    void holeOnBothLinesIsDeclaredAfterTheTimeoutAndLateCopiesAreDropped()
    {
        rx(LINE_A, 1, 10, q(1, 1, 2));
        rx(LINE_A, 4, 20, q(1, 4, 5), q(1, 5, 6)); // 2..3 missing
        rx(LINE_A, 6, 21, q(1, 6, 7));
        arb.checkTimeouts(20 + GAP_TIMEOUT - 1);
        assertEquals(seqs(1, 1), rec.quoteSeqs);

        arb.checkTimeouts(20 + GAP_TIMEOUT);
        assertEquals(seqs(1, 6).stream().filter(s -> s != 2 && s != 3).toList(), rec.quoteSeqs);
        assertTrue(rec.events.contains("ST 2 GAP 2"));
        assertTrue(rec.events.indexOf("ST 2 GAP 2") < rec.events.indexOf("Q 2 1 4 4 5"), "GAP before the held quotes");
        assertEquals(1, arb.gaps());
        assertEquals(2, arb.gapMessages());
        assertEquals(0, arb.pendingCount());

        rec.clear();
        rx(LINE_B, 2, 11, q(1, 2, 3), q(1, 3, 4)); // too late
        assertTrue(rec.events.isEmpty());
        assertEquals(7, arb.nextExpectedSeq());
    }

    @Test
    void everyExpiredHoleIsDeclaredInOneCheckButNotAYoungerOne()
    {
        rx(LINE_A, 1, 10, q(1, 1, 2));
        rx(LINE_A, 3, 20, q(1, 3, 4)); // 2 missing
        rx(LINE_A, 5, 21, q(1, 5, 6)); // 4 missing
        rx(LINE_A, 8, 500, q(1, 8, 9)); // 6..7 missing, revealed later
        arb.checkTimeouts(21 + GAP_TIMEOUT);
        assertEquals(List.of(1L, 3L, 5L), rec.quoteSeqs);
        assertEquals(List.of("ST 2 UP 0", "ST 2 GAP 1", "ST 2 GAP 1"), rec.statuses());
        assertEquals(1, arb.pendingCount());

        arb.checkTimeouts(500 + GAP_TIMEOUT);
        assertEquals(List.of(1L, 3L, 5L, 8L), rec.quoteSeqs);
        assertEquals(3, arb.gaps());
        assertEquals(4, arb.gapMessages());
    }

    @Test
    void fullHoldBufferDeclaresTheGapImmediately()
    {
        rx(LINE_A, 1, 10, q(1, 1, 2));
        for (int i = 0; i < LineArbitrator.PENDING_CAPACITY; i++)
        {
            rx(LINE_A, 3 + i, 11 + i, q(1, 3 + i, 4 + i));
        }
        assertEquals(LineArbitrator.PENDING_CAPACITY, arb.pendingCount());
        assertEquals(0, arb.gaps());
        rx(LINE_A, 3 + LineArbitrator.PENDING_CAPACITY, 100, q(1, 1, 2));

        assertEquals(1, arb.gaps());
        assertEquals(1, arb.gapMessages());
        assertEquals(0, arb.pendingCount());
        assertEquals(3L + LineArbitrator.PENDING_CAPACITY + 1, arb.nextExpectedSeq());
        assertEquals(LineArbitrator.PENDING_CAPACITY + 2, rec.quoteSeqs.size());
    }

    @Test
    void heartbeatAheadRevealsTailLoss()
    {
        rx(LINE_A, 1, 10, q(1, 1, 2));
        rx(LINE_A, 2, 20); // in-sequence heartbeat
        rx(LINE_A, 4, 30); // venue assigned 2 and 3, both lines lost them
        rx(LINE_B, 4, 30); // same heartbeat on B: duplicate of the held one
        assertEquals(1, arb.pendingCount());
        assertEquals(1, arb.duplicates());
        arb.checkTimeouts(30 + GAP_TIMEOUT);

        assertEquals(List.of("ST 2 UP 0", "Q 2 1 1 1 2", "HB 2 10", "HB 2 20", "ST 2 GAP 2", "HB 2 30"), rec.events);
    }

    @Test
    void packetOverlappingWhatWasDeliveredDeliversOnlyTheNewPart()
    {
        rx(LINE_A, 1, 10, q(1, 1, 2), q(1, 2, 3));
        rx(LINE_B, 2, 10, q(1, 2, 3), q(1, 3, 4)); // B packetised differently
        assertEquals(seqs(1, 3), rec.quoteSeqs);
    }

    @Test
    void sessionRestartReportsGapAndRestartsFromOne()
    {
        rx(LINE_A, 1, 10, q(1, 1, 2));
        rx(LINE_A, 2, 11, q(1, 2, 3));
        rec.clear();

        final long newSession = SESSION + 500;
        rx(LINE_B, newSession, 2, 40, q(1, 22, 23)); // B's seq 1 of the new session lost, A's still in flight
        assertEquals(List.of("ST 2 GAP 0"), rec.events);
        assertEquals(1, arb.sessionResets());
        rx(LINE_A, newSession, 1, 39, q(1, 21, 22));
        assertEquals(List.of("ST 2 GAP 0", "Q 2 1 1 21 22", "HB 2 39", "Q 2 1 2 22 23", "HB 2 40"), rec.events);

        rec.clear();
        rx(LINE_A, SESSION, 3, 41, q(1, 1, 2)); // old session: ignored
        assertTrue(rec.events.isEmpty());
    }

    @Test
    void silenceIsReportedOnceAsStaleAndRecoversOnTheNextPacket()
    {
        assertEquals(0, arb.checkTimeouts(1_000_000), "never heard from: neither UP nor STALE");
        rx(LINE_A, 1, 10, q(1, 1, 2));
        arb.checkTimeouts(10 + SILENCE);
        assertTrue(arb.isUp());
        arb.checkTimeouts(10 + SILENCE + 1);
        arb.checkTimeouts(10 + SILENCE + 2);
        assertFalse(arb.isUp());
        assertEquals(1, arb.staleEvents());
        rx(LINE_B, 2, 100_000);
        assertEquals(List.of("ST 2 UP 0", "ST 2 STALE 0", "ST 2 UP 0"), rec.statuses());
    }

    @Test
    void malformedAndForeignPacketsAreCountedAndIgnored()
    {
        final UnsafeBuffer good = packet(LINE_A, VENUE, SESSION, 1, 10, q(1, 1, 2));
        arb.onPacket(LINE_A, good, 0, good.capacity() - 1, 10); // truncated
        final UnsafeBuffer otherVenue = packet(LINE_A, VENUE + 1, SESSION, 1, 10, q(1, 1, 2));
        arb.onPacket(LINE_A, otherVenue, 0, otherVenue.capacity(), 10);
        assertEquals(2, arb.malformed());
        assertTrue(rec.events.isEmpty());
        assertEquals(0, arb.linePackets(LINE_A));
    }

    @Test
    void nonQuoteMessagesAreSkippedButConsumeSequence()
    {
        final UnsafeBuffer b = packet(LINE_A, VENUE, SESSION, 1, 10, q(1, 1, 2), q(1, 2, 3));
        b.putShort(SimFeedProtocol.messageOffset(0, 0) + 4, (short)7, SimFeedProtocol.ORDER);
        arb.onPacket(LINE_A, b, 0, b.capacity(), 10);
        assertEquals(List.of(2L), rec.quoteSeqs);
        assertEquals(3, arb.nextExpectedSeq());
    }
}
