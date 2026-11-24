package io.lmb.replayer;

import io.aeron.archive.client.AeronArchive;
import io.lmb.api.EventSink;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.ShardStateMachineFactory;
import io.lmb.api.SnapshotSink;
import io.lmb.api.TimerScheduler;
import io.lmb.cluster.RaftLogReader;
import io.lmb.common.Events;
import io.lmb.sbe.MessageHeaderDecoder;
import io.lmb.sbe.NewOrderDecoder;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Records a real 2-member shard once (orders, a timer, a snapshot in the middle), then verifies copies of its
 * member directories offline, clean and with injected corruption.
 */
class OfflineVerifierTest
{
    private static final int FRAME_HEADER = 32;
    private static final int SESSION_HEADER = 32;
    private static final int QUANTITY_OFFSET = MessageHeaderDecoder.ENCODED_LENGTH + NewOrderDecoder.quantityEncodingOffset();

    static TestShard shard;
    static long totalEntries;
    static long lastSeq;

    @BeforeAll
    static void record()
    {
        shard = new TestShard("offline", 2, 38000);
        shard.startAll();
        final int leader = shard.awaitLeader();
        long commands = 0;
        shard.send("SymbolDefinition correlationId=1 symbolId=1 ticker=ACME tickSize=1 lotSize=1 referencePrice=10000 " +
            "maxOrderQty=100000 priceBandBps=1000 initialState=CONTINUOUS");
        shard.send("RiskLimitUpdate correlationId=2 firmId=1 creditBudget=1000000000000 maxPositionPerSymbol=1000000 enabled=TRUE");
        shard.send("RiskLimitUpdate correlationId=3 firmId=2 creditBudget=1000000000000 maxPositionPerSymbol=1000000 enabled=TRUE");
        commands += 3;
        commands += sendOrders(0, 20);
        // timer entry: a transition with an effective time already past fires on the next cluster tick
        shard.send("SymbolStateCommand correlationId=4 symbolId=1 effectiveTimeNs=1 targetState=HALTED");
        commands += 2; // the command and its timer
        shard.awaitEntries(commands);
        shard.send("SymbolStateCommand correlationId=5 symbolId=1 targetState=CONTINUOUS");
        commands++;
        shard.awaitEntries(commands);

        TestShard.await("a snapshot", 15_000, () -> shard.nodes[leader].triggerSnapshot());
        TestShard.await("snapshot on every member", 15_000, () ->
            shard.nodes[0].snapshotCount() > 0 && shard.nodes[1].snapshotCount() > 0);

        commands += sendOrders(20, 14);
        lastSeq = shard.awaitEntries(commands);
        shard.awaitCommitSettles();
        totalEntries = commands;
        shard.stopAll();
    }

    private static int sendOrders(final int first, final int count)
    {
        for (int i = first; i < first + count; i++)
        {
            final boolean buy = (i & 1) == 0;
            shard.send("NewOrder sessionId=1 clOrdId=" + (100 + i) + " firmId=" + (buy ? 1 : 2) + " accountId=1 symbolId=1 side=" +
                (buy ? "BUY" : "SELL") + " ordType=LIMIT timeInForce=DAY price=" + (buy ? 10000 + i % 5 : 10000 - i % 3) +
                " quantity=" + (100 + 10 * (i % 4)) + " possResend=FALSE");
        }
        return count;
    }

    @AfterAll
    static void stop()
    {
        if (shard != null)
        {
            shard.close();
        }
    }

    private static File copy(final int member, final String name)
    {
        final File src = shard.memberDir(member);
        final File dst = new File(new File(shard.dir(), name), src.getName());
        TestShard.copyTree(src, dst);
        return dst;
    }

    @Test
    void cleanLogVerifiesOnBothMembers()
    {
        final OfflineVerifier.Report r0 = OfflineVerifier.verify(copy(0, "clean0"), ShardStateMachineFactory.load(), OfflineVerifier.Options.defaults());
        final OfflineVerifier.Report r1 = OfflineVerifier.verify(copy(1, "clean1"), ShardStateMachineFactory.load(), OfflineVerifier.Options.defaults());
        for (final OfflineVerifier.Report r : List.of(r0, r1))
        {
            assertTrue(r.ok(), r::toString);
            assertEquals(0, r.shardId());
            assertEquals(-1, r.snapshotPosition());
            assertEquals(totalEntries, r.entries(), r::toString);
            assertEquals(0, r.entriesUnverified(), r::toString);
            assertEquals(0, r.eventsUnverified(), r::toString);
            assertEquals(lastSeq, r.verifiedSeq(), r::toString);
            assertEquals(r.endPosition(), r.verifiedPosition());
            assertTrue(r.eventsCompared() > totalEntries, r::toString);
        }
        assertEquals(r0.lastChainHash(), r1.lastChainHash());
        assertEquals(r0.endPosition(), r1.endPosition());
    }

    @Test
    void cleanLogVerifiesFromTheSnapshot()
    {
        final OfflineVerifier.Report full = OfflineVerifier.verify(copy(1, "full1"), ShardStateMachineFactory.load(), OfflineVerifier.Options.defaults());
        final OfflineVerifier.Report r = OfflineVerifier.verify(
            copy(1, "snap1"), ShardStateMachineFactory.load(), new OfflineVerifier.Options(-1, true, -1));
        assertTrue(r.ok(), r::toString);
        assertTrue(r.snapshotPosition() > 0, r::toString);
        assertEquals(r.snapshotPosition(), r.startPosition());
        assertTrue(r.entries() >= 14 && r.entries() < totalEntries, r::toString);
        assertEquals(0, r.entriesUnverified(), r::toString);
        assertEquals(lastSeq, r.verifiedSeq(), r::toString);
        assertEquals(full.lastChainHash(), r.lastChainHash());
    }

    @Test
    void verifyMainExitStatus()
    {
        assertEquals(0, VerifyMain.run(new String[] { copy(0, "main0").getPath() }));
        assertEquals(1, VerifyMain.run(new String[] { new File(shard.dir(), "nowhere").getPath(), "--bogus" }));
    }

    @Test
    void flippedCommandByteIsReportedAtItsEntry() throws IOException
    {
        final File dir = copy(0, "flipcmd");
        final Entry target = newOrders(dir).get(25);
        final long frameLength = FRAME_HEADER + SESSION_HEADER + target.length;
        final long frameStart = target.logPosition - BitUtil.align(frameLength, 32);
        final long recordingId;
        try (RaftLogReader log = RaftLogReader.open(dir))
        {
            recordingId = log.terms().get(0).recordingId();
        }
        flip(dir, recordingId, frameStart + FRAME_HEADER + SESSION_HEADER + QUANTITY_OFFSET, (byte)1);

        final OfflineVerifier.Report r = OfflineVerifier.verify(dir, ShardStateMachineFactory.load(), OfflineVerifier.Options.defaults());
        assertNotNull(r.divergence(), r::toString);
        assertEquals(Divergence.Kind.EVENT_BYTES, r.divergence().kind(), r::toString);
        assertEquals(target.logPosition, r.divergence().logPosition(), r::toString);
        assertEquals(target.index, r.divergence().entry(), r::toString);
        assertTrue(r.verifiedPosition() < target.logPosition);
    }

    @Test
    void flippedChainHashIsReportedAsChainHashDivergence() throws IOException
    {
        final File dir = copy(1, "fliphash");
        final List<EventFiles.Segment> segments = EventFiles.readSegments(new File(dir, "events"));
        assertEquals(1, segments.size());
        final long recordingId = segments.get(0).recordingId();
        final ArchiveFiles archive = new ArchiveFiles(new File(dir, "archive"));
        final long[] found = { -1, -1 }; // eventSeq, file position of the chain hash
        final int[] applied = { 0 };
        try (ArchiveFiles.Cursor cursor = archive.cursor(recordingId, 0))
        {
            while (found[0] < 0 && cursor.next((b, o, l, p) ->
            {
                if (Events.isEvent(b, o) && Events.templateId(b, o) == ReplayVerifier.COMMAND_APPLIED && ++applied[0] == 12)
                {
                    found[0] = Events.eventSeq(b, o);
                    found[1] = p - BitUtil.align(FRAME_HEADER + l, 32) + FRAME_HEADER + ReplayVerifier.CHAIN_HASH_OFFSET + 5;
                }
            }))
            {
                // scan
            }
        }
        assertTrue(found[0] > 0);
        flip(dir, recordingId, found[1], (byte)0x80);

        final OfflineVerifier.Report r = OfflineVerifier.verify(dir, ShardStateMachineFactory.load(), OfflineVerifier.Options.defaults());
        assertNotNull(r.divergence(), r::toString);
        assertEquals(Divergence.Kind.CHAIN_HASH, r.divergence().kind(), r::toString);
        assertEquals(found[0], r.divergence().eventSeq(), r::toString);
        assertEquals(11, r.divergence().entry(), r::toString);
        assertEquals(found[0] - 1, r.verifiedSeq());
    }

    @Test
    void nondeterministicMachineIsCaughtAtTheRightEntry()
    {
        final File dir = copy(0, "nondet");
        final long badEntry = 17;
        final List<Entry> entries = entries(dir);
        final ShardStateMachineFactory real = ShardStateMachineFactory.load();
        final ShardStateMachineFactory skewed = shardId -> new SkewedMachine(real.create(shardId), badEntry);
        final OfflineVerifier.Report r = OfflineVerifier.verify(dir, skewed, OfflineVerifier.Options.defaults());
        assertNotNull(r.divergence(), r::toString);
        assertEquals(badEntry, r.divergence().entry(), r::toString);
        assertEquals(entries.get((int)badEntry).logPosition, r.divergence().logPosition(), r::toString);
        assertEquals(entries.get((int)badEntry - 1).logPosition, r.verifiedPosition(), r::toString);
        assertEquals(badEntry + 1, r.entries(), "replay stops applying after the diverged entry");
    }

    record Entry(long index, long logPosition, int templateId, int length)
    {
    }

    /** Commands and timers of the log, in order (timers with templateId -1). */
    static List<Entry> entries(final File dir)
    {
        final List<Entry> result = new ArrayList<>();
        try (RaftLogReader log = RaftLogReader.open(dir))
        {
            log.read(0, log.recordedPosition(), new RaftLogReader.Handler()
            {
                @Override
                public void onCommand(final long logPosition, final long timestampNs, final long clusterSessionId,
                    final DirectBuffer buffer, final int offset, final int length)
                {
                    result.add(new Entry(result.size(), logPosition, buffer.getShort(offset + 2) & 0xFFFF, length));
                }

                @Override
                public void onTimer(final long logPosition, final long timestampNs, final long correlationId)
                {
                    result.add(new Entry(result.size(), logPosition, -1, 0));
                }
            });
        }
        return result;
    }

    static List<Entry> newOrders(final File dir)
    {
        return entries(dir).stream().filter(e -> e.templateId == NewOrderDecoder.TEMPLATE_ID).toList();
    }

    /** XORs one byte of a recording at a recording position. */
    static void flip(final File memberDir, final long recordingId, final long position, final byte mask) throws IOException
    {
        final File archiveDir = new File(memberDir, "archive");
        final ArchiveFiles.Recording r = new ArchiveFiles(archiveDir).recording(recordingId);
        final long base = AeronArchive.segmentFileBasePosition(r.startPosition(), position, r.termLength(), r.segmentLength());
        try (RandomAccessFile f = new RandomAccessFile(new File(archiveDir, recordingId + "-" + base + ".rec"), "rw"))
        {
            f.seek(position - base);
            final byte b = f.readByte();
            f.seek(position - base);
            f.writeByte(b ^ mask);
        }
    }

    /** Delegates to the real machine but shifts the timestamp of one entry by 1 ns. */
    static final class SkewedMachine implements ShardStateMachine
    {
        private final ShardStateMachine delegate;
        private final long badEntry;
        private long entry;

        SkewedMachine(final ShardStateMachine delegate, final long badEntry)
        {
            this.delegate = delegate;
            this.badEntry = badEntry;
        }

        private long skew(final long timestampNs)
        {
            return entry++ == badEntry ? timestampNs + 1 : timestampNs;
        }

        @Override
        public int shardId()
        {
            return delegate.shardId();
        }

        @Override
        public void bind(final EventSink events, final TimerScheduler timers)
        {
            delegate.bind(events, timers);
        }

        @Override
        public void onCommand(final DirectBuffer buffer, final int offset, final int length, final long logPosition, final long timestampNs)
        {
            delegate.onCommand(buffer, offset, length, logPosition, skew(timestampNs));
        }

        @Override
        public void onTimer(final long correlationId, final long logPosition, final long timestampNs)
        {
            delegate.onTimer(correlationId, logPosition, skew(timestampNs));
        }

        @Override
        public void takeSnapshot(final SnapshotSink sink)
        {
            delegate.takeSnapshot(sink);
        }

        @Override
        public void onSnapshotChunk(final DirectBuffer buffer, final int offset, final int length)
        {
            delegate.onSnapshotChunk(buffer, offset, length);
        }

        @Override
        public void onSnapshotLoaded()
        {
            delegate.onSnapshotLoaded();
        }

        @Override
        public long lastEventSeq()
        {
            return delegate.lastEventSeq();
        }
    }
}
