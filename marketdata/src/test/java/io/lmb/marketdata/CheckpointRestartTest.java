package io.lmb.marketdata;

import io.lmb.api.VenueClients;
import io.lmb.marketdata.wire.MdWire;
import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Restart from {@code state.bin}: md_seq and message bytes continue exactly as an instance that never stopped,
 * events redelivered after the checkpoint are dropped by eventSeq, and a checkpoint that cannot be trusted
 * stops the publisher instead of publishing inconsistent sequence numbers. Engines are driven on the test
 * thread, so every run is deterministic.
 */
class CheckpointRestartTest
{
    static final long NEVER = Long.MAX_VALUE / 4;

    @TempDir
    Path tmp;

    // ---- helpers shared with TwoPublishersTest ----

    static MdConfig engineConfig(final int shards, final Path dataDir, final int pollLimit)
    {
        final MdConfig c = TestSupport.config(0, shards, 0, false, dataDir);
        c.pollLimit = pollLimit;
        c.historyDepth = 1 << 16;
        c.checkpointIntervalMs = NEVER / 1_000_000L;
        return c;
    }

    static MarketDataEngine engine(final MdConfig c, final VenueClients venue, final RetransmitHistory history)
        throws IOException
    {
        final MarketDataEngine e = new MarketDataEngine(c, venue, history, null, null, new PublisherMetrics());
        e.start();
        return e;
    }

    /** Polls until the engine has consumed every event the venue has produced. */
    static void drain(final MarketDataEngine e, final VenueDriver v, final long nowNs)
    {
        for (int guard = 0; guard < 1_000_000; guard++)
        {
            boolean done = true;
            for (int s = 0; s < v.venue.shardCount(); s++)
            {
                done &= e.builder().lastEventSeq(s) == v.venue.eventCount(s);
            }
            if (done)
            {
                return;
            }
            e.doWork(nowNs);
        }
        throw new AssertionError("engine did not drain");
    }

    /** symbolId -> messages by md_seq (index seq-1; null where the history does not hold it). */
    static TreeMap<Integer, byte[][]> messages(final RetransmitHistory h, final BookBuilder builder)
    {
        final TreeMap<Integer, byte[][]> out = new TreeMap<>();
        final IntArrayList ids = builder.sortedSymbolIds();
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[RetransmitHistory.SLOT_SIZE]);
        final RetransmitHistory.Range range = new RetransmitHistory.Range();
        for (int i = 0; i < ids.size(); i++)
        {
            final int id = ids.getInt(i);
            final byte[][] bySeq = new byte[(int)builder.symbol(id).seq()][];
            final RetransmitHistory.SymbolHistory sh = h.get(id);
            for (long seq = 1; sh != null && seq <= bySeq.length; seq++)
            {
                sh.copyRange(seq, seq, 1, buf, 0, range);
                if (range.count == 1)
                {
                    final byte[] m = new byte[MdWire.frameLength(buf, 0)];
                    buf.getBytes(MdWire.FRAME_PREFIX_LENGTH, m);
                    assertEquals(seq, MdWire.seq(new UnsafeBuffer(m), 0));
                    bySeq[(int)seq - 1] = m;
                }
            }
            out.put(id, bySeq);
        }
        return out;
    }

    static void activity(final VenueDriver v, final long seed, final int rounds)
    {
        for (int i = 0; i < rounds; i++)
        {
            v.randomSession(0, new int[] {1, 2}, seed + i, 20);
            if (v.venue.shardCount() > 1)
            {
                v.randomSession(1, new int[] {3}, seed + 1000 + i, 10);
            }
        }
    }

    /** Asserts that every message in {@code part} equals the reference message at the same md_seq; returns how many. */
    static int assertSubsetOf(final TreeMap<Integer, byte[][]> reference, final TreeMap<Integer, byte[][]> part)
    {
        int n = 0;
        for (final var e : part.entrySet())
        {
            final byte[][] ref = reference.get(e.getKey());
            for (int i = 0; i < e.getValue().length; i++)
            {
                if (e.getValue()[i] != null)
                {
                    assertArrayEquals(ref[i], e.getValue()[i], "symbol " + e.getKey() + " md_seq " + (i + 1));
                    n++;
                }
            }
        }
        return n;
    }

    private static int total(final TreeMap<Integer, byte[][]> m)
    {
        return m.values().stream().mapToInt(a -> a.length).sum();
    }

    // ---- tests ----

    @Test
    void cleanStopAndRestartContinuesTheSameSequenceWithoutRepeatingMessages() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        final GatedVenue gated = new GatedVenue(v.venue);
        final Path dir = tmp.resolve("md");
        activity(v, 1, 15);

        final RetransmitHistory h1 = new RetransmitHistory(1 << 16);
        final MarketDataEngine e1 = engine(engineConfig(2, dir, 64), gated, h1);
        drain(e1, v, 0);
        final TreeMap<Integer, byte[][]> before = messages(h1, e1.builder());
        e1.close(); // final checkpoint
        assertTrue(Files.exists(dir.resolve(StateCheckpoint.FILE_NAME)));

        activity(v, 500, 15);
        gated.rewind = 25; // the stream redelivers events the checkpoint already covers
        final RetransmitHistory h2 = new RetransmitHistory(1 << 16);
        final MarketDataEngine e2 = engine(engineConfig(2, dir, 64), gated, h2);
        drain(e2, v, 0);
        assertTrue(e2.builder().duplicateEvents() >= 25, "redelivered events dropped: " + e2.builder().duplicateEvents());
        assertEquals(0, e2.builder().eventGaps());
        final TreeMap<Integer, byte[][]> after = messages(h2, e2.builder());

        final RetransmitHistory hr = new RetransmitHistory(1 << 16);
        final MarketDataEngine ref = engine(engineConfig(2, null, 1_000), v.venue, hr);
        drain(ref, v, 0);
        final TreeMap<Integer, byte[][]> reference = messages(hr, ref.builder());

        // Together the two runs published exactly the reference stream, each message once.
        assertEquals(total(reference), assertSubsetOf(reference, before) + assertSubsetOf(reference, after));
        for (final int id : before.keySet())
        {
            final byte[][] b = before.get(id);
            final byte[][] a = after.get(id);
            assertEquals(reference.get(id).length, a.length, "symbol " + id + " final md_seq");
            for (int i = 0; i < b.length; i++)
            {
                assertTrue(a[i] == null, "symbol " + id + " md_seq " + (i + 1) + " re-published after a clean stop");
            }
        }
        assertTrue(assertSubsetOf(reference, after) > 50, "the restarted engine published the new activity");
        e2.close();
        ref.close();
    }

    @Test
    void crashRestartFromAnOlderCheckpointRepublishesIdenticalBytesAndReachesTheSameState() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        final Path live = tmp.resolve("live");
        final Path crashed = tmp.resolve("crashed");
        activity(v, 7, 10);

        final RetransmitHistory h1 = new RetransmitHistory(1 << 16);
        final MarketDataEngine e1 = engine(engineConfig(2, live, 100_000), v.venue, h1);
        e1.doWork(0); // consumes the backlog, then the first checkpoint is due (written asynchronously)
        TestSupport.await("first checkpoint", 5_000, () -> Files.exists(live.resolve(StateCheckpoint.FILE_NAME)));
        Files.createDirectories(crashed);
        Files.copy(live.resolve(StateCheckpoint.FILE_NAME), crashed.resolve(StateCheckpoint.FILE_NAME));

        activity(v, 300, 10);
        drain(e1, v, 1); // published beyond the checkpoint; the copy in 'crashed' does not know about it
        final TreeMap<Integer, byte[][]> published = messages(h1, e1.builder());
        e1.close();

        final RetransmitHistory h2 = new RetransmitHistory(1 << 16);
        final MarketDataEngine e2 = engine(engineConfig(2, crashed, 50), v.venue, h2);
        drain(e2, v, 0);
        final TreeMap<Integer, byte[][]> republished = messages(h2, e2.builder());
        assertTrue(assertSubsetOf(published, republished) > 50, "messages after the checkpoint were published again");
        assertEquals(published.keySet(), republished.keySet());
        for (final int id : published.keySet())
        {
            assertEquals(published.get(id).length, republished.get(id).length, "symbol " + id + " final md_seq");
        }
        e2.close();
        // Same events, same state: both final checkpoints are byte-identical.
        assertArrayEquals(Files.readAllBytes(live.resolve(StateCheckpoint.FILE_NAME)),
            Files.readAllBytes(crashed.resolve(StateCheckpoint.FILE_NAME)));
    }

    @Test
    void untrustworthyCheckpointOrStreamStopsThePublisher() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(1);
        final Path dir = tmp.resolve("md");
        activity(v, 11, 5);
        final MarketDataEngine e1 = engine(engineConfig(1, dir, 64), v.venue, new RetransmitHistory(64));
        drain(e1, v, 0);
        final long committed = e1.builder().committedEventSeq(0);
        e1.close();
        final Path file = dir.resolve(StateCheckpoint.FILE_NAME);
        final byte[] good = Files.readAllBytes(file);

        // Another sessionId: md_seq of this file belongs to another session.
        final MdConfig otherSession = engineConfig(1, dir, 64);
        otherSession.sessionId = 2;
        final IOException wrongSession = assertThrows(IOException.class,
            () -> new MarketDataEngine(otherSession, v.venue, new RetransmitHistory(64), null, null, new PublisherMetrics()).start());
        assertTrue(wrongSession.getMessage().contains("sessionId"), wrongSession.getMessage());

        // A flipped byte fails the CRC.
        final byte[] bad = Arrays.copyOf(good, good.length);
        bad[good.length / 2] ^= 0x10;
        Files.write(file, bad);
        final IOException corrupt = assertThrows(IOException.class,
            () -> new MarketDataEngine(engineConfig(1, dir, 64), v.venue, new RetransmitHistory(64), null, null,
                new PublisherMetrics()).start());
        assertTrue(corrupt.getMessage().contains("CRC"), corrupt.getMessage());

        // A stream that is not contiguous with the checkpoint (an event lost) is refused, not papered over.
        Files.write(file, good);
        activity(v, 900, 3);
        final GatedVenue gated = new GatedVenue(v.venue);
        gated.dropEventSeq = committed + 3;
        final MarketDataEngine e3 = engine(engineConfig(1, dir, 64), gated, new RetransmitHistory(64));
        final IllegalStateException gap = assertThrows(IllegalStateException.class, () -> drain(e3, v, 0));
        assertTrue(gap.getMessage().contains("not contiguous"), gap.getMessage());
        assertFalse(e3.isCaughtUp());
        e3.close();
        // A failed engine writes no final checkpoint: a restart resumes from the last consistent one.
        assertArrayEquals(good, Files.readAllBytes(file));
    }
}
