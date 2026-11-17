package io.lmb.matcher;

import io.lmb.api.TimerScheduler;
import io.lmb.common.Events;
import io.lmb.common.HashChain;
import io.lmb.sbe.CommandAppliedDecoder;
import io.lmb.sbe.SymbolState;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives one MatcherShard directly: deterministic log positions and time, a fake TimerScheduler,
 * capture of every event, and independent checks of the stream contract (dense eventSeq, eventIdx,
 * CommandApplied last with the right count, and the hash chain recomputed from scratch).
 */
final class Harness
{
    /** A log entry: a command (bytes) or a timer (correlationId). */
    record Entry(byte[] command, long timerId, long logPosition, long timestampNs)
    {
    }

    final MatcherShard m;
    final List<Ev> events = new ArrayList<>();
    final List<Entry> entries = new ArrayList<>();
    final TreeMap<Long, Long> timersById = new TreeMap<>();
    long logPosition;
    long now = 1_700_000_000_000_000_000L;
    boolean checkInvariants = true;

    private final HashChain shadowChain = new HashChain();
    private int entryStart;

    Harness()
    {
        this(new MatcherShard(0, new MatcherConfig(64, 16, 256, 16)));
    }

    Harness(final MatcherShard m)
    {
        this.m = m;
        m.bind(
            (buffer, offset, length) ->
            {
                final byte[] copy = new byte[length];
                buffer.getBytes(offset, copy);
                events.add(new Ev(copy));
            },
            new TimerScheduler()
            {
                public void schedule(final long correlationId, final long deadlineNs)
                {
                    timersById.put(correlationId, deadlineNs);
                }

                public void cancel(final long correlationId)
                {
                    timersById.remove(correlationId);
                }
            });
    }

    /** Defines symbol 1 (ref $100.00, tick 1, lot 1, max 1,000,000, band 10%) CONTINUOUS and firms 100, 200, 300. */
    Harness standard()
    {
        send(Cmd.symbol(1, 1, "AAPL", 1, 1, 10_000, 1_000_000, 1000, SymbolState.CONTINUOUS));
        for (final int f : new int[] {100, 200, 300})
        {
            send(Cmd.risk(f, f, 1_000_000_000_000L, 10_000_000, true));
        }
        return this;
    }

    List<Ev> send(final byte[] cmd)
    {
        now += 1000;
        logPosition += 64;
        return apply(new Entry(cmd, 0, logPosition, now));
    }

    /** Fires every timer due at or before t, in (deadline, id) order. */
    List<Ev> advanceTo(final long t)
    {
        final List<Ev> out = new ArrayList<>();
        while (true)
        {
            Map.Entry<Long, Long> due = null;
            for (final Map.Entry<Long, Long> e : timersById.entrySet())
            {
                if (e.getValue() <= t && (due == null || e.getValue() < due.getValue()))
                {
                    due = e;
                }
            }
            if (due == null)
            {
                break;
            }
            timersById.remove(due.getKey());
            now = Math.max(now, due.getValue());
            logPosition += 64;
            out.addAll(apply(new Entry(null, due.getKey(), logPosition, now)));
        }
        now = Math.max(now, t);
        return out;
    }

    List<Ev> apply(final Entry e)
    {
        entries.add(e);
        entryStart = events.size();
        if (e.command != null)
        {
            m.onCommand(new UnsafeBuffer(e.command), 0, e.command.length, e.logPosition, e.timestampNs);
        }
        else
        {
            m.onTimer(e.timerId, e.logPosition, e.timestampNs);
        }
        final List<Ev> out = new ArrayList<>(events.subList(entryStart, events.size()));
        checkEntry(e, out);
        if (checkInvariants)
        {
            Invariants.check(m);
        }
        return out.subList(0, out.size() - 1); // without CommandApplied
    }

    private void checkEntry(final Entry e, final List<Ev> out)
    {
        assertTrue(!out.isEmpty(), "every entry emits at least CommandApplied");
        final Ev last = out.get(out.size() - 1);
        assertEquals(CommandAppliedDecoder.TEMPLATE_ID, last.templateId, "last event must be CommandApplied");
        assertEquals(out.size() - 1, last.applied().eventCount());
        final int template = e.command == null ? 0 :
            e.command.length >= 8 ? (e.command[2] & 0xFF) | ((e.command[3] & 0xFF) << 8) : 0;
        assertEquals(template, last.applied().commandTemplateId());
        shadowChain.begin(e.logPosition, e.timestampNs);
        if (e.command != null)
        {
            shadowChain.update(new UnsafeBuffer(e.command), 0, e.command.length);
        }
        else
        {
            shadowChain.updateLong(e.timerId);
        }
        long expectedSeq = entryStart == 0 ? 1 : events.get(entryStart - 1).seq() + 1;
        for (int i = 0; i < out.size(); i++)
        {
            final Ev ev = out.get(i);
            assertEquals(expectedSeq++, ev.seq(), "dense eventSeq");
            assertEquals(i, ev.idx(), "eventIdx");
            assertEquals(e.logPosition, Events.logPosition(ev.buf, 0));
            assertEquals(e.timestampNs, Events.timestampNs(ev.buf, 0));
            assertEquals(m.shardId(), Events.shardId(ev.buf, 0));
            if (i < out.size() - 1)
            {
                shadowChain.update(ev.buf, 0, ev.bytes.length);
            }
        }
        final byte[] expected = shadowChain.finish().clone();
        final byte[] actual = new byte[32];
        last.applied().getChainHash(actual, 0, 32);
        assertArrayEquals(expected, actual, "chain hash");
        assertEquals(m.lastEventSeq(), last.seq());
    }

    /** Snapshot bytes concatenated, asserting the chunk size limit. */
    static byte[] snapshot(final MatcherShard m)
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        m.takeSnapshot((buffer, offset, length) ->
        {
            assertTrue(length > 0 && length <= io.lmb.api.SnapshotSink.MAX_CHUNK_LENGTH, "chunk length " + length);
            final byte[] chunk = new byte[length];
            buffer.getBytes(offset, chunk);
            out.writeBytes(chunk);
        });
        return out.toByteArray();
    }

    /** A fresh machine restored from the snapshot of this one (chunked the same way the cluster does). */
    Harness restoreCopy()
    {
        final List<byte[]> chunks = new ArrayList<>();
        m.takeSnapshot((buffer, offset, length) ->
        {
            final byte[] chunk = new byte[length];
            buffer.getBytes(offset, chunk);
            chunks.add(chunk);
        });
        final MatcherShard fresh = new MatcherShard(m.shardId(), new MatcherConfig(32, 8, 128, 16));
        final Harness h = new Harness(fresh);
        for (final byte[] c : chunks)
        {
            fresh.onSnapshotChunk(new UnsafeBuffer(c), 0, c.length);
        }
        fresh.onSnapshotLoaded();
        h.logPosition = logPosition;
        h.now = now;
        h.timersById.putAll(timersById);
        h.shadowChain.set(m.chainHash(), 0);
        h.seedEvents(events.isEmpty() ? null : events.get(events.size() - 1));
        return h;
    }

    private void seedEvents(final Ev last)
    {
        if (last != null)
        {
            events.add(last);
            entryStart = 1;
        }
    }

    /** Texts of a list of events. */
    static List<String> texts(final List<Ev> evs)
    {
        final List<String> out = new ArrayList<>();
        for (final Ev e : evs)
        {
            out.add(e.text());
        }
        return out;
    }

    static byte[] concat(final List<Ev> evs, final int from)
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = from; i < evs.size(); i++)
        {
            out.writeBytes(evs.get(i).bytes);
        }
        return out.toByteArray();
    }

    static boolean sameBytes(final List<Ev> a, final List<Ev> b)
    {
        if (a.size() != b.size())
        {
            return false;
        }
        for (int i = 0; i < a.size(); i++)
        {
            if (!Arrays.equals(a.get(i).bytes, b.get(i).bytes))
            {
                return false;
            }
        }
        return true;
    }
}
