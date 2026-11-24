package io.lmb.replayer;

import io.lmb.api.ShardStateMachine;
import io.lmb.api.TimerScheduler;
import io.lmb.cluster.RaftLogReader;
import io.lmb.common.Events;
import io.lmb.common.HashChain;
import io.lmb.sbe.CommandAppliedDecoder;
import io.lmb.sbe.MessageHeaderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Re-executes log entries on a fresh state machine and compares every event it emits, byte for byte, with the
 * recorded event stream (which includes each entry's {@code CommandApplied.chainHash}). Stops comparing at the
 * first {@link Divergence}: past that point the replica state is no longer comparable.
 *
 * <p>Use as the {@link RaftLogReader.Handler} of {@code RaftLogReader.read}. Expected events are pulled from the
 * {@link EventSource} on demand; {@link #fill} pulls ahead (live mode) and tracks {@link #confirmedPosition()},
 * the log position of the last complete entry received from the stream, which is a committed position and so a
 * safe bound for reading the log. Timers are not scheduled: the log carries every timer that fired.
 * Single-threaded; allocation-free per entry in steady state.
 */
public final class ReplayVerifier implements RaftLogReader.Handler
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplayVerifier.class);
    static final int COMMAND_APPLIED = CommandAppliedDecoder.TEMPLATE_ID;
    static final int CHAIN_HASH_OFFSET = MessageHeaderDecoder.ENCODED_LENGTH + CommandAppliedDecoder.chainHashEncodingOffset();

    private static final TimerScheduler NO_TIMERS = new TimerScheduler()
    {
        @Override
        public void schedule(final long correlationId, final long deadlineNs)
        {
        }

        @Override
        public void cancel(final long correlationId)
        {
        }
    };

    private final ShardStateMachine machine;
    private final EventSource source;
    private final EventQueue expected = new EventQueue();
    private final ExpandableArrayBuffer produced = new ExpandableArrayBuffer(16 * 1024);
    private final IntArrayList producedOffsets = new IntArrayList();
    private final IntArrayList producedLengths = new IntArrayList();
    private final byte[] lastChainHash = new byte[HashChain.HASH_LENGTH];
    private int producedLength;
    private long firstComparableSeq = 1;

    private long entries;
    private long eventsCompared;
    private long eventsUnverified;
    private long entriesUnverified;
    private long verifiedSeq;
    private long verifiedPosition;
    private long appliedPosition;
    private long confirmedPosition;
    private long streamSeq;
    private Divergence divergence;
    private final io.lmb.api.EventStreamClient.EventHandler onExpected = this::onExpected;

    /** Binds the (fresh) machine; restore it from a snapshot through {@link #machine()} before the first entry. */
    public ReplayVerifier(final ShardStateMachine machine, final EventSource source)
    {
        this.machine = machine;
        this.source = source;
        machine.bind(this::onProduced, NO_TIMERS);
    }

    public ShardStateMachine machine()
    {
        return machine;
    }

    /** Replayed events with a smaller eventSeq are applied but not compared (the stream no longer has them). */
    public ReplayVerifier firstComparableSeq(final long seq)
    {
        this.firstComparableSeq = seq;
        return this;
    }

    /** Sets the starting point after a snapshot restore (log position of the snapshot). */
    public void startAt(final long logPosition)
    {
        appliedPosition = logPosition;
        verifiedPosition = logPosition;
        confirmedPosition = logPosition;
        verifiedSeq = machine.lastEventSeq();
    }

    /** Pulls up to {@code limit} events from the source into the queue; returns how many. */
    public int fill(final int limit)
    {
        return source.poll(onExpected, limit);
    }

    @Override
    public void onCommand(
        final long logPosition, final long timestampNs, final long clusterSessionId,
        final DirectBuffer buffer, final int offset, final int length)
    {
        if (divergence != null)
        {
            return;
        }
        producedLength = 0;
        producedOffsets.clear();
        producedLengths.clear();
        machine.onCommand(buffer, offset, length, logPosition, timestampNs);
        endEntry(logPosition, timestampNs);
    }

    @Override
    public void onTimer(final long logPosition, final long timestampNs, final long correlationId)
    {
        if (divergence != null)
        {
            return;
        }
        producedLength = 0;
        producedOffsets.clear();
        producedLengths.clear();
        machine.onTimer(correlationId, logPosition, timestampNs);
        endEntry(logPosition, timestampNs);
    }

    private void onProduced(final DirectBuffer buffer, final int offset, final int length)
    {
        produced.putBytes(producedLength, buffer, offset, length);
        producedOffsets.addInt(producedLength);
        producedLengths.addInt(length);
        producedLength += length;
    }

    private void onExpected(final DirectBuffer buffer, final int offset, final int length)
    {
        expected.add(buffer, offset, length);
        streamSeq = Events.eventSeq(buffer, offset);
        if (Events.templateId(buffer, offset) == COMMAND_APPLIED)
        {
            confirmedPosition = Math.max(confirmedPosition, Events.logPosition(buffer, offset));
        }
    }

    private void endEntry(final long logPosition, final long timestampNs)
    {
        final long entry = entries++;
        appliedPosition = logPosition;
        boolean unverified = false;
        final int n = producedOffsets.size();
        for (int i = 0; i < n; i++)
        {
            final int off = producedOffsets.getInt(i);
            final int len = producedLengths.getInt(i);
            final int template = Events.templateId(produced, off);
            if (template == COMMAND_APPLIED)
            {
                produced.getBytes(off + CHAIN_HASH_OFFSET, lastChainHash);
            }
            final long seq = Events.eventSeq(produced, off);
            if (seq < firstComparableSeq)
            {
                eventsUnverified++;
                unverified = true;
                continue;
            }
            if (expected.isEmpty() && fill(1) == 0)
            {
                eventsUnverified++;
                unverified = true;
                continue;
            }
            final int eOff = expected.headOffset();
            final int eLen = expected.headLength();
            final DirectBuffer e = expected.buffer();
            if (!equal(e, eOff, eLen, produced, off, len))
            {
                divergence = describe(entry, logPosition, timestampNs, e, eOff, eLen, off, len);
                LOG.error("replay diverged from the recorded event stream: {}", divergence);
                expected.removeHead();
                return;
            }
            expected.removeHead();
            eventsCompared++;
            verifiedSeq = seq;
        }
        if (unverified)
        {
            entriesUnverified++;
        }
        else
        {
            verifiedPosition = logPosition;
        }
    }

    private Divergence describe(
        final long entry, final long logPosition, final long timestampNs,
        final DirectBuffer e, final int eOff, final int eLen, final int off, final int len)
    {
        final int eTemplate = Events.templateId(e, eOff);
        final int aTemplate = Events.templateId(produced, off);
        final Divergence.Kind kind;
        if (eTemplate == COMMAND_APPLIED && aTemplate == COMMAND_APPLIED && eLen == len &&
            equal(e, eOff, CHAIN_HASH_OFFSET, produced, off, CHAIN_HASH_OFFSET) &&
            equal(e, eOff + CHAIN_HASH_OFFSET + HashChain.HASH_LENGTH, len - CHAIN_HASH_OFFSET - HashChain.HASH_LENGTH,
                produced, off + CHAIN_HASH_OFFSET + HashChain.HASH_LENGTH, len - CHAIN_HASH_OFFSET - HashChain.HASH_LENGTH))
        {
            kind = Divergence.Kind.CHAIN_HASH;
        }
        else if ((eTemplate == COMMAND_APPLIED) != (aTemplate == COMMAND_APPLIED))
        {
            kind = Divergence.Kind.EVENT_COUNT;
        }
        else
        {
            kind = Divergence.Kind.EVENT_BYTES;
        }
        return new Divergence(kind, entry, logPosition, timestampNs, Events.eventSeq(e, eOff), Events.eventIdx(e, eOff),
            "template " + eTemplate + " " + hex(e, eOff, eLen), "template " + aTemplate + " " + hex(produced, off, len));
    }

    /**
     * Offline end check: if the stream still has events after the whole log was replayed, the log is missing
     * entries the stream recorded. Returns the divergence (also stored), or null.
     */
    public Divergence checkStreamExhausted()
    {
        if (divergence == null && (!expected.isEmpty() || fill(1) > 0))
        {
            final DirectBuffer e = expected.buffer();
            final int off = expected.headOffset();
            divergence = new Divergence(Divergence.Kind.LOG_MISSING_ENTRIES, -1, Events.logPosition(e, off),
                Events.timestampNs(e, off), Events.eventSeq(e, off), Events.eventIdx(e, off),
                "template " + Events.templateId(e, off) + " " + hex(e, off, expected.headLength()),
                "log ends at " + appliedPosition);
            LOG.error("replay diverged from the recorded event stream: {}", divergence);
        }
        return divergence;
    }

    private static boolean equal(
        final DirectBuffer a, final int aOff, final int aLen, final DirectBuffer b, final int bOff, final int bLen)
    {
        if (aLen != bLen)
        {
            return false;
        }
        for (int i = 0; i < aLen; i++)
        {
            if (a.getByte(aOff + i) != b.getByte(bOff + i))
            {
                return false;
            }
        }
        return true;
    }

    static String hex(final DirectBuffer b, final int off, final int len)
    {
        final int n = Math.min(len, 128);
        final StringBuilder sb = new StringBuilder(n * 2 + 16);
        for (int i = 0; i < n; i++)
        {
            sb.append(Character.forDigit((b.getByte(off + i) >> 4) & 0xF, 16)).append(Character.forDigit(b.getByte(off + i) & 0xF, 16));
        }
        if (n < len)
        {
            sb.append("...(").append(len).append(" bytes)");
        }
        return sb.toString();
    }

    public Divergence divergence()
    {
        return divergence;
    }

    /** Log entries (commands and timers) applied. */
    public long entries()
    {
        return entries;
    }

    public long eventsCompared()
    {
        return eventsCompared;
    }

    /** Replayed events not compared (before {@link #firstComparableSeq} or beyond the end of the stream). */
    public long eventsUnverified()
    {
        return eventsUnverified;
    }

    public long entriesUnverified()
    {
        return entriesUnverified;
    }

    /** eventSeq of the last event that matched. */
    public long verifiedSeq()
    {
        return verifiedSeq;
    }

    /** Log position of the last entry whose events all matched. */
    public long verifiedPosition()
    {
        return verifiedPosition;
    }

    /** Log position of the last entry applied. */
    public long appliedPosition()
    {
        return appliedPosition;
    }

    /** Log position of the last CommandApplied received from the stream (committed). */
    public long confirmedPosition()
    {
        return confirmedPosition;
    }

    /** eventSeq of the last event received from the stream. */
    public long streamSeq()
    {
        return streamSeq;
    }

    /** Received but not yet compared events. */
    public int pendingEvents()
    {
        return expected.size();
    }

    /** Chain hash of the last replayed CommandApplied (the internal array: copy it). */
    public byte[] lastChainHash()
    {
        return lastChainHash;
    }
}
