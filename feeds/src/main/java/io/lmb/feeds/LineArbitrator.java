package io.lmb.feeds;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.atomic.AtomicLongArray;

import static io.lmb.feeds.SimFeedProtocol.*;

/**
 * A/B line arbitration and gap detection for one venue's sequenced feed (format {@link SimFeedProtocol}).
 *
 * <ul>
 *   <li>The first copy of each sequence number, from whichever line, is delivered; later copies are dropped.</li>
 *   <li>A packet that arrives ahead of the next expected sequence is held in a small preallocated
 *       buffer, waiting for the other line to fill the hole. If the hole is still open
 *       {@code gapTimeoutNs} after the held packet arrived (or the buffer is full), the hole is
 *       declared lost: {@link VenueStatus#GAP} is reported with the number of messages missed, then
 *       the held packets are delivered in order. Copies of lost messages arriving later are dropped.</li>
 *   <li>A heartbeat (count 0) is in sequence when its firstSeq equals the next expected sequence; one
 *       that is ahead reveals loss at the tail and is held like any other early packet.</li>
 *   <li>A larger session id means the venue restarted its feed: reported as GAP (old quotes are void)
 *       and sequencing restarts. Packets from an older session are ignored.</li>
 *   <li>Silence on both lines for {@code silenceTimeoutNs} is reported as {@link VenueStatus#STALE};
 *       the next packet reports {@link VenueStatus#UP}.</li>
 * </ul>
 *
 * Single-threaded and allocation-free after construction. Counters are readable from other threads.
 */
final class LineArbitrator
{
    static final int PENDING_CAPACITY = 64;

    private final int venueId;
    private final VenueFeedHandler.Listener listener;
    private final long gapTimeoutNs;
    private final long silenceTimeoutNs;
    private final NormalizedQuote quote = new NormalizedQuote();

    // packets held while waiting for the other line to fill a hole
    private final UnsafeBuffer[] slotBuffer = new UnsafeBuffer[PENDING_CAPACITY];
    private final boolean[] slotUsed = new boolean[PENDING_CAPACITY];
    private final long[] slotFirstSeq = new long[PENDING_CAPACITY];
    private final int[] slotCount = new int[PENDING_CAPACITY];
    private final long[] slotRecvTs = new long[PENDING_CAPACITY];
    private int pendingCount;

    private long session = Long.MIN_VALUE;
    private long nextSeq = -1;
    private final long[] lineNextSeq = {-1, -1};
    private long lastReceiveNs = Long.MIN_VALUE;
    private boolean up;

    // counters (single writer, read by the metrics thread)
    final AtomicLongArray linePackets = new AtomicLongArray(2);
    final AtomicLongArray lineMissed = new AtomicLongArray(2);
    final AtomicLongArray lineLastReceiveNs = new AtomicLongArray(2);
    private volatile long gaps;
    private volatile long gapMessages;
    private volatile long sessionResets;
    private volatile long duplicates;
    private volatile long malformed;
    private volatile long messagesDelivered;
    private volatile long staleEvents;

    LineArbitrator(
        final int venueId,
        final VenueFeedHandler.Listener listener,
        final long gapTimeoutNs,
        final long silenceTimeoutNs)
    {
        this.venueId = venueId;
        this.listener = listener;
        this.gapTimeoutNs = gapTimeoutNs;
        this.silenceTimeoutNs = silenceTimeoutNs;
        for (int i = 0; i < PENDING_CAPACITY; i++)
        {
            slotBuffer[i] = new UnsafeBuffer(new byte[MAX_PACKET_LENGTH]);
        }
        lineLastReceiveNs.set(0, Long.MIN_VALUE);
        lineLastReceiveNs.set(1, Long.MIN_VALUE);
    }

    /** One datagram received on {@code line} at {@code receiveTsNs}. */
    void onPacket(final int line, final DirectBuffer buffer, final int offset, final int length, final long receiveTsNs)
    {
        if (!SimFeedProtocol.isValid(buffer, offset, length) || SimFeedProtocol.venueId(buffer, offset) != venueId)
        {
            malformed++;
            return;
        }

        linePackets.incrementAndGet(line);
        lineLastReceiveNs.set(line, receiveTsNs);

        final long packetSession = SimFeedProtocol.session(buffer, offset);
        final long firstSeq = SimFeedProtocol.firstSeq(buffer, offset);
        final int count = SimFeedProtocol.count(buffer, offset);

        if (packetSession < session)
        {
            duplicates++;
            return;
        }
        if (receiveTsNs > lastReceiveNs)
        {
            lastReceiveNs = receiveTsNs;
        }
        if (packetSession > session)
        {
            final boolean restart = session != Long.MIN_VALUE;
            session = packetSession;
            // First contact: join mid-stream. Restart: the new session numbers from 1, so anything
            // before this packet that is still missing is held for the other line like any hole.
            nextSeq = restart ? 1 : firstSeq;
            lineNextSeq[0] = -1;
            lineNextSeq[1] = -1;
            clearPending();
            if (restart)
            {
                sessionResets++;
                listener.onVenueStatus(venueId, VenueStatus.GAP, 0, receiveTsNs);
            }
        }

        if (!up)
        {
            up = true;
            listener.onVenueStatus(venueId, VenueStatus.UP, 0, receiveTsNs);
        }

        trackLine(line, firstSeq, count);
        arbitrate(buffer, offset, firstSeq, count, receiveTsNs);
    }

    /** Gap and silence timeouts. Returns the number of status events raised. */
    int checkTimeouts(final long nowNs)
    {
        int work = 0;
        // several holes can expire together (packets behind a second hole arrived as long ago)
        while (pendingCount > 0 && nowNs - oldestPendingReceiveNs() >= gapTimeoutNs)
        {
            declareGap(nowNs);
            work++;
        }
        if (up && nowNs - lastReceiveNs > silenceTimeoutNs)
        {
            up = false;
            staleEvents++;
            listener.onVenueStatus(venueId, VenueStatus.STALE, 0, nowNs);
            work++;
        }
        return work;
    }

    private void trackLine(final int line, final long firstSeq, final int count)
    {
        final long expected = lineNextSeq[line];
        if (expected >= 0 && firstSeq > expected)
        {
            lineMissed.addAndGet(line, firstSeq - expected);
        }
        final long end = firstSeq + count;
        if (end > expected)
        {
            lineNextSeq[line] = end;
        }
    }

    private void arbitrate(
        final DirectBuffer buffer, final int offset, final long firstSeq, final int count, final long receiveTsNs)
    {
        if (firstSeq <= nextSeq)
        {
            deliver(buffer, offset, firstSeq, count, receiveTsNs);
            drainPending();
            return;
        }

        for (int i = 0; i < PENDING_CAPACITY; i++)
        {
            if (slotUsed[i] && slotFirstSeq[i] == firstSeq && slotCount[i] == count)
            {
                duplicates++; // the other line's copy of a packet already held
                return;
            }
        }
        if (pendingCount == PENDING_CAPACITY)
        {
            declareGap(receiveTsNs);
            if (firstSeq <= nextSeq)
            {
                deliver(buffer, offset, firstSeq, count, receiveTsNs);
                drainPending();
                return;
            }
        }
        for (int i = 0; i < PENDING_CAPACITY; i++)
        {
            if (!slotUsed[i])
            {
                final int length = SimFeedProtocol.packetLength(count);
                slotBuffer[i].putBytes(0, buffer, offset, length);
                slotUsed[i] = true;
                slotFirstSeq[i] = firstSeq;
                slotCount[i] = count;
                slotRecvTs[i] = receiveTsNs;
                pendingCount++;
                return;
            }
        }
    }

    /** Precondition: firstSeq <= nextSeq. Delivers the part of the packet at or after nextSeq. */
    private void deliver(
        final DirectBuffer buffer, final int offset, final long firstSeq, final int count, final long receiveTsNs)
    {
        final long end = firstSeq + count;
        if (count == 0)
        {
            if (firstSeq == nextSeq)
            {
                listener.onHeartbeat(venueId, SimFeedProtocol.sendTsNs(buffer, offset), receiveTsNs);
            }
            else
            {
                duplicates++;
            }
            return;
        }
        if (end <= nextSeq)
        {
            duplicates++;
            return;
        }

        for (int i = (int)(nextSeq - firstSeq); i < count; i++)
        {
            final int m = SimFeedProtocol.messageOffset(offset, i);
            if (SimFeedProtocol.msgType(buffer, m) != MSG_QUOTE)
            {
                continue;
            }
            quote.set(
                venueId,
                SimFeedProtocol.msgSymbolId(buffer, m),
                SimFeedProtocol.msgBidPrice(buffer, m),
                SimFeedProtocol.msgBidSize(buffer, m),
                SimFeedProtocol.msgAskPrice(buffer, m),
                SimFeedProtocol.msgAskSize(buffer, m),
                SimFeedProtocol.msgVenueTsNs(buffer, m),
                receiveTsNs,
                firstSeq + i);
            listener.onQuote(quote);
            messagesDelivered++;
        }
        nextSeq = end;
        listener.onHeartbeat(venueId, SimFeedProtocol.sendTsNs(buffer, offset), receiveTsNs);
    }

    private void drainPending()
    {
        boolean progress = true;
        while (progress && pendingCount > 0)
        {
            progress = false;
            for (int i = 0; i < PENDING_CAPACITY; i++)
            {
                if (slotUsed[i] && slotFirstSeq[i] <= nextSeq)
                {
                    slotUsed[i] = false;
                    pendingCount--;
                    // slots are only written by arbitrate(), never while draining
                    deliver(slotBuffer[i], 0, slotFirstSeq[i], slotCount[i], slotRecvTs[i]);
                    progress = true;
                }
            }
        }
    }

    private void declareGap(final long nowNs)
    {
        long minFirst = Long.MAX_VALUE;
        for (int i = 0; i < PENDING_CAPACITY; i++)
        {
            if (slotUsed[i] && slotFirstSeq[i] < minFirst)
            {
                minFirst = slotFirstSeq[i];
            }
        }
        if (minFirst == Long.MAX_VALUE || minFirst <= nextSeq)
        {
            drainPending();
            return;
        }
        final long missed = minFirst - nextSeq;
        gaps++;
        gapMessages += missed;
        nextSeq = minFirst;
        listener.onVenueStatus(venueId, VenueStatus.GAP, missed, nowNs);
        drainPending();
    }

    private long oldestPendingReceiveNs()
    {
        long oldest = Long.MAX_VALUE;
        for (int i = 0; i < PENDING_CAPACITY; i++)
        {
            if (slotUsed[i] && slotRecvTs[i] < oldest)
            {
                oldest = slotRecvTs[i];
            }
        }
        return oldest;
    }

    private void clearPending()
    {
        for (int i = 0; i < PENDING_CAPACITY; i++)
        {
            slotUsed[i] = false;
        }
        pendingCount = 0;
    }

    int venueId()
    {
        return venueId;
    }

    long nextExpectedSeq()
    {
        return nextSeq;
    }

    int pendingCount()
    {
        return pendingCount;
    }

    boolean isUp()
    {
        return up;
    }

    long gaps()
    {
        return gaps;
    }

    long gapMessages()
    {
        return gapMessages;
    }

    long sessionResets()
    {
        return sessionResets;
    }

    long duplicates()
    {
        return duplicates;
    }

    long malformed()
    {
        return malformed;
    }

    long messagesDelivered()
    {
        return messagesDelivered;
    }

    long staleEvents()
    {
        return staleEvents;
    }

    long lineMissed(final int line)
    {
        return lineMissed.get(line);
    }

    long linePackets(final int line)
    {
        return linePackets.get(line);
    }
}
