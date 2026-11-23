package io.lmb.feeds;

import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

/** Test helpers: packet builder, recording listener, manual clock. */
final class TestFeeds
{
    private TestFeeds()
    {
    }

    /** A quote for a packet: symbol, bid, ask (sizes 100). */
    record Q(int symbolId, long bid, long ask)
    {
    }

    static Q q(final int symbolId, final long bid, final long ask)
    {
        return new Q(symbolId, bid, ask);
    }

    /** One packet; the venue timestamp of message i is sendTs + i. */
    static UnsafeBuffer packet(
        final int line, final int venue, final long session, final long firstSeq, final long sendTs, final Q... quotes)
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[SimFeedProtocol.packetLength(quotes.length)]);
        SimFeedProtocol.writeHeader(b, 0, line, venue, quotes.length, session, firstSeq, sendTs);
        for (int i = 0; i < quotes.length; i++)
        {
            SimFeedProtocol.writeQuote(b, 0, i, quotes[i].symbolId, sendTs + i, quotes[i].bid, 100, quotes[i].ask, 100);
        }
        return b;
    }

    static final class ManualClock implements EpochNanoClock
    {
        long now;

        ManualClock(final long now)
        {
            this.now = now;
        }

        @Override
        public long nanoTime()
        {
            return now;
        }
    }

    /** Records callbacks as strings: "Q v s seq bid ask", "HB v ts", "ST v STATUS missed". */
    static final class Recorder implements VenueFeedHandler.Listener
    {
        final List<String> events = new ArrayList<>();
        final List<Long> quoteSeqs = new ArrayList<>();

        @Override
        public void onQuote(final NormalizedQuote q)
        {
            events.add("Q " + q.venueId() + " " + q.symbolId() + " " + q.sequence() + " " + q.bidPrice() + " " + q.askPrice());
            quoteSeqs.add(q.sequence());
        }

        @Override
        public void onHeartbeat(final int venueId, final long venueTsNs, final long receiveTsNs)
        {
            events.add("HB " + venueId + " " + venueTsNs);
        }

        @Override
        public void onVenueStatus(final int venueId, final VenueStatus status, final long missedMessages, final long tsNs)
        {
            events.add("ST " + venueId + " " + status + " " + missedMessages);
        }

        List<String> statuses()
        {
            return events.stream().filter(e -> e.startsWith("ST")).toList();
        }

        void clear()
        {
            events.clear();
            quoteSeqs.clear();
        }
    }
}
