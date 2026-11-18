package io.lmb.cluster;

import java.util.Arrays;

/**
 * This member's view of its materialised event stream: an ordered list of recording segments, each a
 * dense run of eventSeqs held in one Archive recording, with a sparse eventSeq -> position index.
 *
 * <p>Written by the clustered service thread (single writer), read by the event stream server thread.
 * Publication is via volatile fields; arrays are never mutated after being published except for slots
 * beyond the published count.
 */
public final class EventIndex
{
    /** A dense run of events [firstSeq, lastSeq] stored in one recording. */
    public static final class Segment
    {
        final long recordingId;
        final long firstSeq;
        private volatile long lastSeq;
        private volatile boolean live;
        /** pairs (seq, position), ascending. */
        private volatile long[] entries = new long[64];
        private volatile int count;

        Segment(final long recordingId, final long firstSeq, final long startPosition, final boolean live)
        {
            this.recordingId = recordingId;
            this.firstSeq = firstSeq;
            this.lastSeq = firstSeq - 1;
            this.live = live;
            add(firstSeq, startPosition);
        }

        public long recordingId()
        {
            return recordingId;
        }

        public long firstSeq()
        {
            return firstSeq;
        }

        public long lastSeq()
        {
            return lastSeq;
        }

        public boolean isLive()
        {
            return live;
        }

        void lastSeq(final long seq)
        {
            lastSeq = seq;
        }

        void stop()
        {
            live = false;
        }

        /** Single writer. Entries must be added with increasing seq. */
        void add(final long seq, final long position)
        {
            final int n = count;
            long[] e = entries;
            if (2 * n + 2 > e.length)
            {
                e = Arrays.copyOf(e, e.length * 2);
                e[2 * n] = seq;
                e[2 * n + 1] = position;
                entries = e;
            }
            else
            {
                e[2 * n] = seq;
                e[2 * n + 1] = position;
            }
            count = n + 1;
        }

        int count()
        {
            return count;
        }

        long seqAt(final int i)
        {
            return entries[2 * i];
        }

        long positionAt(final int i)
        {
            return entries[2 * i + 1];
        }

        /**
         * Start position of the latest indexed event with seq <= target whose position is at most
         * maxPosition (positions beyond what the Archive has recorded cannot be replayed yet).
         */
        long floorPosition(final long target, final long maxPosition)
        {
            final int n = count;
            final long[] e = entries;
            int lo = 0;
            int hi = n - 1;
            int found = 0;
            while (lo <= hi)
            {
                final int mid = (lo + hi) >>> 1;
                if (e[2 * mid] <= target)
                {
                    found = mid;
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }
            while (found > 0 && e[2 * found + 1] > maxPosition)
            {
                found--;
            }
            return e[2 * found + 1];
        }

        @Override
        public String toString()
        {
            return "Segment{recordingId=" + recordingId + ", seq=" + firstSeq + ".." + lastSeq + ", live=" + live + "}";
        }
    }

    private volatile Segment[] segments = new Segment[0];
    private volatile boolean ready;
    private volatile int leaderMemberId = -1;
    private volatile boolean leader;

    public Segment[] segments()
    {
        return segments;
    }

    void addSegment(final Segment s)
    {
        final Segment[] old = segments;
        final Segment[] next = Arrays.copyOf(old, old.length + 1);
        next[old.length] = s;
        segments = next;
    }

    void ready(final boolean ready)
    {
        this.ready = ready;
    }

    public boolean isReady()
    {
        return ready;
    }

    /** First eventSeq this member can replay (1 for a member holding the full stream), 0 if empty. */
    public long firstSeq()
    {
        final Segment[] s = segments;
        for (final Segment seg : s)
        {
            if (seg.lastSeq() >= seg.firstSeq() || seg.isLive())
            {
                return seg.firstSeq();
            }
        }
        return 0;
    }

    /** Last eventSeq appended to this member's stream, 0 if none. */
    public long lastSeq()
    {
        final Segment[] s = segments;
        long last = 0;
        for (final Segment seg : s)
        {
            last = Math.max(last, seg.lastSeq());
        }
        return last;
    }

    /** The segment holding seq, or null. */
    public Segment segmentFor(final long seq)
    {
        final Segment[] s = segments;
        for (int i = s.length - 1; i >= 0; i--)
        {
            final Segment seg = s[i];
            if (seq >= seg.firstSeq() && seq <= seg.lastSeq())
            {
                return seg;
            }
        }
        return null;
    }

    public Segment liveSegment()
    {
        final Segment[] s = segments;
        return s.length > 0 && s[s.length - 1].isLive() ? s[s.length - 1] : null;
    }

    void role(final boolean isLeader, final int leaderMemberId)
    {
        this.leader = isLeader;
        this.leaderMemberId = leaderMemberId;
    }

    void leaderMemberId(final int id)
    {
        this.leaderMemberId = id;
    }

    void isLeader(final boolean isLeader)
    {
        this.leader = isLeader;
    }

    public int leaderMemberId()
    {
        return leaderMemberId;
    }

    public boolean isLeader()
    {
        return leader;
    }
}
