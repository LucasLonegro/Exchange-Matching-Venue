package io.lmb.testkit;

import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.api.ShardStateMachine;
import io.lmb.api.TimerScheduler;
import io.lmb.api.VenueClients;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * A whole venue in memory, with no Aeron: each shard is a {@link ShardStateMachine} driven directly.
 * Commands offered through {@link #commandClient} are applied synchronously and their events become
 * visible to {@link #eventStream} readers. Time only moves when the test moves it.
 *
 * <p>Thread-safe (one lock per shard), so services under test may poll from their own threads.
 */
public final class InMemoryVenue implements VenueClients
{
    private final Shard[] shards;
    private volatile long nowNs;

    public InMemoryVenue(final long startTimeNs, final ShardStateMachine... machines)
    {
        this.nowNs = startTimeNs;
        this.shards = new Shard[machines.length];
        for (int i = 0; i < machines.length; i++)
        {
            shards[i] = new Shard(i, machines[i]);
        }
    }

    public long nowNs()
    {
        return nowNs;
    }

    /** Moves time forward and fires every timer that became due, in deadline order, on every shard. */
    public void advanceTimeTo(final long timeNs)
    {
        if (timeNs < nowNs)
        {
            throw new IllegalArgumentException("time cannot go backwards");
        }
        for (final Shard s : shards)
        {
            s.fireTimersUpTo(timeNs);
        }
        nowNs = timeNs;
    }

    public void advanceTimeBy(final long deltaNs)
    {
        advanceTimeTo(nowNs + deltaNs);
    }

    /** While unavailable, offers to the shard return false (simulates election or back pressure). */
    public void setAvailable(final int shardId, final boolean available)
    {
        shards[shardId].available = available;
    }

    /** The next n accepted offers are silently dropped (simulates loss of uncommitted entries on leader failure). */
    public void dropNextOffers(final int shardId, final int n)
    {
        shards[shardId].dropCount = n;
    }

    /** Number of events emitted so far by the shard. */
    public long eventCount(final int shardId)
    {
        return shards[shardId].eventCount();
    }

    /** Copy of event number eventSeq (1-based) of the shard. */
    public byte[] event(final int shardId, final long eventSeq)
    {
        return shards[shardId].event(eventSeq);
    }

    @Override
    public int shardCount()
    {
        return shards.length;
    }

    @Override
    public CommandClient commandClient(final int shardId)
    {
        final Shard shard = shards[shardId];
        return new CommandClient()
        {
            public int shardId()
            {
                return shardId;
            }

            public boolean offer(final DirectBuffer buffer, final int offset, final int length)
            {
                return shard.offer(buffer, offset, length);
            }

            public int poll()
            {
                return 0;
            }

            public boolean isConnected()
            {
                return shard.available;
            }

            public void close()
            {
            }
        };
    }

    @Override
    public EventStreamClient eventStream(final int shardId, final long fromEventSeq)
    {
        final Shard shard = shards[shardId];
        final long start = fromEventSeq == LATEST ? shard.eventCount() : fromEventSeq;
        return new EventStreamClient()
        {
            private long last = start;

            public int shardId()
            {
                return shardId;
            }

            public int poll(final EventHandler handler, final int limit)
            {
                int n = 0;
                while (n < limit)
                {
                    final byte[] e = shard.eventOrNull(last + 1);
                    if (e == null)
                    {
                        break;
                    }
                    last++;
                    handler.onEvent(new UnsafeBuffer(e), 0, e.length);
                    n++;
                }
                return n;
            }

            public long lastEventSeq()
            {
                return last;
            }

            public boolean isConnected()
            {
                return true;
            }

            public void close()
            {
            }
        };
    }

    @Override
    public void close()
    {
    }

    private final class Shard implements TimerScheduler
    {
        private final ShardStateMachine machine;
        private final List<byte[]> events = new ArrayList<>();
        private final TreeMap<Long, List<Long>> timersByDeadline = new TreeMap<>();
        private long logPosition;
        private volatile boolean available = true;
        private volatile int dropCount;

        Shard(final int id, final ShardStateMachine machine)
        {
            this.machine = machine;
            if (machine.shardId() != id)
            {
                throw new IllegalArgumentException("machine " + id + " reports shardId " + machine.shardId());
            }
            machine.bind(this::append, this);
        }

        private void append(final DirectBuffer buffer, final int offset, final int length)
        {
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            events.add(copy);
        }

        synchronized boolean offer(final DirectBuffer buffer, final int offset, final int length)
        {
            if (!available)
            {
                return false;
            }
            if (dropCount > 0)
            {
                dropCount--;
                return true;
            }
            logPosition += ((length + 32 + 31) & ~31);
            machine.onCommand(buffer, offset, length, logPosition, nowNs);
            return true;
        }

        synchronized void fireTimersUpTo(final long timeNs)
        {
            while (!timersByDeadline.isEmpty() && timersByDeadline.firstKey() <= timeNs)
            {
                final var entry = timersByDeadline.pollFirstEntry();
                for (final long correlationId : entry.getValue())
                {
                    logPosition += 64;
                    machine.onTimer(correlationId, logPosition, Math.max(entry.getKey(), nowNs));
                }
            }
        }

        synchronized long eventCount()
        {
            return events.size();
        }

        synchronized byte[] event(final long eventSeq)
        {
            return events.get((int)(eventSeq - 1)).clone();
        }

        synchronized byte[] eventOrNull(final long eventSeq)
        {
            return eventSeq <= events.size() ? events.get((int)(eventSeq - 1)) : null;
        }

        @Override
        public synchronized void schedule(final long correlationId, final long deadlineNs)
        {
            cancel(correlationId);
            timersByDeadline.computeIfAbsent(deadlineNs, k -> new ArrayList<>()).add(correlationId);
        }

        @Override
        public synchronized void cancel(final long correlationId)
        {
            timersByDeadline.values().forEach(list -> list.remove((Long)correlationId));
            timersByDeadline.values().removeIf(List::isEmpty);
        }
    }
}
