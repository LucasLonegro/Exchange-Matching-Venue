package io.lmb.posttrade;

import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.common.Events;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Runs one {@link PostTradeSink} on its own thread with its own event stream reader per shard.
 * Restart dedupe: events at or below the sink's cursor (redelivery after reconnect, index granularity,
 * restart) are dropped. A gap (eventSeq > last + 1) fails the attempt. Any failure closes the sink
 * without a checkpoint and re-opens it from its durable cursor after {@code restartBackoffMs}.
 */
public final class SinkRunner implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(SinkRunner.class);

    private final PostTradeSink sink;
    private final VenueClients venue;
    private final int shards;
    private final int pollLimit;
    private final long restartBackoffMs;
    private final Thread thread;
    private final AtomicLongArray lastSeq;
    private final boolean[] caughtUpShard;
    private final IdleStrategy idle = new BackoffIdleStrategy(1, 10, TimeUnit.MICROSECONDS.toNanos(10),
        TimeUnit.MILLISECONDS.toNanos(1));

    public final AtomicLong events = new AtomicLong();
    public final AtomicLong duplicates = new AtomicLong();
    public final AtomicLong restarts = new AtomicLong();

    private volatile boolean running = true;
    private volatile boolean up;
    private volatile boolean caughtUp;
    private volatile Throwable lastFailure;
    private EventStreamClient[] streams;
    private int currentShard;

    private final EventStreamClient.EventHandler handler = this::onEvent;

    public SinkRunner(final PostTradeSink sink, final VenueClients venue, final int pollLimit, final long restartBackoffMs)
    {
        this.sink = sink;
        this.venue = venue;
        this.shards = venue.shardCount();
        this.pollLimit = pollLimit;
        this.restartBackoffMs = restartBackoffMs;
        this.lastSeq = new AtomicLongArray(shards);
        this.caughtUpShard = new boolean[shards];
        this.thread = new Thread(this::run, "posttrade-" + sink.name());
    }

    public SinkRunner start()
    {
        thread.start();
        return this;
    }

    public String name()
    {
        return sink.name();
    }

    public PostTradeSink sink()
    {
        return sink;
    }

    /** True once every shard's stream was connected and returned a short poll (stays true across restarts). */
    public boolean isCaughtUp()
    {
        return caughtUp;
    }

    /** True while the sink is open (false during a restart backoff). */
    public boolean isUp()
    {
        return up;
    }

    public long lastEventSeq(final int shard)
    {
        return lastSeq.get(shard);
    }

    public int shardCount()
    {
        return shards;
    }

    public Throwable lastFailure()
    {
        return lastFailure;
    }

    private void run()
    {
        while (running)
        {
            boolean clean = false;
            try
            {
                openAll();
                up = true;
                while (running)
                {
                    int work = 0;
                    for (int s = 0; s < shards; s++)
                    {
                        currentShard = s;
                        final int n = streams[s].poll(handler, pollLimit);
                        work += n;
                        if (!caughtUpShard[s] && n < pollLimit && streams[s].isConnected())
                        {
                            caughtUpShard[s] = true;
                            LOG.info("{}: shard {} caught up at eventSeq {}", sink.name(), s, lastSeq.get(s));
                            updateCaughtUp();
                        }
                    }
                    work += sink.doWork(System.currentTimeMillis());
                    idle.idle(work);
                }
                clean = true;
            }
            catch (final Throwable t)
            {
                lastFailure = t;
                restarts.incrementAndGet();
                LOG.error("{}: sink failed, restarting from its durable cursor in {} ms", sink.name(), restartBackoffMs, t);
            }
            finally
            {
                up = false;
                closeAll(clean);
            }
            if (!clean)
            {
                sleepBackoff();
            }
        }
    }

    private void openAll() throws Exception
    {
        final long[] cursors = sink.open(shards);
        streams = new EventStreamClient[shards];
        for (int s = 0; s < shards; s++)
        {
            lastSeq.set(s, cursors[s]);
            streams[s] = venue.eventStream(s, cursors[s]);
        }
        LOG.info("{}: opened after eventSeq {}", sink.name(), java.util.Arrays.toString(cursors));
    }

    private void closeAll(final boolean clean)
    {
        if (streams != null)
        {
            for (final EventStreamClient c : streams)
            {
                if (c != null)
                {
                    try
                    {
                        c.close();
                    }
                    catch (final RuntimeException e)
                    {
                        LOG.warn("{}: closing an event stream failed", sink.name(), e);
                    }
                }
            }
            streams = null;
        }
        try
        {
            sink.close(clean);
        }
        catch (final RuntimeException e)
        {
            LOG.warn("{}: close failed", sink.name(), e);
        }
    }

    private void onEvent(final DirectBuffer buffer, final int offset, final int length)
    {
        final int s = currentShard;
        final long seq = Events.eventSeq(buffer, offset);
        final long last = lastSeq.get(s);
        if (seq <= last)
        {
            duplicates.incrementAndGet();
            return;
        }
        if (seq != last + 1)
        {
            throw new IllegalStateException(sink.name() + ": event gap on shard " + s + ": expected eventSeq " +
                (last + 1) + ", got " + seq);
        }
        try
        {
            sink.onEvent(s, seq, buffer, offset, length);
        }
        catch (final RuntimeException e)
        {
            throw e;
        }
        catch (final Exception e)
        {
            throw new SinkException(e);
        }
        lastSeq.set(s, seq);
        events.incrementAndGet();
    }

    private void updateCaughtUp()
    {
        for (final boolean b : caughtUpShard)
        {
            if (!b)
            {
                return;
            }
        }
        caughtUp = true;
    }

    private void sleepBackoff()
    {
        final long until = System.currentTimeMillis() + restartBackoffMs;
        while (running && System.currentTimeMillis() < until)
        {
            try
            {
                Thread.sleep(Math.min(50, restartBackoffMs));
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Stops the thread; the sink is closed cleanly (final checkpoint). */
    @Override
    public void close()
    {
        running = false;
        try
        {
            thread.join(30_000);
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive())
        {
            LOG.warn("{}: runner thread did not stop within 30 s", sink.name());
        }
    }

    /** Wraps a checked exception thrown by a sink. */
    static final class SinkException extends RuntimeException
    {
        SinkException(final Throwable cause)
        {
            super(cause);
        }
    }
}
