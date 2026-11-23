package io.lmb.feeds;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

/**
 * One duty thread: runs {@code work} until closed or until it throws, then runs {@code onExit} on the
 * same thread, so resources that are single-threaded (sockets, command clients) are opened, used and
 * closed by one thread. Idles with a backoff that parks at most 1 ms (the local machine is small).
 */
final class DutyLoop implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(DutyLoop.class);

    private final Thread thread;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile boolean running = true;
    private volatile Throwable failure;

    DutyLoop(final String name, final IntSupplier work, final Runnable onExit)
    {
        final IdleStrategy idle = new BackoffIdleStrategy(100, 10, 1_000, 1_000_000);
        thread = new Thread(() ->
        {
            try
            {
                while (running)
                {
                    idle.idle(work.getAsInt());
                }
            }
            catch (final Throwable t)
            {
                failure = t;
                LOG.error("{} failed", name, t);
            }
            finally
            {
                try
                {
                    onExit.run();
                }
                catch (final Throwable t)
                {
                    LOG.warn("{} cleanup failed", name, t);
                }
                stopped.countDown();
            }
        }, name);
    }

    void start()
    {
        thread.start();
    }

    Throwable failure()
    {
        return failure;
    }

    boolean isStopped()
    {
        return stopped.getCount() == 0;
    }

    boolean awaitStopped(final long timeout, final TimeUnit unit) throws InterruptedException
    {
        return stopped.await(timeout, unit);
    }

    /** Stops the loop and waits for its cleanup. Idempotent. */
    @Override
    public void close()
    {
        running = false;
        if (Thread.currentThread() != thread && thread.isAlive())
        {
            try
            {
                stopped.await(10, TimeUnit.SECONDS);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
    }
}
