package io.lmb.replayer;

import io.lmb.api.EventStreamClient;
import io.lmb.api.ShardStateMachineFactory;
import io.lmb.api.VenueClients;
import io.lmb.cluster.RaftLogReader;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import org.agrona.CloseHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Continuous verification of one shard: follows the live event stream ({@link VenueClients#eventStream}) and,
 * as each entry's {@code CommandApplied} arrives (proof that the entry is committed), reads the Raft log up to
 * that position from the files of a member on this host ({@link RaftLogReader}), replays it on its own machine
 * and compares every event byte for byte ({@link ReplayVerifier}). The member files are re-opened (picking the
 * member with the most recorded log) whenever the stream is ahead of what was recorded when they were opened.
 *
 * <p>On the first divergence it stops verifying, logs it, sets {@code lmb_replayer_divergence} to 1 and keeps
 * serving metrics. One thread ({@code replayer-<shard>}); the getters are safe from any thread.
 */
public final class ShadowReplayer implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(ShadowReplayer.class);
    private static final int FILL_LIMIT = 256;

    private final ReplayerConfig config;
    private final ShardStateMachineFactory factory;
    private final VenueClients venue;
    private final ReplayerMetrics metrics;
    private final CountDownLatch readyLatch = new CountDownLatch(1);
    private final CountDownLatch stoppedLatch = new CountDownLatch(1);
    private HTTPServer http;
    private Thread thread;
    private volatile boolean running;
    private volatile Throwable failure;

    // replay state, owned by the replayer thread
    private RaftLogReader log;
    private File logDir;
    private EventStreamClient stream;
    private ReplayVerifier verifier;
    private long readPosition;
    private long lastReopenNs;
    private int lastFill;
    private long quietSinceNs;

    // published for other threads
    private volatile long startPosition = -1;
    private volatile long publishedVerifiedSeq;
    private volatile long publishedVerifiedPosition;
    private volatile long publishedAppliedPosition;
    private volatile long publishedStreamSeq;
    private volatile long publishedEntries;
    private volatile Divergence publishedDivergence;

    public ShadowReplayer(final ReplayerConfig config, final ShardStateMachineFactory factory, final VenueClients venue)
    {
        this.config = config;
        this.factory = factory;
        this.venue = venue;
        this.metrics = new ReplayerMetrics(config.shardId);
    }

    /** Opens the log, restores from a snapshot if needed, subscribes to the stream, starts metrics and the thread. */
    public ShadowReplayer start()
    {
        open();
        if (config.metricsPort >= 0)
        {
            try
            {
                http = HTTPServer.builder().port(config.metricsPort).registry(metrics.registry()).buildAndStart();
            }
            catch (final IOException e)
            {
                closeReplayState();
                throw new UncheckedIOException("metrics port " + config.metricsPort, e);
            }
        }
        running = true;
        thread = new Thread(this::run, "replayer-" + config.shardId);
        thread.start();
        return this;
    }

    private void open()
    {
        log = openBestLog();
        long afterSeq = 0;
        long start = 0;
        RaftLogReader.Snapshot snapshot = null;
        if (config.fromSnapshot)
        {
            snapshot = SnapshotLoader.latestServiceSnapshot(log.snapshots(), log.recordedPosition());
        }
        else if (log.startPosition() > 0)
        {
            snapshot = OfflineVerifier.earliestServiceSnapshotFrom(log, log.startPosition());
        }
        if ((config.fromSnapshot || log.startPosition() > 0) && snapshot == null)
        {
            closeReplayState();
            throw new IllegalStateException("the log in " + logDir + " starts at " + log.startPosition() +
                " and there is no service snapshot to start from");
        }
        final DeferredSource source = new DeferredSource();
        verifier = new ReplayVerifier(factory.create(config.shardId), source);
        if (snapshot != null)
        {
            afterSeq = SnapshotLoader.load(new ArchiveFiles(new File(logDir, "archive")), snapshot, verifier.machine()).lastEventSeq();
            start = snapshot.logPosition();
            verifier.startAt(start);
        }
        stream = venue.eventStream(config.shardId, afterSeq);
        source.target = EventSource.of(stream);
        verifier.firstComparableSeq(afterSeq + 1);
        readPosition = start;
        startPosition = start;
        LOG.info("replayer shard {}: log from {} in {}{}, events after eventSeq {}", config.shardId, start, logDir,
            snapshot != null ? " (snapshot)" : "", afterSeq);
    }

    private RaftLogReader openBestLog()
    {
        RaftLogReader best = null;
        File bestDir = null;
        RuntimeException firstError = null;
        for (final File dir : config.memberDirs)
        {
            try
            {
                final RaftLogReader r = RaftLogReader.open(dir);
                if (best == null || r.recordedPosition() > best.recordedPosition())
                {
                    CloseHelper.quietClose(best);
                    best = r;
                    bestDir = dir;
                }
                else
                {
                    r.close();
                }
            }
            catch (final RuntimeException e)
            {
                if (firstError == null)
                {
                    firstError = e;
                }
            }
        }
        if (best == null)
        {
            throw new IllegalStateException("no readable Raft log in " + config.memberDirs, firstError);
        }
        logDir = bestDir;
        return best;
    }

    private void run()
    {
        try
        {
            while (running)
            {
                if (doWork() == 0)
                {
                    LockSupport.parkNanos(500_000);
                }
            }
        }
        catch (final Throwable t)
        {
            failure = t;
            LOG.error("replayer shard {} failed", config.shardId, t);
        }
        finally
        {
            closeReplayState();
            stoppedLatch.countDown();
            readyLatch.countDown();
        }
    }

    /** One duty cycle; returns the amount of work done. */
    int doWork()
    {
        if (verifier.divergence() != null)
        {
            return 0;
        }
        int work = 0;
        lastFill = verifier.pendingEvents() < config.maxPendingEvents ? verifier.fill(FILL_LIMIT) : 0;
        work += lastFill;
        final long confirmed = verifier.confirmedPosition();
        if (confirmed > readPosition)
        {
            if (confirmed > log.recordedPosition() && System.nanoTime() - lastReopenNs >= TimeUnit.MILLISECONDS.toNanos(config.reopenIntervalMs))
            {
                lastReopenNs = System.nanoTime();
                final RaftLogReader old = log;
                final File oldDir = logDir;
                log = openBestLog();
                old.close();
                metrics.logReopens.inc();
                if (!logDir.equals(oldDir))
                {
                    LOG.info("replayer shard {} now reads the log of {}", config.shardId, logDir);
                }
            }
            final long to = Math.min(confirmed, log.recordedPosition());
            if (to > readPosition)
            {
                final long reached = log.read(readPosition, to, verifier);
                if (reached > readPosition)
                {
                    readPosition = reached;
                    work++;
                }
            }
        }
        publish();
        if (readyLatch.getCount() > 0)
        {
            // caught up = connected, everything received is verified, and the stream stayed quiet for a while
            // (a freshly connected stream may deliver nothing for a moment, so one empty poll is not enough)
            final long now = System.nanoTime();
            if (lastFill > 0 || !stream.isConnected() || quietSinceNs == 0)
            {
                quietSinceNs = now;
            }
            else if (now - quietSinceNs >= TimeUnit.MILLISECONDS.toNanos(config.readyQuietMs) &&
                readPosition >= confirmed && verifier.pendingEvents() == 0)
            {
                readyLatch.countDown();
            }
        }
        return work;
    }

    private void publish()
    {
        final Divergence d = verifier.divergence();
        if (d != null && publishedDivergence == null)
        {
            LOG.error("SHADOW REPLAY DIVERGED on shard {} (log of {}): {}", config.shardId, logDir, d);
            publishedDivergence = d;
            metrics.divergence.set(1);
        }
        publishedVerifiedSeq = verifier.verifiedSeq();
        publishedVerifiedPosition = verifier.verifiedPosition();
        publishedAppliedPosition = verifier.appliedPosition();
        publishedStreamSeq = verifier.streamSeq();
        publishedEntries = verifier.entries();
        metrics.verifiedSeq.set(publishedVerifiedSeq);
        metrics.verifiedPosition.set(publishedVerifiedPosition);
        metrics.appliedPosition.set(publishedAppliedPosition);
        metrics.streamSeq.set(publishedStreamSeq);
        metrics.lagEvents.set(Math.max(0, publishedStreamSeq - publishedVerifiedSeq));
        metrics.lagBytes.set(Math.max(0, verifier.confirmedPosition() - readPosition));
        metrics.entries.set(publishedEntries);
        metrics.eventsCompared.set(verifier.eventsCompared());
    }

    /** The verifier binds the machine before the snapshot decides where the stream starts. */
    private static final class DeferredSource implements EventSource
    {
        EventSource target;

        @Override
        public int poll(final EventStreamClient.EventHandler handler, final int limit)
        {
            return target == null ? 0 : target.poll(handler, limit);
        }

        @Override
        public long lastEventSeq()
        {
            return target == null ? 0 : target.lastEventSeq();
        }

        @Override
        public void close()
        {
            if (target != null)
            {
                target.close();
            }
        }
    }

    /** True once the replayer has caught up with the live edge of the stream (or stopped); false on timeout. */
    public boolean awaitReady(final long timeoutMs) throws InterruptedException
    {
        return readyLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public boolean awaitStopped(final long timeout, final TimeUnit unit) throws InterruptedException
    {
        return stoppedLatch.await(timeout, unit);
    }

    public boolean isRunning()
    {
        return running && stoppedLatch.getCount() > 0;
    }

    public Throwable failure()
    {
        return failure;
    }

    public Divergence divergence()
    {
        return publishedDivergence;
    }

    public long verifiedSeq()
    {
        return publishedVerifiedSeq;
    }

    public long verifiedPosition()
    {
        return publishedVerifiedPosition;
    }

    public long appliedPosition()
    {
        return publishedAppliedPosition;
    }

    public long streamSeq()
    {
        return publishedStreamSeq;
    }

    public long entries()
    {
        return publishedEntries;
    }

    /** Log position replay started from (0, or a snapshot position); -1 before {@link #start}. */
    public long startPosition()
    {
        return startPosition;
    }

    /** Actual metrics port (useful with port 0), or -1 when disabled. */
    public int metricsPort()
    {
        return http != null ? http.getPort() : -1;
    }

    private void closeReplayState()
    {
        CloseHelper.quietClose(stream);
        stream = null;
        CloseHelper.quietClose(log);
        log = null;
    }

    @Override
    public void close()
    {
        running = false;
        if (thread != null)
        {
            try
            {
                thread.join(10_000);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
        else
        {
            closeReplayState();
        }
        if (http != null)
        {
            http.close();
            http = null;
        }
    }
}
