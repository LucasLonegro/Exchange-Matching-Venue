package io.lmb.marketdata;

import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.marketdata.grpc.MarketDataFanout;
import io.lmb.marketdata.wire.MdWire;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The publisher thread: polls every shard's event stream into the {@link BookBuilder} and fans each
 * sequenced message out to the retransmission history, the UDP feed and the gRPC fan-out. Also paces the
 * snapshot channel, sends heartbeats, answers TCP snapshot requests and writes checkpoints. Every piece of
 * publisher state is touched only by the thread calling {@link #doWork}.
 *
 * <p>Restart: {@link #start} restores the last checkpoint (if any) and opens each shard's stream after the
 * checkpoint's eventSeq; without a checkpoint it reads each stream from the start. Either way the output
 * from then on is byte-identical to what an instance that never stopped publishes. At start-up, after the
 * first poll, while a shard's polls come back full (a backlog), its messages go to history and gRPC but
 * not to UDP; receivers fill that hole from the other feed or by retransmission.
 */
public final class MarketDataEngine implements BookBuilder.Output, AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(MarketDataEngine.class);

    private final MdConfig config;
    private final VenueClients venue;
    private final PublisherMetrics metrics;
    private final FeedPublisher feed;
    private final MarketDataFanout fanout;
    private final RetransmitHistory history;
    private final BookBuilder builder = new BookBuilder(this);
    private final SnapshotWriter snapshotWriter = new SnapshotWriter();
    private final ExpandableArrayBuffer snapshotBuffer = new ExpandableArrayBuffer(64 * 1024);
    private final ConcurrentLinkedQueue<SnapshotRequest> snapshotRequests = new ConcurrentLinkedQueue<>();
    private final ExecutorService checkpointWriter;

    private EventStreamClient[] streams;
    /** Per shard: caught up with the live edge once / currently not multicasting because of a start-up backlog. */
    private boolean[] caughtUp;
    private boolean[] suppress;
    private volatile int shardsCaughtUp;
    /** Per shard: last eventSeq consumed, published for other threads (metrics, logs). */
    private final AtomicLongArray lastSeqs;
    private int currentShard;
    private long nowNs;
    private long pollNanos;
    private long nextSnapshotCycleNs;
    private int snapshotCursor = -1;
    private long nextCheckpointNs;
    private long lastCheckpointEvents = -1;
    private volatile boolean failed;

    private final EventStreamClient.EventHandler handler = this::onEvent;

    private record SnapshotRequest(int symbolId, CompletableFuture<byte[]> result)
    {
    }

    /**
     * @param feed   null to publish no UDP feed
     * @param fanout null to serve no gRPC
     */
    public MarketDataEngine(
        final MdConfig config, final VenueClients venue, final RetransmitHistory history, final FeedPublisher feed,
        final MarketDataFanout fanout, final PublisherMetrics metrics)
    {
        this.config = config;
        this.venue = venue;
        this.history = history;
        this.feed = feed;
        this.fanout = fanout;
        this.metrics = metrics;
        this.checkpointWriter = config.dataDir == null ? null : Executors.newSingleThreadExecutor(
            r -> Thread.ofPlatform().daemon(true).name("md-checkpoint").unstarted(r));
        this.lastSeqs = new AtomicLongArray(config.shards);
        metrics.bindEngine(() -> builder.sortedSymbolIds().size(), lastSeqs::get, config.shards);
    }

    public BookBuilder builder()
    {
        return builder;
    }

    /** Restores the checkpoint, opens the streams and primes the gRPC fan-out. Call on the publisher thread. */
    public void start() throws IOException
    {
        final boolean restored = config.dataDir != null && StateCheckpoint.restore(config.dataDir, builder, config.sessionId);
        streams = new EventStreamClient[config.shards];
        caughtUp = new boolean[config.shards];
        suppress = new boolean[config.shards];
        for (int shard = 0; shard < config.shards; shard++)
        {
            final long from = builder.committedEventSeq(shard);
            streams[shard] = venue.eventStream(shard, from);
            lastSeqs.set(shard, from);
            LOG.info("shard {}: reading events after eventSeq {}{}", shard, from, restored ? " (checkpoint)" : "");
        }
        if (fanout != null)
        {
            final IntArrayList ids = builder.sortedSymbolIds();
            for (int i = 0; i < ids.size(); i++)
            {
                final int len = snapshotWriter.write(builder.symbol(ids.getInt(i)), snapshotBuffer, 0);
                fanout.offerSnapshot(snapshotBuffer, 0, len);
            }
        }
    }

    /**
     * Any thread: true once every shard's stream has connected and reached its live edge at least once
     * (stays true across later reconnects). The process prints its READY line on this.
     */
    public boolean isCaughtUp()
    {
        return streams != null && shardsCaughtUp == streams.length;
    }

    /** Any thread: last eventSeq consumed per shard, for logs. */
    public String lastEventSeqs()
    {
        return lastSeqs.toString();
    }

    /** One duty cycle; returns the amount of work done. */
    public int doWork(final long nowNs)
    {
        if (failed)
        {
            throw new IllegalStateException("engine stopped");
        }
        try
        {
            return duty(nowNs);
        }
        catch (final Throwable t)
        {
            failed = true; // state may sit mid-batch or past a gap: no further checkpoint from it
            throw t;
        }
    }

    private int duty(final long nowNs)
    {
        this.nowNs = nowNs;
        int work = 0;
        for (int shard = 0; shard < streams.length; shard++)
        {
            currentShard = shard;
            pollNanos = System.nanoTime();
            final long gapsBefore = builder.eventGaps();
            final int n = streams[shard].poll(handler, config.pollLimit);
            if (builder.eventGaps() != gapsBefore)
            {
                throw new IllegalStateException("shard " + shard + " event stream is not contiguous with this publisher's "
                    + "state; refusing to publish inconsistent md_seq (see docs/modules/marketdata.md, Restart)");
            }
            if (!caughtUp[shard])
            {
                // Start-up: the first full poll is multicast; while later polls come back full the backlog is
                // not (suppressCatchUp). Caught up = the stream is connected and a poll came back short. A
                // stream still connecting (polls return 0) is not caught up.
                if (n >= config.pollLimit)
                {
                    suppress[shard] = config.suppressCatchUp;
                }
                else if (streams[shard].isConnected())
                {
                    caughtUp[shard] = true;
                    suppress[shard] = false;
                    shardsCaughtUp = shardsCaughtUp + 1; // only this thread writes
                    LOG.info("shard {} caught up at eventSeq {}", shard, builder.lastEventSeq(shard));
                }
            }
            if (n > 0)
            {
                lastSeqs.lazySet(shard, builder.lastEventSeq(shard));
            }
            work += n;
        }
        metrics.events.set(builder.eventsProcessed());
        work += serveSnapshotRequests();
        if (feed != null)
        {
            work += snapshotChannel(nowNs);
            work += feed.heartbeats(nowNs);
        }
        work += checkpointIfDue(nowNs);
        return work;
    }

    private void onEvent(final DirectBuffer buffer, final int offset, final int length)
    {
        builder.onEvent(buffer, offset, length, pollNanos);
    }

    // ---- BookBuilder.Output ----

    @Override
    public void onMessage(final int shardId, final SymbolBook symbol, final DirectBuffer buffer, final int offset, final int length)
    {
        metrics.messages.incrementAndGet();
        RetransmitHistory.SymbolHistory h = symbol.history;
        if (h == null)
        {
            h = history.forSymbol(symbol.symbolId);
            symbol.history = h;
        }
        h.append(MdWire.seq(buffer, offset), buffer, offset, length);
        if (feed != null)
        {
            if (!suppress[currentShard])
            {
                feed.addIncremental(buffer, offset, length, nowNs);
            }
            else
            {
                metrics.suppressedMessages.incrementAndGet();
            }
        }
        if (fanout != null)
        {
            fanout.offerSequenced(buffer, offset, length);
        }
    }

    @Override
    public void onBatchEnd(final int shardId, final long firstReceiveNanos, final long eventTimestampNs)
    {
        if (feed != null && !suppress[currentShard])
        {
            feed.flushIncremental(nowNs);
            metrics.receiveToSend.record(System.nanoTime() - firstReceiveNanos);
            metrics.eventToPublish.record(System.currentTimeMillis() * 1_000_000L - eventTimestampNs);
        }
        if (fanout != null)
        {
            fanout.offerBatchEnd();
        }
    }

    // ---- snapshot channel ----

    private int snapshotChannel(final long nowNs)
    {
        if (!feed.hasSnapshotChannel())
        {
            return 0;
        }
        if (snapshotCursor < 0)
        {
            if (nowNs - nextSnapshotCycleNs < 0)
            {
                return 0;
            }
            snapshotCursor = 0;
            nextSnapshotCycleNs = nowNs + config.snapshotIntervalMs * 1_000_000L;
        }
        final IntArrayList ids = builder.sortedSymbolIds();
        int sent = 0;
        while (snapshotCursor < ids.size() && sent < config.snapshotSymbolsPerPoll)
        {
            final SymbolBook s = builder.symbol(ids.getInt(snapshotCursor++));
            final int len = snapshotWriter.write(s, snapshotBuffer, 0);
            feed.sendSnapshot(snapshotBuffer, 0, len, nowNs);
            metrics.snapshotsSent.incrementAndGet();
            sent++;
        }
        if (snapshotCursor >= ids.size())
        {
            snapshotCursor = -1;
        }
        return sent;
    }

    // ---- TCP snapshot requests ----

    /** Any thread: the symbol's current snapshot frames, or null if unknown. Completed by the publisher thread. */
    public CompletableFuture<byte[]> requestSnapshot(final int symbolId)
    {
        final CompletableFuture<byte[]> f = new CompletableFuture<>();
        if (failed)
        {
            f.completeExceptionally(new IllegalStateException("publisher stopped"));
        }
        else
        {
            snapshotRequests.add(new SnapshotRequest(symbolId, f));
        }
        return f;
    }

    private int serveSnapshotRequests()
    {
        int n = 0;
        SnapshotRequest r;
        while ((r = snapshotRequests.poll()) != null)
        {
            final SymbolBook s = builder.symbol(r.symbolId());
            if (s == null)
            {
                r.result().complete(null);
            }
            else
            {
                final int len = snapshotWriter.write(s, snapshotBuffer, 0);
                final byte[] copy = new byte[len];
                snapshotBuffer.getBytes(0, copy);
                r.result().complete(copy);
            }
            n++;
        }
        return n;
    }

    // ---- checkpoints ----

    private int checkpointIfDue(final long nowNs)
    {
        if (checkpointWriter == null || nowNs - nextCheckpointNs < 0)
        {
            return 0;
        }
        nextCheckpointNs = nowNs + config.checkpointIntervalMs * 1_000_000L;
        if (builder.eventsProcessed() == lastCheckpointEvents)
        {
            return 0;
        }
        lastCheckpointEvents = builder.eventsProcessed();
        final byte[] data = StateCheckpoint.serialize(builder, config.sessionId);
        checkpointWriter.execute(() -> writeCheckpoint(data));
        return 1;
    }

    private void writeCheckpoint(final byte[] data)
    {
        try
        {
            StateCheckpoint.write(config.dataDir, data);
        }
        catch (final IOException e)
        {
            LOG.warn("checkpoint write failed: {}", e.toString());
        }
    }

    /**
     * Writes a final checkpoint (publisher thread) and releases the streams. After a failure no checkpoint is
     * written: the last periodic one, taken from consistent state, is what a restart resumes from.
     */
    @Override
    public void close()
    {
        final boolean healthy = !failed;
        failed = true;
        SnapshotRequest r;
        while ((r = snapshotRequests.poll()) != null)
        {
            r.result().completeExceptionally(new IllegalStateException("publisher stopped"));
        }
        if (checkpointWriter != null)
        {
            if (streams != null && healthy)
            {
                final byte[] data = StateCheckpoint.serialize(builder, config.sessionId);
                checkpointWriter.execute(() -> writeCheckpoint(data));
            }
            checkpointWriter.shutdown();
            try
            {
                checkpointWriter.awaitTermination(5, TimeUnit.SECONDS);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
        if (streams != null)
        {
            for (final EventStreamClient s : streams)
            {
                s.close();
            }
        }
    }
}
