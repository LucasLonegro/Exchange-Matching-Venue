package io.lmb.cluster.client;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.common.Topology;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Production {@link VenueClients}: one media driver (embedded, or external via {@code lmb.aeron.dir})
 * and one Aeron client shared by every command and event stream client it creates.
 */
public final class ClusterVenueClients implements VenueClients
{
    private static final Logger LOG = LoggerFactory.getLogger(ClusterVenueClients.class);
    private static final AtomicInteger INSTANCE = new AtomicInteger();

    private final Topology topology;
    private final String clientName;
    private final ClientConfig config;
    private final MediaDriver driver;
    private final Aeron aeron;
    private volatile boolean closed;

    /** Registered with {@link java.util.ServiceLoader}. */
    public static final class Provider implements VenueClients.Provider
    {
        @Override
        public VenueClients connect(final Topology topology, final String clientName)
        {
            return new ClusterVenueClients(topology, clientName, ClientConfig.from(topology));
        }
    }

    public ClusterVenueClients(final Topology topology, final String clientName, final ClientConfig config)
    {
        this.topology = topology;
        this.clientName = clientName;
        this.config = config;
        final String dir;
        if (config.aeronDir == null)
        {
            final String safeName = clientName.replaceAll("[^A-Za-z0-9_.-]", "_");
            dir = new File(config.embeddedDirBase(),
                "lmb-client-" + safeName + "-" + ProcessHandle.current().pid() + "-" + INSTANCE.incrementAndGet()).getPath();
            driver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(dir)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new BackoffIdleStrategy(10, 20, TimeUnit.MICROSECONDS.toNanos(10),
                    TimeUnit.MILLISECONDS.toNanos(1)))
                .termBufferSparseFile(true)
                .publicationConnectionTimeoutNs(config.peerTimeoutNs)
                .imageLivenessTimeoutNs(config.peerTimeoutNs)
                .publicationTermBufferLength(config.termLength)
                .ipcTermBufferLength(config.termLength));
        }
        else
        {
            dir = config.aeronDir;
            driver = null;
        }
        try
        {
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir).clientName("lmb-" + clientName));
        }
        catch (final RuntimeException e)
        {
            CloseHelper.quietClose(driver);
            throw e;
        }
        LOG.info("venue clients '{}' connected: driver={} dir={}", clientName, driver != null ? "embedded" : "external", dir);
    }

    public Aeron aeron()
    {
        return aeron;
    }

    public ClientConfig config()
    {
        return config;
    }

    @Override
    public int shardCount()
    {
        return topology.shardCount();
    }

    @Override
    public CommandClient commandClient(final int shardId)
    {
        checkShard(shardId);
        return new ClusterCommandClient(aeron, topology, shardId, config, clientName);
    }

    @Override
    public EventStreamClient eventStream(final int shardId, final long fromEventSeq)
    {
        checkShard(shardId);
        if (fromEventSeq < 0 && fromEventSeq != LATEST)
        {
            throw new IllegalArgumentException("fromEventSeq must be >= 0 or LATEST: " + fromEventSeq);
        }
        return new ClusterEventStreamClient(aeron, topology, shardId, fromEventSeq, config);
    }

    private void checkShard(final int shardId)
    {
        if (closed)
        {
            throw new IllegalStateException("closed");
        }
        if (shardId < 0 || shardId >= topology.shardCount())
        {
            throw new IllegalArgumentException("no such shard: " + shardId);
        }
    }

    @Override
    public void close()
    {
        if (!closed)
        {
            closed = true;
            CloseHelper.quietClose(aeron);
            CloseHelper.quietClose(driver);
        }
    }
}
