package io.lmb.marketdata;

import io.lmb.api.VenueClients;
import io.lmb.common.Config;
import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Market data publisher process. Instance from {@code lmb.instance.id} (0 = feed A, 1 = feed B), everything
 * else from the topology (see {@link MdConfig}). Prints {@code LMB_READY marketdata <instance>} once every
 * shard's event stream has caught up with its live edge; stops cleanly on SIGTERM (final checkpoint); exits
 * with status 1 if the publisher thread fails.
 */
public final class MarketDataPublisherMain
{
    private static final Logger LOG = LoggerFactory.getLogger(MarketDataPublisherMain.class);

    private MarketDataPublisherMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final int instance = Config.getInt("lmb.instance.id", 0);
        final Topology topology = Topology.load();
        final MdConfig config = MdConfig.load(topology, instance);
        final VenueClients venue = VenueClients.connect(topology, "marketdata-" + instance);
        final MarketDataPublisher publisher;
        try
        {
            publisher = new MarketDataPublisher(config, venue);
            publisher.start();
        }
        catch (final Exception e)
        {
            venue.close();
            throw e;
        }
        final Thread hook = new Thread(() ->
        {
            LOG.info("marketdata {} shutting down", instance);
            publisher.close();
            venue.close();
        }, "md-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);

        if (awaitReadyAndAnnounce(publisher, config, System.out))
        {
            publisher.awaitStopped(Long.MAX_VALUE, TimeUnit.DAYS);
        }
        if (publisher.failure() != null) // otherwise stopped by SIGTERM: the hook cleans up
        {
            Runtime.getRuntime().removeShutdownHook(hook);
            publisher.close();
            venue.close();
            System.exit(1);
        }
    }

    /**
     * Waits (logging every 5 s) until the publisher has caught up with every shard, then prints the READY line.
     * Returns false if the publisher stopped first (thread failure, or close on SIGTERM).
     */
    static boolean awaitReadyAndAnnounce(final MarketDataPublisher publisher, final MdConfig config, final PrintStream out)
        throws InterruptedException
    {
        try
        {
            while (!publisher.awaitReady(5_000))
            {
                LOG.info("marketdata {} waiting for the event streams to catch up (last eventSeq per shard: {})",
                    config.instance, publisher.engine().lastEventSeqs());
            }
        }
        catch (final IllegalStateException e)
        {
            if (publisher.failure() != null)
            {
                LOG.error("marketdata {} failed before it was ready", config.instance, publisher.failure());
            }
            else
            {
                LOG.info("marketdata {} stopped before it was ready", config.instance);
            }
            return false;
        }
        LOG.info("marketdata {}: retransmit {}, gRPC {}, metrics {}, incremental {}, snapshot {}", config.instance,
            publisher.retransmitPort(), publisher.grpcPort(), config.metricsPort, config.incrementalDestinations,
            config.snapshotDestinations);
        out.println("LMB_READY marketdata " + config.instance);
        out.flush();
        return true;
    }
}
