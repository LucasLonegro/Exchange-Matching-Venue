package io.lmb.marketdata;

import io.lmb.api.VenueClients;
import io.lmb.common.Config;
import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Market data publisher process. Instance from {@code lmb.instance.id} (0 = feed A, 1 = feed B), everything
 * else from the topology (see {@link MdConfig}). Prints {@code LMB_READY marketdata <instance>} once serving;
 * stops cleanly on SIGTERM (final checkpoint); exits with status 1 if the publisher thread fails.
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
        final MarketDataPublisher publisher = new MarketDataPublisher(config, venue);
        final Thread hook = new Thread(() ->
        {
            LOG.info("marketdata {} shutting down", instance);
            publisher.close();
            venue.close();
        }, "md-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        publisher.start();
        LOG.info("marketdata {}: retransmit {}, gRPC {}, metrics {}, incremental {}, snapshot {}", instance,
            publisher.retransmitPort(), publisher.grpcPort(), config.metricsPort, config.incrementalDestinations,
            config.snapshotDestinations);
        System.out.println("LMB_READY marketdata " + instance);
        System.out.flush();
        publisher.awaitStopped(Long.MAX_VALUE, TimeUnit.DAYS);
        if (publisher.failure() != null)
        {
            Runtime.getRuntime().removeShutdownHook(hook);
            publisher.close();
            venue.close();
            System.exit(1);
        }
    }
}
