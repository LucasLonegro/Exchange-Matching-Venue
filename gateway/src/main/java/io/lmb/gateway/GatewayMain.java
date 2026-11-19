package io.lmb.gateway;

import io.lmb.api.VenueClients;
import io.lmb.common.Config;
import io.lmb.common.FirmDirectory;
import io.lmb.common.SymbolDirectory;
import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/** Order gateway service. Identity: {@code lmb.instance.id} (also the gatewayId stamped on commands). */
public final class GatewayMain
{
    private static final Logger LOG = LoggerFactory.getLogger(GatewayMain.class);

    private GatewayMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final Topology topology = Topology.load();
        final int instanceId = Config.getInt("lmb.instance.id", 0);
        final GatewayConfig cfg = GatewayConfig.fromTopology(topology, instanceId);
        final SymbolDirectory symbols = SymbolDirectory.load(topology.symbolsFile());
        final FirmDirectory firms = FirmDirectory.load(topology.firmsFile());

        final VenueClients venue = VenueClients.connect(topology, "gateway-" + instanceId);
        final Gateway gateway;
        try
        {
            gateway = new Gateway(cfg, venue, symbols, firms).start();
        }
        catch (final Exception e)
        {
            venue.close();
            throw e;
        }

        final CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("gateway {} shutting down", instanceId);
            gateway.close();
            venue.close();
            stopped.countDown();
        }, "gateway-shutdown"));

        while (!gateway.awaitReady(5_000))
        {
            LOG.info("gateway {} waiting for the event streams to catch up", instanceId);
        }
        System.out.println("LMB_READY gateway " + instanceId);
        System.out.flush();
        stopped.await();
    }
}
