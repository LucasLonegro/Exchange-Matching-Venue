package io.lmb.gateway;

import io.lmb.api.VenueClients;
import io.lmb.common.Config;
import io.lmb.common.SymbolDirectorySource;
import io.lmb.common.Topology;
import io.lmb.common.access.AccessDirectorySource;
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
        final SymbolDirectorySource symbols = SymbolDirectorySource.fromTopology(topology,
            cfg.dataDir.resolve("symbol-directory.json"), "gateway-" + instanceId).start();
        if (cfg.accessUrl == null || cfg.accessUrl.isBlank())
        {
            throw new IllegalStateException("lmb.gateway.access.url is not set: credentials come only from the access service");
        }
        final AccessDirectorySource access = AccessDirectorySource.polling(cfg.accessUrl, cfg.accessToken, cfg.accessPollMs,
            cfg.dataDir.resolve("access-directory.json"), "gateway-" + instanceId).start();
        LOG.info("gateway {} credentials from the access service {}", instanceId, cfg.accessUrl);

        final VenueClients venue = VenueClients.connect(topology, "gateway-" + instanceId);
        final Gateway gateway;
        try
        {
            gateway = new Gateway(cfg, venue, symbols, access).start();
        }
        catch (final Exception e)
        {
            venue.close();
            access.close();
            symbols.close();
            throw e;
        }

        final CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("gateway {} shutting down", instanceId);
            gateway.close();
            venue.close();
            access.close();
            symbols.close();
            stopped.countDown();
        }, "gateway-shutdown"));

        while (!gateway.awaitReady(5_000))
        {
            LOG.info("gateway {} waiting for {}", instanceId, access.current() == null ?
                "a first access directory (service unreachable or refusing the token, and no cached copy)" :
                symbols.current() == null ?
                "a first symbol directory (control plane unreachable or refusing lmb.symbols.token, and no cached copy)" :
                "the event streams to catch up");
        }
        System.out.println("LMB_READY gateway " + instanceId);
        System.out.flush();
        stopped.await();
    }
}
