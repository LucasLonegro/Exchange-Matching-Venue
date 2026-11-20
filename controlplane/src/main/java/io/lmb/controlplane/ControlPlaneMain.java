package io.lmb.controlplane;

import io.lmb.api.VenueClients;
import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Control plane service: {@code scripts/lmb-run io.lmb.controlplane.ControlPlaneMain -Dlmb.instance.id=0}.
 * Configuration keys are listed in docs/modules/controlplane.md.
 */
public final class ControlPlaneMain
{
    private static final Logger LOG = LoggerFactory.getLogger(ControlPlaneMain.class);

    private ControlPlaneMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final Topology topology = Topology.load();
        final ControlPlaneConfig cfg = ControlPlaneConfig.load(topology);
        final VenueClients venue = VenueClients.connect(topology, "controlplane-" + cfg.instanceId);
        final ControlPlane cp = new ControlPlane(cfg, venue, Clock.system());
        final CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("shutting down control plane {}", cfg.instanceId);
            try
            {
                cp.close();
                venue.close();
            }
            finally
            {
                stopped.countDown();
            }
        }, "controlplane-shutdown"));
        try
        {
            cp.start(true);
        }
        catch (final Exception e)
        {
            LOG.error("control plane failed to start", e);
            System.exit(1);
        }
        System.out.println("LMB_READY controlplane " + cfg.instanceId);
        System.out.flush();
        stopped.await();
    }
}
