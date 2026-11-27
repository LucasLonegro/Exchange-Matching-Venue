package io.lmb.ledger;

import io.lmb.api.VenueClients;
import io.lmb.common.Config;
import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Ledger process. Instance from {@code lmb.instance.id}, everything else from the topology (see {@link LedgerConfig}).
 * Prints {@code LMB_READY ledger <instance>} once it has caught up with every shard; stops cleanly on SIGTERM (the
 * sink commits what it has applied, then the venue connection closes). A failure restarts the sink from its
 * committed cursors; it does not stop the process.
 */
public final class LedgerMain
{
    private static final Logger LOG = LoggerFactory.getLogger(LedgerMain.class);

    private LedgerMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final int instance = Config.getInt("lmb.instance.id", 0);
        final Topology topology = Topology.load();
        final LedgerConfig config = LedgerConfig.load(topology, instance);
        final VenueClients venue = VenueClients.connect(topology, "ledger-" + instance);
        final Ledger ledger;
        try
        {
            ledger = new Ledger(config, venue).start();
        }
        catch (final Exception e)
        {
            venue.close();
            throw e;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("ledger {} shutting down", instance);
            ledger.close();
            venue.close();
        }, "ledger-shutdown"));

        try
        {
            while (!ledger.awaitReady(5_000))
            {
                final Throwable failure = ledger.runner().lastFailure();
                LOG.info("ledger {} waiting: caughtUp={} up={} lastFailure={}", instance, ledger.isCaughtUp(),
                    ledger.runner().isUp(), failure == null ? "-" : failure.toString());
            }
        }
        catch (final IllegalStateException e)
        {
            LOG.info("ledger {} stopped before it was ready", instance);
            return;
        }
        System.out.println("LMB_READY ledger " + instance);
        System.out.flush();
        ledger.awaitStopped(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
