package io.lmb.posttrade;

import io.lmb.api.VenueClients;
import io.lmb.common.Config;
import io.lmb.common.FirmDirectory;
import io.lmb.common.SymbolDirectory;
import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Post-trade process. Instance from {@code lmb.instance.id}, everything else from the topology (see
 * {@link PostTradeConfig}). Prints {@code LMB_READY posttrade <instance>} once every enabled sink has caught up
 * with every shard; stops cleanly on SIGTERM (runners close their sinks with a final commit/checkpoint, then the
 * drop copy engine, then the venue connection). A failing sink is restarted from its durable cursor by its runner;
 * it does not stop the process.
 */
public final class PostTradeMain
{
    private static final Logger LOG = LoggerFactory.getLogger(PostTradeMain.class);

    private PostTradeMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final int instance = Config.getInt("lmb.instance.id", 0);
        final Topology topology = Topology.load();
        final PostTradeConfig config = PostTradeConfig.load(topology, instance);
        final FirmDirectory firms = FirmDirectory.load(topology.firmsFile());
        final SymbolDirectory symbols = SymbolDirectory.load(topology.symbolsFile());
        final VenueClients venue = VenueClients.connect(topology, "posttrade-" + instance);
        final PostTrade postTrade;
        try
        {
            postTrade = new PostTrade(config, venue, firms, symbols).start();
        }
        catch (final Exception e)
        {
            venue.close();
            throw e;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("posttrade {} shutting down", instance);
            postTrade.close();
            venue.close();
        }, "posttrade-shutdown"));

        if (awaitReadyAndAnnounce(postTrade, System.out))
        {
            postTrade.awaitStopped(Long.MAX_VALUE, TimeUnit.DAYS);
        }
    }

    /** Waits (logging every 5 s) until every sink has caught up, then prints the READY line; false if closed first. */
    static boolean awaitReadyAndAnnounce(final PostTrade postTrade, final PrintStream out) throws InterruptedException
    {
        final int instance = postTrade.config().instance;
        try
        {
            while (!postTrade.awaitReady(5_000))
            {
                for (final SinkRunner r : postTrade.runners())
                {
                    LOG.info("posttrade {} waiting: sink {} caughtUp={} up={} lastFailure={}", instance, r.name(),
                        r.isCaughtUp(), r.isUp(), r.lastFailure() == null ? "-" : r.lastFailure().toString());
                }
            }
        }
        catch (final IllegalStateException e)
        {
            LOG.info("posttrade {} stopped before it was ready", instance);
            return false;
        }
        out.println("LMB_READY posttrade " + instance);
        out.flush();
        return true;
    }
}
