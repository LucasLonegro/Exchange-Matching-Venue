package io.lmb.posttrade;

import io.lmb.api.SinkRunner;
import io.lmb.api.VenueClients;
import io.lmb.common.Config;
import io.lmb.common.SymbolDirectorySource;
import io.lmb.common.access.AccessDirectorySource;
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
        if (config.accessUrl == null || config.accessUrl.isBlank())
        {
            throw new IllegalStateException("lmb.posttrade.access.url is not set: drop copy credentials come only from the access service");
        }
        final AccessDirectorySource access = AccessDirectorySource.polling(config.accessUrl, config.accessToken,
            config.accessPollMs, config.dataDir.resolve("access-directory.json"), "posttrade-" + instance).start();
        // tickers for the drop copy: wait for a first snapshot (or the cached one) so reports never carry bare ids
        final SymbolDirectorySource symbols = SymbolDirectorySource.fromTopology(topology,
            config.dataDir.resolve("symbol-directory.json"), "posttrade-" + instance).start();
        if (!symbols.awaitFirst("posttrade " + instance))
        {
            access.close();
            symbols.close();
            return;
        }
        final VenueClients venue = VenueClients.connect(topology, "posttrade-" + instance);
        final PostTrade postTrade;
        try
        {
            postTrade = new PostTrade(config, venue, access, symbols).start();
        }
        catch (final Exception e)
        {
            venue.close();
            access.close();
            symbols.close();
            throw e;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("posttrade {} shutting down", instance);
            postTrade.close();
            venue.close();
            access.close();
            symbols.close();
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
