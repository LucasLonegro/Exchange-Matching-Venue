package io.lmb.replayer;

import io.lmb.api.ShardStateMachineFactory;
import io.lmb.api.VenueClients;
import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Shadow replayer process, one per shard (configuration: {@link ReplayerConfig}). Prints
 * {@code LMB_READY replayer <shard>} once it has caught up with the live edge of the event stream; stops cleanly
 * on SIGTERM; exits with status 1 if the replayer thread fails. A divergence does not stop the process: it is
 * reported by the {@code lmb_replayer_divergence} metric and an ERROR log line.
 */
public final class ShadowReplayerMain
{
    private static final Logger LOG = LoggerFactory.getLogger(ShadowReplayerMain.class);

    private ShadowReplayerMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final Topology topology = Topology.load();
        final ReplayerConfig config = ReplayerConfig.load(topology);
        final VenueClients venue = VenueClients.connect(topology, "replayer-" + config.shardId);
        final ShadowReplayer replayer;
        try
        {
            replayer = new ShadowReplayer(config, ShardStateMachineFactory.load(), venue).start();
        }
        catch (final RuntimeException e)
        {
            venue.close();
            throw e;
        }
        final Thread hook = new Thread(() ->
        {
            LOG.info("replayer {} shutting down", config.shardId);
            replayer.close();
            venue.close();
        }, "replayer-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);

        if (awaitReadyAndAnnounce(replayer, config, System.out))
        {
            replayer.awaitStopped(Long.MAX_VALUE, TimeUnit.DAYS);
        }
        if (replayer.failure() != null) // otherwise stopped by SIGTERM: the hook cleans up
        {
            Runtime.getRuntime().removeShutdownHook(hook);
            replayer.close();
            venue.close();
            System.exit(1);
        }
    }

    /** Waits (logging every 5 s) until caught up, then prints the READY line; false if the replayer stopped first. */
    static boolean awaitReadyAndAnnounce(final ShadowReplayer replayer, final ReplayerConfig config, final PrintStream out)
        throws InterruptedException
    {
        while (!replayer.awaitReady(5_000))
        {
            LOG.info("replayer {} catching up: applied log position {}, verified eventSeq {}, stream eventSeq {}",
                config.shardId, replayer.appliedPosition(), replayer.verifiedSeq(), replayer.streamSeq());
        }
        if (!replayer.isRunning())
        {
            LOG.error("replayer {} stopped before it was ready", config.shardId, replayer.failure());
            return false;
        }
        LOG.info("replayer {} ready ({}), metrics port {}", config.shardId, config, replayer.metricsPort());
        out.println("LMB_READY replayer " + config.shardId);
        out.flush();
        return true;
    }
}
