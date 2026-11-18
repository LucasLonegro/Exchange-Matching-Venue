package io.lmb.cluster;

import io.lmb.api.ShardStateMachineFactory;
import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Shard member process. Configuration: topology ({@code -Dlmb.topology}), {@code lmb.shard.id},
 * {@code lmb.member.id}, {@code lmb.data.dir} and the keys in docs/modules/cluster.md.
 * Prints {@code LMB_READY cluster-node <shard>-<member>} once started; stops cleanly on SIGTERM.
 */
public final class ClusterNodeMain
{
    private static final Logger LOG = LoggerFactory.getLogger(ClusterNodeMain.class);

    private ClusterNodeMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final Topology topology = Topology.load();
        final NodeConfig config = NodeConfig.from(topology);
        final ClusterNode node = ClusterNode.launch(config, ShardStateMachineFactory.load());
        NodeMetrics metrics = null;
        if (config.metricsEnabled)
        {
            metrics = NodeMetrics.start(node, config.port(Topology.METRICS_OFFSET));
        }
        final NodeMetrics m = metrics;
        final CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("shutting down");
            if (m != null)
            {
                m.close();
            }
            node.close();
            done.countDown();
        }, "lmb-shutdown"));
        System.out.println("LMB_READY cluster-node " + config.shardId + "-" + config.memberId);
        System.out.flush();
        done.await();
    }
}
