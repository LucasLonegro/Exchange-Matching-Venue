package io.lmb.cluster;

import io.lmb.common.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * DR backup of one shard ({@code lmb.shard.id}): runs Aeron ClusterBackup against the shard's members.
 * Listens on {@code lmb.backup.host} (default 127.0.0.1) and ports from
 * {@code lmb.backup.<shard>.basePort} (default 20000 + 1000*shard + 900). Data under
 * {@code <lmb.data.dir>/cluster-backup/shard-S}. Prints {@code LMB_READY cluster-backup <shard>}.
 */
public final class ClusterBackupMain
{
    private static final Logger LOG = LoggerFactory.getLogger(ClusterBackupMain.class);

    private ClusterBackupMain()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final Topology topology = Topology.load();
        final NodeConfig config = NodeConfig.from(topology);
        final String host = topology.get("lmb.backup.host", "127.0.0.1");
        final ClusterBackupNode backup = ClusterBackupNode.launch(
            config, host, ClusterBackupNode.basePort(topology, config.shardId));
        final CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("shutting down backup");
            backup.close();
            done.countDown();
        }, "lmb-shutdown"));
        System.out.println("LMB_READY cluster-backup " + config.shardId);
        System.out.flush();
        done.await();
    }
}
