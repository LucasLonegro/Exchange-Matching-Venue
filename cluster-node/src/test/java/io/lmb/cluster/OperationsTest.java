package io.lmb.cluster;

import io.aeron.cluster.ClusterBackup;
import io.lmb.api.CommandClient;
import io.lmb.cluster.client.ClientConfig;
import io.lmb.cluster.client.ClusterVenueClients;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class OperationsTest
{
    @Test
    void sharedSecretAuthenticationAndPeriodicSnapshots()
    {
        try (TestCluster cluster = new TestCluster("auth", 1, 31600))
        {
            cluster.secret = "s3cret";
            cluster.snapshotIntervalNs = TimeUnit.SECONDS.toNanos(1);
            cluster.startAll();
            cluster.awaitLeader();

            final ClusterVenueClients good = cluster.clients("auth-good");
            try (CommandClient cmd = good.commandClient(0))
            {
                TestCluster.send(cmd, 1, 10_000);
                final TestMachine m = cluster.factory.created.get(0);
                TestCluster.await("applied", 10_000, () -> m.commands() == 1);
            }

            final ClientConfig badCfg = cluster.clientConfig();
            badCfg.clusterSecret = "wrong";
            final ClusterVenueClients bad = cluster.clients("auth-bad", badCfg);
            try (CommandClient cmd = bad.commandClient(0))
            {
                final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (System.nanoTime() < end)
                {
                    cmd.poll();
                    assertFalse(cmd.isConnected(), "rejected credentials must not connect");
                    FailoverTest.sleep(5);
                }
            }

            final ClusterNode node = cluster.nodes[0];
            TestCluster.await("periodic snapshot", 10_000, () -> node.snapshotCount() >= 1);
        }
    }

    @Test
    void metricsEndpointAndClusterBackupFollowTheShard() throws Exception
    {
        try (TestCluster cluster = new TestCluster("ops", 2, 31700).startAll())
        {
            final int leader = cluster.awaitLeader();
            final ClusterVenueClients clients = cluster.clients("ops");
            try (CommandClient cmd = clients.commandClient(0);
                NodeMetrics metrics = NodeMetrics.start(cluster.nodes[leader], 31790);
                ClusterBackupNode backup = ClusterBackupNode.launch(cluster.config(0), "127.0.0.1", 31750))
            {
                for (long id = 1; id <= 20; id++)
                {
                    TestCluster.send(cmd, id, 10_000);
                }
                final TestMachine m = cluster.factory.created.get(leader);
                TestCluster.await("applied", 10_000, () -> m.commands() == 20);
                assertTrue(cluster.nodes[leader].triggerSnapshot());

                final String body = scrape(metrics.port());
                assertTrue(body.contains("lmb_cluster_role{member=\"" + leader + "\",shard=\"0\"} 2.0"), body);
                assertTrue(body.contains("lmb_cluster_last_event_seq{member=\"" + leader + "\",shard=\"0\"} " +
                    (double)m.lastEventSeq()), body);
                assertTrue(body.contains("lmb_cluster_service_time_ns{member=\"" + leader + "\",quantile=\"p99\",shard=\"0\"}"),
                    body);

                TestCluster.await("backup retrieved a snapshot and follows the live log", 30_000, () ->
                    backup.state() == ClusterBackup.State.BACKING_UP && backup.snapshotsRetrieved() >= 1 &&
                    backup.liveLogPosition() >= cluster.nodes[leader].commitPosition());
                for (long id = 21; id <= 25; id++)
                {
                    TestCluster.send(cmd, id, 10_000);
                }
                TestCluster.await("backup keeps up", 10_000, () ->
                    backup.liveLogPosition() >= cluster.nodes[leader].commitPosition() &&
                    cluster.nodes[leader].commitPosition() > 0);
            }
        }
    }

    private static String scrape(final int port) throws Exception
    {
        try (HttpClient http = HttpClient.newHttpClient())
        {
            return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics")).build(),
                HttpResponse.BodyHandlers.ofString()).body();
        }
    }
}
