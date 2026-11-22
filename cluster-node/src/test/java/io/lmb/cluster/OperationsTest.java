package io.lmb.cluster;

import io.aeron.cluster.ClusterBackup;
import io.lmb.api.CommandClient;
import io.lmb.cluster.client.ClientConfig;
import io.lmb.cluster.client.ClusterVenueClients;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void metricsEndpointReportsRoleEventsAndServiceTime() throws Exception
    {
        try (TestCluster cluster = new TestCluster("metrics", 2, 31800).startAll())
        {
            final int leader = cluster.awaitLeader();
            final int follower = 1 - leader;
            final ClusterVenueClients clients = cluster.clients("metrics");
            try (CommandClient cmd = clients.commandClient(0);
                NodeMetrics leaderMetrics = NodeMetrics.start(cluster.nodes[leader], 31890);
                NodeMetrics followerMetrics = NodeMetrics.start(cluster.nodes[follower], 31891))
            {
                for (long id = 1; id <= 20; id++)
                {
                    TestCluster.send(cmd, id, 10_000);
                }
                final TestMachine lm = cluster.factory.created.get(leader);
                final TestMachine fm = cluster.factory.created.get(follower);
                TestCluster.await("applied", 10_000, () -> lm.commands() == 20 && fm.commands() == 20);
                TestCluster.await("events recorded on both", 10_000, () ->
                    cluster.nodes[leader].eventIndex().lastSeq() == lm.lastEventSeq() &&
                    cluster.nodes[follower].eventIndex().lastSeq() == lm.lastEventSeq());
                assertTrue(cluster.nodes[leader].triggerSnapshot());
                TestCluster.await("snapshot", 10_000, () -> cluster.nodes[leader].snapshotCount() >= 1);

                final String lb = scrape(leaderMetrics.port());
                final String l = "member=\"" + leader + "\",shard=\"0\"";
                assertEquals(2.0, metric(lb, "lmb_cluster_role", l), lb);
                assertEquals(lm.lastEventSeq(), (long)metric(lb, "lmb_cluster_last_event_seq", l), lb);
                assertEquals(1.0, metric(lb, "lmb_cluster_first_event_seq", l), lb);
                assertEquals(20.0, metric(lb, "lmb_cluster_commands_applied", l), lb);
                assertTrue(metric(lb, "lmb_cluster_commit_position", l) > 0, lb);
                assertTrue(metric(lb, "lmb_cluster_snapshot_count", l) >= 1, lb);
                assertTrue(metric(lb, "lmb_cluster_election_count", l) >= 1, lb);
                assertEquals(0.0, metric(lb, "lmb_cluster_event_consumers", l), lb);
                final String q = "member=\"" + leader + "\",quantile=\"%s\",shard=\"0\"";
                final double p50 = metric(lb, "lmb_cluster_service_time_ns", q.formatted("p50"));
                final double p99 = metric(lb, "lmb_cluster_service_time_ns", q.formatted("p99"));
                final double max = metric(lb, "lmb_cluster_service_time_ns", q.formatted("max"));
                assertTrue(p50 > 0 && p50 <= p99 && p99 <= max, lb);

                final String fb = scrape(followerMetrics.port());
                final String f = "member=\"" + follower + "\",shard=\"0\"";
                assertEquals(0.0, metric(fb, "lmb_cluster_role", f), fb);
                assertEquals(lm.lastEventSeq(), (long)metric(fb, "lmb_cluster_last_event_seq", f), fb);
                assertEquals(20.0, metric(fb, "lmb_cluster_commands_applied", f), fb);
            }
        }
    }

    @Test
    void clusterBackupRetrievesSnapshotsAndFollowsTheLog()
    {
        try (TestCluster cluster = new TestCluster("backup", 2, 31700).startAll())
        {
            final int leader = cluster.awaitLeader();
            final ClusterVenueClients clients = cluster.clients("backup");
            try (CommandClient cmd = clients.commandClient(0))
            {
                for (long id = 1; id <= 20; id++)
                {
                    TestCluster.send(cmd, id, 10_000);
                }
                final TestMachine m = cluster.factory.created.get(leader);
                TestCluster.await("applied", 10_000, () -> m.commands() == 20);
                assertTrue(cluster.nodes[leader].triggerSnapshot());
                TestCluster.await("snapshot", 10_000, () -> cluster.nodes[leader].snapshotCount() >= 1);
                for (long id = 21; id <= 25; id++)
                {
                    TestCluster.send(cmd, id, 10_000);
                }
                TestCluster.await("applied", 10_000, () -> m.commands() == 25);

                // FOLLOWER source: when the leader answers first the backup resets, so keep the cool-down short
                final long commit;
                try (ClusterBackupNode backup = ClusterBackupNode.launch(cluster.config(0), "127.0.0.1", 31750, ctx -> ctx
                    .clusterBackupCoolDownIntervalNs(TimeUnit.MILLISECONDS.toNanos(200))
                    .clusterBackupResponseTimeoutNs(TimeUnit.SECONDS.toNanos(1))))
                {
                    TestCluster.await("backup state=" + backup.state() + " snapshots=" + backup.snapshotsRetrieved(), 20_000,
                        () -> backup.state() == ClusterBackup.State.BACKING_UP && backup.snapshotsRetrieved() >= 1 &&
                        backup.liveLogPosition() >= cluster.nodes[leader].commitPosition());

                    for (long id = 26; id <= 30; id++)
                    {
                        TestCluster.send(cmd, id, 10_000);
                    }
                    TestCluster.await("applied", 10_000, () -> m.commands() == 30);
                    TestCluster.await("commit settles", 5_000, () ->
                        cluster.nodes[0].commitPosition() == cluster.nodes[1].commitPosition());
                    commit = cluster.nodes[leader].commitPosition();
                    TestCluster.await("backup keeps up", 10_000, () -> backup.liveLogPosition() >= commit);
                }

                // the backup's directory holds the same committed log as the members
                final File backupDir = new File(cluster.config(0).dataDir, "cluster-backup/shard-0");
                try (RaftLogReader member = RaftLogReader.open(cluster.config(leader).memberDir());
                    RaftLogReader copy = RaftLogReader.open(backupDir))
                {
                    assertTrue(copy.snapshots().stream().anyMatch(s -> s.serviceId() == 0), "" + copy.snapshots());
                    final long from = copy.startPosition();
                    final RaftLogReaderTest.Recorder expected = new RaftLogReaderTest.Recorder();
                    final RaftLogReaderTest.Recorder actual = new RaftLogReaderTest.Recorder();
                    member.read(from, commit, expected);
                    assertEquals(commit, copy.read(from, commit, actual));
                    assertTrue(actual.entries.size() >= 5, "entries " + actual.entries.size() + " from " + from);
                    assertEquals(expected.entries, actual.entries);
                }
            }
        }
    }

    /** Value of the sample {@code name{labels}} in a Prometheus text exposition; fails if absent. */
    static double metric(final String body, final String name, final String labels)
    {
        final String prefix = name + "{" + labels + "} ";
        for (final String line : body.split("\n"))
        {
            if (line.startsWith(prefix))
            {
                return Double.parseDouble(line.substring(prefix.length()).trim().split(" ")[0]);
            }
        }
        throw new AssertionError("no sample " + prefix + " in\n" + body);
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
