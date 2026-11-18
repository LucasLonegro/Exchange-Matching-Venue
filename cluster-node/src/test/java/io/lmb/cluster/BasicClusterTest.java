package io.lmb.cluster;

import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.cluster.client.ClientConfig;
import io.lmb.cluster.client.ClusterVenueClients;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class BasicClusterTest
{
    @Test
    void singleMemberProcessesCommandsAndServesStreamFromStartOffsetAndLatest()
    {
        try (TestCluster cluster = new TestCluster("single", 1, 31000).startAll())
        {
            cluster.awaitLeader();
            final ClusterVenueClients clients = cluster.clients("single");
            try (CommandClient cmd = clients.commandClient(0);
                EventStreamClient all = clients.eventStream(0, 0))
            {
                final TestCluster.Collector c = new TestCluster.Collector(all);
                for (int i = 1; i <= 30; i++)
                {
                    TestCluster.send(cmd, i, 10_000);
                }
                c.awaitCommandApplied(30, 15_000);
                assertEquals(List.of(1L, 2L, 3L), c.clOrdIds.subList(0, 3));
                assertEquals(30, c.clOrdIds.size());
                final long last = c.stream.lastEventSeq();
                final TestMachine machine = cluster.factory.created.get(0);
                assertEquals(machine.lastEventSeq(), last);
                assertEquals(30, machine.commands());

                // resume from an arbitrary eventSeq: exactly the suffix, byte-identical
                try (EventStreamClient from = clients.eventStream(0, 17))
                {
                    final TestCluster.Collector f = new TestCluster.Collector(from);
                    f.awaitSeq(last, 10_000);
                    assertEquals(18L, f.seqs.get(0));
                    for (int i = 0; i < f.events.size(); i++)
                    {
                        assertArrayEquals(c.events.get(17 + i), f.events.get(i));
                    }
                }

                // LATEST: only events produced after subscribing
                try (EventStreamClient latest = clients.eventStream(0, VenueClients.LATEST))
                {
                    final TestCluster.Collector l = new TestCluster.Collector(latest);
                    TestCluster.await("LATEST resolved", 10_000, () ->
                    {
                        l.poll();
                        return latest.isConnected() && latest.lastEventSeq() >= 0;
                    });
                    assertEquals(last, latest.lastEventSeq());
                    TestCluster.send(cmd, 1000, 10_000);
                    l.awaitCommandApplied(1, 10_000);
                    assertEquals(last + 1, l.seqs.get(0));
                    assertEquals(List.of(1000L), l.clOrdIds);
                }
            }
        }
    }

    @Test
    void twoMemberClusterReplicatesAndBothMembersServeIdenticalStreams()
    {
        try (TestCluster cluster = new TestCluster("two", 2, 31100).startAll())
        {
            cluster.awaitLeader();
            final ClusterVenueClients clients = cluster.clients("two");
            final List<TestCluster.Collector> perMember = new ArrayList<>();
            final List<ClusterVenueClients> pinned = new ArrayList<>();
            for (int m = 0; m < 2; m++)
            {
                final ClientConfig cfg = cluster.clientConfig();
                cfg.eventPreferredMember = m;
                final ClusterVenueClients pc = cluster.clients("two-m" + m, cfg);
                pinned.add(pc);
                perMember.add(new TestCluster.Collector(pc.eventStream(0, 0)));
            }
            try (CommandClient cmd = clients.commandClient(0))
            {
                for (int i = 1; i <= 25; i++)
                {
                    TestCluster.send(cmd, i, 10_000);
                }
                for (final TestCluster.Collector c : perMember)
                {
                    c.awaitCommandApplied(25, 15_000);
                }
                final long last = perMember.get(0).stream.lastEventSeq();
                perMember.get(1).awaitSeq(last, 5_000);
                for (int m = 0; m < 2; m++)
                {
                    final io.lmb.cluster.client.ClusterEventStreamClient s =
                        (io.lmb.cluster.client.ClusterEventStreamClient)perMember.get(m).stream;
                    assertEquals(m, s.connectedMemberId(), "stream pinned to member " + m);
                }
                assertEquals(perMember.get(0).seqs.size(), perMember.get(1).seqs.size());
                for (int i = 0; i < perMember.get(0).events.size(); i++)
                {
                    assertTrue(Arrays.equals(perMember.get(0).events.get(i), perMember.get(1).events.get(i)),
                        "event " + (i + 1) + " differs between members");
                }
                assertEquals(cluster.factory.created.get(0).checksum(), cluster.factory.created.get(1).checksum());
            }
            finally
            {
                perMember.forEach(c -> c.stream.close());
            }
        }
    }
}
