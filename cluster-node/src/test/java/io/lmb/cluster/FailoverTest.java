package io.lmb.cluster;

import io.lmb.cluster.client.ClientConfig;
import io.lmb.cluster.client.ClusterCommandClient;
import io.lmb.cluster.client.ClusterEventStreamClient;
import io.lmb.cluster.client.ClusterVenueClients;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(180)
class FailoverTest
{
    private static final Logger LOG = LoggerFactory.getLogger(FailoverTest.class);

    @Test
    void leaderKillRedirectsClientsAndLosesOrDuplicatesNoEvent()
    {
        try (TestCluster cluster = new TestCluster("failover", 3, 31200).startAll())
        {
            final int leader = cluster.awaitLeader();
            final ClientConfig cfg = cluster.clientConfig();
            cfg.eventRehomeAfterNs = TimeUnit.MILLISECONDS.toNanos(300);
            final ClusterVenueClients clients = cluster.clients("failover", cfg);
            final ClusterCommandClient cmd = (ClusterCommandClient)clients.commandClient(0);
            final ClusterEventStreamClient stream = (ClusterEventStreamClient)clients.eventStream(0, 0);
            final TestCluster.Collector collector = new TestCluster.Collector(stream);

            long nextId = 1;
            for (; nextId <= 50; nextId++)
            {
                TestCluster.send(cmd, nextId, 10_000);
                collector.poll();
            }
            collector.awaitCommandApplied(50, 15_000);
            // the stream prefers the leader: make sure it streams from the member we are about to kill
            TestCluster.await("stream on leader", 10_000, () ->
            {
                collector.poll();
                cmd.poll();
                return stream.connectedMemberId() == leader;
            });

            final Set<Long> accepted = new HashSet<>();
            final Set<Long> rejected = new HashSet<>();
            final Set<Long> acceptedAfterRecovery = new HashSet<>();
            for (long id = 1; id < nextId; id++)
            {
                accepted.add(id);
            }

            LOG.info("killing leader {}", leader);
            cluster.stop(leader);

            final UnsafeBuffer b = new UnsafeBuffer(new byte[256]);
            boolean recovered = false;
            int afterRecovery = 0;
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (afterRecovery < 50)
            {
                assertTrue(System.nanoTime() < deadline, "cluster did not recover");
                cmd.poll();
                collector.poll();
                if (!recovered && cmd.isConnected() && cmd.leaderMemberId() != leader && cmd.leaderMemberId() >= 0)
                {
                    recovered = true;
                    LOG.info("command client now on leader {}", cmd.leaderMemberId());
                }
                final long id = nextId++;
                final int len = TestMachine.newOrder(b, id);
                if (cmd.offer(b, 0, len))
                {
                    accepted.add(id);
                    if (recovered)
                    {
                        acceptedAfterRecovery.add(id);
                        afterRecovery++;
                    }
                }
                else
                {
                    rejected.add(id);
                }
                sleep(2);
            }

            final int newLeader = cluster.awaitLeaderOtherThan(leader);
            final TestMachine survivor = cluster.factory.created.get(newLeader);
            TestCluster.await("survivor applied all accepted-after-recovery commands", 20_000, () ->
            {
                collector.poll();
                return collector.clOrdIds.containsAll(acceptedAfterRecovery);
            });
            // let the stream reach the survivor's end
            sleep(300);
            final long expectedLast = survivor.lastEventSeq();
            collector.awaitSeq(expectedLast, 20_000);

            assertEquals(expectedLast, stream.lastEventSeq());
            assertNotEquals(leader, stream.connectedMemberId(), "stream failed over to a live member");
            assertTrue(stream.reconnects() >= 2, "stream reconnected at least once");

            final Set<Long> seen = new HashSet<>();
            for (final long id : collector.clOrdIds)
            {
                assertTrue(seen.add(id), "command " + id + " applied twice");
                assertFalse(rejected.contains(id), "command " + id + " applied although offer returned false");
                assertTrue(accepted.contains(id), "unknown command " + id);
            }
            final long lost = accepted.stream().filter(id -> !seen.contains(id)).count();
            LOG.info("accepted {}, rejected {}, accepted-but-lost (offered to the dying leader) {}",
                accepted.size(), rejected.size(), lost);
            assertTrue(seen.containsAll(acceptedAfterRecovery));
            for (long id = 1; id <= 50; id++)
            {
                assertTrue(seen.contains(id), "pre-failover command " + id + " lost");
            }

            // the two survivors hold byte-identical streams
            for (int m = 0; m < 3; m++)
            {
                if (m == leader)
                {
                    continue;
                }
                final ClientConfig pinned = cluster.clientConfig();
                pinned.eventPreferredMember = m;
                final ClusterVenueClients pc = cluster.clients("failover-check-" + m, pinned);
                final TestCluster.Collector check = new TestCluster.Collector(pc.eventStream(0, 0));
                check.awaitSeq(expectedLast, 20_000);
                assertEquals(m, ((ClusterEventStreamClient)check.stream).connectedMemberId());
                for (int i = 0; i < expectedLast; i++)
                {
                    assertArrayEquals(collector.events.get(i), check.events.get(i), "event " + (i + 1) + " member " + m);
                }
                check.stream.close();
            }
            stream.close();
            cmd.close();
        }
    }

    @Test
    void followerRestartedMidReplayDoesNotDuplicateRecordedEvents()
    {
        try (TestCluster cluster = new TestCluster("restart", 3, 31300).startAll())
        {
            final int leader = cluster.awaitLeader();
            final int follower = (leader + 1) % 3;
            final ClusterVenueClients clients = cluster.clients("restart");
            final ClientConfig pinnedCfg = cluster.clientConfig();
            pinnedCfg.eventPreferredMember = follower;
            final ClusterVenueClients pinned = cluster.clients("restart-pinned", pinnedCfg);
            final ClusterCommandClient cmd = (ClusterCommandClient)clients.commandClient(0);
            final ClusterEventStreamClient stream = (ClusterEventStreamClient)pinned.eventStream(0, 0);
            final TestCluster.Collector collector = new TestCluster.Collector(stream);

            for (long id = 1; id <= 40; id++)
            {
                TestCluster.send(cmd, id, 10_000);
                collector.poll();
            }
            collector.awaitCommandApplied(40, 15_000);
            assertEquals(follower, stream.connectedMemberId());
            final long recordedBefore = cluster.nodes[follower].eventIndex().lastSeq();

            cluster.stop(follower);
            for (long id = 41; id <= 70; id++)
            {
                TestCluster.send(cmd, id, 10_000);
                collector.poll();
            }
            final ClusterNode restarted = cluster.start(follower);
            for (long id = 71; id <= 100; id++)
            {
                TestCluster.send(cmd, id, 10_000);
                collector.poll();
            }
            final TestMachine leaderMachine = cluster.factory.created.get(leader);
            TestCluster.await("leader applied 100", 10_000, () -> leaderMachine.commands() == 100);
            final long last = leaderMachine.lastEventSeq();
            collector.awaitSeq(last, 30_000);
            assertEquals(List.copyOf(rangeList(1, 100)), collector.clOrdIds);

            TestCluster.await("restarted member caught up", 20_000, () -> restarted.eventIndex().lastSeq() == last);
            final EventIndex.Segment[] segments = restarted.eventIndex().segments();
            assertEquals(2, segments.length, "one segment per run");
            assertEquals(1, segments[0].firstSeq());
            assertEquals(recordedBefore, segments[0].lastSeq(), "first run's recording intact");
            assertEquals(recordedBefore + 1, segments[1].firstSeq(), "second run continues exactly after");
            assertEquals(last, segments[1].lastSeq());
            assertEquals(recordedBefore, restarted.service().store().skippedCount(),
                "events replayed from the log and already recorded were skipped, not appended again");

            // the restarted member serves the whole stream across both segments
            final ClientConfig again = cluster.clientConfig();
            again.eventPreferredMember = follower;
            final ClusterVenueClients c2 = cluster.clients("restart-check", again);
            final TestCluster.Collector check = new TestCluster.Collector(c2.eventStream(0, 0));
            check.awaitSeq(last, 20_000);
            assertEquals(follower, ((ClusterEventStreamClient)check.stream).connectedMemberId());
            for (int i = 0; i < last; i++)
            {
                assertArrayEquals(collector.events.get(i), check.events.get(i));
            }
            check.stream.close();
            stream.close();
            cmd.close();
        }
    }

    static List<Long> rangeList(final long from, final long to)
    {
        final java.util.ArrayList<Long> l = new java.util.ArrayList<>();
        for (long i = from; i <= to; i++)
        {
            l.add(i);
        }
        return l;
    }

    static void sleep(final long ms)
    {
        try
        {
            Thread.sleep(ms);
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
