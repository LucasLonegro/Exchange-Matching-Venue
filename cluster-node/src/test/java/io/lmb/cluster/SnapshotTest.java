package io.lmb.cluster;

import io.lmb.api.CommandClient;
import io.lmb.cluster.client.ClientConfig;
import io.lmb.cluster.client.ClusterEventStreamClient;
import io.lmb.cluster.client.ClusterVenueClients;
import io.lmb.cluster.client.EventProtocol;
import io.lmb.sbe.MessageHeaderEncoder;
import io.lmb.sbe.SymbolState;
import io.lmb.sbe.SymbolStateCommandEncoder;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(180)
class SnapshotTest
{
    @Test
    void snapshotThenRestartRestoresStateTimersAndContinuesEventSeq()
    {
        try (TestCluster cluster = new TestCluster("snapshot", 1, 31400).startAll())
        {
            cluster.awaitLeader();
            final ClusterVenueClients clients = cluster.clients("snapshot");
            final CommandClient cmd = clients.commandClient(0);
            for (long id = 1; id <= 20; id++)
            {
                TestCluster.send(cmd, id, 10_000);
            }
            // a timer due well after the restart: it lives in the consensus module's snapshot
            final long dueNs = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()) + TimeUnit.SECONDS.toNanos(4);
            sendTimerCommand(cmd, 777, dueNs);
            final TestMachine first = cluster.factory.created.get(0);
            TestCluster.await("21 commands", 10_000, () -> first.commands() == 21);

            final ClusterNode node = cluster.nodes[0];
            assertTrue(node.triggerSnapshot());
            TestCluster.await("snapshot", 10_000, () -> node.snapshotCount() == 1 && node.service().snapshotsTaken() == 1);
            final long seqAtSnapshot = first.lastEventSeq();

            for (long id = 21; id <= 30; id++)
            {
                TestCluster.send(cmd, id, 10_000);
            }
            TestCluster.await("31 commands", 10_000, () -> first.commands() == 31);
            final long seqBeforeStop = first.lastEventSeq();
            final long checksumBeforeStop = first.checksum();
            assertEquals(0, first.timersFired());
            cmd.close();

            cluster.stop(0);
            final ClusterNode restarted = cluster.start(0);
            final TestMachine second = cluster.factory.created.get(1);
            cluster.awaitLeader();
            TestCluster.await("log replayed", 15_000, () -> second.commands() == 31);
            assertTrue(second.restoredFromSnapshot, "machine was loaded from the snapshot");
            assertTrue(restarted.service().snapshotLoaded());
            assertEquals(seqBeforeStop, second.lastEventSeq());
            assertEquals(checksumBeforeStop, second.checksum());
            assertEquals(seqBeforeStop - seqAtSnapshot, restarted.service().store().skippedCount(),
                "only the events after the snapshot were regenerated, and none was re-recorded");

            final CommandClient cmd2 = clients.commandClient(0);
            for (long id = 31; id <= 35; id++)
            {
                TestCluster.send(cmd2, id, 10_000);
            }
            TestCluster.await("timer fired after restart", 15_000, () -> second.timersFired() == 1);
            TestCluster.await("36 entries", 10_000, () -> second.commands() == 36);

            final TestCluster.Collector all = new TestCluster.Collector(clients.eventStream(0, 0));
            all.awaitSeq(second.lastEventSeq(), 15_000);
            assertEquals(second.lastEventSeq(), all.seqs.size(), "stream is dense from 1 across the restart");
            assertEquals(FailoverTest.rangeList(1, 35), all.clOrdIds.stream().filter(id -> id != 0).toList());
            assertEquals(1, all.clOrdIds.stream().filter(id -> id == 0).count(), "the timer command, once");
            assertEquals(seqBeforeStop + 1, restarted.eventIndex().segments()[1].firstSeq());
            all.stream.close();
            cmd2.close();
        }
    }

    @Test
    void memberRebuiltFromSnapshotSaysTooOldAndClientsGoElsewhere()
    {
        try (TestCluster cluster = new TestCluster("wiped", 3, 31500).startAll())
        {
            final int leader = cluster.awaitLeader();
            final int wiped = (leader + 1) % 3;
            final ClusterVenueClients clients = cluster.clients("wiped");
            final CommandClient cmd = clients.commandClient(0);
            for (long id = 1; id <= 30; id++)
            {
                TestCluster.send(cmd, id, 10_000);
            }
            final TestMachine leaderMachine = cluster.factory.created.get(leader);
            TestCluster.await("30 applied", 10_000, () -> leaderMachine.commands() == 30);
            assertTrue(cluster.nodes[leader].triggerSnapshot());
            TestCluster.await("snapshot on every member", 15_000, () ->
                cluster.nodes[0].snapshotCount() == 1 && cluster.nodes[1].snapshotCount() == 1 &&
                cluster.nodes[2].snapshotCount() == 1);
            final long snapshotSeq = leaderMachine.lastEventSeq();
            for (long id = 31; id <= 40; id++)
            {
                TestCluster.send(cmd, id, 10_000);
            }
            TestCluster.await("40 applied", 10_000, () -> leaderMachine.commands() == 40);

            // lose the member's event recordings: it restarts from its snapshot without events 1..snapshotSeq
            final NodeConfig wipedConfig = cluster.config(wiped);
            cluster.stop(wiped);
            IoUtil.delete(new File(wipedConfig.memberDir(), "events"), false);
            final ClusterNode rebuilt = cluster.start(wipedConfig);
            final long last = leaderMachine.lastEventSeq();
            TestCluster.await("rebuilt member caught up", 20_000, () -> rebuilt.eventIndex().lastSeq() == last);
            assertEquals(snapshotSeq + 1, rebuilt.eventIndex().firstSeq());

            // a consumer that prefers the rebuilt member is told TOO_OLD and served by another member
            final ClientConfig pinned = cluster.clientConfig();
            pinned.eventPreferredMember = wiped;
            final ClusterVenueClients pc = cluster.clients("wiped-pinned", pinned);
            final ClusterEventStreamClient fromStart = (ClusterEventStreamClient)pc.eventStream(0, 0);
            final TestCluster.Collector c = new TestCluster.Collector(fromStart);
            c.awaitSeq(last, 20_000);
            assertTrue(fromStart.unavailableResponses() >= 1);
            assertEquals(EventProtocol.REASON_TOO_OLD, fromStart.lastUnavailableReason());
            assertFalse(fromStart.connectedMemberId() == wiped && c.seqs.get(0) <= snapshotSeq);
            assertEquals(FailoverTest.rangeList(1, 40), c.clOrdIds);

            // ...but it can serve anything after its snapshot, byte-identical
            final ClusterEventStreamClient tail = (ClusterEventStreamClient)pc.eventStream(0, snapshotSeq + 3);
            final TestCluster.Collector t = new TestCluster.Collector(tail);
            t.awaitSeq(last, 20_000);
            assertEquals(wiped, tail.connectedMemberId());
            for (int i = 0; i < t.events.size(); i++)
            {
                assertArrayEquals(c.events.get((int)(snapshotSeq + 3) + i), t.events.get(i));
            }
            fromStart.close();
            tail.close();
            cmd.close();
        }
    }

    static void sendTimerCommand(final CommandClient cmd, final long correlationId, final long effectiveTimeNs)
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[128]);
        final SymbolStateCommandEncoder e = new SymbolStateCommandEncoder();
        e.wrapAndApplyHeader(b, 0, new MessageHeaderEncoder())
            .correlationId(correlationId).effectiveTimeNs(effectiveTimeNs).symbolId(1).reasonCode(0)
            .targetState(SymbolState.HALTED);
        final int len = MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength();
        TestCluster.await("timer command", 10_000, () ->
        {
            cmd.poll();
            return cmd.offer(b, 0, len);
        });
    }
}
