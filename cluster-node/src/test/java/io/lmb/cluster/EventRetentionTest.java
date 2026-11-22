package io.lmb.cluster;

import io.lmb.api.CommandClient;
import io.lmb.cluster.client.ClusterEventStreamClient;
import io.lmb.cluster.client.ClusterVenueClients;
import io.lmb.cluster.client.EventProtocol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class EventRetentionTest
{
    @Test
    void purgeCountKeepsAContiguousNewestRun()
    {
        final long now = 1_000_000;
        final long[] stops = { now - 5000, now - 4000, now - 100, now - 50 };
        assertEquals(0, EventRetention.NONE.purgeCount(stops, 4, now));
        assertEquals(1, new EventRetention(3, 0, 1).purgeCount(stops, 4, now));
        assertEquals(0, new EventRetention(4, 0, 1).purgeCount(stops, 4, now));
        assertEquals(2, new EventRetention(0, 1000, 1).purgeCount(stops, 4, now));
        assertEquals(3, new EventRetention(1, 1000, 1).purgeCount(stops, 4, now), "the stricter rule wins");
        assertEquals(4, new EventRetention(0, 10, 1).purgeCount(stops, 4, now));
        // an old segment after a young one (clock step) does not punch a hole: only a prefix is purged
        final long[] unordered = { now - 100, now - 5000 };
        assertEquals(0, new EventRetention(0, 1000, 1).purgeCount(unordered, 2, now));
        assertEquals(0, new EventRetention(0, 1000, 1).purgeCount(new long[0], 0, now));
    }

    @Test
    void closedSegmentsArePurgedAndTheMemberServesTheRest()
    {
        try (TestCluster cluster = new TestCluster("retention", 1, 31960))
        {
            final NodeConfig keepOne = cluster.config(0);
            keepOne.eventRetention = new EventRetention(1, 0, TimeUnit.MILLISECONDS.toNanos(50));
            final ClusterVenueClients clients = cluster.clients("retention");

            // three service starts => segments 1 and 2 closed, 3 live
            long id = 0;
            final long[] segmentIds = new long[3];
            final long[] firstSeqs = new long[3];
            for (int run = 0; run < 3; run++)
            {
                final ClusterNode node = cluster.start(keepOne);
                cluster.awaitLeader();
                final TestMachine m = cluster.factory.created.get(run);
                try (CommandClient cmd = clients.commandClient(0))
                {
                    for (int i = 0; i < 5; i++)
                    {
                        TestCluster.send(cmd, ++id, 10_000);
                    }
                    final long commands = id;
                    TestCluster.await("applied", 10_000, () -> m.commands() == commands &&
                        node.eventIndex().lastSeq() == m.lastEventSeq());
                }
                final EventIndex.Segment live = node.eventIndex().liveSegment();
                segmentIds[run] = live.recordingId();
                firstSeqs[run] = live.firstSeq();
                if (run < 2)
                {
                    cluster.stop(0);
                }
            }

            final ClusterNode node = cluster.nodes[0];
            final EventStore store = node.service().store();
            TestCluster.await("segment 1 purged", 10_000, () -> store.purgedCount() == 1);
            final EventIndex index = node.eventIndex();
            assertEquals(2, index.segments().length);
            assertEquals(firstSeqs[1], index.firstSeq());
            final File memberDir = keepOne.memberDir();
            assertFalse(new File(memberDir, "events/seg-" + segmentIds[0] + ".idx").exists());
            assertTrue(new File(memberDir, "events/seg-" + segmentIds[1] + ".idx").exists());
            assertEquals(32, new File(memberDir, "events/segments.dat").length(), "two segment records left");
            final String[] recFiles = new File(memberDir, "archive").list((d, n) -> n.startsWith(segmentIds[0] + "-"));
            assertEquals(0, recFiles.length, "recording files of the purged segment are deleted");

            // the member still serves everything from segment 2 on, and says TOO_OLD for older events
            final TestCluster.Collector rest = new TestCluster.Collector(clients.eventStream(0, firstSeqs[1] - 1));
            final long last = cluster.factory.created.get(2).lastEventSeq();
            rest.awaitSeq(last, 10_000);
            assertEquals(firstSeqs[1], rest.seqs.get(0));
            assertEquals(last - firstSeqs[1] + 1, rest.seqs.size());
            final ClusterEventStreamClient old = (ClusterEventStreamClient)clients.eventStream(0, 0);
            TestCluster.await("TOO_OLD", 10_000, () ->
            {
                old.poll((b, o, l) -> { throw new AssertionError("no event expected"); }, 10);
                return old.unavailableResponses() > 0;
            });
            assertEquals(EventProtocol.REASON_TOO_OLD, old.lastUnavailableReason());
            old.close();

            // restart with an age limit only: every closed segment goes, the stream continues in a new segment
            cluster.stop(0);
            final NodeConfig byAge = cluster.config(0);
            byAge.eventRetention = new EventRetention(0, 300, TimeUnit.MILLISECONDS.toNanos(50));
            final ClusterNode aged = cluster.start(byAge);
            cluster.awaitLeader();
            final EventStore agedStore = aged.service().store();
            TestCluster.await("segments 2 and 3 purged", 10_000, () -> agedStore.purgedCount() == 2);
            assertEquals(1, aged.eventIndex().segments().length);
            assertTrue(aged.eventIndex().segments()[0].isLive());
            assertEquals(16, new File(memberDir, "events/segments.dat").length());
            final long firstAfter = aged.eventIndex().firstSeq();
            assertEquals(last + 1, firstAfter);
            try (CommandClient cmd = clients.commandClient(0))
            {
                TestCluster.send(cmd, ++id, 10_000);
            }
            final TestCluster.Collector tail = new TestCluster.Collector(clients.eventStream(0, firstAfter - 1));
            tail.awaitCommandApplied(1, 10_000);
            assertEquals(firstAfter, tail.seqs.get(0));
        }
    }
}
