package io.lmb.cluster;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.lmb.api.CommandClient;
import io.lmb.api.TimerScheduler;
import io.lmb.cluster.client.ClusterVenueClients;
import io.lmb.sbe.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class RaftLogReaderTest
{
    /** A command (timer = false) or a timer (bytes = correlationId as 8 bytes). */
    record Entry(boolean timer, long logPosition, long timestampNs, byte[] bytes)
    {
        @Override
        public boolean equals(final Object o)
        {
            return o instanceof Entry e && e.timer == timer && e.logPosition == logPosition &&
                e.timestampNs == timestampNs && Arrays.equals(e.bytes, bytes);
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(logPosition);
        }

        @Override
        public String toString()
        {
            return (timer ? "timer@" : "cmd@") + logPosition + "/" + timestampNs + "/" + bytes.length;
        }
    }

    static final class Recorder implements RaftLogReader.Handler
    {
        final List<Entry> entries = new ArrayList<>();
        final Set<Integer> otherTemplates = new HashSet<>();
        long lastPosition;

        @Override
        public void onCommand(
            final long logPosition, final long timestampNs, final long clusterSessionId,
            final DirectBuffer buffer, final int offset, final int length)
        {
            final byte[] b = new byte[length];
            buffer.getBytes(offset, b);
            add(new Entry(false, logPosition, timestampNs, b));
        }

        @Override
        public void onTimer(final long logPosition, final long timestampNs, final long correlationId)
        {
            final UnsafeBuffer b = new UnsafeBuffer(new byte[8]);
            b.putLong(0, correlationId);
            add(new Entry(true, logPosition, timestampNs, b.byteArray()));
        }

        @Override
        public void onClusterMessage(
            final int templateId, final long logPosition, final DirectBuffer buffer, final int offset, final int length)
        {
            otherTemplates.add(templateId);
            positionAdvances(logPosition);
        }

        private void add(final Entry e)
        {
            positionAdvances(e.logPosition());
            entries.add(e);
        }

        private void positionAdvances(final long logPosition)
        {
            if (logPosition <= lastPosition)
            {
                throw new AssertionError("log position " + logPosition + " after " + lastPosition);
            }
            lastPosition = logPosition;
        }
    }

    /** Feeds entries to a fresh machine (timers come from the log) and returns the events it emits. */
    static List<byte[]> replay(final List<Entry> entries)
    {
        final TestMachine machine = new TestMachine(0);
        final List<byte[]> events = new ArrayList<>();
        machine.bind((buffer, offset, length) ->
        {
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            events.add(copy);
        }, new TimerScheduler()
        {
            @Override
            public void schedule(final long correlationId, final long deadlineNs)
            {
            }

            @Override
            public void cancel(final long correlationId)
            {
            }
        });
        for (final Entry e : entries)
        {
            if (e.timer())
            {
                machine.onTimer(new UnsafeBuffer(e.bytes()).getLong(0), e.logPosition(), e.timestampNs());
            }
            else
            {
                machine.onCommand(new UnsafeBuffer(e.bytes()), 0, e.bytes().length, e.logPosition(), e.timestampNs());
            }
        }
        return events;
    }

    @Test
    void readsEveryMembersLogAndReplayReproducesTheEventStream()
    {
        try (TestCluster cluster = new TestCluster("raftlog", 2, 31900).startAll())
        {
            cluster.awaitLeader();
            final ClusterVenueClients clients = cluster.clients("raftlog");
            CommandClient cmd = clients.commandClient(0);
            for (long id = 1; id <= 10; id++)
            {
                TestCluster.send(cmd, id, 10_000);
            }
            final long dueNs = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()) + TimeUnit.MILLISECONDS.toNanos(300);
            SnapshotTest.sendTimerCommand(cmd, 4242, dueNs);
            sendLarge(cmd, 4000); // fragmented in the log (MTU 1408)
            for (long id = 11; id <= 14; id++)
            {
                TestCluster.send(cmd, id, 10_000);
            }
            final TestMachine m0 = cluster.factory.created.get(0);
            final TestMachine m1 = cluster.factory.created.get(1);
            TestCluster.await("16 commands and the timer", 10_000, () ->
                m0.commands() == 16 && m1.commands() == 16 && m0.timersFired() == 1 && m1.timersFired() == 1);
            final int leader = cluster.awaitLeader();
            assertTrue(cluster.nodes[leader].triggerSnapshot());
            TestCluster.await("snapshot", 10_000, () -> cluster.nodes[leader].snapshotCount() >= 1);
            cmd.close();

            // restart the whole shard: a second leadership term in the log
            cluster.stop(0);
            cluster.stop(1);
            cluster.startAll();
            cluster.awaitLeader();
            cmd = clients.commandClient(0);
            for (long id = 15; id <= 24; id++)
            {
                TestCluster.send(cmd, id, 10_000);
            }
            final TestMachine r0 = cluster.factory.created.get(2);
            final TestMachine r1 = cluster.factory.created.get(3);
            TestCluster.await("26 commands after restart", 15_000, () -> r0.commands() == 26 && r1.commands() == 26);
            final long lastSeq = r0.lastEventSeq();
            final TestCluster.Collector stream = new TestCluster.Collector(clients.eventStream(0, 0));
            stream.awaitSeq(lastSeq, 15_000);
            TestCluster.await("commit settles", 5_000, () ->
                cluster.nodes[0].commitPosition() == cluster.nodes[1].commitPosition());
            final long commit = cluster.nodes[0].commitPosition();

            // 1. live member directory, bounded by the commit position
            final List<Entry> live;
            final long snapshotPosition;
            final long logRecordingId;
            try (RaftLogReader log = RaftLogReader.open(cluster.config(0).memberDir()))
            {
                assertTrue(log.terms().size() >= 2, "terms " + log.terms());
                assertEquals(0, log.startPosition());
                assertTrue(log.recordedPosition() >= commit);
                final RaftLogReader.Snapshot snapshot = log.snapshots().stream()
                    .filter(s -> s.serviceId() == 0).findFirst().orElseThrow();
                snapshotPosition = snapshot.logPosition();
                assertTrue(log.knownCommittedPosition() >= snapshotPosition);
                logRecordingId = log.terms().get(0).recordingId();

                final Recorder all = new Recorder();
                assertEquals(commit, log.read(0, commit, all));
                live = all.entries;
                assertEquals(27, live.size(), "26 commands + 1 timer: " + live);
                assertEquals(1, live.stream().filter(Entry::timer).count());
                assertTrue(live.stream().anyMatch(e -> e.bytes().length == 4000), "fragmented command reassembled");
                assertTrue(all.otherTemplates.contains(RaftLogDecoder.NEW_LEADERSHIP_TERM_TEMPLATE_ID), "" + all.otherTemplates);
                assertTrue(all.otherTemplates.contains(RaftLogDecoder.CLUSTER_ACTION_TEMPLATE_ID), "" + all.otherTemplates);

                // the replay reproduces the stream byte for byte (logPosition and timestamp included)
                final List<byte[]> replayed = replay(live);
                assertEquals(stream.events.size(), replayed.size());
                for (int i = 0; i < replayed.size(); i++)
                {
                    assertArrayEquals(stream.events.get(i), replayed.get(i), "event " + (i + 1));
                }

                // resume in pieces, at arbitrary bounds
                final Recorder pieces = new Recorder();
                long reached = 0;
                for (long bound = 777; reached < commit; bound += 777)
                {
                    reached = log.read(reached, Math.min(bound, commit), pieces);
                }
                assertEquals(live, pieces.entries);

                // start at the snapshot
                final Recorder tail = new Recorder();
                log.read(snapshotPosition, commit, tail);
                assertEquals(live.stream().filter(e -> e.logPosition() > snapshotPosition).toList(), tail.entries);
                assertThrows(IllegalStateException.class, () -> log.read(snapshotPosition + 32, commit, new Recorder()));
            }

            // 2. the same entries through an Archive replay of the live member and RaftLogDecoder
            try (AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(cluster.config(0).aeronDir)
                .controlRequestChannel("aeron:udp?endpoint=127.0.0.1:31900")
                .controlResponseChannel("aeron:udp?endpoint=127.0.0.1:0"));
                Subscription replay = archive.replay(logRecordingId, 0, commit, "aeron:ipc", 7099))
            {
                final Recorder viaArchive = new Recorder();
                final FragmentAssembler assembler = new FragmentAssembler(new RaftLogDecoder(viaArchive));
                TestCluster.await("archive replay", 10_000, () ->
                {
                    replay.poll(assembler, 64);
                    return viaArchive.lastPosition >= commit;
                });
                assertEquals(live, viaArchive.entries);
            }

            // 3. both members' directories offline: identical committed logs
            cmd.close();
            cluster.stop(0);
            cluster.stop(1);
            for (int m = 0; m < 2; m++)
            {
                try (RaftLogReader log = RaftLogReader.open(cluster.config(m).memberDir()))
                {
                    final Recorder offline = new Recorder();
                    log.read(0, commit, offline);
                    assertEquals(live, offline.entries, "member " + m);
                    final Recorder everything = new Recorder();
                    log.read(everything);
                    assertEquals(live, everything.entries.subList(0, live.size()), "member " + m);
                }
            }
        }
    }

    private static void sendLarge(final CommandClient cmd, final int length)
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[length]);
        new MessageHeaderEncoder().wrap(b, 0).blockLength(0).templateId(999).schemaId(1).version(0);
        for (int i = MessageHeaderEncoder.ENCODED_LENGTH; i < length; i++)
        {
            b.putByte(i, (byte)i);
        }
        TestCluster.await("large command", 10_000, () ->
        {
            cmd.poll();
            return cmd.offer(b, 0, length);
        });
    }
}
