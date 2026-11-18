package io.lmb.cluster;

import io.aeron.cluster.service.Cluster;
import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.cluster.client.ClientConfig;
import io.lmb.cluster.client.ClusterVenueClients;
import io.lmb.common.Events;
import io.lmb.common.Topology;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** In-process shard of 1..3 members with small buffers and short timeouts; everything under target/. */
final class TestCluster implements AutoCloseable
{
    static final File ROOT = new File("target/cluster-tests");

    final String name;
    final int members;
    final Topology topology;
    final TestMachine.Factory factory = new TestMachine.Factory();
    final ClusterNode[] nodes;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    String secret;
    long snapshotIntervalNs;

    TestCluster(final String name, final int members, final int basePort)
    {
        this.name = name;
        this.members = members;
        this.nodes = new ClusterNode[members];
        final Properties p = new Properties();
        p.setProperty("lmb.shards", "1");
        p.setProperty("lmb.members", Integer.toString(members));
        for (int m = 0; m < members; m++)
        {
            p.setProperty("lmb.shard.0.member." + m + ".basePort", Integer.toString(basePort + 10 * m));
        }
        topology = Topology.of(p);
        IoUtil.delete(dir(), true);
    }

    File dir()
    {
        return new File(ROOT, name);
    }

    NodeConfig config(final int member)
    {
        final NodeConfig c = NodeConfig.from(topology, 0, member);
        c.dataDir = new File(dir(), "data");
        c.aeronDir = new File(dir(), "aeron-node-" + member).getAbsolutePath();
        c.driverTermLength = 64 * 1024;
        c.logTermLength = 256 * 1024;
        c.eventsTermLength = 256 * 1024;
        c.snapshotTermLength = 1024 * 1024;
        c.archiveSegmentLength = 1024 * 1024;
        c.leaderHeartbeatIntervalNs = TimeUnit.MILLISECONDS.toNanos(100);
        c.leaderHeartbeatTimeoutNs = TimeUnit.MILLISECONDS.toNanos(1500);
        c.electionTimeoutNs = TimeUnit.MILLISECONDS.toNanos(500);
        c.startupCanvassTimeoutNs = TimeUnit.SECONDS.toNanos(5);
        c.sessionTimeoutNs = TimeUnit.SECONDS.toNanos(5);
        c.eventIndexInterval = 8;
        c.eventHeartbeatIntervalNs = TimeUnit.MILLISECONDS.toNanos(100);
        c.eventSessionTimeoutNs = TimeUnit.SECONDS.toNanos(5);
        c.metricsEnabled = false;
        c.clusterSecret = secret;
        c.snapshotIntervalNs = snapshotIntervalNs;
        return c;
    }

    ClusterNode start(final int member)
    {
        return start(config(member));
    }

    ClusterNode start(final NodeConfig config)
    {
        final ClusterNode node = ClusterNode.launch(config, factory);
        nodes[config.memberId] = node;
        return node;
    }

    TestCluster startAll()
    {
        for (int m = 0; m < members; m++)
        {
            start(m);
        }
        return this;
    }

    void stop(final int member)
    {
        if (nodes[member] != null)
        {
            nodes[member].close();
            nodes[member] = null;
        }
    }

    int awaitLeader()
    {
        final int[] leader = { -1 };
        await("a leader", 20_000, () ->
        {
            for (int m = 0; m < members; m++)
            {
                if (nodes[m] != null && nodes[m].role() == Cluster.Role.LEADER)
                {
                    leader[0] = m;
                    return true;
                }
            }
            return false;
        });
        return leader[0];
    }

    int awaitLeaderOtherThan(final int old)
    {
        final int[] leader = { -1 };
        await("a new leader", 30_000, () ->
        {
            for (int m = 0; m < members; m++)
            {
                if (m != old && nodes[m] != null && nodes[m].role() == Cluster.Role.LEADER)
                {
                    leader[0] = m;
                    return true;
                }
            }
            return false;
        });
        return leader[0];
    }

    ClientConfig clientConfig()
    {
        final ClientConfig c = new ClientConfig();
        c.embeddedDirBase = dir().getAbsolutePath();
        c.termLength = 64 * 1024;
        c.messageTimeoutNs = TimeUnit.SECONDS.toNanos(3);
        c.eventConnectTimeoutNs = TimeUnit.SECONDS.toNanos(1);
        c.eventHeartbeatTimeoutNs = TimeUnit.SECONDS.toNanos(1);
        c.eventStallTimeoutNs = TimeUnit.SECONDS.toNanos(3);
        c.eventMemberCooldownNs = TimeUnit.MILLISECONDS.toNanos(500);
        c.clusterSecret = secret;
        return c;
    }

    ClusterVenueClients clients(final String clientName)
    {
        return clients(clientName, clientConfig());
    }

    ClusterVenueClients clients(final String clientName, final ClientConfig config)
    {
        final ClusterVenueClients c = new ClusterVenueClients(topology, clientName, config);
        closeables.add(c);
        return c;
    }

    /** Offers until accepted (polling the client), failing after timeoutMs. */
    static void send(final CommandClient client, final long clOrdId, final long timeoutMs)
    {
        final UnsafeBuffer b = new UnsafeBuffer(new byte[256]);
        final int len = TestMachine.newOrder(b, clOrdId);
        await("offer of " + clOrdId, timeoutMs, () ->
        {
            client.poll();
            return client.offer(b, 0, len);
        });
    }

    static void await(final String what, final long timeoutMs, final BooleanSupplier condition)
    {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
            {
                throw new AssertionError("timed out waiting for " + what);
            }
            try
            {
                Thread.sleep(1);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    /** Collects events from a stream, checking order invariants on the way. */
    static final class Collector
    {
        final EventStreamClient stream;
        final List<byte[]> events = new ArrayList<>();
        final List<Long> seqs = new ArrayList<>();
        final List<Long> clOrdIds = new ArrayList<>();
        long commandApplied;

        Collector(final EventStreamClient stream)
        {
            this.stream = stream;
        }

        int poll()
        {
            return stream.poll((buffer, offset, length) ->
            {
                final long seq = Events.eventSeq(buffer, offset);
                if (!seqs.isEmpty() && seq != seqs.get(seqs.size() - 1) + 1)
                {
                    throw new AssertionError("out of order: " + seq + " after " + seqs.get(seqs.size() - 1));
                }
                final byte[] copy = new byte[length];
                buffer.getBytes(offset, copy);
                events.add(copy);
                seqs.add(seq);
                final int templateId = Events.templateId(buffer, offset);
                if (templateId == io.lmb.sbe.ExecutionReportDecoder.TEMPLATE_ID && Events.eventIdx(buffer, offset) == 0)
                {
                    clOrdIds.add(buffer.getLong(offset + 8 + io.lmb.sbe.ExecutionReportDecoder.clOrdIdEncodingOffset(),
                        java.nio.ByteOrder.LITTLE_ENDIAN));
                }
                if (templateId == 120)
                {
                    commandApplied++;
                }
            }, 100);
        }

        void awaitSeq(final long seq, final long timeoutMs)
        {
            await("eventSeq " + seq + " (have " + stream.lastEventSeq() + ")", timeoutMs, () ->
            {
                poll();
                return stream.lastEventSeq() >= seq;
            });
        }

        void awaitCommandApplied(final long count, final long timeoutMs)
        {
            await(count + " CommandApplied (have " + commandApplied + ")", timeoutMs, () ->
            {
                poll();
                return commandApplied >= count;
            });
        }
    }

    @Override
    public void close()
    {
        for (int i = closeables.size() - 1; i >= 0; i--)
        {
            try
            {
                closeables.get(i).close();
            }
            catch (final Exception ignore)
            {
            }
        }
        for (int m = 0; m < members; m++)
        {
            stop(m);
        }
        IoUtil.delete(dir(), true);
    }
}
