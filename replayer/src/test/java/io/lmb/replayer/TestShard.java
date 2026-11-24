package io.lmb.replayer;

import io.aeron.cluster.service.Cluster;
import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.api.ShardStateMachineFactory;
import io.lmb.cluster.ClusterNode;
import io.lmb.cluster.NodeConfig;
import io.lmb.cluster.client.ClientConfig;
import io.lmb.cluster.client.ClusterVenueClients;
import io.lmb.common.Events;
import io.lmb.common.Topology;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/** In-process shard (1..3 members) running the real matcher, small buffers, everything under target/. */
final class TestShard implements AutoCloseable
{
    static final File ROOT = new File("target/replayer-tests");

    final String name;
    final int members;
    final Topology topology;
    final ClusterNode[] nodes;
    final CommandText text = new CommandText();
    final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
    long snapshotIntervalNs;
    private ClusterVenueClients clients;
    private CommandClient commands;

    TestShard(final String name, final int members, final int basePort)
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
        c.snapshotIntervalNs = snapshotIntervalNs;
        return c;
    }

    File memberDir(final int member)
    {
        return config(member).memberDir();
    }

    TestShard startAll()
    {
        for (int m = 0; m < members; m++)
        {
            nodes[m] = ClusterNode.launch(config(m), ShardStateMachineFactory.load());
        }
        return this;
    }

    void stopAll()
    {
        closeClients();
        for (int m = 0; m < members; m++)
        {
            if (nodes[m] != null)
            {
                nodes[m].close();
                nodes[m] = null;
            }
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
        return c;
    }

    ClusterVenueClients clients()
    {
        if (clients == null)
        {
            clients = new ClusterVenueClients(topology, name + "-client", clientConfig());
        }
        return clients;
    }

    /** Sends one command written as {@link CommandText}. */
    void send(final String command)
    {
        if (commands == null)
        {
            commands = clients().commandClient(0);
        }
        final int length = text.encode(command, buffer);
        await("offer of " + command, 10_000, () ->
        {
            commands.poll();
            return commands.offer(buffer, 0, length);
        });
    }

    /** Waits until the event stream holds {@code entries} CommandApplied events; returns the last eventSeq. */
    long awaitEntries(final long entries)
    {
        final long[] seen = { 0, 0 };
        try (EventStreamClient stream = clients().eventStream(0, 0))
        {
            await(entries + " entries in the event stream", 20_000, () ->
            {
                stream.poll((b, o, l) ->
                {
                    seen[1] = Events.eventSeq(b, o);
                    if (Events.templateId(b, o) == ReplayVerifier.COMMAND_APPLIED)
                    {
                        seen[0]++;
                    }
                }, 256);
                if (commands != null)
                {
                    commands.poll();
                }
                return seen[0] >= entries;
            });
        }
        return seen[1];
    }

    void awaitCommitSettles()
    {
        await("commit settles", 5_000, () ->
        {
            long p = -1;
            for (final ClusterNode n : nodes)
            {
                if (n != null)
                {
                    if (p >= 0 && n.commitPosition() != p)
                    {
                        return false;
                    }
                    p = n.commitPosition();
                }
            }
            return true;
        });
    }

    void closeClients()
    {
        if (clients != null)
        {
            clients.close();
            clients = null;
            commands = null;
        }
    }

    @Override
    public void close()
    {
        stopAll();
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

    static void copyTree(final File from, final File to)
    {
        IoUtil.delete(to, true);
        try (Stream<Path> paths = Files.walk(from.toPath()))
        {
            for (final Path p : (Iterable<Path>)paths::iterator)
            {
                final Path target = to.toPath().resolve(from.toPath().relativize(p));
                if (Files.isDirectory(p))
                {
                    Files.createDirectories(target);
                }
                else
                {
                    Files.copy(p, target);
                }
            }
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
