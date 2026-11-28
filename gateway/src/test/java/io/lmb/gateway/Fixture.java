package io.lmb.gateway;

import io.lmb.api.CommandClient;
import io.lmb.api.EventStreamClient;
import io.lmb.api.VenueClients;
import io.lmb.testkit.TestAccess;
import io.lmb.common.SymbolDirectory;
import io.lmb.common.access.AccessDirectory;
import io.lmb.common.access.AccessDirectorySource;
import io.lmb.testkit.InMemoryVenue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Two scripted shards in an {@link InMemoryVenue}, reference data, and gateways on loopback ports
 * from the module's range (33000-33999). Event streams can be paused per shard to model a slow or
 * failed-over event stream while commands still apply.
 */
final class Fixture implements AutoCloseable
{
    private static final AtomicInteger PORTS = new AtomicInteger(33000 + (int)(ProcessHandle.current().pid() % 20) * 10);

    static final SymbolDirectory SYMBOLS = new SymbolDirectory(List.of(
        new SymbolDirectory.Symbol(1, "AAPL", 0, 1, 1, 19000, 1000, 100_000),
        new SymbolDirectory.Symbol(2, "MSFT", 0, 1, 1, 42000, 1000, 100_000),
        new SymbolDirectory.Symbol(5, "GOOG", 1, 1, 1, 17000, 1000, 100_000)));
    static final AccessDirectory ACCESS = TestAccess.directory(
        TestAccess.firm(100, "Alpha", "alpha-key", 1001, 1002),
        TestAccess.firm(200, "Beta", "beta-key", 2001));

    final ScriptedShard[] machines = {new ScriptedShard(0), new ScriptedShard(1)};
    final InMemoryVenue inMemory = new InMemoryVenue(1_700_000_000_000_000_000L, machines);
    final PausableVenue venue = new PausableVenue(inMemory);
    final List<Gateway> gateways = new ArrayList<>();
    final List<TestClient> clients = new ArrayList<>();

    static int nextPort()
    {
        final int p = PORTS.getAndIncrement();
        if (p > 33499)
        {
            throw new IllegalStateException("out of test ports");
        }
        return p;
    }

    static GatewayConfig config(final int gatewayId)
    {
        final GatewayConfig c = new GatewayConfig();
        c.gatewayId = gatewayId;
        c.bindHost = "127.0.0.1";
        c.grpcPort = nextPort();
        c.fixPort = 0;
        c.metricsPort = 0;
        c.fixEnabled = false;
        c.dataDir = Path.of("target", "test-data", "gw-" + gatewayId + "-" + c.grpcPort);
        c.heartbeatIntervalMs = 5_000;
        c.throttleRatePerSec = 100_000;
        c.throttleBurst = 10_000;
        c.messageRatePerSec = 100_000;
        c.messageBurst = 20_000;
        c.inflightTimeoutMs = 150;
        c.sendTimeoutMs = 300;
        c.idleMaxParkUs = 200;
        c.shutdownFlushMs = 300;
        return c;
    }

    Gateway gateway(final GatewayConfig cfg) throws Exception
    {
        return gateway(cfg, AccessDirectorySource.fixed(ACCESS));
    }

    Gateway gateway(final GatewayConfig cfg, final AccessDirectorySource access) throws Exception
    {
        final Gateway g = new Gateway(cfg, venue, SYMBOLS, access).start();
        gateways.add(g);
        if (!g.awaitReady(5_000))
        {
            throw new IllegalStateException("gateway not ready");
        }
        return g;
    }

    Gateway gateway(final int gatewayId) throws Exception
    {
        return gateway(config(gatewayId));
    }

    TestClient client(final Gateway g)
    {
        final TestClient c = new TestClient(g.grpcPort());
        clients.add(c);
        return c;
    }

    TestClient client(final Gateway g, final boolean manualFlow, final int window)
    {
        final TestClient c = new TestClient(g.grpcPort(), manualFlow, window);
        clients.add(c);
        return c;
    }

    TestClient alpha(final Gateway g)
    {
        final TestClient c = client(g);
        c.logon(100, 1001, "alpha-key");
        return c;
    }

    TestClient beta(final Gateway g)
    {
        final TestClient c = client(g);
        c.logon(200, 2001, "beta-key");
        return c;
    }

    static void await(final java.util.function.BooleanSupplier condition, final String what)
    {
        final long deadline = System.currentTimeMillis() + TestClient.TIMEOUT_MS;
        while (!condition.getAsBoolean())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new AssertionError("timed out waiting for " + what);
            }
            try
            {
                Thread.sleep(5);
            }
            catch (final InterruptedException e)
            {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public void close()
    {
        clients.forEach(TestClient::close);
        gateways.forEach(Gateway::close);
    }

    /** Delegating venue whose event streams can be frozen per shard. */
    static final class PausableVenue implements VenueClients
    {
        private final VenueClients delegate;
        private final boolean[] paused = new boolean[2];

        PausableVenue(final VenueClients delegate)
        {
            this.delegate = delegate;
        }

        synchronized void pause(final int shard, final boolean p)
        {
            paused[shard] = p;
        }

        private synchronized boolean paused(final int shard)
        {
            return paused[shard];
        }

        public int shardCount()
        {
            return delegate.shardCount();
        }

        public CommandClient commandClient(final int shardId)
        {
            return delegate.commandClient(shardId);
        }

        public EventStreamClient eventStream(final int shardId, final long fromEventSeq)
        {
            final EventStreamClient d = delegate.eventStream(shardId, fromEventSeq);
            return new EventStreamClient()
            {
                public int shardId()
                {
                    return shardId;
                }

                public int poll(final EventHandler handler, final int limit)
                {
                    return paused(shardId) ? 0 : d.poll(handler, limit);
                }

                public long lastEventSeq()
                {
                    return d.lastEventSeq();
                }

                public boolean isConnected()
                {
                    return d.isConnected();
                }

                public void close()
                {
                    d.close();
                }
            };
        }

        public void close()
        {
            delegate.close();
        }
    }
}
