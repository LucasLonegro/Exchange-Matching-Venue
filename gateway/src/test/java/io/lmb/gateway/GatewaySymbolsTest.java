package io.lmb.gateway;

import io.lmb.common.SymbolDirectory;
import io.lmb.common.SymbolDirectorySource;
import io.lmb.common.access.AccessDirectorySource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The control plane's symbol snapshot: the gateway waits for one, routes by it, and picks up a new one live. */
class GatewaySymbolsTest
{
    private final Fixture fx = new Fixture();

    @AfterEach
    void tearDown()
    {
        fx.close();
    }

    private static <T> T onCore(final Gateway g, final Supplier<T> read) throws Exception
    {
        final CompletableFuture<T> f = new CompletableFuture<>();
        g.core().submit(() -> f.complete(read.get()));
        return f.get(5, TimeUnit.SECONDS);
    }

    @Test
    void readyOnlyWithASnapshotWhichSeedsAndThenExtendsRouting() throws Exception
    {
        final SymbolDirectorySource source = SymbolDirectorySource.fixed(null);
        final Gateway g = new Gateway(Fixture.config(1), fx.venue, source, AccessDirectorySource.fixed(Fixture.ACCESS)).start();
        fx.gateways.add(g);
        assertFalse(g.awaitReady(300), "no symbol snapshot yet (control plane unreachable, no cache)");

        source.set(new SymbolDirectory("v1", List.of(Fixture.SYMBOLS.byTicker("AAPL"))));
        assertTrue(g.awaitReady(5_000));
        assertEquals(0, onCore(g, () -> g.core().symbols().byTicker("AAPL").shard()));
        assertNull(onCore(g, () -> g.core().symbols().byTicker("GOOG")));

        source.set(new SymbolDirectory("v2", Fixture.SYMBOLS.all()));
        final long deadline = System.currentTimeMillis() + 5_000;
        while (onCore(g, () -> g.core().symbols().byTicker("GOOG")) == null && System.currentTimeMillis() < deadline)
        {
            Thread.sleep(10);
        }
        assertEquals(1, onCore(g, () -> g.core().symbols().byTicker("GOOG").shard()), "a new snapshot is applied live");
    }
}
