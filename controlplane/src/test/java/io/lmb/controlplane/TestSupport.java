package io.lmb.controlplane;

import io.lmb.common.SymbolDirectory;
import io.lmb.controlplane.db.Database;
import io.lmb.controlplane.db.Migrator;
import io.lmb.controlplane.db.Store;
import io.lmb.testkit.InMemoryVenue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

final class TestSupport
{
    static final long MS = 1_000_000L;
    /** Monday 2026-11-16 12:00 New York. */
    static final long NOON = ZonedDateTime.of(2026, 11, 16, 12, 0, 0, 0, ZoneId.of("America/New_York")).toInstant().toEpochMilli() * MS;

    static final List<Store.SymbolRow> SYMBOLS = List.of(
        symbol(1, "AAPL", 0, 19000), symbol(2, "MSFT", 0, 42000), symbol(3, "AMZN", 0, 18500),
        symbol(5, "GOOG", 1, 17000), symbol(6, "META", 1, 50000), symbol(7, "TSLA", 1, 25000));
    static final List<Store.FirmRow> FIRMS = List.of(
        new Store.FirmRow(100, "Alpha Capital", true, 100_000_000L, 5_000_000L, 0),
        new Store.FirmRow(200, "Beta Trading", true, 60_000_000L, 2_000_000L, 0));

    private TestSupport()
    {
    }

    static String h2Url(final String name)
    {
        return "jdbc:h2:mem:" + name + "-" + UUID.randomUUID() +
            ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000";
    }

    /** Tick 1, lot 1, 10% band, 100k max quantity, no dynamic collar: what the operator API defaults to. */
    static Store.SymbolRow symbol(final int symbolId, final String ticker, final int shard, final long referencePrice)
    {
        return symbol(symbolId, ticker, shard, referencePrice, SymbolDirectory.DEFAULT_DYNAMIC_BAND_BPS,
            SymbolDirectory.DEFAULT_VOLATILITY_AUCTION_NS);
    }

    static Store.SymbolRow symbol(final int symbolId, final String ticker, final int shard, final long referencePrice,
        final int dynamicBandBps, final long volatilityAuctionNs)
    {
        return new Store.SymbolRow(symbolId, ticker, shard, 1, 1, referencePrice, 1000, 100_000, dynamicBandBps,
            volatilityAuctionNs, "ACTIVE", null, 0);
    }

    static Path dataDir()
    {
        try
        {
            final Path dir = Path.of("target", "test-data", UUID.randomUUID().toString());
            Files.createDirectories(dir);
            return dir;
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Writes reference data into the database the way the operator API would (the control plane reads only its
     * database): the symbols if it has none yet, the firms if it has none yet.
     */
    static void seed(final String dbUrl, final List<Store.SymbolRow> symbols, final List<Store.FirmRow> firms)
    {
        try (Database db = Database.open(dbUrl, "sa", "", 1))
        {
            Migrator.migrate(db);
            db.tx(c ->
            {
                if (Store.symbols(c).isEmpty())
                {
                    for (final Store.SymbolRow s : symbols)
                    {
                        Store.upsertSymbol(c, s, 1);
                    }
                }
                if (Database.queryLong(c, "SELECT COUNT(*) FROM firm") == 0)
                {
                    for (final Store.FirmRow f : firms)
                    {
                        Store.insertFirm(c, f, 1);
                    }
                }
                return null;
            });
        }
    }

    /**
     * Fast timings, no REST, no metrics, calendar enforcement off unless a test turns it on. Seeds {@link #SYMBOLS}
     * and {@link #FIRMS} into a database that has none (a test that wants others seeds them first).
     */
    static ControlPlaneConfig config(final String dbUrl, final Path dataDir, final String instanceId)
    {
        seed(dbUrl, SYMBOLS, FIRMS);
        final ControlPlaneConfig c = new ControlPlaneConfig();
        c.instanceId = instanceId;
        c.dbUrl = dbUrl;
        c.restPort = -1;
        c.metricsPort = -1;
        c.commandTimeoutMs = 200;
        c.persistIntervalMs = 20;
        c.reconcileIntervalMs = 1_000;
        c.refDataReloadMs = 200;
        c.leaseTtlMs = 1_000;
        c.leaseRenewMs = 100;
        c.leaseMarginMs = 300;
        c.allocatorIntervalMs = 50;
        c.allocatorRefreshMs = 200;
        c.schedulerEnabled = false;
        c.enforce = false;
        c.autoOpenDelayMs = 500;
        return c;
    }

    /** Clock driven by the venue's simulated time (wall and monotonic). */
    static Clock venueClock(final InMemoryVenue venue)
    {
        return new Clock()
        {
            public long epochNs()
            {
                return venue.nowNs();
            }

            public long monoNs()
            {
                return venue.nowNs();
            }
        };
    }

    /** Steps time by stepMs and every control plane once per step until the condition holds. */
    static boolean runUntil(final InMemoryVenue venue, final long stepMs, final long maxMs, final BooleanSupplier done,
        final ControlPlane... cps)
    {
        for (long t = 0; t <= maxMs; t += stepMs)
        {
            if (done.getAsBoolean())
            {
                return true;
            }
            venue.advanceTimeBy(stepMs * MS);
            for (final ControlPlane cp : cps)
            {
                cp.step();
            }
        }
        return done.getAsBoolean();
    }

    static void run(final InMemoryVenue venue, final long stepMs, final long forMs, final ControlPlane... cps)
    {
        runUntil(venue, stepMs, forMs, () -> false, cps);
    }
}
