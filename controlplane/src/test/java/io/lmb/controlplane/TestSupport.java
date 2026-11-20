package io.lmb.controlplane;

import io.lmb.testkit.InMemoryVenue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.function.BooleanSupplier;

final class TestSupport
{
    static final long MS = 1_000_000L;
    /** Monday 2026-11-16 12:00 New York. */
    static final long NOON = ZonedDateTime.of(2026, 11, 16, 12, 0, 0, 0, ZoneId.of("America/New_York")).toInstant().toEpochMilli() * MS;

    static final String SYMBOLS = """
        symbolId,ticker,shard,tickSize,lotSize,referencePrice,priceBandBps,maxOrderQty
        1,AAPL,0,1,1,19000,1000,100000
        2,MSFT,0,1,1,42000,1000,100000
        3,AMZN,0,1,1,18500,1000,100000
        5,GOOG,1,1,1,17000,1000,100000
        6,META,1,1,1,50000,1000,100000
        7,TSLA,1,1,1,25000,1000,100000
        """;
    static final String FIRMS = """
        firmId,name,apiKey,sessionIds,creditLimit,maxPositionPerSymbol
        100,Alpha Capital,alpha-key,1001;1002,100000000,5000000
        200,Beta Trading,beta-key,2001,60000000,2000000
        """;

    private TestSupport()
    {
    }

    static String h2Url(final String name)
    {
        return "jdbc:h2:mem:" + name + "-" + UUID.randomUUID() +
            ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000";
    }

    static Path dataDir()
    {
        try
        {
            final Path dir = Path.of("target", "test-data", UUID.randomUUID().toString());
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("symbols.csv"), SYMBOLS);
            Files.writeString(dir.resolve("firms.csv"), FIRMS);
            return dir;
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    /** Fast timings, no REST, no metrics, calendar enforcement off unless a test turns it on. */
    static ControlPlaneConfig config(final String dbUrl, final Path dataDir, final String instanceId)
    {
        final ControlPlaneConfig c = new ControlPlaneConfig();
        c.instanceId = instanceId;
        c.dbUrl = dbUrl;
        c.symbolsFile = dataDir.resolve("symbols.csv");
        c.firmsFile = dataDir.resolve("firms.csv");
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
