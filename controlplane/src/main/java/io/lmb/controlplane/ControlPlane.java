package io.lmb.controlplane;

import io.lmb.api.VenueClients;
import io.lmb.common.FirmDirectory;
import io.lmb.common.SymbolDirectory;
import io.lmb.controlplane.db.Database;
import io.lmb.controlplane.db.Migrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.locks.LockSupport;

/**
 * One control plane instance: database (migrated, bootstrapped), leadership lease, leader engine with its
 * duties, admin REST API and metrics. {@link #start(boolean)} with threads=false lets tests drive it
 * deterministically through {@link #step()}.
 */
public final class ControlPlane implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(ControlPlane.class);
    public static final String LEASE_NAME = "controlplane";

    private final ControlPlaneConfig cfg;
    private final VenueClients venue;
    private final Clock clock;
    private Database db;
    private Metrics metrics;
    private LeaderLease lease;
    private Engine engine;
    private RiskBudgetAllocator allocator;
    private AdminExecutor admin;
    private Scheduler scheduler;
    private RestApi rest;
    private volatile boolean running;
    private Thread engineThread;
    private Thread leaseThread;

    public ControlPlane(final ControlPlaneConfig cfg, final VenueClients venue, final Clock clock)
    {
        this.cfg = cfg;
        this.venue = venue;
        this.clock = clock;
    }

    public void start(final boolean threads) throws IOException
    {
        db = Database.open(cfg.dbUrl, cfg.dbUser, cfg.dbPassword, cfg.dbPoolSize);
        final int applied = Migrator.migrate(db);
        LOG.info("database {} ready ({} migrations applied)", db.dialect(), applied);
        if (cfg.symbolsFile != null && cfg.firmsFile != null && Files.exists(cfg.symbolsFile) && Files.exists(cfg.firmsFile))
        {
            Bootstrap.bootstrapIfEmpty(db, SymbolDirectory.load(cfg.symbolsFile), FirmDirectory.load(cfg.firmsFile),
                cfg.symbolsFile + "," + cfg.firmsFile, clock.epochMs());
        }
        else if (!Bootstrap.isBootstrapped(db))
        {
            LOG.warn("database is empty and reference data files {} / {} are missing", cfg.symbolsFile, cfg.firmsFile);
        }
        if (cfg.adminTokenDefaulted)
        {
            LOG.warn("lmb.controlplane.adminToken not set; using the local default token");
        }
        metrics = new Metrics();
        metrics.start(cfg.metricsPort);
        rest = cfg.restPort >= 0 ? new RestApi(cfg, db, venue.shardCount(), clock, metrics) : null;
        final String url = rest == null ? null : "http://" + cfg.restHost + ":" + cfg.restPort;
        lease = new LeaderLease(db, LEASE_NAME, cfg.instanceId, url, clock, cfg.leaseTtlMs, cfg.leaseRenewMs, cfg.leaseMarginMs);
        engine = new Engine(cfg, db, venue, clock, lease, metrics);
        allocator = new RiskBudgetAllocator();
        admin = new AdminExecutor(allocator);
        engine.addDuty(admin);
        engine.addDuty(new Reconciler());
        scheduler = new Scheduler(new CalendarScheduler());
        admin.scheduler(scheduler);
        engine.addDuty(scheduler);
        engine.addDuty(allocator);
        if (rest != null)
        {
            rest.start(lease);
        }
        if (threads)
        {
            running = true;
            leaseThread = new Thread(() ->
            {
                while (running)
                {
                    lease.tick();
                    LockSupport.parkNanos(Math.max(10, cfg.leaseRenewMs / 5) * 1_000_000L);
                }
            }, "controlplane-lease-" + cfg.instanceId);
            engineThread = new Thread(() ->
            {
                while (running)
                {
                    engine.tick();
                    LockSupport.parkNanos(cfg.tickMs * 1_000_000L);
                }
                engine.close();
            }, "controlplane-engine-" + cfg.instanceId);
            leaseThread.setDaemon(true);
            engineThread.setDaemon(true);
            leaseThread.start();
            engineThread.start();
        }
    }

    /** Manual mode: one lease tick and one engine tick. */
    public void step()
    {
        lease.tick();
        engine.tick();
    }

    public Engine engine()
    {
        return engine;
    }

    public LeaderLease lease()
    {
        return lease;
    }

    public Database db()
    {
        return db;
    }

    public RestApi rest()
    {
        return rest;
    }

    public Metrics metrics()
    {
        return metrics;
    }

    /** Simulates a crash: stops everything without releasing the lease or persisting. */
    public void kill()
    {
        running = false;
        join();
        if (rest != null)
        {
            rest.stop();
        }
        metrics.close();
        db.close();
    }

    @Override
    public void close()
    {
        running = false;
        join();
        if (engineThread == null && engine != null)
        {
            engine.close();
        }
        if (lease != null)
        {
            lease.release();
        }
        if (rest != null)
        {
            rest.stop();
        }
        if (metrics != null)
        {
            metrics.close();
        }
        if (db != null)
        {
            db.close();
        }
    }

    private void join()
    {
        for (final Thread t : new Thread[] {engineThread, leaseThread})
        {
            if (t != null)
            {
                try
                {
                    t.join(5_000);
                }
                catch (final InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
