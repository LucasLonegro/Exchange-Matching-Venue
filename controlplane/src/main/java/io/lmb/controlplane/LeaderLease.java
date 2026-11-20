package io.lmb.controlplane;

import io.lmb.controlplane.db.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Database-backed leadership lease that needs no synchronised clocks and only plain UPDATEs, so it
 * behaves the same on PostgreSQL and H2.
 *
 * <p>Row {@code leader_lease(name, holder, epoch, heartbeat, fence)}. The holder bumps {@code heartbeat}
 * every renew interval with {@code WHERE holder = me AND epoch = myEpoch}. A challenger never compares
 * timestamps across hosts: it remembers the (epoch, heartbeat) pair it last read and the time, on its own
 * monotonic clock, at which it first saw that pair; once the pair has not changed for {@code ttl} it takes
 * over with a compare-and-set on (epoch, heartbeat), incrementing epoch. The holder considers itself leader
 * only while less than {@code ttl - margin} has elapsed since the start of its last successful renewal,
 * which is strictly before any challenger can take over (assuming clock rates agree within the margin).
 *
 * <p>Fencing: every leader write transaction first runs {@link #fence(Connection)}, an UPDATE on the
 * lease row conditioned on (holder, epoch). A deposed leader's write therefore fails, and because the row
 * lock serialises it with the takeover, everything a deposed leader managed to commit is visible to the
 * new leader when it loads its state after acquiring.
 */
public final class LeaderLease
{
    private static final Logger LOG = LoggerFactory.getLogger(LeaderLease.class);

    public record Info(String holder, String holderUrl, long epoch, long heartbeat, long renewedMs)
    {
    }

    public static final class NotLeaderException extends RuntimeException
    {
        public NotLeaderException(final String message)
        {
            super(message);
        }
    }

    private final Database db;
    private final String name;
    private final String instanceId;
    private final String url;
    private final Clock clock;
    private final long ttlNs;
    private final long renewNs;
    private final long validNs;

    private volatile boolean held;
    private volatile long epoch;
    private volatile long lastRenewStartNs;
    private long seenEpoch = -1;
    private long seenHeartbeat = -1;
    private long seenSinceNs;
    private long nextActionNs = Long.MIN_VALUE;

    public LeaderLease(final Database db, final String name, final String instanceId, final String url, final Clock clock,
        final long ttlMs, final long renewMs, final long marginMs)
    {
        if (marginMs >= ttlMs || renewMs >= ttlMs - marginMs)
        {
            throw new IllegalArgumentException("need renewMs < ttlMs - marginMs");
        }
        this.db = db;
        this.name = name;
        this.instanceId = instanceId;
        this.url = url;
        this.clock = clock;
        this.ttlNs = ttlMs * 1_000_000L;
        this.renewNs = renewMs * 1_000_000L;
        this.validNs = (ttlMs - marginMs) * 1_000_000L;
    }

    /** Renew or try to acquire; does nothing until the renew interval has elapsed since the last attempt. */
    public synchronized void tick()
    {
        final long now = clock.monoNs();
        if (now < nextActionNs)
        {
            return;
        }
        nextActionNs = now + renewNs;
        try
        {
            ensureRow();
            if (held)
            {
                renew(now);
            }
            else
            {
                challenge(now);
            }
        }
        catch (final RuntimeException e)
        {
            LOG.warn("lease {} tick failed: {}", name, e.toString());
        }
    }

    private void renew(final long start)
    {
        final int n = db.read(c -> Database.update(c, "UPDATE leader_lease SET heartbeat = heartbeat + 1, renewed_ms = ?, " +
            "holder_url = ? WHERE name = ? AND holder = ? AND epoch = ?", clock.epochMs(), url, name, instanceId, epoch));
        if (n == 1)
        {
            lastRenewStartNs = start;
        }
        else
        {
            LOG.warn("lease {} lost by {} (epoch {})", name, instanceId, epoch);
            held = false;
        }
    }

    private void challenge(final long now)
    {
        final Info row = info();
        if (row == null)
        {
            return;
        }
        if (row.holder() == null)
        {
            tryAcquire(row, now);
            return;
        }
        if (row.epoch() != seenEpoch || row.heartbeat() != seenHeartbeat)
        {
            seenEpoch = row.epoch();
            seenHeartbeat = row.heartbeat();
            seenSinceNs = now;
            return;
        }
        if (now - seenSinceNs >= ttlNs)
        {
            LOG.info("lease {} held by {} has not been renewed for {} ms; taking over", name, row.holder(), ttlNs / 1_000_000);
            tryAcquire(row, now);
        }
    }

    private void tryAcquire(final Info row, final long start)
    {
        final int n = db.read(c -> Database.update(c, "UPDATE leader_lease SET holder = ?, holder_url = ?, epoch = epoch + 1, " +
                "heartbeat = heartbeat + 1, renewed_ms = ? WHERE name = ? AND epoch = ? AND heartbeat = ?",
            instanceId, url, clock.epochMs(), name, row.epoch(), row.heartbeat()));
        if (n == 1)
        {
            epoch = row.epoch() + 1;
            lastRenewStartNs = start;
            held = true;
            LOG.info("lease {} acquired by {} (epoch {})", name, instanceId, epoch);
        }
    }

    private void ensureRow()
    {
        db.read(c ->
        {
            if (Database.queryLong(c, "SELECT COUNT(*) FROM leader_lease WHERE name = ?", name) == 0)
            {
                try
                {
                    Database.update(c, "INSERT INTO leader_lease (name, holder, holder_url, epoch, heartbeat, fence, renewed_ms) " +
                        "VALUES (?, NULL, NULL, 0, 0, 0, ?)", name, clock.epochMs());
                }
                catch (final SQLException e)
                {
                    if (!Database.isUniqueViolation(e))
                    {
                        throw e;
                    }
                }
            }
            return null;
        });
    }

    /** True while the lease is held and was renewed recently enough that no challenger can have taken over. */
    public boolean isLeader()
    {
        return held && clock.monoNs() - lastRenewStartNs < validNs;
    }

    public long epoch()
    {
        return epoch;
    }

    public String instanceId()
    {
        return instanceId;
    }

    /** Call first inside every leader write transaction. Throws if this instance no longer holds the lease. */
    public void fence(final Connection c) throws SQLException
    {
        if (!isLeader())
        {
            throw new NotLeaderException("lease not held or expired locally");
        }
        if (Database.update(c, "UPDATE leader_lease SET fence = fence + 1 WHERE name = ? AND holder = ? AND epoch = ?",
            name, instanceId, epoch) != 1)
        {
            held = false;
            throw new NotLeaderException("lease epoch " + epoch + " no longer held by " + instanceId);
        }
    }

    /** Clean shutdown: hand over immediately instead of waiting for the ttl. */
    public synchronized void release()
    {
        if (!held)
        {
            return;
        }
        held = false;
        try
        {
            db.read(c -> Database.update(c, "UPDATE leader_lease SET holder = NULL, holder_url = NULL, heartbeat = heartbeat + 1 " +
                "WHERE name = ? AND holder = ? AND epoch = ?", name, instanceId, epoch));
            LOG.info("lease {} released by {}", name, instanceId);
        }
        catch (final RuntimeException e)
        {
            LOG.warn("lease release failed: {}", e.toString());
        }
    }

    public Info info()
    {
        return db.read(c -> Database.queryOne(c, "SELECT holder, holder_url, epoch, heartbeat, renewed_ms FROM leader_lease " +
            "WHERE name = ?", rs -> new Info(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getLong(4), rs.getLong(5)), name));
    }
}
