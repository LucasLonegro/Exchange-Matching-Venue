package io.lmb.controlplane;

import io.lmb.common.FirmDirectory;
import io.lmb.common.SymbolDirectory;
import io.lmb.controlplane.db.Database;
import io.lmb.controlplane.db.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Loads the CSV reference data into an empty database, exactly once. The first statement of the
 * transaction claims {@code bootstrap_marker}; a second instance bootstrapping concurrently fails on
 * that primary key and rolls back. Afterwards the database is the source of truth and the CSV files
 * are ignored.
 */
public final class Bootstrap
{
    private static final Logger LOG = LoggerFactory.getLogger(Bootstrap.class);

    private Bootstrap()
    {
    }

    /** @return true if this call loaded the data */
    public static boolean bootstrapIfEmpty(
        final Database db, final SymbolDirectory symbols, final FirmDirectory firms, final String source, final long nowMs)
    {
        if (isBootstrapped(db))
        {
            return false;
        }
        try
        {
            db.tx(c ->
            {
                Database.update(c, "INSERT INTO bootstrap_marker (id, source, created_ms) VALUES (1, ?, ?)", source, nowMs);
                for (final SymbolDirectory.Symbol s : symbols.all())
                {
                    Store.upsertSymbol(c, new Store.SymbolRow(s.symbolId(), s.ticker(), s.shard(), s.tickSize(), s.lotSize(),
                        s.referencePrice(), s.priceBandBps(), s.maxOrderQty(), "ACTIVE", null, nowMs), nowMs);
                }
                for (final FirmDirectory.Firm f : firms.all())
                {
                    Store.insertFirm(c, new Store.FirmRow(f.firmId(), f.name(), f.apiKey(), true, f.sessionIds(),
                        f.creditLimit(), f.maxPositionPerSymbol(), nowMs), nowMs);
                }
                return null;
            });
            LOG.info("bootstrapped {} symbols and {} firms from {}", symbols.all().size(), firms.all().size(), source);
            return true;
        }
        catch (final RuntimeException e)
        {
            if (isBootstrapped(db))
            {
                LOG.info("another instance bootstrapped the database concurrently");
                return false;
            }
            throw e;
        }
    }

    public static boolean isBootstrapped(final Database db)
    {
        return db.read(c ->
        {
            try
            {
                return Database.queryLong(c, "SELECT COUNT(*) FROM bootstrap_marker") > 0;
            }
            catch (final SQLException e)
            {
                throw e;
            }
        });
    }
}
