package io.lmb.controlplane.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal versioned migration runner. Scripts live under {@code io/lmb/controlplane/db/} and are listed,
 * in order, in {@code migrations.txt} (file names {@code V<n>__<name>.sql}). Applied versions are
 * recorded in {@code schema_version}. Scripts are written idempotently (IF NOT EXISTS) so two instances
 * starting at once cannot break each other even on H2, where DDL is not transactional; on PostgreSQL
 * the run is additionally serialised with an advisory lock.
 */
public final class Migrator
{
    private static final Logger LOG = LoggerFactory.getLogger(Migrator.class);
    private static final String BASE = "/io/lmb/controlplane/db/";
    private static final long PG_LOCK_KEY = 0x4C4D4243_50L;

    private Migrator()
    {
    }

    /** @return number of migrations applied by this call */
    public static int migrate(final Database db)
    {
        return db.read(c ->
        {
            if (db.dialect() == Database.Dialect.POSTGRESQL)
            {
                Database.queryLong(c, "SELECT 1 FROM (SELECT pg_advisory_lock(?)) x", PG_LOCK_KEY);
            }
            try
            {
                return apply(c);
            }
            finally
            {
                if (db.dialect() == Database.Dialect.POSTGRESQL)
                {
                    Database.queryLong(c, "SELECT 1 FROM (SELECT pg_advisory_unlock(?)) x", PG_LOCK_KEY);
                }
            }
        });
    }

    private static int apply(final Connection c) throws SQLException
    {
        exec(c, "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, name VARCHAR(200) NOT NULL, " +
            "applied_ms BIGINT NOT NULL)");
        int applied = 0;
        for (final String file : resource("migrations.txt").lines().map(String::trim)
            .filter(l -> !l.isEmpty() && !l.startsWith("#")).toList())
        {
            final int version = Integer.parseInt(file.substring(1, file.indexOf("__")));
            if (Database.queryLong(c, "SELECT COUNT(*) FROM schema_version WHERE version = ?", version) > 0)
            {
                continue;
            }
            LOG.info("applying migration {}", file);
            for (final String statement : statements(resource(file)))
            {
                exec(c, statement);
            }
            try
            {
                Database.update(c, "INSERT INTO schema_version (version, name, applied_ms) VALUES (?, ?, ?)",
                    version, file, System.currentTimeMillis());
                applied++;
            }
            catch (final SQLException e)
            {
                if (!Database.isUniqueViolation(e))
                {
                    throw e;
                }
                // another instance recorded it concurrently; the scripts are idempotent
            }
        }
        return applied;
    }

    static List<String> statements(final String script)
    {
        final List<String> out = new ArrayList<>();
        final StringBuilder sb = new StringBuilder();
        for (final String raw : script.split("\n"))
        {
            final String line = raw.strip();
            if (line.isEmpty() || line.startsWith("--"))
            {
                continue;
            }
            sb.append(line).append(' ');
            if (line.endsWith(";"))
            {
                final String s = sb.toString().trim();
                out.add(s.substring(0, s.length() - 1));
                sb.setLength(0);
            }
        }
        if (!sb.toString().isBlank())
        {
            out.add(sb.toString().trim());
        }
        return out;
    }

    private static void exec(final Connection c, final String sql) throws SQLException
    {
        try (Statement st = c.createStatement())
        {
            st.execute(sql);
        }
    }

    private static String resource(final String name)
    {
        try (InputStream in = Migrator.class.getResourceAsStream(BASE + name))
        {
            if (in == null)
            {
                throw new IllegalStateException("missing migration resource " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e);
        }
    }
}
