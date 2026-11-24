package io.lmb.posttrade.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Minimal forward-only schema migration run at start-up: classpath scripts
 * {@code io/lmb/posttrade/store/V<n>__*.sql} listed in {@link #SCRIPTS}, applied in order, each recorded in
 * {@code pt_schema_version}. Statements are separated by ';' at end of line; '--' comments are dropped.
 * Works on PostgreSQL and H2 (PostgreSQL mode).
 */
public final class SchemaMigrator
{
    private static final Logger LOG = LoggerFactory.getLogger(SchemaMigrator.class);

    /** Ordered; index + 1 is the version. Append only. */
    static final String[] SCRIPTS = {"V1__trade_store.sql"};

    private SchemaMigrator()
    {
    }

    public static int latestVersion()
    {
        return SCRIPTS.length;
    }

    /** Applies missing versions; returns the schema version afterwards. Fails if the database is newer. */
    public static int migrate(final Connection c) throws SQLException
    {
        final boolean autoCommit = c.getAutoCommit();
        c.setAutoCommit(false);
        try
        {
            try (Statement st = c.createStatement())
            {
                st.execute("CREATE TABLE IF NOT EXISTS pt_schema_version (version INT NOT NULL PRIMARY KEY, " +
                    "script VARCHAR(200) NOT NULL, applied_at TIMESTAMP NOT NULL)");
            }
            c.commit();
            int current = currentVersion(c);
            if (current > SCRIPTS.length)
            {
                throw new SQLException("database schema version " + current + " is newer than this build (" +
                    SCRIPTS.length + ")");
            }
            while (current < SCRIPTS.length)
            {
                final String script = SCRIPTS[current];
                final int version = current + 1;
                try (Statement st = c.createStatement())
                {
                    for (final String sql : statements(load(script)))
                    {
                        st.execute(sql);
                    }
                }
                try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO pt_schema_version (version, script, applied_at) VALUES (?, ?, CURRENT_TIMESTAMP)"))
                {
                    ps.setInt(1, version);
                    ps.setString(2, script);
                    ps.executeUpdate(); // a concurrent migrator fails here on the primary key and rolls back
                }
                c.commit();
                LOG.info("trade store schema migrated to version {} ({})", version, script);
                current = version;
            }
            return current;
        }
        catch (final SQLException | RuntimeException e)
        {
            c.rollback();
            throw e;
        }
        finally
        {
            c.setAutoCommit(autoCommit);
        }
    }

    static int currentVersion(final Connection c) throws SQLException
    {
        try (Statement st = c.createStatement();
            ResultSet rs = st.executeQuery("SELECT COALESCE(MAX(version), 0) FROM pt_schema_version"))
        {
            rs.next();
            return rs.getInt(1);
        }
    }

    static String[] statements(final String script)
    {
        final StringBuilder sb = new StringBuilder();
        for (final String line : script.split("\n"))
        {
            final int comment = line.indexOf("--");
            sb.append(comment >= 0 ? line.substring(0, comment) : line).append('\n');
        }
        return java.util.Arrays.stream(sb.toString().split(";\\s*\n"))
            .map(String::trim).map(s -> s.endsWith(";") ? s.substring(0, s.length() - 1).trim() : s)
            .filter(s -> !s.isEmpty()).toArray(String[]::new);
    }

    private static String load(final String script)
    {
        try (InputStream in = SchemaMigrator.class.getResourceAsStream(script))
        {
            if (in == null)
            {
                throw new IllegalStateException("missing migration script " + script);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (final IOException e)
        {
            throw new IllegalStateException("cannot read migration script " + script, e);
        }
    }
}
