package io.lmb.controlplane.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * HikariCP pool plus a few JDBC helpers. Works with PostgreSQL and with H2 (any mode): the SQL used
 * by the control plane is the common subset of both, and the few differences go through {@link Dialect}.
 */
public final class Database implements AutoCloseable
{
    public enum Dialect
    {
        H2, POSTGRESQL
    }

    @FunctionalInterface
    public interface SqlWork<T>
    {
        T run(Connection c) throws SQLException;
    }

    @FunctionalInterface
    public interface RowMapper<T>
    {
        T map(ResultSet rs) throws SQLException;
    }

    /** Typed SQL NULL for parameters (PostgreSQL wants a type for nulls). */
    public record Null(int sqlType)
    {
    }

    public static final Null NULL_BIGINT = new Null(Types.BIGINT);
    public static final Null NULL_INT = new Null(Types.INTEGER);
    public static final Null NULL_VARCHAR = new Null(Types.VARCHAR);

    private final HikariDataSource ds;
    private final Dialect dialect;

    private Database(final HikariDataSource ds, final Dialect dialect)
    {
        this.ds = ds;
        this.dialect = dialect;
    }

    public static Database open(final String url, final String user, final String password, final int poolSize)
    {
        final HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(url);
        hc.setUsername(user);
        hc.setPassword(password);
        hc.setMaximumPoolSize(Math.max(2, poolSize));
        hc.setMinimumIdle(1);
        hc.setPoolName("lmb-controlplane");
        hc.setConnectionTimeout(5_000);
        hc.setAutoCommit(true);
        final HikariDataSource ds = new HikariDataSource(hc);
        final Dialect dialect;
        try (Connection c = ds.getConnection())
        {
            final String product = c.getMetaData().getDatabaseProductName().toLowerCase();
            dialect = product.contains("postgres") ? Dialect.POSTGRESQL : Dialect.H2;
        }
        catch (final SQLException e)
        {
            ds.close();
            throw new DbException("cannot connect to " + url, e);
        }
        return new Database(ds, dialect);
    }

    public Dialect dialect()
    {
        return dialect;
    }

    public String nextvalSql(final String sequence)
    {
        return dialect == Dialect.POSTGRESQL ? "SELECT nextval('" + sequence + "')" : "SELECT NEXT VALUE FOR " + sequence;
    }

    /** Runs work in one transaction; commits on success, rolls back on any exception. */
    public <T> T tx(final SqlWork<T> work)
    {
        try (Connection c = ds.getConnection())
        {
            c.setAutoCommit(false);
            try
            {
                final T result = work.run(c);
                c.commit();
                return result;
            }
            catch (final Throwable t)
            {
                try
                {
                    c.rollback();
                }
                catch (final SQLException ignore)
                {
                    // the original failure matters more
                }
                throw t;
            }
            finally
            {
                c.setAutoCommit(true);
            }
        }
        catch (final SQLException e)
        {
            throw new DbException(e.getMessage(), e);
        }
    }

    /** Runs work on an auto-commit connection. */
    public <T> T read(final SqlWork<T> work)
    {
        try (Connection c = ds.getConnection())
        {
            return work.run(c);
        }
        catch (final SQLException e)
        {
            throw new DbException(e.getMessage(), e);
        }
    }

    public static int update(final Connection c, final String sql, final Object... params) throws SQLException
    {
        try (PreparedStatement ps = c.prepareStatement(sql))
        {
            bind(ps, params);
            return ps.executeUpdate();
        }
    }

    public static <T> List<T> query(final Connection c, final String sql, final RowMapper<T> mapper, final Object... params)
        throws SQLException
    {
        try (PreparedStatement ps = c.prepareStatement(sql))
        {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery())
            {
                final List<T> out = new ArrayList<>();
                while (rs.next())
                {
                    out.add(mapper.map(rs));
                }
                return out;
            }
        }
    }

    /** First row or null. */
    public static <T> T queryOne(final Connection c, final String sql, final RowMapper<T> mapper, final Object... params)
        throws SQLException
    {
        final List<T> rows = query(c, sql, mapper, params);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public static long queryLong(final Connection c, final String sql, final Object... params) throws SQLException
    {
        final Long v = queryOne(c, sql, rs -> rs.getLong(1), params);
        return v == null ? 0 : v;
    }

    /** Insert returning the generated identity column "id". */
    public static long insertReturningId(final Connection c, final String sql, final Object... params) throws SQLException
    {
        try (PreparedStatement ps = c.prepareStatement(sql, new String[] {"id"}))
        {
            bind(ps, params);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys())
            {
                if (!rs.next())
                {
                    throw new SQLException("no generated key");
                }
                return rs.getLong(1);
            }
        }
    }

    public static boolean isUniqueViolation(final Throwable t)
    {
        Throwable e = t;
        while (e != null)
        {
            if (e instanceof SQLException se && "23505".equals(se.getSQLState()))
            {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

    private static void bind(final PreparedStatement ps, final Object... params) throws SQLException
    {
        for (int i = 0; i < params.length; i++)
        {
            final Object p = params[i];
            final int idx = i + 1;
            if (p == null)
            {
                ps.setNull(idx, Types.VARCHAR);
            }
            else if (p instanceof Null n)
            {
                ps.setNull(idx, n.sqlType());
            }
            else if (p instanceof Long l)
            {
                ps.setLong(idx, l);
            }
            else if (p instanceof Integer n)
            {
                ps.setInt(idx, n);
            }
            else if (p instanceof Boolean b)
            {
                ps.setBoolean(idx, b);
            }
            else if (p instanceof String s)
            {
                ps.setString(idx, s);
            }
            else
            {
                ps.setObject(idx, p);
            }
        }
    }

    @Override
    public void close()
    {
        ds.close();
    }

    public static final class DbException extends RuntimeException
    {
        public DbException(final String message, final Throwable cause)
        {
            super(message, cause);
        }
    }
}
