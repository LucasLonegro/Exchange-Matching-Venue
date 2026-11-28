package io.lmb.access;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.lmb.common.access.AccessDirectory;
import io.lmb.common.access.Scope;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * The access service's tables ({@code access_schema.sql}). Every change runs in one transaction that also bumps
 * {@code access_meta.version} (the row lock serialises writers, so two service instances on one database are safe)
 * and writes an {@code access_audit} row. {@link #snapshot()} reads the version and every row under that same
 * lock, so a snapshot is always the state after some whole number of changes.
 */
public final class AccessStore implements AutoCloseable
{
    /** Thrown for a request the current state does not allow; the REST layer maps {@code status}. */
    public static final class Refused extends RuntimeException
    {
        final int status;

        Refused(final int status, final String message)
        {
            super(message);
            this.status = status;
        }
    }

    @FunctionalInterface
    interface Work<T>
    {
        T run(Connection c) throws SQLException;
    }

    public record FirmRow(int firmId, String name, String tier, boolean enabled, long createdMs, long updatedMs)
    {
    }

    public record SessionRow(long sessionId, int firmId, String label, String tier, boolean enabled, long createdMs, long updatedMs)
    {
    }

    public record KeyRow(String keyId, int firmId, long sessionId, String sha256, Set<Scope> scopes, String label,
        long createdMs, long notAfterMs, long revokedMs, String replacedBy)
    {
    }

    public record AuditRow(long auditId, long tsMs, String actor, String action, String detail)
    {
    }

    /** A key as issued: the plaintext exists only here, once. */
    public record Issued(KeyRow key, String apiKey)
    {
    }

    private final HikariDataSource ds;

    public AccessStore(final String url, final String user, final String password)
    {
        final HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(url);
        hc.setUsername(user);
        hc.setPassword(password);
        hc.setMaximumPoolSize(4);
        hc.setAutoCommit(false);
        hc.setPoolName("access");
        ds = new HikariDataSource(hc);
        createSchema();
    }

    private void createSchema()
    {
        final String script;
        try (InputStream in = AccessStore.class.getResourceAsStream("access_schema.sql"))
        {
            script = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e);
        }
        final StringBuilder sql = new StringBuilder();
        for (final String line : script.split("\n"))
        {
            final int dash = line.indexOf("--");
            sql.append(dash >= 0 ? line.substring(0, dash) : line).append('\n');
        }
        tx(c ->
        {
            try (Statement st = c.createStatement())
            {
                for (final String stmt : sql.toString().split(";"))
                {
                    if (!stmt.isBlank())
                    {
                        st.execute(stmt);
                    }
                }
            }
            if (!exists(c, "SELECT 1 FROM access_meta WHERE id = 1"))
            {
                update(c, "INSERT INTO access_meta (id, version, bootstrapped_ms, updated_ms) VALUES (1, 1, 0, ?)", System.currentTimeMillis());
            }
            return null;
        });
    }

    <T> T tx(final Work<T> work)
    {
        try (Connection c = ds.getConnection())
        {
            try
            {
                final T out = work.run(c);
                c.commit();
                return out;
            }
            catch (final SQLException | RuntimeException e)
            {
                c.rollback();
                throw e;
            }
        }
        catch (final SQLException e)
        {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /** Locks the meta row: the start of every change and snapshot. */
    private static long lockVersion(final Connection c) throws SQLException
    {
        try (PreparedStatement ps = c.prepareStatement("SELECT version FROM access_meta WHERE id = 1 FOR UPDATE");
             ResultSet rs = ps.executeQuery())
        {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** One change: lock, work, bump the version, audit. */
    <T> T change(final String actor, final String action, final Work<T> work, final java.util.function.Function<T, String> detail)
    {
        return tx(c ->
        {
            lockVersion(c);
            final T out = work.run(c);
            final long now = System.currentTimeMillis();
            update(c, "UPDATE access_meta SET version = version + 1, updated_ms = ? WHERE id = 1", now);
            String d = detail.apply(out);
            d = d.length() > 4000 ? d.substring(0, 4000) : d;
            update(c, "INSERT INTO access_audit (ts_ms, actor, action, detail) VALUES (?, ?, ?, ?)", now, actor, action, d);
            return out;
        });
    }

    public long version()
    {
        return tx(c ->
        {
            try (PreparedStatement ps = c.prepareStatement("SELECT version FROM access_meta WHERE id = 1"); ResultSet rs = ps.executeQuery())
            {
                rs.next();
                return rs.getLong(1);
            }
        });
    }

    // ------------------------------------------------------------------ bootstrap

    public static final List<AccessDirectory.Tier> DEFAULT_TIERS = List.of(
        new AccessDirectory.Tier("restricted", 10, 10, 50, 50),
        new AccessDirectory.Tier(AccessDirectory.DEFAULT_TIER, 500, 100, 2_000, 400),
        new AccessDirectory.Tier("bot", 5_000, 1_000, 20_000, 4_000));

    /**
     * On a database the service has never bootstrapped: the default tiers. Exactly once. Teams, sessions and keys
     * only ever come through the operators' API: there is no file of credentials.
     * @return true if this call created them
     */
    public boolean bootstrapIfEmpty()
    {
        return tx(c ->
        {
            lockVersion(c);
            try (PreparedStatement ps = c.prepareStatement("SELECT bootstrapped_ms FROM access_meta WHERE id = 1");
                 ResultSet rs = ps.executeQuery())
            {
                rs.next();
                if (rs.getLong(1) != 0)
                {
                    return false;
                }
            }
            final long now = System.currentTimeMillis();
            for (final AccessDirectory.Tier t : DEFAULT_TIERS)
            {
                if (!exists(c, "SELECT 1 FROM access_tier WHERE name = ?", t.name()))
                {
                    putTier(c, t, now);
                }
            }
            update(c, "UPDATE access_meta SET version = version + 1, bootstrapped_ms = ?, updated_ms = ? WHERE id = 1", now, now);
            update(c, "INSERT INTO access_audit (ts_ms, actor, action, detail) VALUES (?, ?, ?, ?)", now, "bootstrap", "BOOTSTRAP",
                "default tiers");
            return true;
        });
    }

    // ------------------------------------------------------------------ snapshot

    public AccessDirectory snapshot()
    {
        return tx(c ->
        {
            final long version = lockVersion(c);
            final List<AccessDirectory.Tier> tiers = new ArrayList<>();
            for (final AccessDirectory.Tier t : tiers(c))
            {
                tiers.add(t);
            }
            final List<AccessDirectory.FirmEntry> firms = new ArrayList<>();
            for (final FirmRow f : firms(c))
            {
                firms.add(new AccessDirectory.FirmEntry(f.firmId(), f.name(), f.tier(), f.enabled()));
            }
            final List<AccessDirectory.SessionEntry> sessions = new ArrayList<>();
            for (final SessionRow s : sessions(c, null))
            {
                sessions.add(new AccessDirectory.SessionEntry(s.sessionId(), s.firmId(), s.label(), s.tier(), s.enabled()));
            }
            final long now = System.currentTimeMillis();
            final List<AccessDirectory.KeyEntry> keys = new ArrayList<>();
            for (final KeyRow k : keys(c, null))
            {
                // revoked keys and keys whose grace period is over are left out altogether
                if (k.revokedMs() == 0 && (k.notAfterMs() == 0 || k.notAfterMs() > now))
                {
                    keys.add(new AccessDirectory.KeyEntry(k.keyId(), k.sha256(), k.firmId(), k.sessionId(), k.scopes(), k.notAfterMs()));
                }
            }
            return new AccessDirectory(version, now, tiers, firms, sessions, keys);
        });
    }

    // ------------------------------------------------------------------ reads

    public List<FirmRow> firms()
    {
        return tx(AccessStore::firms);
    }

    public FirmRow firm(final int firmId)
    {
        return tx(c -> firm(c, firmId));
    }

    public List<SessionRow> sessions(final Integer firmId)
    {
        return tx(c -> sessions(c, firmId));
    }

    public List<KeyRow> keys(final Integer firmId)
    {
        return tx(c -> keys(c, firmId));
    }

    public List<AccessDirectory.Tier> tiers()
    {
        return tx(AccessStore::tiers);
    }

    public List<AuditRow> audit(final int limit)
    {
        return tx(c ->
        {
            final List<AuditRow> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                "SELECT audit_id, ts_ms, actor, action, detail FROM access_audit ORDER BY audit_id DESC LIMIT ?"))
            {
                ps.setInt(1, limit);
                try (ResultSet rs = ps.executeQuery())
                {
                    while (rs.next())
                    {
                        out.add(new AuditRow(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getString(5)));
                    }
                }
            }
            return out;
        });
    }

    private static List<FirmRow> firms(final Connection c) throws SQLException
    {
        final List<FirmRow> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT firm_id, name, tier, enabled, created_ms, updated_ms FROM access_firm ORDER BY firm_id"); ResultSet rs = ps.executeQuery())
        {
            while (rs.next())
            {
                out.add(new FirmRow(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getBoolean(4), rs.getLong(5), rs.getLong(6)));
            }
        }
        return out;
    }

    private static FirmRow firm(final Connection c, final int firmId) throws SQLException
    {
        for (final FirmRow f : firms(c))
        {
            if (f.firmId() == firmId)
            {
                return f;
            }
        }
        return null;
    }

    private static List<SessionRow> sessions(final Connection c, final Integer firmId) throws SQLException
    {
        final List<SessionRow> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT session_id, firm_id, label, tier, enabled, created_ms, updated_ms FROM access_session" +
                (firmId == null ? "" : " WHERE firm_id = ?") + " ORDER BY session_id"))
        {
            if (firmId != null)
            {
                ps.setInt(1, firmId);
            }
            try (ResultSet rs = ps.executeQuery())
            {
                while (rs.next())
                {
                    out.add(new SessionRow(rs.getLong(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getBoolean(5),
                        rs.getLong(6), rs.getLong(7)));
                }
            }
        }
        return out;
    }

    private static SessionRow session(final Connection c, final long sessionId) throws SQLException
    {
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT session_id, firm_id, label, tier, enabled, created_ms, updated_ms FROM access_session WHERE session_id = ?"))
        {
            ps.setLong(1, sessionId);
            try (ResultSet rs = ps.executeQuery())
            {
                return rs.next() ? new SessionRow(rs.getLong(1), rs.getInt(2), rs.getString(3), rs.getString(4), rs.getBoolean(5),
                    rs.getLong(6), rs.getLong(7)) : null;
            }
        }
    }

    private static List<KeyRow> keys(final Connection c, final Integer firmId) throws SQLException
    {
        final List<KeyRow> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT key_id, firm_id, session_id, sha256, scopes, label, created_ms, not_after_ms, revoked_ms, replaced_by FROM access_key" +
                (firmId == null ? "" : " WHERE firm_id = ?") + " ORDER BY created_ms, key_id"))
        {
            if (firmId != null)
            {
                ps.setInt(1, firmId);
            }
            try (ResultSet rs = ps.executeQuery())
            {
                while (rs.next())
                {
                    out.add(keyRow(rs));
                }
            }
        }
        return out;
    }

    private static KeyRow key(final Connection c, final String keyId) throws SQLException
    {
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT key_id, firm_id, session_id, sha256, scopes, label, created_ms, not_after_ms, revoked_ms, replaced_by FROM access_key " +
                "WHERE key_id = ?"))
        {
            ps.setString(1, keyId);
            try (ResultSet rs = ps.executeQuery())
            {
                return rs.next() ? keyRow(rs) : null;
            }
        }
    }

    private static KeyRow keyRow(final ResultSet rs) throws SQLException
    {
        return new KeyRow(rs.getString(1), rs.getInt(2), rs.getLong(3), rs.getString(4), parseScopes(rs.getString(5)),
            rs.getString(6), rs.getLong(7), rs.getLong(8), rs.getLong(9), rs.getString(10));
    }

    private static List<AccessDirectory.Tier> tiers(final Connection c) throws SQLException
    {
        final List<AccessDirectory.Tier> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(
            "SELECT name, order_rate, order_burst, message_rate, message_burst FROM access_tier ORDER BY name"); ResultSet rs = ps.executeQuery())
        {
            while (rs.next())
            {
                out.add(new AccessDirectory.Tier(rs.getString(1), rs.getDouble(2), rs.getInt(3), rs.getDouble(4), rs.getInt(5)));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ changes

    /** A member to create with a new team or on its own. sessionId 0: the next free id. */
    public record NewSession(long sessionId, String label, String tier)
    {
    }

    public record Registration(FirmRow firm, List<SessionRow> sessions, List<Issued> keys)
    {
    }

    /**
     * Registers a team: the firm, its members' sessions and one key per member (bound to that member's session,
     * with {@code scopes}; null: sessions without keys). The keys are returned in plaintext, once.
     */
    public Registration registerFirm(final String actor, final int firmId, final String name, final String tier,
        final List<NewSession> members, final Set<Scope> scopes)
    {
        return change(actor, "REGISTER_FIRM", c ->
        {
            if (firm(c, firmId) != null)
            {
                throw new Refused(409, "firm " + firmId + " already exists");
            }
            requireTier(c, tier);
            final long now = System.currentTimeMillis();
            update(c, "INSERT INTO access_firm (firm_id, name, tier, enabled, created_ms, updated_ms) VALUES (?, ?, ?, TRUE, ?, ?)",
                firmId, name, tier, now, now);
            final List<SessionRow> sessions = new ArrayList<>();
            final List<Issued> keys = new ArrayList<>();
            for (final NewSession m : members)
            {
                final SessionRow s = insertSession(c, firmId, m, now);
                sessions.add(s);
                if (scopes != null)
                {
                    keys.add(issue(c, firmId, s.sessionId(), scopes, "member " + s.label(), now, 0));
                }
            }
            return new Registration(firm(c, firmId), sessions, keys);
        }, r -> "firmId=" + firmId + " name=" + name + " tier=" + tier + " sessions=" + r.sessions().size());
    }

    public FirmRow updateFirm(final String actor, final int firmId, final String name, final String tier, final Boolean enabled)
    {
        return change(actor, "UPDATE_FIRM", c ->
        {
            final FirmRow f = firm(c, firmId);
            if (f == null)
            {
                throw new Refused(404, "unknown firm " + firmId);
            }
            if (tier != null)
            {
                requireTier(c, tier);
            }
            update(c, "UPDATE access_firm SET name = ?, tier = ?, enabled = ?, updated_ms = ? WHERE firm_id = ?",
                name != null ? name : f.name(), tier != null ? tier : f.tier(), enabled != null ? enabled : f.enabled(),
                System.currentTimeMillis(), firmId);
            return firm(c, firmId);
        }, f -> "firmId=" + firmId + " name=" + f.name() + " tier=" + f.tier() + " enabled=" + f.enabled());
    }

    /** A member's session, and with {@code scopes} non-null a key bound to it. */
    public Registration addSession(final String actor, final int firmId, final NewSession m, final Set<Scope> scopes)
    {
        return change(actor, "ADD_SESSION", c ->
        {
            if (firm(c, firmId) == null)
            {
                throw new Refused(404, "unknown firm " + firmId);
            }
            final long now = System.currentTimeMillis();
            final SessionRow s = insertSession(c, firmId, m, now);
            final List<Issued> keys = new ArrayList<>();
            if (scopes != null)
            {
                keys.add(issue(c, firmId, s.sessionId(), scopes, "member " + s.label(), now, 0));
            }
            return new Registration(firm(c, firmId), List.of(s), keys);
        }, r -> "firmId=" + firmId + " sessionId=" + r.sessions().get(0).sessionId() + " label=" + r.sessions().get(0).label());
    }

    /** {@code tier}: null keeps it, empty string clears it (the firm's tier applies). */
    public SessionRow updateSession(final String actor, final long sessionId, final String label, final String tier, final Boolean enabled)
    {
        return change(actor, "UPDATE_SESSION", c ->
        {
            final SessionRow s = session(c, sessionId);
            if (s == null)
            {
                throw new Refused(404, "unknown session " + sessionId);
            }
            final String newTier = tier == null ? s.tier() : tier.isEmpty() ? null : tier;
            if (newTier != null)
            {
                requireTier(c, newTier);
            }
            update(c, "UPDATE access_session SET label = ?, tier = ?, enabled = ?, updated_ms = ? WHERE session_id = ?",
                label != null ? label : s.label(), newTier == null ? new NullVarchar() : newTier,
                enabled != null ? enabled : s.enabled(), System.currentTimeMillis(), sessionId);
            return session(c, sessionId);
        }, s -> "sessionId=" + sessionId + " label=" + s.label() + " tier=" + s.tier() + " enabled=" + s.enabled());
    }

    /** A new key; {@code sessionId} 0 = firm-wide; {@code notAfterMs} 0 = no expiry. */
    public Issued issueKey(final String actor, final int firmId, final long sessionId, final Set<Scope> scopes, final String label,
        final long notAfterMs)
    {
        return change(actor, "ISSUE_KEY", c ->
        {
            if (firm(c, firmId) == null)
            {
                throw new Refused(404, "unknown firm " + firmId);
            }
            if (sessionId != 0)
            {
                final SessionRow s = session(c, sessionId);
                if (s == null || s.firmId() != firmId)
                {
                    throw new Refused(422, "session " + sessionId + " is not a session of firm " + firmId);
                }
            }
            return issue(c, firmId, sessionId, scopes, label, System.currentTimeMillis(), notAfterMs);
        }, k -> "keyId=" + k.key().keyId() + " firmId=" + firmId + " sessionId=" + sessionId + " scopes=" + scopeText(scopes));
    }

    /**
     * Issues a replacement with the same firm, session and scopes, and lets the old key work for {@code graceMs} more
     * (never longer than it would have anyway). Connections made with the old key end when it expires.
     */
    public Issued rotateKey(final String actor, final String keyId, final long graceMs)
    {
        return change(actor, "ROTATE_KEY", c ->
        {
            final KeyRow old = key(c, keyId);
            final long now = System.currentTimeMillis();
            if (old == null)
            {
                throw new Refused(404, "unknown key " + keyId);
            }
            if (old.revokedMs() != 0 || (old.notAfterMs() != 0 && old.notAfterMs() <= now) || old.replacedBy() != null)
            {
                throw new Refused(422, "key " + keyId + " is revoked, expired or already rotated");
            }
            final Issued fresh = issue(c, old.firmId(), old.sessionId(), old.scopes(), old.label(), now, 0);
            final long end = now + Math.max(0, graceMs);
            update(c, "UPDATE access_key SET not_after_ms = ?, replaced_by = ? WHERE key_id = ?",
                old.notAfterMs() == 0 ? end : Math.min(old.notAfterMs(), end), fresh.key().keyId(), keyId);
            return fresh;
        }, k -> "keyId=" + keyId + " replacedBy=" + k.key().keyId() + " graceMs=" + graceMs);
    }

    /** At once: connections made with it end at the gateways' next check. */
    public KeyRow revokeKey(final String actor, final String keyId)
    {
        return change(actor, "REVOKE_KEY", c ->
        {
            final KeyRow k = key(c, keyId);
            if (k == null)
            {
                throw new Refused(404, "unknown key " + keyId);
            }
            if (k.revokedMs() == 0)
            {
                update(c, "UPDATE access_key SET revoked_ms = ? WHERE key_id = ?", System.currentTimeMillis(), keyId);
            }
            return key(c, keyId);
        }, k -> "keyId=" + keyId);
    }

    public AccessDirectory.Tier putTier(final String actor, final AccessDirectory.Tier t)
    {
        return change(actor, "PUT_TIER", c ->
        {
            putTier(c, t, System.currentTimeMillis());
            return t;
        }, x -> "tier=" + t);
    }

    // ------------------------------------------------------------------ helpers

    private static void putTier(final Connection c, final AccessDirectory.Tier t, final long now) throws SQLException
    {
        if (exists(c, "SELECT 1 FROM access_tier WHERE name = ?", t.name()))
        {
            update(c, "UPDATE access_tier SET order_rate = ?, order_burst = ?, message_rate = ?, message_burst = ?, updated_ms = ? WHERE name = ?",
                t.orderRatePerSec(), t.orderBurst(), t.messageRatePerSec(), t.messageBurst(), now, t.name());
        }
        else
        {
            update(c, "INSERT INTO access_tier (name, order_rate, order_burst, message_rate, message_burst, updated_ms) VALUES (?, ?, ?, ?, ?, ?)",
                t.name(), t.orderRatePerSec(), t.orderBurst(), t.messageRatePerSec(), t.messageBurst(), now);
        }
    }

    private static void requireTier(final Connection c, final String tier) throws SQLException
    {
        if (!exists(c, "SELECT 1 FROM access_tier WHERE name = ?", tier))
        {
            throw new Refused(422, "unknown tier " + tier);
        }
    }

    private static SessionRow insertSession(final Connection c, final int firmId, final NewSession m, final long now) throws SQLException
    {
        long id = m.sessionId();
        if (id == 0)
        {
            try (PreparedStatement ps = c.prepareStatement("SELECT COALESCE(MAX(session_id), 0) FROM access_session");
                 ResultSet rs = ps.executeQuery())
            {
                rs.next();
                id = Math.max(rs.getLong(1) + 1, 10_001);
            }
        }
        else if (session(c, id) != null)
        {
            throw new Refused(409, "session " + id + " already exists");
        }
        if (m.tier() != null)
        {
            requireTier(c, m.tier());
        }
        update(c, "INSERT INTO access_session (session_id, firm_id, label, tier, enabled, created_ms, updated_ms) VALUES (?, ?, ?, ?, TRUE, ?, ?)",
            id, firmId, m.label(), m.tier() == null ? new NullVarchar() : m.tier(), now, now);
        return session(c, id);
    }

    private static Issued issue(final Connection c, final int firmId, final long sessionId, final Set<Scope> scopes, final String label,
        final long now, final long notAfterMs) throws SQLException
    {
        if (scopes.isEmpty())
        {
            throw new Refused(400, "a key needs at least one scope");
        }
        final String keyId = ApiKeys.newKeyId();
        final String apiKey = ApiKeys.newKey(keyId);
        insertKey(c, keyId, firmId, sessionId, AccessDirectory.sha256Hex(apiKey), scopes, label, now, notAfterMs);
        return new Issued(key(c, keyId), apiKey);
    }

    private static void insertKey(final Connection c, final String keyId, final int firmId, final long sessionId, final String sha256,
        final Set<Scope> scopes, final String label, final long now, final long notAfterMs) throws SQLException
    {
        update(c, "INSERT INTO access_key (key_id, firm_id, session_id, sha256, scopes, label, created_ms, not_after_ms, revoked_ms) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)", keyId, firmId, sessionId, sha256, scopeText(scopes), label, now, notAfterMs);
    }

    static String scopeText(final Set<Scope> scopes)
    {
        final StringJoiner j = new StringJoiner(",");
        for (final Scope s : scopes)
        {
            j.add(s.name());
        }
        return j.toString();
    }

    static Set<Scope> parseScopes(final String text)
    {
        final EnumSet<Scope> out = EnumSet.noneOf(Scope.class);
        for (final String s : text.split(","))
        {
            final Scope x = s.isBlank() ? null : Scope.parse(s);
            if (x != null)
            {
                out.add(x);
            }
        }
        return out;
    }

    private record NullVarchar()
    {
    }

    private static boolean exists(final Connection c, final String sql, final Object... args) throws SQLException
    {
        try (PreparedStatement ps = prepare(c, sql, args); ResultSet rs = ps.executeQuery())
        {
            return rs.next();
        }
    }

    private static int update(final Connection c, final String sql, final Object... args) throws SQLException
    {
        try (PreparedStatement ps = prepare(c, sql, args))
        {
            return ps.executeUpdate();
        }
    }

    private static PreparedStatement prepare(final Connection c, final String sql, final Object... args) throws SQLException
    {
        final PreparedStatement ps = c.prepareStatement(sql);
        for (int i = 0; i < args.length; i++)
        {
            if (args[i] instanceof NullVarchar)
            {
                ps.setNull(i + 1, java.sql.Types.VARCHAR);
            }
            else
            {
                ps.setObject(i + 1, args[i]);
            }
        }
        return ps;
    }

    /** For listings: a key without its hash. */
    static Map<String, Object> keyView(final KeyRow k, final long nowMs)
    {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("keyId", k.keyId());
        m.put("firmId", (long)k.firmId());
        m.put("sessionId", k.sessionId() == 0 ? null : k.sessionId());
        final List<Object> scopes = new ArrayList<>();
        for (final Scope s : k.scopes())
        {
            scopes.add(s.name());
        }
        m.put("scopes", scopes);
        m.put("label", k.label());
        m.put("createdMs", k.createdMs());
        m.put("notAfterMs", k.notAfterMs() == 0 ? null : k.notAfterMs());
        m.put("revokedMs", k.revokedMs() == 0 ? null : k.revokedMs());
        m.put("replacedBy", k.replacedBy());
        m.put("status", k.revokedMs() != 0 ? "REVOKED" : k.notAfterMs() != 0 && k.notAfterMs() <= nowMs ? "EXPIRED" :
            k.notAfterMs() != 0 ? "EXPIRING" : "ACTIVE");
        return m;
    }

    @Override
    public void close()
    {
        ds.close();
    }
}
