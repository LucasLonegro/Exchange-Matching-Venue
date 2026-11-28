package io.lmb.common.access;

import io.lmb.common.Json;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * An immutable snapshot of who may connect: firms (teams), their sessions (members), API keys (SHA-256 hashes
 * only, never the keys) and throttle tiers. Published by the access service ({@code io.lmb.access}) and read by
 * the gateways and the drop copy, which check credentials once, at logon, and keep the resulting {@link Grant}
 * in the session's own state. Nothing here is ever seen by the matcher. Thread-safe (immutable).
 *
 * <p>A key belongs to one firm and either to one session or ({@code sessionId == 0}) to every session of the firm.
 * Its scopes say what a connection made with it may do. A key past {@code notAfterMs} (the end of a rotation's
 * grace period) is no longer valid; revoked keys are not in the snapshot at all.
 */
public final class AccessDirectory
{
    /** The tier a firm without one gets. */
    public static final String DEFAULT_TIER = "standard";

    public record Tier(String name, double orderRatePerSec, int orderBurst, double messageRatePerSec, int messageBurst)
    {
    }

    public record FirmEntry(int firmId, String name, String tier, boolean enabled)
    {
    }

    /** {@code tier} null: the firm's. */
    public record SessionEntry(long sessionId, int firmId, String label, String tier, boolean enabled)
    {
    }

    /** {@code sessionId} 0: any session of the firm. {@code notAfterMs} 0: no expiry. */
    public record KeyEntry(String keyId, String sha256, int firmId, long sessionId, Set<Scope> scopes, long notAfterMs)
    {
        public boolean activeAt(final long nowMs)
        {
            return notAfterMs == 0 || nowMs < notAfterMs;
        }
    }

    /**
     * What a logon was allowed: kept in the session's state for the life of the connection. {@code tier} is null
     * when the directory has no tier of that name (the gateway's configured limits apply).
     */
    public record Grant(int firmId, long sessionId, String keyId, Set<Scope> scopes, Tier tier, long notAfterMs)
    {
        public boolean has(final Scope s)
        {
            return scopes.contains(s);
        }
    }

    private static final AccessDirectory EMPTY = new AccessDirectory(0, 0, List.of(), List.of(), List.of(), List.of());

    private final long version;
    private final long generatedMs;
    private final Map<String, Tier> tiers = new TreeMap<>();
    private final Map<Integer, FirmEntry> firms = new TreeMap<>();
    private final Map<Long, SessionEntry> sessions = new TreeMap<>();
    private final Map<String, KeyEntry> keysByHash = new HashMap<>();
    private final Map<String, KeyEntry> keysById = new TreeMap<>();

    public AccessDirectory(
        final long version, final long generatedMs, final List<Tier> tiers, final List<FirmEntry> firms,
        final List<SessionEntry> sessions, final List<KeyEntry> keys)
    {
        this.version = version;
        this.generatedMs = generatedMs;
        for (final Tier t : tiers)
        {
            this.tiers.put(t.name(), t);
        }
        for (final FirmEntry f : firms)
        {
            if (this.firms.put(f.firmId(), f) != null)
            {
                throw new IllegalArgumentException("duplicate firm " + f.firmId());
            }
        }
        for (final SessionEntry s : sessions)
        {
            if (this.sessions.put(s.sessionId(), s) != null)
            {
                throw new IllegalArgumentException("duplicate session " + s.sessionId());
            }
        }
        for (final KeyEntry k : keys)
        {
            final KeyEntry copy = new KeyEntry(k.keyId(), k.sha256(), k.firmId(), k.sessionId(),
                Collections.unmodifiableSet(k.scopes().isEmpty() ? EnumSet.noneOf(Scope.class) : EnumSet.copyOf(k.scopes())),
                k.notAfterMs());
            if (keysByHash.put(copy.sha256(), copy) != null || keysById.put(copy.keyId(), copy) != null)
            {
                throw new IllegalArgumentException("duplicate key " + k.keyId());
            }
        }
    }

    public static AccessDirectory empty()
    {
        return EMPTY;
    }

    public long version()
    {
        return version;
    }

    public long generatedMs()
    {
        return generatedMs;
    }

    public Map<Integer, FirmEntry> firms()
    {
        return Collections.unmodifiableMap(firms);
    }

    public Map<Long, SessionEntry> sessions()
    {
        return Collections.unmodifiableMap(sessions);
    }

    public Map<String, Tier> tiers()
    {
        return Collections.unmodifiableMap(tiers);
    }

    public List<KeyEntry> keys()
    {
        return List.copyOf(keysById.values());
    }

    /** null if unknown. */
    public SessionEntry session(final long sessionId)
    {
        return sessions.get(sessionId);
    }

    /**
     * An order entry logon: the session exists, belongs to {@code firmId}, it and its firm are enabled, and
     * {@code apiKey} is a valid key of that firm for that session. Returns null otherwise (the caller says only
     * "authentication failed").
     */
    public Grant authenticateSession(final int firmId, final long sessionId, final String apiKey, final long nowMs)
    {
        final SessionEntry s = sessions.get(sessionId);
        final FirmEntry f = firms.get(firmId);
        if (s == null || f == null || s.firmId() != firmId || !s.enabled() || !f.enabled())
        {
            return null;
        }
        final KeyEntry k = key(apiKey, nowMs);
        if (k == null || k.firmId() != firmId || (k.sessionId() != 0 && k.sessionId() != sessionId))
        {
            return null;
        }
        final String tier = s.tier() != null ? s.tier() : f.tier();
        return new Grant(firmId, sessionId, k.keyId(), k.scopes(), tier == null ? null : tiers.get(tier), k.notAfterMs());
    }

    /**
     * A firm-level credential (queries, drop copy): a valid key of an enabled firm with {@code scope}. Returns
     * null otherwise.
     */
    public KeyEntry authenticateFirm(final String apiKey, final Scope scope, final long nowMs)
    {
        final KeyEntry k = key(apiKey, nowMs);
        if (k == null || !k.scopes().contains(scope))
        {
            return null;
        }
        final FirmEntry f = firms.get(k.firmId());
        return f != null && f.enabled() ? k : null;
    }

    /**
     * Whether a connection holding {@code g} may stay connected under this snapshot: its key is still present and
     * unexpired, its session and firm still exist and are enabled. Scopes and tier stay as granted at logon.
     */
    public boolean stillValid(final Grant g, final long nowMs)
    {
        final KeyEntry k = keysById.get(g.keyId());
        final SessionEntry s = sessions.get(g.sessionId());
        final FirmEntry f = firms.get(g.firmId());
        return k != null && k.activeAt(nowMs) && k.firmId() == g.firmId() &&
            s != null && s.enabled() && s.firmId() == g.firmId() && f != null && f.enabled();
    }

    private KeyEntry key(final String apiKey, final long nowMs)
    {
        if (apiKey == null || apiKey.isEmpty())
        {
            return null;
        }
        final KeyEntry k = keysByHash.get(sha256Hex(apiKey));
        return k != null && k.activeAt(nowMs) ? k : null;
    }

    public static String sha256Hex(final String text)
    {
        try
        {
            final byte[] d = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            final StringBuilder sb = new StringBuilder(64);
            for (final byte b : d)
            {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        }
        catch (final NoSuchAlgorithmException e)
        {
            throw new IllegalStateException(e);
        }
    }

    // ------------------------------------------------------------------ JSON

    public String toJson()
    {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("version", version);
        m.put("generatedMs", generatedMs);
        final List<Object> t = new ArrayList<>();
        for (final Tier x : tiers.values())
        {
            final Map<String, Object> e = new LinkedHashMap<>();
            e.put("name", x.name());
            e.put("orderRatePerSec", x.orderRatePerSec());
            e.put("orderBurst", (long)x.orderBurst());
            e.put("messageRatePerSec", x.messageRatePerSec());
            e.put("messageBurst", (long)x.messageBurst());
            t.add(e);
        }
        m.put("tiers", t);
        final List<Object> f = new ArrayList<>();
        for (final FirmEntry x : firms.values())
        {
            final Map<String, Object> e = new LinkedHashMap<>();
            e.put("firmId", (long)x.firmId());
            e.put("name", x.name());
            e.put("tier", x.tier());
            e.put("enabled", x.enabled());
            f.add(e);
        }
        m.put("firms", f);
        final List<Object> s = new ArrayList<>();
        for (final SessionEntry x : sessions.values())
        {
            final Map<String, Object> e = new LinkedHashMap<>();
            e.put("sessionId", x.sessionId());
            e.put("firmId", (long)x.firmId());
            e.put("label", x.label());
            e.put("tier", x.tier());
            e.put("enabled", x.enabled());
            s.add(e);
        }
        m.put("sessions", s);
        final List<Object> k = new ArrayList<>();
        for (final KeyEntry x : keysById.values())
        {
            final Map<String, Object> e = new LinkedHashMap<>();
            e.put("keyId", x.keyId());
            e.put("sha256", x.sha256());
            e.put("firmId", (long)x.firmId());
            e.put("sessionId", x.sessionId());
            final List<Object> sc = new ArrayList<>();
            for (final Scope y : x.scopes())
            {
                sc.add(y.name());
            }
            e.put("scopes", sc);
            e.put("notAfterMs", x.notAfterMs());
            k.add(e);
        }
        m.put("keys", k);
        return Json.write(m);
    }

    @SuppressWarnings("unchecked")
    public static AccessDirectory fromJson(final String text)
    {
        final Map<String, Object> m = Json.parseObject(text);
        final List<Tier> tiers = new ArrayList<>();
        for (final Object o : list(m, "tiers"))
        {
            final Map<String, Object> e = (Map<String, Object>)o;
            tiers.add(new Tier(str(e, "name"), num(e, "orderRatePerSec"), (int)lng(e, "orderBurst"),
                num(e, "messageRatePerSec"), (int)lng(e, "messageBurst")));
        }
        final List<FirmEntry> firms = new ArrayList<>();
        for (final Object o : list(m, "firms"))
        {
            final Map<String, Object> e = (Map<String, Object>)o;
            firms.add(new FirmEntry((int)lng(e, "firmId"), str(e, "name"), str(e, "tier"), bool(e, "enabled")));
        }
        final List<SessionEntry> sessions = new ArrayList<>();
        for (final Object o : list(m, "sessions"))
        {
            final Map<String, Object> e = (Map<String, Object>)o;
            sessions.add(new SessionEntry(lng(e, "sessionId"), (int)lng(e, "firmId"), str(e, "label"), str(e, "tier"),
                bool(e, "enabled")));
        }
        final List<KeyEntry> keys = new ArrayList<>();
        for (final Object o : list(m, "keys"))
        {
            final Map<String, Object> e = (Map<String, Object>)o;
            final EnumSet<Scope> scopes = EnumSet.noneOf(Scope.class);
            for (final Object sc : list(e, "scopes"))
            {
                final Scope parsed = Scope.parse(sc.toString());
                if (parsed != null)
                {
                    scopes.add(parsed); // a scope this build does not know grants nothing
                }
            }
            keys.add(new KeyEntry(str(e, "keyId"), str(e, "sha256"), (int)lng(e, "firmId"), lng(e, "sessionId"), scopes,
                lng(e, "notAfterMs")));
        }
        return new AccessDirectory(lng(m, "version"), lng(m, "generatedMs"), tiers, firms, sessions, keys);
    }

    private static List<Object> list(final Map<String, Object> m, final String k)
    {
        final Object v = m.get(k);
        if (v == null)
        {
            return List.of();
        }
        if (!(v instanceof List<?>))
        {
            throw new IllegalArgumentException(k + " must be a list");
        }
        @SuppressWarnings("unchecked") final List<Object> l = (List<Object>)v;
        return l;
    }

    private static String str(final Map<String, Object> m, final String k)
    {
        final Object v = m.get(k);
        return v == null ? null : v.toString();
    }

    private static double num(final Map<String, Object> m, final String k)
    {
        final Object v = m.get(k);
        if (!(v instanceof Number n))
        {
            throw new IllegalArgumentException(k + " must be a number");
        }
        return n.doubleValue();
    }

    private static long lng(final Map<String, Object> m, final String k)
    {
        final Object v = m.get(k);
        if (!(v instanceof Long l))
        {
            throw new IllegalArgumentException(k + " must be an integer");
        }
        return l;
    }

    private static boolean bool(final Map<String, Object> m, final String k)
    {
        return Boolean.TRUE.equals(m.get(k));
    }
}
