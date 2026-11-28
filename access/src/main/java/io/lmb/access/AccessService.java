package io.lmb.access;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.lmb.common.Json;
import io.lmb.common.access.AccessDirectory;
import io.lmb.common.access.OperatorTokens;
import io.lmb.common.access.Scope;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The access service: who may connect to the venue. It owns teams (firms), their members' sessions, API keys (only
 * their hashes are stored) and throttle tiers, and publishes all of it as one versioned {@link AccessDirectory}
 * snapshot that the gateways and the drop copy poll. It is not on the trading path: the matcher never sees it, and
 * the gateways keep their last snapshot (also on disk) while it is down, so an outage only delays credential changes.
 *
 * <p>Two HTTP listeners: the operators' API ({@code /api/*}, per-operator tokens, loopback by default) and the
 * internal {@code GET /internal/v1/directory} (a service token). Stateless over the database: several instances
 * may serve the same database.
 */
public final class AccessService implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(AccessService.class);
    private static final Pattern TIER = Pattern.compile("[a-z0-9_-]{1,32}");
    private static final Pattern KEY_ID = Pattern.compile("[0-9a-f]{12}|csv-[0-9]+|[0-9a-f]{1,32}");

    private final AccessConfig cfg;
    private final AccessStore store;
    private final OperatorTokens operators;
    private final PrometheusRegistry registry = new PrometheusRegistry();
    private final Counter requests;
    private final Counter fetches;
    private final Counter authFailures;
    private volatile AccessDirectory cached;
    private volatile boolean up;
    private Javalin admin;
    private Javalin internal;
    private HTTPServer metricsServer;

    public AccessService(final AccessConfig cfg)
    {
        this.cfg = cfg;
        if (cfg.operatorsFile != null)
        {
            operators = OperatorTokens.load(cfg.operatorsFile);
        }
        else
        {
            if (AccessConfig.LOCAL_ADMIN_TOKEN.equals(cfg.adminToken))
            {
                if (!cfg.allowDefaultTokens)
                {
                    throw new IllegalStateException("lmb.access.allowDefaultTokens=false: set lmb.access.operatorsFile");
                }
                LOG.warn("no lmb.access.operatorsFile: the built-in local admin token is accepted (operator 'admin')");
            }
            operators = OperatorTokens.single(cfg.adminToken);
        }
        if (AccessConfig.LOCAL_SERVICE_TOKEN.equals(cfg.serviceToken) && !cfg.allowDefaultTokens)
        {
            throw new IllegalStateException("lmb.access.allowDefaultTokens=false: set lmb.access.serviceToken");
        }
        this.store = new AccessStore(cfg.dbUrl, cfg.dbUser, cfg.dbPassword);
        requests = Counter.builder().name("lmb_access_requests_total").help("Operator API changes, by action")
            .labelNames("action").register(registry);
        fetches = Counter.builder().name("lmb_access_directory_fetches_total")
            .help("Directory polls answered, by result (full = 200, unchanged = 304)").labelNames("result").register(registry);
        authFailures = Counter.builder().name("lmb_access_auth_failures_total")
            .help("Calls refused for a missing or wrong token, by listener").labelNames("listener").register(registry);
        GaugeWithCallback.builder().name("lmb_access_up").help("1 while the service is serving")
            .callback(cb -> cb.call(up ? 1 : 0)).register(registry);
        GaugeWithCallback.builder().name("lmb_access_directory_version").help("Version of the directory last served or built")
            .callback(cb -> cb.call(cached == null ? 0 : cached.version())).register(registry);
    }

    public AccessStore store()
    {
        return store;
    }

    public AccessService start()
    {
        if (store.bootstrapIfEmpty())
        {
            LOG.info("access database bootstrapped with the default tiers; register teams with scripts/lmb-team");
        }
        cached = store.snapshot();
        admin = Javalin.create(config ->
        {
            config.startup.showJavalinBanner = false;
            errors(config);
            config.routes.before("/api/*", this::authenticateOperator);
            config.routes.get("/health", ctx -> json(ctx, 200, Map.of("status", "UP", "version", store.version())));
            config.routes.get("/api/firms", ctx -> json(ctx, 200, firmViews(null)));
            config.routes.get("/api/firms/{id}", ctx -> json(ctx, 200, firmViews(intParam(ctx, "id")).get(0)));
            config.routes.post("/api/firms", this::registerFirm);
            config.routes.put("/api/firms/{id}", this::updateFirm);
            config.routes.post("/api/firms/{id}/sessions", this::addSession);
            config.routes.put("/api/sessions/{id}", this::updateSession);
            config.routes.post("/api/firms/{id}/keys", this::issueKey);
            config.routes.get("/api/keys", ctx ->
            {
                final String firm = ctx.queryParam("firmId");
                json(ctx, 200, keyViews(store.keys(firm == null ? null : (int)range("firmId", firm, 0, 0xFFFF_FFFEL))));
            });
            config.routes.post("/api/keys/{keyId}/rotate", this::rotateKey);
            config.routes.delete("/api/keys/{keyId}", this::revokeKey);
            config.routes.get("/api/tiers", ctx -> json(ctx, 200, tierViews()));
            config.routes.put("/api/tiers/{name}", this::putTier);
            config.routes.get("/api/audit", ctx ->
            {
                final List<Object> out = new ArrayList<>();
                for (final AccessStore.AuditRow a : store.audit((int)range("limit", ctx.queryParam("limit") == null ? "100" :
                    ctx.queryParam("limit"), 1, 10_000)))
                {
                    final Map<String, Object> m = new LinkedHashMap<>();
                    m.put("auditId", a.auditId());
                    m.put("tsMs", a.tsMs());
                    m.put("actor", a.actor());
                    m.put("action", a.action());
                    m.put("detail", a.detail());
                    out.add(m);
                }
                json(ctx, 200, out);
            });
        }).start(cfg.adminBindHost, cfg.adminPort);
        internal = Javalin.create(config ->
        {
            config.startup.showJavalinBanner = false;
            errors(config);
            config.routes.get("/health", ctx -> json(ctx, 200, Map.of("status", "UP")));
            config.routes.get("/internal/v1/directory", this::directory);
        }).start(cfg.internalBindHost, cfg.internalPort);
        if (cfg.metricsPort > 0)
        {
            try
            {
                metricsServer = HTTPServer.builder().hostname(cfg.metricsBindHost).port(cfg.metricsPort).registry(registry).buildAndStart();
            }
            catch (final java.io.IOException e)
            {
                throw new IllegalStateException("metrics port " + cfg.metricsPort, e);
            }
        }
        up = true;
        LOG.info("access service {}: operators API {}:{}, directory {}:{}, metrics {}, directory version {}", cfg.instance,
            cfg.adminBindHost, admin.port(), cfg.internalBindHost, internal.port(), cfg.metricsPort, cached.version());
        return this;
    }

    public int adminPort()
    {
        return admin.port();
    }

    public int internalPort()
    {
        return internal.port();
    }

    private void errors(final io.javalin.config.JavalinConfig config)
    {
        config.routes.exception(AccessStore.Refused.class, (e, ctx) -> json(ctx, e.status, Map.of("error", e.getMessage())));
        config.routes.exception(IllegalArgumentException.class, (e, ctx) -> json(ctx, 400, Map.of("error", String.valueOf(e.getMessage()))));
        config.routes.exception(Exception.class, (e, ctx) ->
        {
            LOG.warn("REST {} {} failed", ctx.method(), ctx.path(), e);
            json(ctx, 500, Map.of("error", e.toString()));
        });
    }

    // ------------------------------------------------------------------ auth

    private static String bearer(final Context ctx, final String header)
    {
        String token = header == null ? null : ctx.header(header);
        final String auth = ctx.header("Authorization");
        if (token == null && auth != null && auth.startsWith("Bearer "))
        {
            token = auth.substring(7).trim();
        }
        return token;
    }

    private void authenticateOperator(final Context ctx)
    {
        final String operator = operators.operator(bearer(ctx, "X-LMB-Admin-Token"));
        if (operator == null)
        {
            authFailures.labelValues("operators").inc();
            throw new AccessStore.Refused(401, "missing or invalid operator token");
        }
        ctx.attribute("operator", operator);
    }

    private static String actor(final Context ctx)
    {
        return ctx.attribute("operator");
    }

    // ------------------------------------------------------------------ directory

    private void directory(final Context ctx)
    {
        final String token = bearer(ctx, null);
        if (token == null || !MessageDigest.isEqual(token.getBytes(StandardCharsets.UTF_8), cfg.serviceToken.getBytes(StandardCharsets.UTF_8)))
        {
            authFailures.labelValues("internal").inc();
            json(ctx, 401, Map.of("error", "missing or invalid service token"));
            return;
        }
        final long version = store.version();
        AccessDirectory d = cached;
        if (d == null || d.version() != version || hasExpiredKey(d))
        {
            d = store.snapshot();
            cached = d;
        }
        final String etag = "\"" + d.version() + "\"";
        if (etag.equals(ctx.header("If-None-Match")))
        {
            fetches.labelValues("unchanged").inc();
            ctx.status(304).header("ETag", etag);
            return;
        }
        fetches.labelValues("full").inc();
        ctx.header("ETag", etag);
        ctx.status(200).contentType("application/json").result(d.toJson());
    }

    /** An expired key leaves the snapshot without a version change; gateways also check expiry themselves. */
    private static boolean hasExpiredKey(final AccessDirectory d)
    {
        final long now = System.currentTimeMillis();
        for (final AccessDirectory.KeyEntry k : d.keys())
        {
            if (!k.activeAt(now))
            {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------ operators' API

    private void registerFirm(final Context ctx)
    {
        final Map<String, Object> b = body(ctx);
        final int firmId = (int)range("firmId", b.get("firmId"), 1, 0xFFFF_FFFEL);
        final String name = text("name", b.get("name"), true);
        final String tier = tier(b.get("tier"), AccessDirectory.DEFAULT_TIER);
        final Set<Scope> scopes = scopes(b.get("scopes"), EnumSet.of(Scope.TRADE, Scope.QUERY));
        final List<AccessStore.NewSession> members = new ArrayList<>();
        if (b.get("members") instanceof List<?> l)
        {
            for (final Object o : l)
            {
                if (!(o instanceof Map<?, ?> mm))
                {
                    throw new IllegalArgumentException("members must be objects");
                }
                @SuppressWarnings("unchecked") final Map<String, Object> m = (Map<String, Object>)mm;
                members.add(member(m));
            }
        }
        else if (b.get("members") != null)
        {
            throw new IllegalArgumentException("members must be a list");
        }
        requests.labelValues("REGISTER_FIRM").inc();
        final AccessStore.Registration r = store.registerFirm(actor(ctx), firmId, name, tier, members, scopes);
        json(ctx, 201, registrationView(r));
    }

    private void updateFirm(final Context ctx)
    {
        final Map<String, Object> b = body(ctx);
        requests.labelValues("UPDATE_FIRM").inc();
        final AccessStore.FirmRow f = store.updateFirm(actor(ctx), intParam(ctx, "id"), text("name", b.get("name"), false),
            b.get("tier") == null ? null : tier(b.get("tier"), null), bool("enabled", b.get("enabled")));
        json(ctx, 200, firmViews(f.firmId()).get(0));
    }

    private void addSession(final Context ctx)
    {
        final Map<String, Object> b = body(ctx);
        final Set<Scope> scopes = Boolean.FALSE.equals(b.get("key")) ? null : scopes(b.get("scopes"), EnumSet.of(Scope.TRADE, Scope.QUERY));
        requests.labelValues("ADD_SESSION").inc();
        json(ctx, 201, registrationView(store.addSession(actor(ctx), intParam(ctx, "id"), member(b), scopes)));
    }

    private void updateSession(final Context ctx)
    {
        final Map<String, Object> b = body(ctx);
        final String tier = b.containsKey("tier") ? (b.get("tier") == null ? "" : tier(b.get("tier"), null)) : null;
        requests.labelValues("UPDATE_SESSION").inc();
        final AccessStore.SessionRow s = store.updateSession(actor(ctx), range("id", ctx.pathParam("id"), 1, Long.MAX_VALUE),
            text("label", b.get("label"), false), tier, bool("enabled", b.get("enabled")));
        json(ctx, 200, sessionView(s));
    }

    private void issueKey(final Context ctx)
    {
        final Map<String, Object> b = body(ctx);
        final long sessionId = b.get("sessionId") == null ? 0 : range("sessionId", b.get("sessionId"), 1, Long.MAX_VALUE);
        final Set<Scope> scopes = scopes(b.get("scopes"), null);
        final String label = b.get("label") == null ? "key" : text("label", b.get("label"), true);
        final long expiresIn = b.get("expiresInMs") == null ? 0 : range("expiresInMs", b.get("expiresInMs"), 1, 10L * 365 * 86_400_000L);
        requests.labelValues("ISSUE_KEY").inc();
        final AccessStore.Issued k = store.issueKey(actor(ctx), intParam(ctx, "id"), sessionId, scopes, label,
            expiresIn == 0 ? 0 : System.currentTimeMillis() + expiresIn);
        json(ctx, 201, issuedView(k));
    }

    private void rotateKey(final Context ctx)
    {
        final Map<String, Object> b = body(ctx);
        final long grace = b.get("graceMs") == null ? cfg.rotationGraceMs : range("graceMs", b.get("graceMs"), 0, 30L * 86_400_000L);
        requests.labelValues("ROTATE_KEY").inc();
        json(ctx, 201, issuedView(store.rotateKey(actor(ctx), keyId(ctx), grace)));
    }

    private void revokeKey(final Context ctx)
    {
        requests.labelValues("REVOKE_KEY").inc();
        json(ctx, 200, AccessStore.keyView(store.revokeKey(actor(ctx), keyId(ctx)), System.currentTimeMillis()));
    }

    private void putTier(final Context ctx)
    {
        final Map<String, Object> b = body(ctx);
        final String name = tier(ctx.pathParam("name"), null);
        final AccessDirectory.Tier t = new AccessDirectory.Tier(name, rate("orderRatePerSec", b.get("orderRatePerSec")),
            (int)range("orderBurst", b.get("orderBurst"), 1, 1_000_000), rate("messageRatePerSec", b.get("messageRatePerSec")),
            (int)range("messageBurst", b.get("messageBurst"), 1, 1_000_000));
        requests.labelValues("PUT_TIER").inc();
        store.putTier(actor(ctx), t);
        json(ctx, 200, tierView(t));
    }

    // ------------------------------------------------------------------ views

    private List<Map<String, Object>> firmViews(final Integer only)
    {
        final List<Map<String, Object>> out = new ArrayList<>();
        final long now = System.currentTimeMillis();
        for (final AccessStore.FirmRow f : store.firms())
        {
            if (only != null && f.firmId() != only)
            {
                continue;
            }
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("firmId", (long)f.firmId());
            m.put("name", f.name());
            m.put("tier", f.tier());
            m.put("enabled", f.enabled());
            m.put("createdMs", f.createdMs());
            m.put("updatedMs", f.updatedMs());
            final List<Object> sessions = new ArrayList<>();
            for (final AccessStore.SessionRow s : store.sessions(f.firmId()))
            {
                sessions.add(sessionView(s));
            }
            m.put("sessions", sessions);
            final List<Object> keys = new ArrayList<>();
            for (final AccessStore.KeyRow k : store.keys(f.firmId()))
            {
                keys.add(AccessStore.keyView(k, now));
            }
            m.put("keys", keys);
            out.add(m);
        }
        if (only != null && out.isEmpty())
        {
            throw new AccessStore.Refused(404, "unknown firm " + only);
        }
        return out;
    }

    private static Map<String, Object> sessionView(final AccessStore.SessionRow s)
    {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("sessionId", s.sessionId());
        m.put("firmId", (long)s.firmId());
        m.put("label", s.label());
        m.put("tier", s.tier());
        m.put("enabled", s.enabled());
        return m;
    }

    private static List<Object> keyViews(final List<AccessStore.KeyRow> keys)
    {
        final List<Object> out = new ArrayList<>();
        final long now = System.currentTimeMillis();
        for (final AccessStore.KeyRow k : keys)
        {
            out.add(AccessStore.keyView(k, now));
        }
        return out;
    }

    private static Map<String, Object> issuedView(final AccessStore.Issued k)
    {
        final Map<String, Object> m = AccessStore.keyView(k.key(), System.currentTimeMillis());
        m.put("apiKey", k.apiKey());
        m.put("note", "the api key is shown only once; the service stores only its hash");
        return m;
    }

    private static Map<String, Object> registrationView(final AccessStore.Registration r)
    {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("firmId", (long)r.firm().firmId());
        m.put("name", r.firm().name());
        m.put("tier", r.firm().tier());
        final List<Object> members = new ArrayList<>();
        for (int i = 0; i < r.sessions().size(); i++)
        {
            final Map<String, Object> s = sessionView(r.sessions().get(i));
            if (i < r.keys().size())
            {
                s.put("key", issuedView(r.keys().get(i)));
            }
            members.add(s);
        }
        m.put("members", members);
        return m;
    }

    private List<Object> tierViews()
    {
        final List<Object> out = new ArrayList<>();
        for (final AccessDirectory.Tier t : store.tiers())
        {
            out.add(tierView(t));
        }
        return out;
    }

    private static Map<String, Object> tierView(final AccessDirectory.Tier t)
    {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", t.name());
        m.put("orderRatePerSec", t.orderRatePerSec());
        m.put("orderBurst", (long)t.orderBurst());
        m.put("messageRatePerSec", t.messageRatePerSec());
        m.put("messageBurst", (long)t.messageBurst());
        return m;
    }

    // ------------------------------------------------------------------ validation

    private static AccessStore.NewSession member(final Map<String, Object> m)
    {
        final long sessionId = m.get("sessionId") == null ? 0 : range("sessionId", m.get("sessionId"), 1, Long.MAX_VALUE);
        return new AccessStore.NewSession(sessionId, text("label", m.get("label"), true), m.get("tier") == null ? null : tier(m.get("tier"), null));
    }

    private static void json(final Context ctx, final int status, final Object body)
    {
        ctx.status(status).contentType("application/json").result(Json.write(body));
    }

    private static Map<String, Object> body(final Context ctx)
    {
        return Json.parseObject(ctx.body());
    }

    private static int intParam(final Context ctx, final String name)
    {
        return (int)range(name, ctx.pathParam(name), 1, 0xFFFF_FFFEL);
    }

    private static String keyId(final Context ctx)
    {
        final String id = ctx.pathParam("keyId");
        if (!KEY_ID.matcher(id).matches())
        {
            throw new IllegalArgumentException("bad key id");
        }
        return id;
    }

    static long range(final String name, final Object v, final long min, final long max)
    {
        final long x;
        if (v instanceof Long l)
        {
            x = l;
        }
        else if (v instanceof String s)
        {
            try
            {
                x = Long.parseLong(s.trim());
            }
            catch (final NumberFormatException e)
            {
                throw new IllegalArgumentException(name + " must be an integer");
            }
        }
        else
        {
            throw new IllegalArgumentException(name + " must be an integer");
        }
        if (x < min || x > max)
        {
            throw new IllegalArgumentException(name + " must be in " + min + ".." + max);
        }
        return x;
    }

    private static double rate(final String name, final Object v)
    {
        if (!(v instanceof Number n) || !(n.doubleValue() > 0) || n.doubleValue() > 1_000_000)
        {
            throw new IllegalArgumentException(name + " must be a number in (0, 1000000]");
        }
        return n.doubleValue();
    }

    private static String text(final String name, final Object v, final boolean required)
    {
        if (v == null)
        {
            if (required)
            {
                throw new IllegalArgumentException(name + " is required");
            }
            return null;
        }
        final String s = v.toString().strip();
        if (s.isEmpty() || s.length() > 200 || s.chars().anyMatch(Character::isISOControl))
        {
            throw new IllegalArgumentException(name + " must be 1..200 printable characters");
        }
        return s;
    }

    private static Boolean bool(final String name, final Object v)
    {
        if (v == null)
        {
            return null;
        }
        if (!(v instanceof Boolean b))
        {
            throw new IllegalArgumentException(name + " must be true or false");
        }
        return b;
    }

    private static String tier(final Object v, final String dflt)
    {
        if (v == null)
        {
            return dflt;
        }
        final String s = v.toString();
        if (!TIER.matcher(s).matches())
        {
            throw new IllegalArgumentException("tier must match " + TIER.pattern());
        }
        return s;
    }

    private static Set<Scope> scopes(final Object v, final Set<Scope> dflt)
    {
        if (v == null)
        {
            if (dflt == null)
            {
                throw new IllegalArgumentException("scopes is required (TRADE, CANCEL, QUERY, DROP_COPY)");
            }
            return dflt;
        }
        if (!(v instanceof List<?> l) || l.isEmpty())
        {
            throw new IllegalArgumentException("scopes must be a non-empty list of TRADE, CANCEL, QUERY, DROP_COPY");
        }
        final EnumSet<Scope> out = EnumSet.noneOf(Scope.class);
        for (final Object o : l)
        {
            final Scope s = o == null ? null : Scope.parse(o.toString());
            if (s == null)
            {
                throw new IllegalArgumentException("unknown scope " + o);
            }
            out.add(s);
        }
        return out;
    }

    public boolean isUp()
    {
        return up;
    }

    @Override
    public void close()
    {
        up = false;
        if (admin != null)
        {
            admin.stop();
        }
        if (internal != null)
        {
            internal.stop();
        }
        if (metricsServer != null)
        {
            metricsServer.close();
        }
        store.close();
    }
}
