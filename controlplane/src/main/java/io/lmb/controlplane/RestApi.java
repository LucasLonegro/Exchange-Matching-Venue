package io.lmb.controlplane;

import io.lmb.common.Json;
import io.lmb.common.access.OperatorTokens;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.lmb.common.SymbolDirectory;
import io.lmb.controlplane.db.Database;
import io.lmb.controlplane.db.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Admin REST API (Javalin). Every {@code /api/*} call needs header {@code X-LMB-Admin-Token} (or
 * {@code Authorization: Bearer <token>}). Reads come straight from the database, so both instances serve
 * them. Writes are validated here, recorded in the audit log as PENDING, executed by whichever instance
 * is leader ({@link AdminExecutor}), and answered with the audit row once it reaches a final status or
 * the {@code wait} time (ms, default 3000) elapses (then 202). Endpoint reference: docs/modules/controlplane.md.
 */
public final class RestApi
{
    private static final Logger LOG = LoggerFactory.getLogger(RestApi.class);
    private static final Pattern TICKER = Pattern.compile("[A-Z0-9.]{1,8}");
    private static final Pattern TIME = Pattern.compile("([01][0-9]|2[0-3]):[0-5][0-9]|-");
    private static final Pattern ACTOR = Pattern.compile("[A-Za-z0-9_.@-]{1,100}");
    static final long MAX_VOLATILITY_AUCTION_NS = 86_400_000_000_000L;

    private final ControlPlaneConfig cfg;
    private final Database db;
    private final int shardCount;
    private final OperatorTokens operators;
    private final Clock clock;
    private final Metrics metrics;
    private LeaderLease lease;
    private Javalin app;

    static final class ApiException extends RuntimeException
    {
        final int status;

        ApiException(final int status, final String message)
        {
            super(message);
            this.status = status;
        }
    }

    public RestApi(final ControlPlaneConfig cfg, final Database db, final int shardCount, final Clock clock, final Metrics metrics)
    {
        this.cfg = cfg;
        this.operators = cfg.operatorsFile != null ? OperatorTokens.load(cfg.operatorsFile) : OperatorTokens.single(cfg.adminToken);
        this.db = db;
        this.shardCount = shardCount;
        this.clock = clock;
        this.metrics = metrics;
    }

    public void start(final LeaderLease lease)
    {
        this.lease = lease;
        app = Javalin.create(config ->
        {
            config.startup.showJavalinBanner = false;
            config.routes.exception(ApiException.class, (e, ctx) -> json(ctx, e.status, Map.of("error", e.getMessage())));
            config.routes.exception(IllegalArgumentException.class, (e, ctx) -> json(ctx, 400, Map.of("error", String.valueOf(e.getMessage()))));
            config.routes.exception(Exception.class, (e, ctx) ->
            {
                LOG.warn("REST {} {} failed", ctx.method(), ctx.path(), e);
                json(ctx, 500, Map.of("error", e.toString()));
            });
            config.routes.before("/api/*", this::authenticate);

            config.routes.get("/health", ctx -> json(ctx, 200, health()));
            config.routes.get("/internal/v1/symbols", this::internalSymbols);
            config.routes.get("/api/status", ctx -> json(ctx, 200, status()));

            config.routes.get("/api/symbols", ctx -> json(ctx, 200, symbols(null)));
            config.routes.get("/api/symbols/{id}", ctx -> json(ctx, 200, symbols(intParam(ctx, "id")).get(0)));
            config.routes.post("/api/symbols", ctx -> submit(ctx, "UPSERT_SYMBOL", validSymbol(body(ctx))));
            config.routes.put("/api/symbols/{id}", ctx -> submit(ctx, "UPSERT_SYMBOL", validSymbolPut(intParam(ctx, "id"), body(ctx))));
            config.routes.delete("/api/symbols/{id}", ctx -> submit(ctx, "REMOVE_SYMBOL", Map.of("symbolId", intParam(ctx, "id"))));
            config.routes.post("/api/symbols/{id}/halt", ctx -> submit(ctx, "HALT_SYMBOL", Map.of("symbolId", intParam(ctx, "id"))));
            config.routes.post("/api/symbols/{id}/resume", ctx -> submit(ctx, "RESUME_SYMBOL",
                Map.of("symbolId", intParam(ctx, "id"), "auction", boolQuery(ctx, "auction"), "force", boolQuery(ctx, "force"))));
            config.routes.post("/api/symbols/{id}/auction/start", ctx -> submit(ctx, "AUCTION_START",
                Map.of("symbolId", intParam(ctx, "id"))));
            config.routes.post("/api/symbols/{id}/auction/stop", ctx -> submit(ctx, "AUCTION_STOP",
                Map.of("symbolId", intParam(ctx, "id"), "target", oneOf(ctx.queryParam("target"), "CONTINUOUS", "CONTINUOUS", "CLOSED"),
                    "force", boolQuery(ctx, "force"))));
            config.routes.post("/api/market/halt", ctx -> submit(ctx, "HALT_MARKET", shardQuery(ctx)));
            config.routes.post("/api/market/resume", ctx ->
            {
                final Map<String, Object> p = new java.util.HashMap<>(shardQuery(ctx));
                p.put("force", boolQuery(ctx, "force"));
                submit(ctx, "RESUME_MARKET", p);
            });

            config.routes.get("/api/firms", ctx -> json(ctx, 200, firms(null)));
            config.routes.get("/api/firms/{id}", ctx -> json(ctx, 200, firms(intParam(ctx, "id")).get(0)));
            config.routes.post("/api/firms", ctx -> submit(ctx, "CREATE_FIRM", validNewFirm(body(ctx))));
            config.routes.get("/api/limits", ctx -> json(ctx, 200, limits()));
            config.routes.put("/api/firms/{id}/limits", ctx -> submit(ctx, "UPDATE_LIMITS", validLimits(intParam(ctx, "id"), body(ctx))));
            config.routes.post("/api/firms/{id}/kill", ctx -> submit(ctx, "KILL_FIRM",
                Map.of("firmId", intParam(ctx, "id"), "cancelOrders", boolQuery(ctx, "cancelOrders"))));
            config.routes.post("/api/firms/{id}/enable", ctx -> submit(ctx, "ENABLE_FIRM", Map.of("firmId", intParam(ctx, "id"))));
            config.routes.get("/api/budgets", ctx -> json(ctx, 200, budgets()));
            config.routes.post("/api/masscancel", ctx -> submit(ctx, "MASS_CANCEL", validMassCancel(body(ctx))));
            config.routes.post("/api/dayroll", ctx -> submit(ctx, "DAY_ROLL", validDayRoll(body(ctx))));

            config.routes.get("/api/shards", ctx -> json(ctx, 200, shards()));
            config.routes.post("/api/shards/{id}/resync", ctx -> submit(ctx, "RESYNC_SHARD", Map.of("shardId", shardParam(ctx))));
            config.routes.get("/api/commands", ctx -> json(ctx, 200, commands(ctx)));
            config.routes.get("/api/commands/{id}", ctx -> json(ctx, 200, command(longParam(ctx, "id"))));
            config.routes.get("/api/audit", ctx -> json(ctx, 200, audits(ctx)));
            config.routes.get("/api/audit/{id}", ctx -> json(ctx, 200, audit(longParam(ctx, "id"))));
            config.routes.get("/api/calendar", ctx -> json(ctx, 200, calendar()));
            config.routes.put("/api/calendar/{date}", ctx -> submit(ctx, "SET_CALENDAR", validCalendar(dateParam(ctx), body(ctx))));
            config.routes.delete("/api/calendar/{date}", ctx -> submit(ctx, "DELETE_CALENDAR", Map.of("tradingDate", dateParam(ctx))));
        }).start(cfg.restHost, cfg.restPort);
        LOG.info("admin REST API on {}:{}", cfg.restHost, app.port());
    }

    public int port()
    {
        return app == null ? -1 : app.port();
    }

    public void stop()
    {
        if (app != null)
        {
            app.stop();
            app = null;
        }
    }

    // ------------------------------------------------------------------ plumbing

    private void authenticate(final Context ctx)
    {
        String token = ctx.header("X-LMB-Admin-Token");
        final String auth = ctx.header("Authorization");
        if (token == null && auth != null && auth.startsWith("Bearer "))
        {
            token = auth.substring(7).trim();
        }
        final String operator = operators.operator(token);
        if (operator == null)
        {
            throw new ApiException(401, "missing or invalid admin token");
        }
        ctx.attribute("operator", operator);
    }

    /**
     * The symbol snapshot of the other services (gateways, post-trade, feeds; {@code SymbolDirectorySource}): every
     * ACTIVE symbol with its desired parameters (referencePrice is the listing price), versioned by content so
     * both instances give the same ETag. Needs the service token, not an operator token.
     */
    private void internalSymbols(final Context ctx)
    {
        final String auth = ctx.header("Authorization");
        final String token = auth != null && auth.startsWith("Bearer ") ? auth.substring(7).trim() : null;
        if (token == null || !MessageDigest.isEqual(token.getBytes(StandardCharsets.UTF_8),
            cfg.serviceToken.getBytes(StandardCharsets.UTF_8)))
        {
            throw new ApiException(401, "missing or invalid service token");
        }
        final List<SymbolDirectory.Symbol> list = db.read(c ->
        {
            final List<SymbolDirectory.Symbol> out = new ArrayList<>();
            for (final Store.SymbolRow r : Store.symbols(c))
            {
                if (r.active())
                {
                    out.add(new SymbolDirectory.Symbol(r.symbolId(), r.ticker(), r.shardId(), r.tickSize(), r.lotSize(),
                        r.referencePrice(), r.priceBandBps(), r.maxOrderQty(), r.dynamicBandBps(), r.volatilityAuctionNs()));
                }
            }
            return out;
        });
        final String etag = "\"" + SymbolDirectory.contentVersion(list) + "\"";
        ctx.header("ETag", etag);
        if (etag.equals(ctx.header("If-None-Match")))
        {
            ctx.status(304);
            return;
        }
        ctx.status(200).contentType("application/json").result(SymbolDirectory.toJson(list));
    }

    private static void json(final Context ctx, final int status, final Object body)
    {
        ctx.status(status).contentType("application/json").result(Json.write(body));
    }

    private static Map<String, Object> body(final Context ctx)
    {
        try
        {
            return Json.parseObject(ctx.body());
        }
        catch (final IllegalArgumentException e)
        {
            throw new ApiException(400, "invalid JSON body: " + e.getMessage());
        }
    }

    private void submit(final Context ctx, final String action, final Map<String, Object> params) throws InterruptedException
    {
        // per-operator tokens: the actor is the token's owner. The single shared token has no owner, so there the
        // caller may name themselves with X-LMB-Actor (local setups only)
        final String operator = ctx.attribute("operator");
        final String named = ctx.header("X-LMB-Actor");
        final String actor = cfg.operatorsFile != null || named == null ? operator : named;
        if (!ACTOR.matcher(actor).matches())
        {
            throw new ApiException(400, "invalid X-LMB-Actor header");
        }
        final long waitMs = ctx.queryParam("wait") == null ? 3_000 : rangeLong("wait", ctx.queryParam("wait"), 0, 30_000);
        final long id = db.tx(c -> Store.insertAudit(c, actor, cfg.instanceId, action, Json.write(params), clock.epochMs()));
        metrics.adminRequest(action);
        final long deadline = System.nanoTime() + waitMs * 1_000_000L;
        Store.AuditRow row = db.read(c -> Store.audit(c, id));
        while (!row.terminal() && System.nanoTime() < deadline)
        {
            Thread.sleep(20);
            row = db.read(c -> Store.audit(c, id));
        }
        final int code = switch (row.status())
        {
            case "COMPLETED", "PARTIAL" -> 200;
            case "REJECTED" -> 422;
            case "FAILED" -> 502;
            default -> 202;
        };
        json(ctx, code, audit(row));
    }

    private static int intParam(final Context ctx, final String name)
    {
        return (int)rangeLong(name, ctx.pathParam(name), 0, Integer.MAX_VALUE);
    }

    private static long longParam(final Context ctx, final String name)
    {
        return rangeLong(name, ctx.pathParam(name), 0, Long.MAX_VALUE);
    }

    private int shardParam(final Context ctx)
    {
        return (int)rangeLong("shard", ctx.pathParam("id"), 0, shardCount - 1);
    }

    private static int dateParam(final Context ctx)
    {
        return validDate(ctx.pathParam("date"));
    }

    private Map<String, Object> shardQuery(final Context ctx)
    {
        final String s = ctx.queryParam("shard");
        return s == null ? Map.of() : Map.of("shardId", rangeLong("shard", s, 0, shardCount - 1));
    }

    private static boolean boolQuery(final Context ctx, final String name)
    {
        final String v = ctx.queryParam(name);
        if (v == null)
        {
            return false;
        }
        if (!v.equals("true") && !v.equals("false"))
        {
            throw new ApiException(400, name + " must be true or false");
        }
        return Boolean.parseBoolean(v);
    }

    private static String oneOf(final String v, final String def, final String... allowed)
    {
        if (v == null)
        {
            return def;
        }
        for (final String a : allowed)
        {
            if (a.equals(v))
            {
                return v;
            }
        }
        throw new ApiException(400, "expected one of " + String.join(", ", allowed));
    }

    static long rangeLong(final String name, final Object value, final long min, final long max)
    {
        if (value == null)
        {
            throw new ApiException(400, name + " is required");
        }
        final long v;
        try
        {
            v = value instanceof Number n ? n.longValue() : Long.parseLong(value.toString().trim());
            if (value instanceof Double d && d != Math.rint(d))
            {
                throw new NumberFormatException();
            }
        }
        catch (final NumberFormatException e)
        {
            throw new ApiException(400, name + " must be an integer");
        }
        if (v < min || v > max)
        {
            throw new ApiException(400, name + " must be between " + min + " and " + max);
        }
        return v;
    }

    static int validDate(final Object value)
    {
        final int d = (int)rangeLong("tradingDate", value, 19700101, 99991231);
        try
        {
            LocalDate.parse(Integer.toString(d), DateTimeFormatter.BASIC_ISO_DATE);
        }
        catch (final RuntimeException e)
        {
            throw new ApiException(400, "tradingDate must be a valid yyyymmdd date");
        }
        return d;
    }

    // ------------------------------------------------------------------ validation of write bodies

    private Map<String, Object> validSymbol(final Map<String, Object> b)
    {
        final Map<String, Object> p = new LinkedHashMap<>();
        final long symbolId = rangeLong("symbolId", b.get("symbolId"), 1, 0xFFFF_FFFEL);
        p.put("symbolId", symbolId);
        final Object ticker = b.get("ticker");
        if (ticker == null || !TICKER.matcher(ticker.toString()).matches())
        {
            throw new ApiException(400, "ticker must be 1-8 characters A-Z, 0-9 or '.'");
        }
        p.put("ticker", ticker.toString());
        p.put("shard", rangeLong("shard", b.get("shard"), 0, shardCount - 1));
        final long tick = rangeLong("tickSize", b.get("tickSize"), 1, 1_000_000);
        final long lot = rangeLong("lotSize", b.get("lotSize"), 1, 1_000_000);
        final long ref = rangeLong("referencePrice", b.get("referencePrice"), 1, Long.MAX_VALUE / 4);
        final long maxQty = rangeLong("maxOrderQty", b.get("maxOrderQty"), 1, Long.MAX_VALUE / 4);
        if (ref % tick != 0)
        {
            throw new ApiException(400, "referencePrice must be a multiple of tickSize");
        }
        if (maxQty % lot != 0)
        {
            throw new ApiException(400, "maxOrderQty must be a multiple of lotSize");
        }
        p.put("tickSize", tick);
        p.put("lotSize", lot);
        p.put("referencePrice", ref);
        p.put("priceBandBps", rangeLong("priceBandBps", b.get("priceBandBps"), 1, 10_000));
        p.put("maxOrderQty", maxQty);
        // dynamic collar: an absent field keeps the existing row's value (or the CSV default for a new symbol)
        final int id = (int)symbolId;
        final Store.SymbolRow existing = (b.get("dynamicBandBps") == null || b.get("volatilityAuctionNs") == null)
            ? db.read(c -> Store.symbol(c, id)) : null;
        final long dyn = b.get("dynamicBandBps") != null ? rangeLong("dynamicBandBps", b.get("dynamicBandBps"), 0, 10_000)
            : existing != null ? existing.dynamicBandBps() : SymbolDirectory.DEFAULT_DYNAMIC_BAND_BPS;
        final long vol = b.get("volatilityAuctionNs") != null
            ? rangeLong("volatilityAuctionNs", b.get("volatilityAuctionNs"), 0, MAX_VOLATILITY_AUCTION_NS)
            : existing != null ? existing.volatilityAuctionNs() : SymbolDirectory.DEFAULT_VOLATILITY_AUCTION_NS;
        if (dyn > 0 && vol == 0)
        {
            throw new ApiException(400, "dynamicBandBps > 0 needs volatilityAuctionNs > 0");
        }
        p.put("dynamicBandBps", dyn);
        p.put("volatilityAuctionNs", vol);
        return p;
    }

    /** PUT /api/symbols/{id}: the body may omit symbolId; when present it must equal the path id. */
    private Map<String, Object> validSymbolPut(final int pathId, final Map<String, Object> b)
    {
        final Object bodyId = b.get("symbolId");
        if (bodyId != null && rangeLong("symbolId", bodyId, 1, 0xFFFF_FFFEL) != pathId)
        {
            throw new ApiException(400, "symbolId in the body must match the path");
        }
        final Map<String, Object> withId = new LinkedHashMap<>(b);
        withId.put("symbolId", pathId);
        return validSymbol(withId);
    }

    private static Map<String, Object> validLimits(final int firmId, final Map<String, Object> b)
    {
        final Map<String, Object> p = new LinkedHashMap<>();
        p.put("firmId", firmId);
        if (b.get("creditLimit") == null && b.get("maxPositionPerSymbol") == null)
        {
            throw new ApiException(400, "creditLimit and/or maxPositionPerSymbol required");
        }
        if (b.get("creditLimit") != null)
        {
            p.put("creditLimit", rangeLong("creditLimit", b.get("creditLimit"), 0, Long.MAX_VALUE / 4));
        }
        if (b.get("maxPositionPerSymbol") != null)
        {
            p.put("maxPositionPerSymbol", rangeLong("maxPositionPerSymbol", b.get("maxPositionPerSymbol"), 0, Long.MAX_VALUE / 4));
        }
        return p;
    }

    private static Map<String, Object> validNewFirm(final Map<String, Object> b)
    {
        final Map<String, Object> p = new LinkedHashMap<>();
        p.put("firmId", rangeLong("firmId", b.get("firmId"), 1, 0xFFFF_FFFEL));
        final Object name = b.get("name");
        if (name == null || name.toString().isBlank() || name.toString().length() > 200 ||
            name.toString().chars().anyMatch(Character::isISOControl))
        {
            throw new ApiException(400, "name must be 1..200 printable characters");
        }
        p.put("name", name.toString().strip());
        p.put("creditLimit", rangeLong("creditLimit", b.get("creditLimit"), 0, Long.MAX_VALUE / 4));
        p.put("maxPositionPerSymbol", rangeLong("maxPositionPerSymbol", b.get("maxPositionPerSymbol"), 0, Long.MAX_VALUE / 4));
        return p;
    }

    private Map<String, Object> validMassCancel(final Map<String, Object> b)
    {
        final Map<String, Object> p = new LinkedHashMap<>();
        final String scope = oneOf(b.get("scope") == null ? null : b.get("scope").toString(), null, "FIRM", "SESSION", "SYMBOL", "FIRM_SYMBOL");
        if (scope == null)
        {
            throw new ApiException(400, "scope is required (FIRM, SESSION, SYMBOL, FIRM_SYMBOL)");
        }
        p.put("scope", scope);
        if (scope.startsWith("FIRM") || scope.equals("SESSION"))
        {
            p.put("firmId", rangeLong("firmId", b.get("firmId"), 0, 0xFFFF_FFFEL));
        }
        if (scope.endsWith("SYMBOL"))
        {
            p.put("symbolId", rangeLong("symbolId", b.get("symbolId"), 1, 0xFFFF_FFFEL));
        }
        if (scope.equals("SESSION"))
        {
            p.put("sessionId", rangeLong("sessionId", b.get("sessionId"), 0, Long.MAX_VALUE));
        }
        if (b.get("shardId") != null)
        {
            p.put("shardId", rangeLong("shardId", b.get("shardId"), 0, shardCount - 1));
        }
        return p;
    }

    private Map<String, Object> validDayRoll(final Map<String, Object> b)
    {
        final Map<String, Object> p = new LinkedHashMap<>();
        p.put("tradingDate", validDate(b.get("tradingDate")));
        if (b.get("shardId") != null)
        {
            p.put("shardId", rangeLong("shardId", b.get("shardId"), 0, shardCount - 1));
        }
        return p;
    }

    private static Map<String, Object> validCalendar(final int date, final Map<String, Object> b)
    {
        final Map<String, Object> p = new LinkedHashMap<>();
        p.put("tradingDate", date);
        p.put("tradingDay", b.get("tradingDay") == null || Boolean.parseBoolean(b.get("tradingDay").toString()));
        for (final String k : List.of("preOpen", "openAuction", "continuous", "closeAuction", "close", "dayRoll"))
        {
            final Object v = b.get(k);
            if (v != null)
            {
                if (!TIME.matcher(v.toString()).matches())
                {
                    throw new ApiException(400, k + " must be HH:MM or '-'");
                }
                p.put(k, v.toString());
            }
        }
        if (b.get("note") != null)
        {
            final String note = b.get("note").toString();
            p.put("note", note.length() > 200 ? note.substring(0, 200) : note);
        }
        return p;
    }

    // ------------------------------------------------------------------ read models

    private Map<String, Object> health()
    {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", "UP");
        m.put("instance", cfg.instanceId);
        m.put("leader", lease != null && lease.isLeader());
        return m;
    }

    private Map<String, Object> status()
    {
        final Map<String, Object> m = health();
        final LeaderLease.Info info = lease.info();
        final Map<String, Object> l = new LinkedHashMap<>();
        if (info != null)
        {
            l.put("holder", info.holder());
            l.put("holderUrl", info.holderUrl());
            l.put("epoch", info.epoch());
            l.put("renewedMs", info.renewedMs());
        }
        m.put("lease", l);
        m.put("shards", shards());
        m.put("pendingAdminRequests", db.read(c -> Database.queryLong(c,
            "SELECT COUNT(*) FROM audit_log WHERE status IN ('PENDING', 'EXECUTING')")));
        return m;
    }

    private List<Map<String, Object>> symbols(final Integer only)
    {
        return db.read(c ->
        {
            final Map<Integer, Store.ObservedSymbolRow> observed = new TreeMap<>();
            for (final Store.ObservedSymbolRow o : Store.observedSymbols(c))
            {
                observed.put(o.symbolId(), o);
            }
            final List<Map<String, Object>> out = new ArrayList<>();
            for (final Store.SymbolRow r : Store.symbols(c))
            {
                if (only != null && r.symbolId() != only)
                {
                    continue;
                }
                final Map<String, Object> m = new LinkedHashMap<>();
                m.put("symbolId", r.symbolId());
                m.put("ticker", r.ticker());
                m.put("shard", r.shardId());
                m.put("tickSize", r.tickSize());
                m.put("lotSize", r.lotSize());
                m.put("referencePrice", r.referencePrice());
                m.put("priceBandBps", r.priceBandBps());
                m.put("maxOrderQty", r.maxOrderQty());
                m.put("dynamicBandBps", r.dynamicBandBps());
                m.put("volatilityAuctionNs", r.volatilityAuctionNs());
                m.put("status", r.status());
                m.put("operatorState", r.operatorState());
                final Store.ObservedSymbolRow o = observed.get(r.symbolId());
                m.put("shardState", o == null ? null : o.state());
                m.put("definedOnShard", o != null && o.shardId() == r.shardId());
                m.put("inVolatilityAuction", o != null && o.volatilityAuction());
                m.put("volatilityAuctionSinceNs", o != null && o.volatilityAuction() ? o.volatilityAuctionSinceNs() : null);
                m.put("venueReferencePrice", o != null && o.venueReference() ? price(o.referencePrice()) : null);
                m.put("bandLow", o == null ? null : price(o.bandLow()));
                m.put("bandHigh", o == null ? null : price(o.bandHigh()));
                if (o == null || o.closeSource() == null)
                {
                    m.put("lastClose", null);
                }
                else
                {
                    final Map<String, Object> close = new LinkedHashMap<>();
                    close.put("price", price(o.closePrice()));
                    close.put("previousClose", price(o.previousClose()));
                    close.put("source", o.closeSource());
                    close.put("tradingDate", o.closeTradingDate());
                    m.put("lastClose", close);
                }
                out.add(m);
            }
            if (only != null && out.isEmpty())
            {
                throw new ApiException(404, "unknown symbol " + only);
            }
            return out;
        });
    }

    /** SBE null price (Long.MIN_VALUE) as JSON null. */
    private static Long price(final long p)
    {
        return p == Long.MIN_VALUE ? null : p;
    }

    private List<Map<String, Object>> firms(final Integer only)
    {
        return db.read(c ->
        {
            final List<Map<String, Object>> out = new ArrayList<>();
            for (final Store.FirmRow f : Store.firms(c))
            {
                if (only != null && f.firmId() != only)
                {
                    continue;
                }
                final Map<String, Object> m = new LinkedHashMap<>();
                m.put("firmId", f.firmId());
                m.put("name", f.name());
                m.put("enabled", f.enabled());
                m.put("creditLimit", f.creditLimit());
                m.put("maxPositionPerSymbol", f.maxPositionPerSymbol());
                out.add(m);
            }
            if (only != null && out.isEmpty())
            {
                throw new ApiException(404, "unknown firm " + only);
            }
            return out;
        });
    }

    private List<Map<String, Object>> limits()
    {
        final List<Map<String, Object>> out = new ArrayList<>();
        for (final Map<String, Object> f : firms(null))
        {
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("firmId", f.get("firmId"));
            m.put("enabled", f.get("enabled"));
            m.put("creditLimit", f.get("creditLimit"));
            m.put("maxPositionPerSymbol", f.get("maxPositionPerSymbol"));
            out.add(m);
        }
        return out;
    }

    private List<Map<String, Object>> budgets()
    {
        return db.read(c ->
        {
            final Map<String, Store.ObservedFirmRow> observed = new TreeMap<>();
            for (final Store.ObservedFirmRow o : Store.observedFirms(c))
            {
                observed.put(o.firmId() + ":" + o.shardId(), o);
            }
            final Map<String, Long> targets = new TreeMap<>();
            for (final Store.BudgetRow b : Store.budgets(c))
            {
                targets.put(b.firmId() + ":" + b.shardId(), b.targetBudget());
            }
            final Map<String, Long> pending = new TreeMap<>();
            for (final Store.CommandRow r : Store.unresolvedCommands(c))
            {
                if ("RISK_LIMIT".equals(r.kind()) && r.budget() != null)
                {
                    final String[] k = r.key().split(":");
                    pending.merge(k[2] + ":" + k[1], r.budget(), Math::max);
                }
            }
            final List<Map<String, Object>> out = new ArrayList<>();
            for (final Store.FirmRow f : Store.firms(c))
            {
                final Map<String, Object> m = new LinkedHashMap<>();
                m.put("firmId", f.firmId());
                m.put("creditLimit", f.creditLimit());
                m.put("enabled", f.enabled());
                long sumUpper = 0;
                final List<Map<String, Object>> perShard = new ArrayList<>();
                for (int s = 0; s < shardCount; s++)
                {
                    final String k = f.firmId() + ":" + s;
                    final Store.ObservedFirmRow o = observed.get(k);
                    final Map<String, Object> e = new LinkedHashMap<>();
                    e.put("shard", s);
                    e.put("target", targets.get(k));
                    e.put("confirmedBudget", o == null ? null : o.creditBudget());
                    e.put("creditUsed", o == null ? null : o.creditUsed());
                    e.put("enabledOnShard", o == null ? null : o.enabled());
                    e.put("maxPositionOnShard", o == null ? null : o.maxPositionPerSymbol());
                    e.put("pendingBudget", pending.get(k));
                    final long upper = Math.max(o == null ? 0 : o.creditBudget(), pending.getOrDefault(k, 0L));
                    e.put("upperBound", upper);
                    sumUpper += upper;
                    perShard.add(e);
                }
                m.put("sumUpperBound", sumUpper);
                m.put("shards", perShard);
                out.add(m);
            }
            return out;
        });
    }

    private List<Map<String, Object>> shards()
    {
        return db.read(c ->
        {
            final Map<Integer, Store.CursorRow> cursors = new TreeMap<>();
            Store.cursors(c).forEach(r -> cursors.put(r.shardId(), r));
            final List<Map<String, Object>> out = new ArrayList<>();
            for (int s = 0; s < shardCount; s++)
            {
                final Store.CursorRow r = cursors.get(s);
                final Map<String, Object> m = new LinkedHashMap<>();
                m.put("shard", s);
                m.put("connected", r != null && r.connected());
                m.put("caughtUp", r != null && r.caughtUp());
                m.put("eventSeq", r == null ? 0 : r.eventSeq());
                m.put("tradingDate", r == null ? 0 : r.tradingDate());
                m.put("lastEventTsNs", r == null ? 0 : r.lastEventTsNs());
                m.put("updatedMs", r == null ? null : r.updatedMs());
                m.put("unresolvedCommands", Database.queryLong(c, "SELECT COUNT(*) FROM command_log WHERE shard_id = ? AND " +
                    "status IN ('PENDING', 'SENT', 'SCHEDULED', 'SUPERSEDED')", s));
                out.add(m);
            }
            return out;
        });
    }

    private List<Map<String, Object>> commands(final Context ctx)
    {
        final String status = ctx.queryParam("status");
        if (status != null && !status.matches("[A-Z]{1,16}"))
        {
            throw new ApiException(400, "invalid status");
        }
        final Integer shard = ctx.queryParam("shard") == null ? null : (int)rangeLong("shard", ctx.queryParam("shard"), 0, shardCount - 1);
        final int limit = ctx.queryParam("limit") == null ? 100 : (int)rangeLong("limit", ctx.queryParam("limit"), 1, 1_000);
        return db.read(c -> Store.commands(c, status, shard, limit)).stream().map(RestApi::command).toList();
    }

    private Map<String, Object> command(final long id)
    {
        final Store.CommandRow r = db.read(c -> Store.command(c, id));
        if (r == null)
        {
            throw new ApiException(404, "unknown command " + id);
        }
        return command(r);
    }

    private static Map<String, Object> command(final Store.CommandRow r)
    {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("correlationId", r.correlationId());
        m.put("shard", r.shardId());
        m.put("kind", r.kind());
        m.put("origin", r.origin());
        m.put("detail", r.detail());
        m.put("status", r.status());
        m.put("attempts", r.attempts());
        m.put("rejectReason", r.rejectReason());
        m.put("effectiveNs", r.effectiveNs());
        m.put("auditId", r.auditId());
        m.put("leaderEpoch", r.leaderEpoch());
        m.put("createdMs", r.createdMs());
        m.put("updatedMs", r.updatedMs());
        return m;
    }

    private List<Map<String, Object>> audits(final Context ctx)
    {
        final int limit = ctx.queryParam("limit") == null ? 100 : (int)rangeLong("limit", ctx.queryParam("limit"), 1, 1_000);
        final long before = ctx.queryParam("before") == null ? Long.MAX_VALUE : rangeLong("before", ctx.queryParam("before"), 1, Long.MAX_VALUE);
        return db.read(c -> Store.audits(c, before, limit)).stream().map(RestApi::audit).toList();
    }

    private Map<String, Object> audit(final long id)
    {
        final Store.AuditRow r = db.read(c -> Store.audit(c, id));
        if (r == null)
        {
            throw new ApiException(404, "unknown audit entry " + id);
        }
        return audit(r);
    }

    private static Map<String, Object> audit(final Store.AuditRow r)
    {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r.id());
        m.put("createdMs", r.createdMs());
        m.put("actor", r.actor());
        m.put("receivedBy", r.instanceId());
        m.put("action", r.action());
        m.put("params", Json.parse(r.params()));
        m.put("status", r.status());
        m.put("outcome", r.outcome());
        m.put("correlationIds", r.correlationIds() == null ? List.of() :
            List.of(r.correlationIds().split(",")).stream().map(Long::parseLong).toList());
        m.put("executedBy", r.executedBy());
        m.put("updatedMs", r.updatedMs());
        return m;
    }

    private List<Map<String, Object>> calendar()
    {
        return db.read(Store::calendar).stream().map(r ->
        {
            final Map<String, Object> m = new LinkedHashMap<>();
            m.put("tradingDate", r.tradingDate());
            m.put("tradingDay", r.tradingDay());
            m.put("preOpen", r.preOpen());
            m.put("openAuction", r.openAuction());
            m.put("continuous", r.continuous());
            m.put("closeAuction", r.closeAuction());
            m.put("close", r.closeTime());
            m.put("dayRoll", r.dayRoll());
            m.put("note", r.note());
            return m;
        }).toList();
    }
}
