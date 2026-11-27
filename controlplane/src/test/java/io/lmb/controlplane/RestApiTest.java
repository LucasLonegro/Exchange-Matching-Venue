package io.lmb.controlplane;

import io.lmb.sbe.SymbolState;
import io.lmb.testkit.InMemoryVenue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Full control plane with its own threads, REST on loopback, real time. */
class RestApiTest
{
    private static final int REST_PORT = 36100;
    private static final int METRICS_PORT = 36101;
    private static final String TOKEN = "test-token";

    private final ScriptedShard s0 = new ScriptedShard(0, null);
    private final ScriptedShard s1 = new ScriptedShard(1, null);
    private final InMemoryVenue venue = new InMemoryVenue(TestSupport.NOON, s0, s1);
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private ControlPlane cp;

    @BeforeEach
    void start() throws Exception
    {
        final ControlPlaneConfig cfg = TestSupport.config(TestSupport.h2Url("rest"), TestSupport.dataDir(), "rest-0");
        cfg.restHost = "127.0.0.1";
        cfg.restPort = REST_PORT;
        cfg.metricsPort = METRICS_PORT;
        cfg.adminToken = TOKEN;
        cfg.adminTokenDefaulted = false;
        cfg.autoOpen = true;
        cfg.autoOpenDelayMs = 100;
        cfg.leaseRenewMs = 100;
        cfg.leaseTtlMs = 2_000;
        cfg.leaseMarginMs = 500;
        cp = new ControlPlane(cfg, venue, Clock.system());
        cp.start(true);
        assertTrue(await(() -> s0.state(1) == SymbolState.CONTINUOUS && s1.state(7) == SymbolState.CONTINUOUS &&
            s1.limits(200) != null && s0.limits(200) != null));
    }

    @AfterEach
    void stop()
    {
        cp.close();
    }

    @Test
    void endToEnd() throws Exception
    {
        // auth
        assertEquals(401, call("GET", "/api/symbols", null, null).statusCode());
        assertEquals(401, call("GET", "/api/symbols", null, "wrong").statusCode());
        final HttpResponse<String> health = call("GET", "/health", null, null);
        assertEquals(200, health.statusCode());
        assertTrue(health.body().contains("\"leader\":true"));

        // reads
        final List<?> symbols = (List<?>)Json.parse(get("/api/symbols"));
        assertEquals(6, symbols.size());
        assertTrue(await(() -> get("/api/symbols/1").contains("\"shardState\":\"CONTINUOUS\"")));
        assertTrue(get("/api/firms").contains("\"apiKey\":\"al***\""), "api keys are masked");
        assertTrue(get("/api/limits").contains("\"creditLimit\":100000000"));
        assertEquals(404, call("GET", "/api/symbols/99", null, TOKEN).statusCode());

        // halt / resume one symbol
        Map<?, ?> r = write("POST", "/api/symbols/1/halt", null, 200);
        assertEquals("COMPLETED", r.get("status"));
        assertEquals("ops-alice", r.get("actor"));
        assertEquals(SymbolState.HALTED, s0.state(1));
        assertEquals("HALTED", ((Map<?, ?>)Json.parse(get("/api/symbols/1"))).get("operatorState"));
        write("POST", "/api/symbols/1/resume", null, 200);
        assertEquals(SymbolState.CONTINUOUS, s0.state(1));

        // auctions
        write("POST", "/api/symbols/2/auction/start", null, 200);
        assertEquals(SymbolState.AUCTION_CALL, s0.state(2));
        Thread.sleep(400); // enforcement must respect the operator's auction
        assertEquals(SymbolState.AUCTION_CALL, s0.state(2));
        write("POST", "/api/symbols/2/auction/stop", null, 200);
        assertEquals(SymbolState.CONTINUOUS, s0.state(2));
        assertEquals(422, call("POST", "/api/symbols/2/auction/stop", null, TOKEN).statusCode(), "not in auction");

        // market-wide halt on one shard, then resume
        write("POST", "/api/market/halt?shard=1", null, 200);
        assertEquals(SymbolState.HALTED, s1.state(5));
        assertEquals(SymbolState.HALTED, s1.state(7));
        assertEquals(SymbolState.CONTINUOUS, s0.state(1));
        write("POST", "/api/market/resume?shard=1", null, 200);
        assertEquals(SymbolState.CONTINUOUS, s1.state(6));

        // limits
        assertEquals(400, call("PUT", "/api/firms/200/limits", "{\"maxPositionPerSymbol\":-5}", TOKEN).statusCode());
        assertEquals(400, call("PUT", "/api/firms/200/limits", "{}", TOKEN).statusCode());
        write("PUT", "/api/firms/200/limits", "{\"maxPositionPerSymbol\":1234,\"creditLimit\":40000000}", 200);
        assertEquals(1234, s0.limits(200).maxPos);
        assertEquals(1234, s1.limits(200).maxPos);
        assertTrue(await(() -> s0.limits(200).budget + s1.limits(200).budget <= 40_000_000L));

        // kill switch with order cancellation, then re-enable
        r = write("POST", "/api/firms/100/kill?cancelOrders=true", null, 200);
        assertEquals("COMPLETED", r.get("status"));
        assertFalse(s0.limits(100).enabled);
        assertFalse(s1.limits(100).enabled);
        assertTrue(s0.count("MASS FIRM firm=100") >= 1 && s1.count("MASS FIRM firm=100") >= 1);
        Thread.sleep(300); // refreshes must not re-enable a killed firm
        assertFalse(s0.limits(100).enabled);
        write("POST", "/api/firms/100/enable", null, 200);
        assertTrue(s0.limits(100).enabled && s1.limits(100).enabled);

        // admin mass cancel and validation
        write("POST", "/api/masscancel", "{\"scope\":\"SYMBOL\",\"symbolId\":5}", 200);
        assertEquals(1, s1.count("MASS SYMBOL"));
        assertEquals(0, s0.count("MASS SYMBOL"));
        assertEquals(400, call("POST", "/api/masscancel", "{\"scope\":\"GATEWAY\"}", TOKEN).statusCode());
        assertEquals(400, call("POST", "/api/masscancel", "{\"scope\":\"FIRM\"}", TOKEN).statusCode());
        assertEquals(400, call("POST", "/api/masscancel", "not json", TOKEN).statusCode());

        // day roll
        write("POST", "/api/dayroll", "{\"tradingDate\":20261117}", 200);
        assertEquals(20261117, s0.tradingDate());
        assertEquals(20261117, s1.tradingDate());
        assertEquals(400, call("POST", "/api/dayroll", "{\"tradingDate\":20261341}", TOKEN).statusCode());

        // reference data changes
        assertEquals(400, call("POST", "/api/symbols", "{\"symbolId\":9,\"ticker\":\"ibm!\",\"shard\":0}", TOKEN).statusCode());
        write("POST", "/api/symbols", "{\"symbolId\":9,\"ticker\":\"IBM\",\"shard\":0,\"tickSize\":1,\"lotSize\":1," +
            "\"referencePrice\":15000,\"priceBandBps\":500,\"maxOrderQty\":1000}", 200);
        assertEquals(500, s0.symbol(9).band);
        assertEquals(422, call("POST", "/api/symbols", "{\"symbolId\":9,\"ticker\":\"IBM\",\"shard\":1,\"tickSize\":1,\"lotSize\":1," +
            "\"referencePrice\":15000,\"priceBandBps\":500,\"maxOrderQty\":1000}", TOKEN).statusCode(), "cannot move shards");
        dynamicCollar();
        write("DELETE", "/api/symbols/9", null, 200);
        assertEquals(SymbolState.REMOVED, s0.state(9));
        assertEquals(422, call("POST", "/api/symbols/9/halt", null, TOKEN).statusCode(), "removed symbol");

        // calendar overrides
        write("PUT", "/api/calendar/20261126", "{\"tradingDay\":false,\"note\":\"Thanksgiving\"}", 200);
        assertTrue(get("/api/calendar").contains("Thanksgiving"));
        assertEquals(400, call("PUT", "/api/calendar/20261126", "{\"close\":\"25:00\"}", TOKEN).statusCode());

        // status views
        final List<?> budgets = (List<?>)Json.parse(get("/api/budgets"));
        for (final Object o : budgets)
        {
            final Map<?, ?> b = (Map<?, ?>)o;
            assertTrue(((Number)b.get("sumUpperBound")).longValue() <= ((Number)b.get("creditLimit")).longValue(), b.toString());
        }
        final Map<?, ?> status = (Map<?, ?>)Json.parse(get("/api/status"));
        assertEquals(Boolean.TRUE, status.get("leader"));
        assertEquals("rest-0", ((Map<?, ?>)status.get("lease")).get("holder"));
        assertTrue(get("/api/shards").contains("\"connected\":true"));
        final List<?> commands = (List<?>)Json.parse(get("/api/commands?limit=500"));
        assertFalse(commands.isEmpty());
        final long corr = ((Number)((Map<?, ?>)commands.get(0)).get("correlationId")).longValue();
        assertTrue(get("/api/commands/" + corr).contains("\"correlationId\":" + corr));
        final List<?> audit = (List<?>)Json.parse(get("/api/audit?limit=200"));
        assertTrue(audit.size() >= 15);
        assertTrue(audit.stream().allMatch(a -> ((Map<?, ?>)a).get("status") != null));
        assertTrue(get("/api/audit").contains("\"action\":\"KILL_FIRM\""));

        // metrics
        final HttpResponse<String> metrics = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + METRICS_PORT + "/metrics"))
            .build(), HttpResponse.BodyHandlers.ofString());
        assertTrue(metrics.body().contains("lmb_controlplane_is_leader 1"));
        assertTrue(metrics.body().contains("lmb_controlplane_admin_requests_total"));
    }

    /** Symbol 9 (shard 0) exists without dynamic-collar fields: the defaults, then POST/PUT validation and the GET view. */
    private void dynamicCollar() throws Exception
    {
        final String body = "\"ticker\":\"IBM\",\"shard\":0,\"tickSize\":1,\"lotSize\":1,\"referencePrice\":15000," +
            "\"priceBandBps\":500,\"maxOrderQty\":1000";
        final String post = "{\"symbolId\":9," + body;
        Map<?, ?> g = (Map<?, ?>)Json.parse(get("/api/symbols/9"));
        assertEquals(0L, num(g, "dynamicBandBps"), "CSV default");
        assertEquals(300_000_000_000L, num(g, "volatilityAuctionNs"), "CSV default");
        assertEquals(0, s0.symbol(9).dyn);
        assertEquals(300_000_000_000L, s0.symbol(9).vol);

        // range checks on POST and PUT
        for (final String bad : List.of(",\"dynamicBandBps\":10001}", ",\"dynamicBandBps\":-1}", ",\"dynamicBandBps\":1.5}",
            ",\"dynamicBandBps\":\"x\"}", ",\"volatilityAuctionNs\":86400000000001}", ",\"volatilityAuctionNs\":-1}",
            ",\"dynamicBandBps\":100,\"volatilityAuctionNs\":0}"))
        {
            assertEquals(400, call("POST", "/api/symbols", post + bad, TOKEN).statusCode(), "POST " + bad);
            assertEquals(400, call("PUT", "/api/symbols/9", "{" + body + bad, TOKEN).statusCode(), "PUT " + bad);
        }
        assertEquals(400, call("PUT", "/api/symbols/9", "{\"symbolId\":8," + body + "}", TOKEN).statusCode(), "path/body id mismatch");
        assertEquals(0, s0.symbol(9).dyn, "rejected writes change nothing");

        // POST both, then PUT one at a time: the absent field keeps the stored value
        write("POST", "/api/symbols", post + ",\"dynamicBandBps\":200,\"volatilityAuctionNs\":120000000000}", 200);
        assertTrue(await(() -> s0.symbol(9).dyn == 200 && s0.symbol(9).vol == 120_000_000_000L));
        write("PUT", "/api/symbols/9", "{" + body + ",\"dynamicBandBps\":300}", 200);
        assertTrue(await(() -> s0.symbol(9).dyn == 300 && s0.symbol(9).vol == 120_000_000_000L));
        write("PUT", "/api/symbols/9", "{\"symbolId\":9," + body + ",\"volatilityAuctionNs\":60000000000}", 200);
        assertTrue(await(() -> s0.symbol(9).dyn == 300 && s0.symbol(9).vol == 60_000_000_000L));
        assertEquals(400, call("PUT", "/api/symbols/9", "{" + body + ",\"volatilityAuctionNs\":0}", TOKEN).statusCode(),
            "the kept dynamicBandBps (300) needs a volatility auction");
        write("PUT", "/api/symbols/9", "{" + body + ",\"dynamicBandBps\":10000,\"volatilityAuctionNs\":86400000000000}", 200);
        assertTrue(await(() -> s0.symbol(9).dyn == 10_000 && s0.symbol(9).vol == 86_400_000_000_000L), "upper bounds");
        write("PUT", "/api/symbols/9", "{" + body + ",\"dynamicBandBps\":0,\"volatilityAuctionNs\":0}", 200);
        assertTrue(await(() -> s0.symbol(9).dyn == 0 && s0.symbol(9).vol == 0), "collar off");
        write("PUT", "/api/symbols/9", "{" + body + ",\"dynamicBandBps\":250,\"volatilityAuctionNs\":3600000000000}", 200);
        assertTrue(await(() -> s0.symbol(9).dyn == 250 && s0.symbol(9).vol == 3_600_000_000_000L));
        write("PUT", "/api/symbols/9", "{" + body + "}", 200);
        assertEquals(250, s0.symbol(9).dyn, "both absent: both kept");
        assertEquals(3_600_000_000_000L, s0.symbol(9).vol);

        g = (Map<?, ?>)Json.parse(get("/api/symbols/9"));
        assertEquals(250L, num(g, "dynamicBandBps"));
        assertEquals(3_600_000_000_000L, num(g, "volatilityAuctionNs"));
        assertEquals(15_000L, num(g, "referencePrice"));
        assertEquals(Boolean.FALSE, g.get("inVolatilityAuction"));
        assertEquals(null, g.get("volatilityAuctionSinceNs"));
        assertEquals(null, g.get("venueReferencePrice"));
        assertEquals(null, g.get("lastClose"));

        // venue-side state: own reference, a volatility auction, then a forced close at a last trade
        assertTrue(await(() -> s0.state(9) == SymbolState.CONTINUOUS));
        s0.reference(9, venue.nowNs(), 15_100);
        assertTrue(await(() -> get("/api/symbols/9").contains("\"venueReferencePrice\":15100")));
        g = (Map<?, ?>)Json.parse(get("/api/symbols/9"));
        assertEquals(15_000L, num(g, "referencePrice"), "listing price stays the DB value");
        assertEquals(14_345L, num(g, "bandLow"));
        assertEquals(15_855L, num(g, "bandHigh"));
        s0.interrupt(9, venue.nowNs(), Long.MIN_VALUE);
        assertTrue(await(() -> get("/api/symbols/9").contains("\"inVolatilityAuction\":true")));
        assertTrue(((Map<?, ?>)Json.parse(get("/api/symbols/9"))).get("volatilityAuctionSinceNs") instanceof Number);
        assertEquals(400, call("POST", "/api/symbols/9/auction/stop?target=OPEN", null, TOKEN).statusCode());
        assertEquals(422, call("POST", "/api/symbols/9/auction/stop?target=CLOSED", null, TOKEN).statusCode(),
            "volatility auction needs force");
        assertTrue(s0.inVolatilityAuction(9));
        s0.trade(9, 15_050);
        write("POST", "/api/symbols/9/auction/stop?target=CLOSED&force=true", null, 200);
        assertFalse(s0.inVolatilityAuction(9)); // CLOSED is transient: with no operator hold enforcement reopens it
        assertTrue(await(() -> get("/api/symbols/9").contains("\"source\":\"LAST_TRADE\"")));
        g = (Map<?, ?>)Json.parse(get("/api/symbols/9"));
        assertEquals(Boolean.FALSE, g.get("inVolatilityAuction"));
        assertEquals(null, g.get("volatilityAuctionSinceNs"));
        final Map<?, ?> close = (Map<?, ?>)g.get("lastClose");
        assertEquals(15_050L, num(close, "price"));
        assertEquals(null, close.get("previousClose"));
        assertEquals(20261117L, num(close, "tradingDate"));
    }

    private static long num(final Map<?, ?> m, final String key)
    {
        return ((Number)m.get(key)).longValue();
    }

    private Map<?, ?> write(final String method, final String path, final String body, final int expected) throws Exception
    {
        final HttpResponse<String> r = call(method, path, body, TOKEN);
        assertEquals(expected, r.statusCode(), method + " " + path + " -> " + r.body());
        return (Map<?, ?>)Json.parse(r.body());
    }

    private String get(final String path)
    {
        try
        {
            final HttpResponse<String> r = call("GET", path, null, TOKEN);
            assertEquals(200, r.statusCode(), r.body());
            return r.body();
        }
        catch (final Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    private HttpResponse<String> call(final String method, final String path, final String body, final String token) throws Exception
    {
        final HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + REST_PORT + path))
            .timeout(Duration.ofSeconds(10))
            .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
            .header("X-LMB-Actor", "ops-alice");
        if (token != null)
        {
            b.header("X-LMB-Admin-Token", token);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static boolean await(final BooleanSupplier condition) throws InterruptedException
    {
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (System.nanoTime() < deadline)
        {
            if (condition.getAsBoolean())
            {
                return true;
            }
            Thread.sleep(20);
        }
        return condition.getAsBoolean();
    }
}
