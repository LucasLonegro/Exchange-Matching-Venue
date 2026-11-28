package io.lmb.access;

import io.lmb.common.Json;
import io.lmb.common.access.AccessDirectory;
import io.lmb.common.access.AccessDirectorySource;
import io.lmb.common.access.Scope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccessServiceTest
{
    private static final String ADMIN = "test-admin";
    private static final String SERVICE = "test-service";

    @TempDir
    Path dir;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private AccessStore store;
    private AccessService service;

    @AfterEach
    void close()
    {
        if (service != null)
        {
            service.close();
        }
        else if (store != null)
        {
            store.close();
        }
    }

    private static String h2()
    {
        return "jdbc:h2:mem:access-" + UUID.randomUUID() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";
    }

    @Test
    void bootstrapRegisterRotateRevoke()
    {
        store = new AccessStore(h2(), "sa", "");
        assertTrue(store.bootstrapIfEmpty());
        assertFalse(store.bootstrapIfEmpty(), "bootstraps once");
        assertEquals(3, store.snapshot().tiers().size());
        assertTrue(store.snapshot().firms().isEmpty(), "no team exists until an operator registers it");

        // explicit session ids; a firm-wide key with every scope works for each of the firm's sessions, not another firm's
        store.registerFirm("ops-alice", 100, "Alpha", AccessDirectory.DEFAULT_TIER,
            List.of(new AccessStore.NewSession(1001, "a1", null), new AccessStore.NewSession(1002, "a2", null)), null);
        store.registerFirm("ops-alice", 200, "Beta", AccessDirectory.DEFAULT_TIER,
            List.of(new AccessStore.NewSession(2001, "b1", null)), null);
        final String alphaKey = store.issueKey("ops-alice", 100, 0, EnumSet.allOf(Scope.class), "firm", 0).apiKey();
        final String betaKey = store.issueKey("ops-alice", 200, 0, EnumSet.of(Scope.DROP_COPY), "drop copy", 0).apiKey();
        final long now = System.currentTimeMillis();

        AccessDirectory d = store.snapshot();
        final long v0 = d.version();
        final AccessDirectory.Grant firmWide = d.authenticateSession(100, 1002, alphaKey, now);
        assertNotNull(firmWide);
        assertEquals(EnumSet.allOf(Scope.class), firmWide.scopes());
        assertEquals(AccessDirectory.DEFAULT_TIER, firmWide.tier().name());
        assertNotNull(d.authenticateSession(100, 1001, alphaKey, now));
        assertNull(d.authenticateSession(200, 2001, alphaKey, now));
        assertNull(d.authenticateSession(100, 2001, alphaKey, now), "session of another firm");
        assertNull(d.authenticateSession(100, 1001, "wrong", now));
        assertNotNull(d.authenticateFirm(betaKey, Scope.DROP_COPY, now));
        assertNull(d.authenticateFirm(alphaKey.substring(0, alphaKey.length() - 1) + "x", Scope.DROP_COPY, now));

        // a team with two members: one key each, bound to the member's session, default scopes from the caller
        final AccessStore.Registration r = store.registerFirm("ops-alice", 300, "Gamma", "bot",
            List.of(new AccessStore.NewSession(0, "alice", null), new AccessStore.NewSession(0, "bob", "restricted")),
            EnumSet.of(Scope.TRADE, Scope.QUERY));
        assertEquals(2, r.sessions().size());
        final long alice = r.sessions().get(0).sessionId();
        final long bob = r.sessions().get(1).sessionId();
        assertTrue(alice >= 10_001 && bob == alice + 1, "auto session ids above the CSV range");
        final String aliceKey = r.keys().get(0).apiKey();
        final String bobKey = r.keys().get(1).apiKey();
        assertTrue(aliceKey.startsWith("lmb_" + r.keys().get(0).key().keyId() + "_"));
        assertNotEquals(aliceKey, r.keys().get(0).key().sha256(), "only the hash is stored");
        assertEquals(AccessDirectory.sha256Hex(aliceKey), r.keys().get(0).key().sha256());
        assertThrows(AccessStore.Refused.class, () -> store.registerFirm("ops-alice", 300, "again", "bot", List.of(),
            EnumSet.of(Scope.TRADE)));
        assertThrows(AccessStore.Refused.class, () -> store.registerFirm("ops-alice", 301, "x", "no-such-tier", List.of(),
            EnumSet.of(Scope.TRADE)));

        d = store.snapshot();
        assertTrue(d.version() > v0);
        final AccessDirectory.Grant a = d.authenticateSession(300, alice, aliceKey, now);
        assertNotNull(a);
        assertTrue(a.has(Scope.TRADE) && a.has(Scope.QUERY) && !a.has(Scope.DROP_COPY));
        assertEquals("bot", a.tier().name(), "the firm's tier");
        assertEquals("restricted", d.authenticateSession(300, bob, bobKey, now).tier().name(), "the member's own tier");
        assertNull(d.authenticateSession(300, bob, aliceKey, now), "a member key is bound to its session");
        assertNull(d.authenticateFirm(aliceKey, Scope.DROP_COPY, now));

        // a cancel-only firm-wide key
        final AccessStore.Issued cancelOnly = store.issueKey("ops-bob", 300, 0, EnumSet.of(Scope.CANCEL), "panic", 0);
        d = store.snapshot();
        final AccessDirectory.Grant c = d.authenticateSession(300, bob, cancelOnly.apiKey(), now);
        assertTrue(c.has(Scope.CANCEL) && !c.has(Scope.TRADE));
        assertThrows(AccessStore.Refused.class, () -> store.issueKey("ops-bob", 300, 1001, EnumSet.of(Scope.TRADE), "x", 0),
            "another firm's session");

        // rotation with grace: both keys work, the old one expires
        final AccessStore.Issued rotated = store.rotateKey("ops-alice", r.keys().get(0).key().keyId(), 60_000);
        d = store.snapshot();
        assertNotNull(d.authenticateSession(300, alice, aliceKey, now));
        assertNotNull(d.authenticateSession(300, alice, rotated.apiKey(), now));
        assertNull(d.authenticateSession(300, alice, aliceKey, now + 61_000), "past the grace period");
        assertFalse(d.stillValid(a, now + 61_000));
        assertTrue(d.stillValid(a, now));
        assertThrows(AccessStore.Refused.class, () -> store.rotateKey("ops-alice", r.keys().get(0).key().keyId(), 0),
            "rotates once");

        // rotation without grace: the old key leaves the snapshot at once
        final AccessStore.Issued bob2 = store.rotateKey("ops-alice", r.keys().get(1).key().keyId(), 0);
        d = store.snapshot();
        assertNull(d.authenticateSession(300, bob, bobKey, System.currentTimeMillis() + 1));
        assertNotNull(d.authenticateSession(300, bob, bob2.apiKey(), System.currentTimeMillis()));

        // revocation, then disabling a session and the firm
        final AccessDirectory.Grant fresh = d.authenticateSession(300, alice, rotated.apiKey(), System.currentTimeMillis());
        store.revokeKey("ops-alice", rotated.key().keyId());
        d = store.snapshot();
        assertFalse(d.stillValid(fresh, System.currentTimeMillis()));
        assertNull(d.authenticateSession(300, alice, rotated.apiKey(), System.currentTimeMillis()));

        final AccessDirectory.Grant bobGrant = d.authenticateSession(300, bob, bob2.apiKey(), System.currentTimeMillis());
        store.updateSession("ops-alice", bob, null, null, false);
        d = store.snapshot();
        assertFalse(d.stillValid(bobGrant, System.currentTimeMillis()));
        store.updateSession("ops-alice", bob, null, null, true);
        assertTrue(store.snapshot().stillValid(bobGrant, System.currentTimeMillis()));
        store.updateFirm("ops-alice", 300, null, null, false);
        assertFalse(store.snapshot().stillValid(bobGrant, System.currentTimeMillis()));
        assertNull(store.snapshot().authenticateFirm(cancelOnly.apiKey(), Scope.CANCEL, System.currentTimeMillis()));

        // the snapshot survives JSON exactly, and the audit names the operators
        final AccessDirectory back = AccessDirectory.fromJson(d.toJson());
        assertEquals(d.version(), back.version());
        assertEquals(d.keys().size(), back.keys().size());
        assertEquals(d.sessions().keySet(), back.sessions().keySet());
        final List<AccessStore.AuditRow> audit = store.audit(100);
        assertTrue(audit.stream().anyMatch(x -> x.actor().equals("ops-bob") && x.action().equals("ISSUE_KEY")));
        assertTrue(audit.stream().anyMatch(x -> x.action().equals("BOOTSTRAP")));
        assertTrue(audit.stream().noneMatch(x -> x.detail() != null && x.detail().contains(aliceKey)), "no secret in the audit");
    }

    @Test
    void httpApiDirectoryAndPolling() throws Exception
    {
        final AccessConfig cfg = new AccessConfig();
        cfg.dataDir = dir;
        cfg.adminPort = 0;
        cfg.internalPort = 0;
        cfg.metricsPort = 0;
        cfg.dbUrl = h2();
        cfg.adminToken = ADMIN;
        cfg.serviceToken = SERVICE;
        cfg.allowDefaultTokens = false;
        service = new AccessService(cfg).start();
        final String admin = "http://127.0.0.1:" + service.adminPort();
        final String internal = "http://127.0.0.1:" + service.internalPort();

        assertEquals(401, send(admin + "/api/firms", "GET", null, null).statusCode());
        assertEquals(401, send(admin + "/api/firms", "GET", null, "wrong").statusCode());
        assertEquals(200, send(admin + "/health", "GET", null, null).statusCode());
        assertEquals("[]", send(admin + "/api/firms", "GET", null, ADMIN).body(), "no team before one is registered");
        final HttpResponse<String> alpha = send(admin + "/api/firms", "POST",
            "{\"firmId\":100,\"name\":\"Alpha\",\"members\":[{\"sessionId\":1001,\"label\":\"a1\"}]}", ADMIN);
        assertEquals(201, alpha.statusCode());
        final String alphaKey = alpha.body().replaceAll("(?s).*\"apiKey\":\"([^\"]+)\".*", "$1");
        assertTrue(alphaKey.startsWith("lmb_"), alpha.body());
        assertTrue(send(admin + "/api/firms", "GET", null, ADMIN).body().contains("\"firmId\":100"));
        assertFalse(send(admin + "/api/firms", "GET", null, ADMIN).body().contains(alphaKey), "no secret in listings");

        // the directory: service token, ETag, 304
        assertEquals(401, send(internal + "/internal/v1/directory", "GET", null, null).statusCode());
        assertEquals(401, send(internal + "/internal/v1/directory", "GET", null, ADMIN).statusCode());
        final HttpResponse<String> first = send(internal + "/internal/v1/directory", "GET", null, SERVICE);
        assertEquals(200, first.statusCode());
        final String etag = first.headers().firstValue("ETag").orElseThrow();
        final HttpResponse<String> again = http.send(HttpRequest.newBuilder(URI.create(internal + "/internal/v1/directory"))
            .header("Authorization", "Bearer " + SERVICE).header("If-None-Match", etag).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(304, again.statusCode());

        // a polling client picks up a registration
        final Path cache = dir.resolve("gw").resolve("access-directory.json");
        final AccessDirectorySource source = AccessDirectorySource.polling(internal, SERVICE, 100, cache, "test");
        source.start();
        assertNotNull(source.current());
        final long v1 = source.current().version();

        assertEquals(400, send(admin + "/api/firms", "POST", "{\"firmId\":\"x\"}", ADMIN).statusCode());
        assertEquals(400, send(admin + "/api/firms", "POST", "{\"firmId\":400,\"name\":\"D\",\"scopes\":[\"FLY\"]}", ADMIN).statusCode());
        final HttpResponse<String> reg = send(admin + "/api/firms", "POST",
            "{\"firmId\":400,\"name\":\"Delta\",\"tier\":\"restricted\",\"members\":[{\"label\":\"carol\"}]}", ADMIN);
        assertEquals(201, reg.statusCode(), reg.body());
        final Map<String, Object> body = Json.parseObject(reg.body());
        final Map<?, ?> member = (Map<?, ?>)((List<?>)body.get("members")).get(0);
        final long carol = (Long)member.get("sessionId");
        final String carolKey = (String)((Map<?, ?>)member.get("key")).get("apiKey");
        final String carolKeyId = (String)((Map<?, ?>)member.get("key")).get("keyId");
        assertEquals(409, send(admin + "/api/firms", "POST", "{\"firmId\":400,\"name\":\"Delta\"}", ADMIN).statusCode());

        source.pollOnce();
        assertTrue(source.current().version() > v1);
        final AccessDirectory.Grant g = source.current().authenticateSession(400, carol, carolKey, System.currentTimeMillis());
        assertNotNull(g);
        assertEquals("restricted", g.tier().name());
        assertFalse(send(admin + "/api/keys?firmId=400", "GET", null, ADMIN).body().contains(carolKey));

        // the audit names the operator ("admin" for the single-token setup)
        assertTrue(send(admin + "/api/audit", "GET", null, ADMIN).body().contains("\"actor\":\"admin\""));

        // revocation reaches the poller; then the service goes away and the cache keeps the last snapshot
        assertEquals(200, send(admin + "/api/keys/" + carolKeyId, "DELETE", null, ADMIN).statusCode());
        source.pollOnce();
        assertFalse(source.current().stillValid(g, System.currentTimeMillis()));
        final long last = source.current().version();
        service.close();
        service = null;
        final long failures = source.pollFailures.get();
        source.pollOnce();
        assertTrue(source.pollFailures.get() > failures);
        assertEquals(last, source.current().version(), "keeps the last good snapshot");
        source.close();
        assertTrue(Files.exists(cache));
        try (AccessDirectorySource restarted = AccessDirectorySource.polling(internal, SERVICE, 100, cache, "test2"))
        {
            assertEquals(last, restarted.current().version(), "a restarted gateway starts from its cache");
        }
    }

    @Test
    void refusesDefaultTokensInProduction()
    {
        final AccessConfig cfg = new AccessConfig();
        cfg.dataDir = dir;
        cfg.dbUrl = h2();
        cfg.allowDefaultTokens = false;
        assertThrows(IllegalStateException.class, () -> new AccessService(cfg));
        cfg.adminToken = ADMIN;
        assertThrows(IllegalStateException.class, () -> new AccessService(cfg), "default service token");
    }

    private HttpResponse<String> send(final String url, final String method, final String body, final String token) throws Exception
    {
        final HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5))
            .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (token != null)
        {
            b.header("Authorization", "Bearer " + token);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }
}
