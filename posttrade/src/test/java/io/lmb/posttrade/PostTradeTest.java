package io.lmb.posttrade;

import io.lmb.common.FirmDirectory;
import io.lmb.common.Topology;
import io.lmb.sbe.ExecutionReportDecoder;
import io.lmb.sbe.TradeDecoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The assembled instance with the local profile: H2 file database, JSON-lines surveillance, FIX drop copy. */
class PostTradeTest
{
    private static final int BASE = 37050 + (int)(ProcessHandle.current().pid() % 20) * 2;

    private final TestVenue venue = new TestVenue(2).standardSetup();
    private final Path dataDir = Path.of("target", "test-data", "pt-" + System.nanoTime()).toAbsolutePath();
    private PostTrade postTrade;

    @AfterEach
    void tearDown()
    {
        if (postTrade != null)
        {
            postTrade.close();
        }
    }

    private PostTradeConfig config()
    {
        final Properties p = new Properties();
        p.setProperty("lmb.shards", "2");
        p.setProperty("lmb.data.dir", dataDir.toString());
        p.setProperty("lmb.posttrade.dropCopyFixPort", Integer.toString(BASE));
        p.setProperty("lmb.posttrade.metricsPort", Integer.toString(BASE + 100));
        p.setProperty("lmb.posttrade.bindHost", "127.0.0.1");
        p.setProperty("lmb.posttrade.restartBackoffMs", "100");
        p.setProperty("lmb.posttrade.dropcopy.adminKey", "admin");
        return PostTradeConfig.load(Topology.of(p), 0);
    }

    private PostTrade start() throws Exception
    {
        final FirmDirectory firms = new FirmDirectory(List.of(
            new FirmDirectory.Firm(100, "alpha", "k100", List.of(1000L), 1_000_000, 1_000_000)));
        postTrade = new PostTrade(config(), venue, firms, null).start();
        return postTrade;
    }

    private void stop()
    {
        postTrade.close();
        postTrade = null;
    }

    private long query(final String sql) throws Exception
    {
        final PostTradeConfig c = config();
        try (Connection con = DriverManager.getConnection(c.dbUrl, c.dbUser, c.dbPassword);
             Statement st = con.createStatement(); ResultSet rs = st.executeQuery(sql))
        {
            rs.next();
            return rs.getLong(1);
        }
    }

    private long venueCount(final int templateId)
    {
        return venue.count(0, templateId) + venue.count(1, templateId);
    }

    @Test
    void announcesReadyServesMetricsAndRestartsWithoutDuplicates() throws Exception
    {
        venue.cross(0, 10_000, 5);
        venue.cross(1, 10_100, 7);
        start();
        assertEquals(List.of("tradestore", "dropcopy", "surveillance"), postTrade.sinkNames());

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(PostTradeMain.awaitReadyAndAnnounce(postTrade, new PrintStream(out, true, StandardCharsets.UTF_8)));
        assertEquals("LMB_READY posttrade 0", out.toString(StandardCharsets.UTF_8).trim());

        final String metrics = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + (BASE + 100) + "/metrics")).build(),
            HttpResponse.BodyHandlers.ofString()).body();
        assertTrue(metrics.contains("lmb_posttrade_events_total{sink=\"surveillance\"}") ||
            metrics.contains("lmb_posttrade_events{sink=\"surveillance\"}"), metrics);
        assertTrue(metrics.contains("lmb_posttrade_dropcopy_sessions"), metrics);

        venue.cross(0, 10_000, 2); // live, then a clean stop (SIGTERM path)
        TestVenue.await(() -> postTrade.runners().stream().allMatch(r -> r.lastEventSeq(0) == venue.venue.eventCount(0)),
            10_000, "every sink to apply shard 0");
        stop();
        assertEquals(3, query("SELECT COUNT(*) FROM pt_trade"));
        assertTrue(Files.readString(dataDir.resolve("posttrade/0/dropcopy/dropcopy.ckpt")).contains("clean=true"));

        venue.cross(1, 10_100, 1); // while down
        start(); // same data dir: H2 file, surveillance file, drop copy checkpoint and Artio logs reused
        assertTrue(postTrade.awaitReady(20_000));
        TestVenue.await(() -> postTrade.runners().stream().allMatch(r -> r.lastEventSeq(1) == venue.venue.eventCount(1)),
            10_000, "every sink to apply shard 1");
        stop();
        assertEquals(venueCount(TradeDecoder.TEMPLATE_ID), query("SELECT COUNT(*) FROM pt_trade"));
        assertEquals(venueCount(TradeDecoder.TEMPLATE_ID) + venueCount(ExecutionReportDecoder.TEMPLATE_ID),
            Files.readAllLines(dataDir.resolve("posttrade/0/surveillance.jsonl")).size());
    }
}
