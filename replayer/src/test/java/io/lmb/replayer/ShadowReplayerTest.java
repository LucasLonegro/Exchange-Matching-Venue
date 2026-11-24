package io.lmb.replayer;

import io.lmb.api.ShardStateMachineFactory;
import io.lmb.cluster.client.ClusterVenueClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs the shadow replayer against a live 2-member shard while orders flow. */
class ShadowReplayerTest
{
    private TestShard shard;
    private ClusterVenueClients venue;
    private ShadowReplayer replayer;
    private ShadowReplayer skewed;

    @AfterEach
    void tearDown()
    {
        if (replayer != null)
        {
            replayer.close();
        }
        if (skewed != null)
        {
            skewed.close();
        }
        if (venue != null)
        {
            venue.close();
        }
        if (shard != null)
        {
            shard.close();
        }
    }

    @Test
    void verifiesLiveShardContinuouslyAndCatchesANondeterministicReplica() throws Exception
    {
        shard = new TestShard("shadow", 2, 38100);
        shard.startAll();
        shard.awaitLeader();
        shard.send("SymbolDefinition correlationId=1 symbolId=1 ticker=ACME tickSize=1 lotSize=1 referencePrice=10000 " +
            "maxOrderQty=100000 priceBandBps=1000 initialState=CONTINUOUS");
        shard.send("RiskLimitUpdate correlationId=2 firmId=1 creditBudget=1000000000000 maxPositionPerSymbol=1000000 enabled=TRUE");
        shard.send("RiskLimitUpdate correlationId=3 firmId=2 creditBudget=1000000000000 maxPositionPerSymbol=1000000 enabled=TRUE");
        long commands = 3 + orders(0, 10);
        shard.awaitEntries(commands);

        venue = new ClusterVenueClients(shard.topology, "shadow-replayer", shard.clientConfig());
        final ReplayerConfig config = new ReplayerConfig();
        config.shardId = 0;
        config.memberDirs = List.of(shard.memberDir(0), shard.memberDir(1));
        config.metricsPort = 38150;
        replayer = new ShadowReplayer(config, ShardStateMachineFactory.load(), venue).start();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertTrue(ShadowReplayerMain.awaitReadyAndAnnounce(replayer, config, new PrintStream(out, true, StandardCharsets.UTF_8)));
        assertEquals("LMB_READY replayer 0" + System.lineSeparator(), out.toString(StandardCharsets.UTF_8));
        assertEquals(0, replayer.startPosition());
        assertTrue(replayer.entries() >= commands, () -> "entries " + replayer.entries());

        // a replica whose machine is not deterministic (1 ns off on entry 20) must be caught there
        final ReplayerConfig skewedConfig = new ReplayerConfig();
        skewedConfig.shardId = 0;
        skewedConfig.memberDirs = config.memberDirs;
        skewedConfig.metricsPort = 38151;
        final ShardStateMachineFactory real = ShardStateMachineFactory.load();
        skewed = new ShadowReplayer(skewedConfig, s -> new OfflineVerifierTest.SkewedMachine(real.create(s), 20), venue).start();

        // orders keep flowing while both run
        commands += orders(10, 40);
        commands++;
        shard.send("SymbolStateCommand correlationId=4 symbolId=1 effectiveTimeNs=1 targetState=HALTED"); // + timer entry
        commands++;
        commands += orders(50, 10);
        final long lastSeq = shard.awaitEntries(commands);

        TestShard.await("replayer verified the live edge", 20_000, () -> replayer.verifiedSeq() == lastSeq);
        assertNull(replayer.divergence(), () -> String.valueOf(replayer.divergence()));
        assertEquals(commands, replayer.entries());
        assertEquals(replayer.appliedPosition(), replayer.verifiedPosition());
        assertTrue(replayer.isRunning());

        final String metrics = scrape(config.metricsPort);
        assertTrue(metrics.contains("lmb_replayer_divergence{shard=\"0\"} 0.0"), metrics);
        assertTrue(metrics.contains("lmb_replayer_lag_events{shard=\"0\"} 0.0"), metrics);
        assertTrue(metrics.contains("lmb_replayer_lag_bytes{shard=\"0\"} 0.0"), metrics);
        assertTrue(metrics.contains("lmb_replayer_verified_event_seq{shard=\"0\"} " + (double)lastSeq) ||
            metrics.contains("lmb_replayer_verified_event_seq{shard=\"0\"} " + String.format("%.6e", (double)lastSeq)), metrics);

        TestShard.await("skewed replica diverges", 20_000, () -> skewed.divergence() != null);
        final Divergence d = skewed.divergence();
        assertEquals(20, d.entry(), d::toString);
        assertTrue(skewed.isRunning(), "a divergence does not stop the process");
        assertTrue(scrape(skewedConfig.metricsPort).contains("lmb_replayer_divergence{shard=\"0\"} 1.0"));
        assertTrue(skewed.verifiedSeq() < lastSeq);

        replayer.close();
        assertFalse(replayer.isRunning());
        assertNull(replayer.failure());
        assertNotNull(replayer);
    }

    private int orders(final int first, final int count)
    {
        for (int i = first; i < first + count; i++)
        {
            final boolean buy = (i & 1) == 0;
            shard.send("NewOrder sessionId=1 clOrdId=" + (100 + i) + " firmId=" + (buy ? 1 : 2) + " accountId=1 symbolId=1 side=" +
                (buy ? "BUY" : "SELL") + " ordType=LIMIT timeInForce=DAY price=" + (buy ? 10000 + i % 5 : 10000 - i % 3) +
                " quantity=" + (100 + 10 * (i % 4)) + " possResend=FALSE");
        }
        return count;
    }

    private static String scrape(final int port) throws IOException
    {
        try (InputStream in = URI.create("http://127.0.0.1:" + port + "/metrics").toURL().openStream())
        {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
