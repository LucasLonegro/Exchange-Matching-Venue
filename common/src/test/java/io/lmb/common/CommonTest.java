package io.lmb.common;

import io.lmb.sbe.CommandAppliedEncoder;
import io.lmb.sbe.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommonTest
{
    @Test
    void topologyDefaultsGiveDistinctPortsPerMember()
    {
        final Properties p = new Properties();
        p.setProperty("lmb.shards", "2");
        p.setProperty("lmb.members", "2");
        final Topology t = Topology.of(p);
        assertEquals(20000, t.memberBasePort(0, 0));
        assertEquals(21100, t.memberBasePort(1, 1));
        assertEquals("0=127.0.0.1:20002,1=127.0.0.1:20102", t.ingressEndpoints(0));
        assertEquals(
            "0,127.0.0.1:21002,127.0.0.1:21003,127.0.0.1:21004,127.0.0.1:21005,127.0.0.1:21000|" +
            "1,127.0.0.1:21102,127.0.0.1:21103,127.0.0.1:21104,127.0.0.1:21105,127.0.0.1:21100",
            t.clusterMembers(1));
    }

    @Test
    void eventHeaderAccessorsMatchTheCodec()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final CommandAppliedEncoder encoder = new CommandAppliedEncoder();
        encoder.wrapAndApplyHeader(buffer, 16, new MessageHeaderEncoder());
        encoder.eventHeader().eventSeq(42).logPosition(4096).timestampNs(7).eventIdx(3).shardId(5);
        assertEquals(CommandAppliedEncoder.TEMPLATE_ID, Events.templateId(buffer, 16));
        assertTrue(Events.isEvent(buffer, 16));
        assertEquals(42, Events.eventSeq(buffer, 16));
        assertEquals(4096, Events.logPosition(buffer, 16));
        assertEquals(7, Events.timestampNs(buffer, 16));
        assertEquals(3, Events.eventIdx(buffer, 16));
        assertEquals(5, Events.shardId(buffer, 16));
        assertEquals("5-4096-3", Events.execId(buffer, 16));
    }

    @Test
    void hashChainIsDeterministicAndOrderSensitive()
    {
        final UnsafeBuffer a = new UnsafeBuffer("command-a".getBytes());
        final UnsafeBuffer b = new UnsafeBuffer("command-b".getBytes());
        final byte[] ab = chain(a, b);
        assertArrayEquals(ab, chain(a, b));
        assertFalse(java.util.Arrays.equals(ab, chain(b, a)));
    }

    private static byte[] chain(final UnsafeBuffer first, final UnsafeBuffer second)
    {
        final HashChain chain = new HashChain();
        chain.begin(32, 1);
        chain.update(first, 0, first.capacity());
        chain.finish();
        chain.begin(64, 2);
        chain.update(second, 0, second.capacity());
        return chain.finish().clone();
    }

    // band-stp
    @Test
    void symbolDirectoryOptionalBandColumns() throws Exception
    {
        final Path dir = Path.of("target", "symdir-test");
        Files.createDirectories(dir);
        final Path plain = dir.resolve("plain.csv");
        Files.writeString(plain, "symbolId,ticker,shard,tickSize,lotSize,referencePrice,priceBandBps,maxOrderQty\n" +
            "1,AAA,0,1,1,10000,1000,100000\n");
        final SymbolDirectory.Symbol a = SymbolDirectory.load(plain).byId(1);
        assertEquals(SymbolDirectory.DEFAULT_DYNAMIC_BAND_BPS, a.dynamicBandBps());
        assertEquals(SymbolDirectory.DEFAULT_VOLATILITY_AUCTION_NS, a.volatilityAuctionNs());

        final Path extended = dir.resolve("extended.csv");
        Files.writeString(extended,
            "symbolId,ticker,shard,tickSize,lotSize,referencePrice,priceBandBps,maxOrderQty,volatilityAuctionNs,dynamicBandBps\n" +
            "1,AAA,0,1,1,10000,1000,100000,60000000000,200\n" +
            "2,BBB,0,1,1,10000,1000,100000,,\n");
        final SymbolDirectory d = SymbolDirectory.load(extended);
        assertEquals(200, d.byId(1).dynamicBandBps());
        assertEquals(60_000_000_000L, d.byId(1).volatilityAuctionNs());
        assertEquals(0, d.byId(2).dynamicBandBps());
        assertEquals(SymbolDirectory.DEFAULT_VOLATILITY_AUCTION_NS, d.byId(2).volatilityAuctionNs());

        final Path headerless = dir.resolve("headerless.csv");
        Files.writeString(headerless, "1,AAA,0,1,1,10000,1000,100000,150,1000\n");
        assertEquals(150, SymbolDirectory.load(headerless).byId(1).dynamicBandBps());
        assertEquals(1000, SymbolDirectory.load(headerless).byId(1).volatilityAuctionNs());
    }
}
