package io.lmb.marketdata;

import io.lmb.marketdata.wire.MdWire;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static io.lmb.marketdata.CheckpointRestartTest.drain;
import static io.lmb.marketdata.CheckpointRestartTest.engine;
import static io.lmb.marketdata.CheckpointRestartTest.engineConfig;
import static io.lmb.marketdata.CheckpointRestartTest.messages;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Feeds A and B (and any number of instances) publish the same messages, byte for byte, with the same md_seq. */
class TwoPublishersTest
{
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception
    {
        for (int i = closeables.size() - 1; i >= 0; i--)
        {
            closeables.get(i).close();
        }
    }

    @Test
    void instancesReadingAtDifferentPacesProduceIdenticalMessagesAndSnapshots() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        final RetransmitHistory ha = new RetransmitHistory(1 << 16);
        final RetransmitHistory hb = new RetransmitHistory(1 << 16);
        final MarketDataEngine a = engine(engineConfig(2, null, 3), v.venue, ha); // tiny polls: batches split across polls
        closeables.add(a);
        for (int i = 0; i < 30; i++)
        {
            CheckpointRestartTest.activity(v, 40 + i * 7L, 1);
            for (int k = 0; k < 5; k++)
            {
                a.doWork(i);
            }
        }
        final MarketDataEngine b = engine(engineConfig(2, null, 10_000), v.venue, hb); // joins late, one big read
        closeables.add(b);
        drain(a, v, 99);
        drain(b, v, 99);

        final TreeMap<Integer, byte[][]> ma = messages(ha, a.builder());
        final TreeMap<Integer, byte[][]> mb = messages(hb, b.builder());
        assertEquals(ma.keySet(), mb.keySet());
        int n = 0;
        for (final int id : ma.keySet())
        {
            assertEquals(ma.get(id).length, mb.get(id).length, "symbol " + id + " md_seq");
            for (int i = 0; i < ma.get(id).length; i++)
            {
                assertNotNull(ma.get(id)[i]);
                assertArrayEquals(ma.get(id)[i], mb.get(id)[i], "symbol " + id + " md_seq " + (i + 1));
                n++;
            }
        }
        assertTrue(n > 500, "messages compared: " + n);

        final SnapshotWriter w = new SnapshotWriter();
        final ExpandableArrayBuffer sa = new ExpandableArrayBuffer();
        final ExpandableArrayBuffer sb = new ExpandableArrayBuffer();
        final IntArrayList ids = a.builder().sortedSymbolIds();
        for (int i = 0; i < ids.size(); i++)
        {
            final int la = w.write(a.builder().symbol(ids.getInt(i)), sa, 0);
            final int lb = w.write(b.builder().symbol(ids.getInt(i)), sb, 0);
            final byte[] ba = new byte[la];
            final byte[] bb = new byte[lb];
            sa.getBytes(0, ba);
            sb.getBytes(0, bb);
            assertArrayEquals(ba, bb, "snapshot of symbol " + ids.getInt(i));
        }
    }

    /** Raw UDP receiver: every sequenced message by (symbolId, md_seq), plus the feed ids seen. */
    private static final class Capture implements AutoCloseable
    {
        final DatagramChannel channel;
        final ByteBuffer packet = ByteBuffer.allocateDirect(65536);
        final UnsafeBuffer view = new UnsafeBuffer(packet);
        final TreeMap<Long, byte[]> bySymbolSeq = new TreeMap<>();
        final Set<Integer> feedIds = new HashSet<>();
        final Set<Long> sessionIds = new HashSet<>();
        long duplicates;

        Capture(final int port) throws Exception
        {
            channel = DatagramChannel.open();
            channel.setOption(StandardSocketOptions.SO_RCVBUF, 4 << 20);
            channel.bind(new InetSocketAddress("127.0.0.1", port));
            channel.configureBlocking(false);
        }

        void poll() throws Exception
        {
            while (true)
            {
                packet.clear();
                if (channel.receive(packet) == null)
                {
                    return;
                }
                view.wrap(packet, 0, packet.position());
                feedIds.add(MdWire.feedId(view, 0));
                sessionIds.add(MdWire.sessionId(view, 0));
                int pos = MdWire.PACKET_HEADER_LENGTH;
                for (int m = 0; m < MdWire.messageCount(view, 0); m++)
                {
                    final int len = MdWire.frameLength(view, pos);
                    final int msg = pos + MdWire.FRAME_PREFIX_LENGTH;
                    if (MdWire.isSequenced(MdWire.templateId(view, msg)))
                    {
                        final byte[] bytes = new byte[len];
                        view.getBytes(msg, bytes);
                        final long key = ((long)MdWire.symbolId(view, msg) << 40) | MdWire.seq(view, msg);
                        if (bySymbolSeq.put(key, bytes) != null)
                        {
                            duplicates++;
                        }
                    }
                    pos = msg + len;
                }
            }
        }

        @Override
        public void close() throws Exception
        {
            channel.close();
        }
    }

    @Test
    void feedsAAndBCarryTheSameMessagesOnTheWire() throws Exception
    {
        final VenueDriver v = TestSupport.venueWithSymbols(2);
        final int a = TestSupport.ports();
        final int b = TestSupport.ports();
        final Capture ca = new Capture(a + 2);
        final Capture cb = new Capture(b + 2);
        closeables.add(ca);
        closeables.add(cb);
        final MdConfig cfgA = TestSupport.config(0, 2, a, true, null);
        final MdConfig cfgB = TestSupport.config(1, 2, b, true, null);
        cfgB.pollLimit = 7; // B reads in different chunks than A
        cfgB.suppressCatchUp = false; // with 7-event polls the initial definitions would count as a backlog
        final MarketDataPublisher pa = new MarketDataPublisher(cfgA, v.venue);
        final MarketDataPublisher pb = new MarketDataPublisher(cfgB, v.venue);
        closeables.add(pa);
        closeables.add(pb);
        pa.start();
        pb.start();
        assertTrue(pa.awaitReady(5_000) && pb.awaitReady(5_000));
        for (int i = 0; i < 30; i++)
        {
            CheckpointRestartTest.activity(v, 3_000 + i, 1);
            for (int k = 0; k < 3; k++)
            {
                ca.poll();
                cb.poll();
                Thread.sleep(1);
            }
        }
        final long[] expected = new long[1];
        TestSupport.await("both publishers consumed everything", 10_000, () ->
        {
            for (int s = 0; s < 2; s++)
            {
                if (pa.engine().builder().lastEventSeq(s) != v.venue.eventCount(s)
                    || pb.engine().builder().lastEventSeq(s) != v.venue.eventCount(s))
                {
                    return false;
                }
            }
            return true;
        });
        final IntArrayList ids = pa.engine().builder().sortedSymbolIds();
        TestSupport.await("every message received on both feeds", 10_000, () ->
        {
            try
            {
                ca.poll();
                cb.poll();
            }
            catch (final Exception e)
            {
                throw new AssertionError(e);
            }
            expected[0] = 0;
            for (int i = 0; i < ids.size(); i++)
            {
                expected[0] += pa.engine().builder().symbol(ids.getInt(i)).seq();
            }
            return ca.bySymbolSeq.size() == expected[0] && cb.bySymbolSeq.size() == expected[0];
        });
        assertTrue(expected[0] > 500, "messages: " + expected[0]);
        assertEquals(ca.bySymbolSeq.keySet(), cb.bySymbolSeq.keySet());
        for (final var e : ca.bySymbolSeq.entrySet())
        {
            assertTrue(Arrays.equals(e.getValue(), cb.bySymbolSeq.get(e.getKey())),
                "symbol " + (e.getKey() >>> 40) + " md_seq " + (e.getKey() & 0xFF_FFFF_FFFFL));
        }
        assertEquals(0, ca.duplicates + cb.duplicates);
        assertEquals(Set.of(MdWire.FEED_A), ca.feedIds);
        assertEquals(Set.of(MdWire.FEED_B), cb.feedIds);
        assertEquals(ca.sessionIds, cb.sessionIds);
    }
}
