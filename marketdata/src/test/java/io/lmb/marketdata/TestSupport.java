package io.lmb.marketdata;

import io.lmb.common.Events;
import io.lmb.marketdata.book.OrderBook;
import io.lmb.marketdata.wire.MdWire;
import io.lmb.sbe.BookDeltaDecoder;
import io.lmb.sbe.SymbolState;
import io.lmb.testkit.InMemoryVenue;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

final class TestSupport
{
    /** marketdata test port range is 34000-34999; each test takes a block of 10. */
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(34000 + (int)(ProcessHandle.current().pid() % 50) * 10);

    private TestSupport()
    {
    }

    static int ports()
    {
        final int p = NEXT_PORT.getAndAdd(10);
        if (p + 10 > 35000)
        {
            NEXT_PORT.set(34000);
            return ports();
        }
        return p;
    }

    /** Config on loopback with ports base..base+9: retransmit base, gRPC base+1, incremental base+2, snapshot base+3. */
    static MdConfig config(final int instance, final int shards, final int base, final boolean udp, final Path dataDir)
    {
        final MdConfig c = new MdConfig();
        c.instance = instance;
        c.shards = shards;
        c.bindHost = "127.0.0.1";
        c.retransmitPort = base;
        c.grpcPort = base + 1;
        c.metricsPort = 0;
        c.incrementalDestinations = udp ? MdConfig.parse("127.0.0.1:" + (base + 2)) : MdConfig.parse("");
        c.snapshotDestinations = udp ? MdConfig.parse("127.0.0.1:" + (base + 3)) : MdConfig.parse("");
        c.heartbeatIntervalMs = 100;
        c.snapshotIntervalMs = 200;
        c.dataDir = dataDir;
        c.checkpointIntervalMs = 100;
        return c;
    }

    static VenueDriver venueWithSymbols(final int shards)
    {
        final VenueDriver v = new VenueDriver(shards);
        for (int shard = 0; shard < shards; shard++)
        {
            for (int firm = 1; firm <= 3; firm++)
            {
                v.firm(shard, firm);
            }
        }
        v.symbol(0, 1, "AAA", 10_000, SymbolState.CONTINUOUS);
        v.symbol(0, 2, "BBB", 10_000, SymbolState.CONTINUOUS);
        if (shards > 1)
        {
            v.symbol(1, 3, "CCC", 10_000, SymbolState.CONTINUOUS);
        }
        return v;
    }

    /** Every BookDelta of the raw event streams applied to a book per symbol: the matcher's own book. */
    static Map<Integer, OrderBook> referenceBooks(final InMemoryVenue venue)
    {
        final Map<Integer, OrderBook> books = new TreeMap<>();
        final BookDeltaDecoder delta = new BookDeltaDecoder();
        for (int shard = 0; shard < venue.shardCount(); shard++)
        {
            final long n = venue.eventCount(shard);
            for (long seq = 1; seq <= n; seq++)
            {
                final UnsafeBuffer e = new UnsafeBuffer(venue.event(shard, seq));
                if (Events.templateId(e, 0) == BookDeltaDecoder.TEMPLATE_ID)
                {
                    delta.wrap(e, MdWire.HEADER_LENGTH, MdWire.blockLength(e, 0), MdWire.version(e, 0));
                    books.computeIfAbsent((int)delta.symbolId(), k -> new OrderBook())
                        .set(OrderBook.sideIndex(delta.sideRaw()), delta.price(), delta.quantity(), delta.orderCount());
                }
            }
        }
        return books;
    }

    static void await(final String what, final long timeoutMs, final BooleanSupplier condition)
    {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new AssertionError("timed out waiting for " + what);
            }
            try
            {
                Thread.sleep(5);
            }
            catch (final InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
    }
}
