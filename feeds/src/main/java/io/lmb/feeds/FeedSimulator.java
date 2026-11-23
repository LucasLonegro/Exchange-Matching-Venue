package io.lmb.feeds;

import io.lmb.common.SymbolDirectory;
import io.lmb.common.Topology;
import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Simulated external venues for local use and tests: each venue publishes top-of-book quotes in
 * {@link SimFeedProtocol} on lines A and B. All venues quote around one shared random-walk fair price
 * per symbol (start: the symbol's reference price in the directory), each with its own small offset
 * and spread, so the NBBO is realistic and occasionally locked. Symbols are quoted round robin, so a
 * late joiner sees every symbol within {@code symbols / quotesPerSecond} seconds.
 *
 * <p>Deterministic for a given seed and sequence of {@link #doWork} times. Single-threaded,
 * allocation-free after construction. Loss can be injected per line to exercise arbitration.
 */
public final class FeedSimulator
{
    /** Where packets go. The buffer's position..limit is the packet; returns false if not (fully) sent. */
    public interface Sink
    {
        boolean send(int venueId, int line, ByteBuffer packet);
    }

    /**
     * @param quotesPerSecond quotes per second per venue
     * @param heartbeatNs     a venue that sent nothing for this long sends a heartbeat (must be well under staleNs)
     * @param dropA           probability a packet is not sent on line A (loss injection), likewise B
     */
    public record Settings(int venues, double quotesPerSecond, long heartbeatNs, double dropA, double dropB, long seed)
    {
        public static final String QUOTES_PER_SECOND = "lmb.feedsim.quotesPerSecond";
        public static final String HEARTBEAT_NS = "lmb.feedsim.heartbeatNs";
        public static final String DROP_A = "lmb.feedsim.dropA";
        public static final String DROP_B = "lmb.feedsim.dropB";
        public static final String SEED = "lmb.feedsim.seed";

        public Settings
        {
            if (quotesPerSecond < 0 || heartbeatNs <= 0 || dropA < 0 || dropA > 1 || dropB < 0 || dropB > 1)
            {
                throw new IllegalArgumentException("invalid feed simulator settings");
            }
            if (venues < 1 || venues > FeedsConfig.MAX_VENUES)
            {
                throw new IllegalArgumentException("venues must be 1.." + FeedsConfig.MAX_VENUES);
            }
        }

        public static Settings from(final Topology t)
        {
            return new Settings(
                FeedsConfig.venueCount(t),
                Double.parseDouble(t.get(QUOTES_PER_SECOND, "200")),
                t.getLong(HEARTBEAT_NS, 10_000_000L),
                Double.parseDouble(t.get(DROP_A, "0")),
                Double.parseDouble(t.get(DROP_B, "0")),
                t.getLong(SEED, 42L));
        }
    }

    private static final long MAX_BACKLOG = 1_000;

    private final Settings settings;
    private final Sink sink;
    private final long startNs;
    private final SplittableRandom random;
    private final int symbolCount;
    private final int[] symbolIds;
    private final long[] tick;
    private final long[] fair;

    private final long[] session;
    private final long[] nextSeq;
    private final long[] emitted;
    private final long[] lastSendNs;
    private final int[] nextSymbol;

    private final ByteBuffer packet = ByteBuffer.allocateDirect(SimFeedProtocol.MAX_PACKET_LENGTH);
    private final UnsafeBuffer packetView = new UnsafeBuffer(packet);

    final AtomicLongArray packets;   // venue * 2 + line
    final AtomicLongArray quotes;    // venue
    final AtomicLongArray dropped = new AtomicLongArray(2);
    final AtomicLong sendFailures = new AtomicLong();

    public FeedSimulator(final Settings settings, final SymbolDirectory symbols, final Sink sink, final long startEpochNs)
    {
        this.settings = settings;
        this.sink = sink;
        this.startNs = startEpochNs;
        this.random = new SplittableRandom(settings.seed());
        final List<SymbolDirectory.Symbol> sorted = symbols.all().stream()
            .sorted(Comparator.comparingInt(SymbolDirectory.Symbol::symbolId)).toList();
        symbolCount = sorted.size();
        symbolIds = new int[symbolCount];
        tick = new long[symbolCount];
        fair = new long[symbolCount];
        for (int s = 0; s < symbolCount; s++)
        {
            final SymbolDirectory.Symbol sym = sorted.get(s);
            symbolIds[s] = sym.symbolId();
            tick[s] = Math.max(1, sym.tickSize());
            fair[s] = Math.max(minFair(s), sym.referencePrice() / tick[s] * tick[s]);
        }
        final int v = settings.venues();
        session = new long[v];
        nextSeq = new long[v];
        emitted = new long[v];
        lastSendNs = new long[v];
        nextSymbol = new int[v];
        for (int i = 0; i < v; i++)
        {
            session[i] = startEpochNs + i;
            nextSeq[i] = 1;
            lastSendNs[i] = Long.MIN_VALUE;
            nextSymbol[i] = symbolCount == 0 ? 0 : i % symbolCount;
        }
        packets = new AtomicLongArray(v * 2);
        quotes = new AtomicLongArray(v);
    }

    /** Emits the quotes due by {@code nowNs} (at most one packet per venue per call) and heartbeats. */
    public int doWork(final long nowNs)
    {
        final long due = nowNs <= startNs ? 0 : (long)((nowNs - startNs) / 1e9 * settings.quotesPerSecond());
        int work = 0;
        for (int v = 0; v < settings.venues(); v++)
        {
            if (due - emitted[v] > MAX_BACKLOG)
            {
                emitted[v] = due - SimFeedProtocol.MAX_MESSAGES_PER_PACKET; // after a stall: skip, do not burst
            }
            final int n = symbolCount == 0 ? 0 : (int)Math.min(due - emitted[v], SimFeedProtocol.MAX_MESSAGES_PER_PACKET);
            if (n > 0)
            {
                writeQuotes(v, n, nowNs);
                emitted[v] += n;
                work += n;
            }
            else if (lastSendNs[v] == Long.MIN_VALUE || nowNs - lastSendNs[v] >= settings.heartbeatNs())
            {
                SimFeedProtocol.writeHeader(packetView, 0, SimFeedProtocol.LINE_A, v, 0, session[v], nextSeq[v], nowNs);
                publish(v, 0, nowNs);
                work++;
            }
        }
        return work;
    }

    private void writeQuotes(final int v, final int n, final long nowNs)
    {
        SimFeedProtocol.writeHeader(packetView, 0, SimFeedProtocol.LINE_A, v, n, session[v], nextSeq[v], nowNs);
        for (int i = 0; i < n; i++)
        {
            final int s = nextSymbol[v];
            nextSymbol[v] = (s + 1) % symbolCount;
            final long t = tick[s];
            fair[s] = Math.max(minFair(s), fair[s] + t * (random.nextInt(3) - 1));
            final long centre = fair[s] + t * (random.nextInt(3) - 1);
            final long halfSpread = t * (1 + random.nextInt(2));
            SimFeedProtocol.writeQuote(packetView, 0, i, symbolIds[s], nowNs,
                centre - halfSpread, 100L * (1 + random.nextInt(10)),
                centre + halfSpread, 100L * (1 + random.nextInt(10)));
        }
        nextSeq[v] += n;
        quotes.addAndGet(v, n);
        publish(v, n, nowNs);
    }

    private long minFair(final int s)
    {
        return 10 * tick[s];
    }

    private void publish(final int v, final int count, final long nowNs)
    {
        final int length = SimFeedProtocol.packetLength(count);
        for (int line = 0; line < 2; line++)
        {
            final double drop = line == 0 ? settings.dropA() : settings.dropB();
            if (drop > 0 && random.nextDouble() < drop)
            {
                dropped.incrementAndGet(line);
                continue;
            }
            packetView.putByte(SimFeedProtocol.LINE_OFFSET, (byte)line);
            packet.limit(length).position(0);
            if (sink.send(v, line, packet))
            {
                packets.incrementAndGet(v * 2 + line);
            }
            else
            {
                sendFailures.incrementAndGet();
            }
        }
        lastSendNs[v] = nowNs;
    }

    public long nextSequence(final int venueId)
    {
        return nextSeq[venueId];
    }

    public void registerMetrics(final PrometheusRegistry registry)
    {
        CounterWithCallback.builder().name("lmb_feeds_sim_packets").help("Packets sent per venue and line")
            .labelNames("venue", "line")
            .callback(cb ->
            {
                for (int v = 0; v < settings.venues(); v++)
                {
                    cb.call(packets.get(v * 2), Integer.toString(v), "A");
                    cb.call(packets.get(v * 2 + 1), Integer.toString(v), "B");
                }
            })
            .register(registry);
        CounterWithCallback.builder().name("lmb_feeds_sim_quotes").help("Quotes published per venue")
            .labelNames("venue")
            .callback(cb ->
            {
                for (int v = 0; v < settings.venues(); v++)
                {
                    cb.call(quotes.get(v), Integer.toString(v));
                }
            })
            .register(registry);
        CounterWithCallback.builder().name("lmb_feeds_sim_dropped_packets")
            .help("Packets not sent because of injected loss (lmb.feedsim.dropA/B)").labelNames("line")
            .callback(cb ->
            {
                cb.call(dropped.get(0), "A");
                cb.call(dropped.get(1), "B");
            })
            .register(registry);
        CounterWithCallback.builder().name("lmb_feeds_sim_send_failures")
            .help("Packets the socket did not accept (buffer full or error)")
            .callback(cb -> cb.call(sendFailures.get()))
            .register(registry);
    }
}
