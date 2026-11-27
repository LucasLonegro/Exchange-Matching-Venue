package io.lmb.marketdata.client;

import io.lmb.marketdata.book.OrderBook;
import io.lmb.marketdata.sbe.ClosingPriceDecoder;
import io.lmb.marketdata.sbe.InstrumentDefinitionDecoder;
import io.lmb.marketdata.sbe.LevelUpdateDecoder;
import io.lmb.marketdata.sbe.RetransmitStatus;
import io.lmb.marketdata.sbe.ReferencePriceDecoder;
import io.lmb.marketdata.sbe.SnapshotBeginDecoder;
import io.lmb.marketdata.sbe.SnapshotEndDecoder;
import io.lmb.marketdata.sbe.SnapshotLevelsDecoder;
import io.lmb.marketdata.sbe.SymbolStatusDecoder;
import io.lmb.marketdata.wire.MdWire;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reference receiver of the binary feed. Listens to feeds A and B (incremental and snapshot channels),
 * arbitrates them by per-symbol sequence (first copy wins, later copies are duplicates), keeps a book per
 * symbol and delivers every sequenced message exactly once and in order to a {@link Listener}.
 *
 * <p>Gap handling, per symbol: messages after a gap are buffered; if the other feed has not filled the gap
 * within {@code gapTimeoutMs}, the missing range is requested over TCP from the retransmission servers (in
 * the configured order, falling back to the next on error). If the server no longer holds it (TOO_OLD) the
 * client takes a TCP snapshot and continues from its lastSeq. A loss at the tail of the stream (nothing
 * follows it) is detected from the snapshot channel, whose snapshots carry the symbol's last seq.
 *
 * <p>Single-threaded: call {@link #poll} from one thread. Recovery uses blocking TCP inside poll.
 * Steady state does not allocate except when buffering messages after a gap.
 */
public final class FeedClient implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(FeedClient.class);

    public static final class Config
    {
        /** Addresses to listen on: unicast bind addresses, or multicast groups (then multicastInterface is required). */
        public List<InetSocketAddress> incremental = new ArrayList<>();
        public List<InetSocketAddress> snapshot = new ArrayList<>();
        /** Retransmission servers, tried in order. */
        public List<InetSocketAddress> retransmit = new ArrayList<>();
        public String multicastInterface = "";
        public long gapTimeoutMs = 20;
        public int tcpTimeoutMs = 2000;
    }

    public interface Listener
    {
        /** A sequenced message, applied to the client's book; exactly once per (symbol, seq), in seq order. */
        default void onMessage(int symbolId, long seq, DirectBuffer buffer, int offset, int length)
        {
        }

        /** The symbol's book was replaced by a snapshot; the next message delivered has seq lastSeq + 1. */
        default void onSnapshotApplied(int symbolId, long lastSeq)
        {
        }

        /** The publisher's sessionId changed: every symbol was reset. */
        default void onSessionChange(long sessionId)
        {
        }
    }

    /** Client-side state of one symbol. */
    public static final class Symbol
    {
        public final int symbolId;
        public final OrderBook book = new OrderBook();
        long expected = 1;
        String ticker = "";
        int state;
        final Reference reference = new Reference();
        final TreeMap<Long, byte[]> pending = new TreeMap<>();
        long gapSinceNs;
        long tailTarget;
        SnapshotAssembly assembly;

        Symbol(final int symbolId)
        {
            this.symbolId = symbolId;
        }

        /** Seq of the last message applied (0 = none). */
        public long lastSeq()
        {
            return expected - 1;
        }

        public String ticker()
        {
            return ticker;
        }

        public int state()
        {
            return state;
        }

        /** day-cycle / band-stp: reference price, static band, last official close and state cause. */
        public Reference reference()
        {
            return reference;
        }

        boolean inGap()
        {
            return !pending.isEmpty() || tailTarget >= expected;
        }
    }

    /**
     * day-cycle / band-stp: the reference price and static band (last InstrumentDefinition or ReferencePrice),
     * the last ClosingPrice and the cause of the last SymbolStatus, kept by the client and restored from snapshots.
     * Prices are cents, {@code Long.MIN_VALUE} = none; raw enum values use the marketdata schema's codes.
     */
    public static final class Reference
    {
        long referencePrice = Long.MIN_VALUE;
        long bandLow = Long.MIN_VALUE;
        long bandHigh = Long.MIN_VALUE;
        long closePrice = Long.MIN_VALUE;
        long previousClose = Long.MIN_VALUE;
        int closeTradingDate;
        short closeSource = 255; // null: no ClosingPrice yet
        short stateCause = 255;
        long priceBandBps;
        long dynamicBandBps;
        long volatilityAuctionNs;

        public long priceBandBps()
        {
            return priceBandBps;
        }

        public long dynamicBandBps()
        {
            return dynamicBandBps;
        }

        public long volatilityAuctionNs()
        {
            return volatilityAuctionNs;
        }

        public long referencePrice()
        {
            return referencePrice;
        }

        public long bandLow()
        {
            return bandLow;
        }

        public long bandHigh()
        {
            return bandHigh;
        }

        public long closePrice()
        {
            return closePrice;
        }

        public long previousClose()
        {
            return previousClose;
        }

        public int closeTradingDate()
        {
            return closeTradingDate;
        }

        public io.lmb.marketdata.sbe.CloseSource closeSource()
        {
            return MdWire.closeSource(closeSource);
        }

        public io.lmb.marketdata.sbe.StateCause stateCause()
        {
            return MdWire.stateCause(stateCause);
        }

        void copyFrom(final Reference o)
        {
            referencePrice = o.referencePrice;
            bandLow = o.bandLow;
            bandHigh = o.bandHigh;
            closePrice = o.closePrice;
            previousClose = o.previousClose;
            closeTradingDate = o.closeTradingDate;
            closeSource = o.closeSource;
            stateCause = o.stateCause;
            priceBandBps = o.priceBandBps;
            dynamicBandBps = o.dynamicBandBps;
            volatilityAuctionNs = o.volatilityAuctionNs;
        }
    }

    /** A snapshot being assembled from SnapshotBegin / SnapshotLevels / SnapshotEnd. */
    static final class SnapshotAssembly
    {
        long lastSeq;
        int levelsExpected;
        int levelsReceived;
        int state;
        String ticker;
        final Reference reference = new Reference();
        final OrderBook book = new OrderBook();
    }

    private final Config config;
    private final Listener listener;
    private final List<DatagramChannel> incrementalSockets = new ArrayList<>();
    private final List<DatagramChannel> snapshotSockets = new ArrayList<>();
    private final List<RetransmitClient> retransmitClients = new ArrayList<>();
    private final ByteBuffer receive = ByteBuffer.allocateDirect(65536);
    private final UnsafeBuffer view = new UnsafeBuffer(receive);
    private final Map<Integer, Symbol> symbols = new TreeMap<>();
    private final LinkedHashSet<Symbol> gaps = new LinkedHashSet<>();
    private final long gapTimeoutNs;

    private final LevelUpdateDecoder levelUpdate = new LevelUpdateDecoder();
    private final SymbolStatusDecoder symbolStatus = new SymbolStatusDecoder();
    private final InstrumentDefinitionDecoder instrument = new InstrumentDefinitionDecoder();
    private final ReferencePriceDecoder referencePrice = new ReferencePriceDecoder();
    private final ClosingPriceDecoder closingPrice = new ClosingPriceDecoder();
    private final SnapshotBeginDecoder snapshotBegin = new SnapshotBeginDecoder();
    private final SnapshotLevelsDecoder snapshotLevels = new SnapshotLevelsDecoder();
    private final SnapshotEndDecoder snapshotEnd = new SnapshotEndDecoder();
    private final byte[] tickerBytes = new byte[8];

    private long sessionId = -1;
    private long nowNs;
    private final long[] lastPacketSeq = new long[2];
    private final long[] lastReceiveNs = new long[2];

    private long duplicates;
    private long messagesApplied;
    private long gapsDetected;
    private long retransmitRequests;
    private long retransmittedMessages;
    private long snapshotRecoveries;
    private long packetsLost;
    private long heartbeats;

    public FeedClient(final Config config, final Listener listener) throws IOException
    {
        this.config = config;
        this.listener = listener;
        this.gapTimeoutNs = config.gapTimeoutMs * 1_000_000L;
        for (final InetSocketAddress a : config.incremental)
        {
            incrementalSockets.add(open(a));
        }
        for (final InetSocketAddress a : config.snapshot)
        {
            snapshotSockets.add(open(a));
        }
        for (final InetSocketAddress a : config.retransmit)
        {
            retransmitClients.add(new RetransmitClient(a, config.tcpTimeoutMs));
        }
    }

    private DatagramChannel open(final InetSocketAddress a) throws IOException
    {
        final DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET);
        ch.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        ch.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024 * 1024);
        ch.configureBlocking(false);
        if (a.getAddress().isMulticastAddress())
        {
            final NetworkInterface nif = NetworkInterface.getByName(config.multicastInterface);
            if (nif == null)
            {
                ch.close();
                throw new IllegalArgumentException("multicast address " + a + " needs a valid multicastInterface");
            }
            ch.bind(new InetSocketAddress(a.getPort()));
            ch.join(a.getAddress(), nif);
        }
        else
        {
            ch.bind(a);
        }
        return ch;
    }

    // ============ public view ============

    public Symbol symbol(final int symbolId)
    {
        return symbols.get(symbolId);
    }

    public Iterable<Symbol> symbols()
    {
        return symbols.values();
    }

    public long duplicates()
    {
        return duplicates;
    }

    public long messagesApplied()
    {
        return messagesApplied;
    }

    public long gapsDetected()
    {
        return gapsDetected;
    }

    public long retransmitRequests()
    {
        return retransmitRequests;
    }

    public long retransmittedMessages()
    {
        return retransmittedMessages;
    }

    public long snapshotRecoveries()
    {
        return snapshotRecoveries;
    }

    /** Datagrams missing per packetSeq on any feed's channel (diagnostic: includes losses the other feed covered). */
    public long packetsLost()
    {
        return packetsLost;
    }

    public long heartbeats()
    {
        return heartbeats;
    }

    /** Monotonic time of the last datagram from feed 0 (A) or 1 (B); 0 = never. */
    public long lastReceiveNs(final int feedId)
    {
        return lastReceiveNs[feedId];
    }

    // ============ processing ============

    /** Reads every pending datagram, then runs gap recovery. Returns the number of datagrams read. */
    public int poll(final long nowNs) throws IOException
    {
        this.nowNs = nowNs;
        int n = 0;
        for (final DatagramChannel ch : incrementalSockets)
        {
            n += drain(ch);
        }
        for (final DatagramChannel ch : snapshotSockets)
        {
            n += drain(ch);
        }
        if (!gaps.isEmpty())
        {
            recoverDueGaps();
        }
        return n;
    }

    private int drain(final DatagramChannel ch) throws IOException
    {
        int n = 0;
        while (true)
        {
            receive.clear();
            if (ch.receive(receive) == null)
            {
                return n;
            }
            n++;
            onDatagram(view, receive.position());
        }
    }

    /** Processes one datagram (PacketHeader + frames). Public so tests and tools can inject datagrams. */
    public void onDatagram(final DirectBuffer b, final int length)
    {
        if (length < MdWire.PACKET_HEADER_LENGTH)
        {
            return;
        }
        final long session = MdWire.sessionId(b, 0);
        if (session != sessionId)
        {
            if (sessionId != -1)
            {
                LOG.warn("feed sessionId changed {} -> {}: resetting every symbol", sessionId, session);
                symbols.clear();
                gaps.clear();
            }
            sessionId = session;
            listener.onSessionChange(session);
        }
        final int feed = MdWire.feedId(b, 0) & 1;
        final int channel = MdWire.channel(b, 0);
        if (channel == MdWire.CHANNEL_INCREMENTAL)
        {
            final long packetSeq = MdWire.packetSeq(b, 0);
            if (lastPacketSeq[feed] != 0 && packetSeq > lastPacketSeq[feed] + 1)
            {
                packetsLost += packetSeq - lastPacketSeq[feed] - 1;
            }
            lastPacketSeq[feed] = Math.max(lastPacketSeq[feed], packetSeq);
        }
        lastReceiveNs[feed] = nowNs;
        final int count = MdWire.messageCount(b, 0);
        int pos = MdWire.PACKET_HEADER_LENGTH;
        for (int i = 0; i < count && pos + MdWire.FRAME_PREFIX_LENGTH <= length; i++)
        {
            final int len = MdWire.frameLength(b, pos);
            final int msg = pos + MdWire.FRAME_PREFIX_LENGTH;
            if (msg + len > length || MdWire.schemaId(b, msg) != MdWire.SCHEMA_ID)
            {
                return;
            }
            final int templateId = MdWire.templateId(b, msg);
            if (MdWire.isSequenced(templateId))
            {
                onSequenced(b, msg, len);
            }
            else if (templateId == io.lmb.marketdata.sbe.HeartbeatDecoder.TEMPLATE_ID)
            {
                heartbeats++;
            }
            else
            {
                onSnapshotFrame(b, msg, false);
            }
            pos = msg + len;
        }
    }

    private Symbol symbolFor(final int symbolId)
    {
        Symbol s = symbols.get(symbolId);
        if (s == null)
        {
            s = new Symbol(symbolId);
            symbols.put(symbolId, s);
        }
        return s;
    }

    private void onSequenced(final DirectBuffer b, final int o, final int len)
    {
        final Symbol s = symbolFor(MdWire.symbolId(b, o));
        final long seq = MdWire.seq(b, o);
        if (seq < s.expected)
        {
            duplicates++;
            return;
        }
        if (seq == s.expected)
        {
            apply(s, b, o, len, seq);
            drainPending(s);
            return;
        }
        if (s.pending.containsKey(seq))
        {
            duplicates++;
            return;
        }
        final byte[] copy = new byte[len];
        b.getBytes(o, copy);
        s.pending.put(seq, copy);
        openGap(s);
    }

    private void openGap(final Symbol s)
    {
        if (s.gapSinceNs == 0)
        {
            s.gapSinceNs = nowNs == 0 ? 1 : nowNs;
            gapsDetected++;
            gaps.add(s);
        }
    }

    private void drainPending(final Symbol s)
    {
        while (!s.pending.isEmpty())
        {
            final Map.Entry<Long, byte[]> first = s.pending.firstEntry();
            if (first.getKey() < s.expected)
            {
                s.pending.pollFirstEntry();
                continue;
            }
            if (first.getKey() != s.expected)
            {
                break;
            }
            s.pending.pollFirstEntry();
            final UnsafeBuffer m = new UnsafeBuffer(first.getValue());
            apply(s, m, 0, first.getValue().length, first.getKey());
        }
        if (!s.inGap())
        {
            s.gapSinceNs = 0;
            gaps.remove(s);
        }
    }

    private void apply(final Symbol s, final DirectBuffer b, final int o, final int len, final long seq)
    {
        final int blockLength = MdWire.blockLength(b, o);
        final int version = MdWire.version(b, o);
        final int body = o + MdWire.HEADER_LENGTH;
        switch (MdWire.templateId(b, o))
        {
            case LevelUpdateDecoder.TEMPLATE_ID ->
            {
                levelUpdate.wrap(b, body, blockLength, version);
                s.book.set(OrderBook.sideIndex(levelUpdate.sideRaw()), levelUpdate.price(), levelUpdate.quantity(),
                    levelUpdate.orderCount());
            }
            case SymbolStatusDecoder.TEMPLATE_ID ->
            {
                symbolStatus.wrap(b, body, blockLength, version);
                s.state = symbolStatus.stateRaw();
                s.reference.stateCause = symbolStatus.causeRaw();
            }
            case InstrumentDefinitionDecoder.TEMPLATE_ID ->
            {
                instrument.wrap(b, body, blockLength, version);
                instrument.getTicker(tickerBytes, 0);
                s.ticker = MdWire.ticker(tickerBytes);
                s.state = instrument.stateRaw();
                final Reference r = s.reference;
                final long ref = instrument.referencePrice();
                final long bps = instrument.priceBandBps();
                if (ref != r.referencePrice || bps != r.priceBandBps)
                {
                    r.bandLow = Long.MIN_VALUE; // as the publisher does (see SnapshotBegin.bandLow)
                    r.bandHigh = Long.MIN_VALUE;
                }
                r.referencePrice = ref;
                r.priceBandBps = bps;
                r.dynamicBandBps = instrument.dynamicBandBps();
                r.volatilityAuctionNs = instrument.volatilityAuctionNs();
            }
            case ReferencePriceDecoder.TEMPLATE_ID ->
            {
                referencePrice.wrap(b, body, blockLength, version);
                s.reference.referencePrice = referencePrice.referencePrice();
                s.reference.bandLow = referencePrice.bandLow();
                s.reference.bandHigh = referencePrice.bandHigh();
            }
            case ClosingPriceDecoder.TEMPLATE_ID ->
            {
                closingPrice.wrap(b, body, blockLength, version);
                s.reference.closePrice = closingPrice.price();
                s.reference.previousClose = closingPrice.previousClose();
                s.reference.closeTradingDate = closingPrice.tradingDate();
                s.reference.closeSource = closingPrice.sourceRaw();
            }
            default ->
            {
                // TradePrint, AuctionIndicative: no book state
            }
        }
        s.expected = seq + 1;
        messagesApplied++;
        listener.onMessage(s.symbolId, seq, b, o, len);
    }

    // ============ snapshots ============

    /** @param tcp true for a TCP snapshot answer (applied unconditionally if newer), false for the snapshot channel */
    private Symbol onSnapshotFrame(final DirectBuffer b, final int o, final boolean tcp)
    {
        final int blockLength = MdWire.blockLength(b, o);
        final int version = MdWire.version(b, o);
        final int body = o + MdWire.HEADER_LENGTH;
        switch (MdWire.templateId(b, o))
        {
            case SnapshotBeginDecoder.TEMPLATE_ID ->
            {
                snapshotBegin.wrap(b, body, blockLength, version);
                final Symbol s = symbolFor((int)snapshotBegin.symbolId());
                final SnapshotAssembly a = new SnapshotAssembly();
                a.lastSeq = snapshotBegin.lastSeq();
                a.levelsExpected = (int)(snapshotBegin.bidLevels() + snapshotBegin.askLevels());
                a.state = snapshotBegin.stateRaw();
                final Reference r = a.reference;
                r.referencePrice = snapshotBegin.referencePrice();
                r.bandLow = snapshotBegin.bandLow();
                r.bandHigh = snapshotBegin.bandHigh();
                r.closePrice = snapshotBegin.closePrice();
                r.previousClose = snapshotBegin.previousClose();
                r.closeTradingDate = snapshotBegin.closeTradingDate();
                r.closeSource = snapshotBegin.closeSourceRaw();
                r.stateCause = snapshotBegin.stateCauseRaw();
                r.priceBandBps = snapshotBegin.priceBandBps();
                r.dynamicBandBps = snapshotBegin.dynamicBandBps();
                r.volatilityAuctionNs = snapshotBegin.volatilityAuctionNs();
                snapshotBegin.getTicker(tickerBytes, 0);
                a.ticker = MdWire.ticker(tickerBytes);
                s.assembly = a;
                return null;
            }
            case SnapshotLevelsDecoder.TEMPLATE_ID ->
            {
                snapshotLevels.wrap(b, body, blockLength, version);
                final Symbol s = symbols.get((int)snapshotLevels.symbolId());
                final long lastSeq = snapshotLevels.lastSeq();
                for (final SnapshotLevelsDecoder.LevelsDecoder l : snapshotLevels.levels())
                {
                    if (s != null && s.assembly != null && s.assembly.lastSeq == lastSeq)
                    {
                        s.assembly.book.set(OrderBook.sideIndex(l.sideRaw()), l.price(), l.quantity(), l.orderCount());
                        s.assembly.levelsReceived++;
                    }
                }
                return null;
            }
            case SnapshotEndDecoder.TEMPLATE_ID ->
            {
                snapshotEnd.wrap(b, body, blockLength, version);
                final Symbol s = symbols.get((int)snapshotEnd.symbolId());
                if (s == null || s.assembly == null || s.assembly.lastSeq != snapshotEnd.lastSeq()
                    || s.assembly.levelsReceived != s.assembly.levelsExpected)
                {
                    if (s != null)
                    {
                        s.assembly = null; // lost a piece: wait for the next cycle
                    }
                    return null;
                }
                final SnapshotAssembly a = s.assembly;
                s.assembly = null;
                if (tcp)
                {
                    if (a.lastSeq >= s.expected - 1)
                    {
                        applySnapshot(s, a);
                    }
                }
                else if (a.lastSeq >= s.expected)
                {
                    // We are behind the publisher with nothing buffered to show it: a tail loss (or a late join).
                    s.tailTarget = Math.max(s.tailTarget, a.lastSeq);
                    openGap(s);
                }
                return s;
            }
            default ->
            {
                return null;
            }
        }
    }

    private void applySnapshot(final Symbol s, final SnapshotAssembly a)
    {
        s.book.copyFrom(a.book);
        s.state = a.state;
        s.ticker = a.ticker;
        s.reference.copyFrom(a.reference);
        s.expected = a.lastSeq + 1;
        snapshotRecoveries++;
        listener.onSnapshotApplied(s.symbolId, a.lastSeq);
        drainPending(s);
    }

    // ============ recovery ============

    private void recoverDueGaps()
    {
        final Iterator<Symbol> it = new ArrayList<>(gaps).iterator();
        while (it.hasNext())
        {
            final Symbol s = it.next();
            if (s.inGap() && nowNs - s.gapSinceNs >= gapTimeoutNs)
            {
                recover(s);
                if (s.inGap())
                {
                    s.gapSinceNs = nowNs; // try again after another timeout
                }
            }
            else if (!s.inGap())
            {
                gaps.remove(s);
                s.gapSinceNs = 0;
            }
        }
    }

    private void recover(final Symbol s)
    {
        for (final RetransmitClient client : retransmitClients)
        {
            try
            {
                for (int round = 0; round < 1000 && s.inGap(); round++)
                {
                    final long target = s.pending.isEmpty() ? s.tailTarget : s.pending.firstKey() - 1;
                    if (target < s.expected)
                    {
                        drainPending(s);
                        continue;
                    }
                    retransmitRequests++;
                    final RetransmitClient.Response r = client.retransmit(s.symbolId, s.expected, target);
                    if (r.status == RetransmitStatus.TOO_OLD)
                    {
                        snapshotRecover(s, client);
                        continue;
                    }
                    if (r.status != RetransmitStatus.OK || r.messages.isEmpty())
                    {
                        break; // not published yet, or unknown here: try the next server / later
                    }
                    for (final byte[] m : r.messages)
                    {
                        retransmittedMessages++;
                        onSequenced(new UnsafeBuffer(m), 0, m.length);
                    }
                }
                if (!s.inGap())
                {
                    return;
                }
            }
            catch (final IOException e)
            {
                LOG.debug("retransmission from {} failed: {}", client.address(), e.toString());
            }
        }
    }

    private void snapshotRecover(final Symbol s, final RetransmitClient client) throws IOException
    {
        final RetransmitClient.Response r = client.snapshot(s.symbolId);
        if (r.status != RetransmitStatus.OK)
        {
            throw new IOException("snapshot of " + s.symbolId + " refused: " + r.status);
        }
        for (final byte[] m : r.messages)
        {
            onSnapshotFrame(new UnsafeBuffer(m), 0, true);
        }
        if (s.lastSeq() < r.lastSeq)
        {
            throw new IOException("snapshot of " + s.symbolId + " incomplete");
        }
    }

    @Override
    public void close()
    {
        for (final DatagramChannel ch : incrementalSockets)
        {
            closeQuietly(ch);
        }
        for (final DatagramChannel ch : snapshotSockets)
        {
            closeQuietly(ch);
        }
        for (final RetransmitClient c : retransmitClients)
        {
            c.close();
        }
    }

    private static void closeQuietly(final DatagramChannel ch)
    {
        try
        {
            ch.close();
        }
        catch (final IOException ignore)
        {
        }
    }
}
