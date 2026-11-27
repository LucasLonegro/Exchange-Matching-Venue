package io.lmb.marketdata.grpc;

import io.grpc.Status;
import io.lmb.marketdata.PublisherMetrics;
import io.lmb.marketdata.book.OrderBook;
import io.lmb.marketdata.sbe.AuctionIndicativeDecoder;
import io.lmb.marketdata.sbe.CloseSource;
import io.lmb.marketdata.sbe.ClosingPriceDecoder;
import io.lmb.marketdata.sbe.ReferencePriceDecoder;
import io.lmb.marketdata.sbe.StateCause;
import io.lmb.marketdata.sbe.BookSide;
import io.lmb.marketdata.sbe.InstrumentDefinitionDecoder;
import io.lmb.marketdata.sbe.LevelUpdateDecoder;
import io.lmb.marketdata.sbe.SnapshotBeginDecoder;
import io.lmb.marketdata.sbe.SnapshotLevelsDecoder;
import io.lmb.marketdata.sbe.SymbolStatusDecoder;
import io.lmb.marketdata.sbe.TradePrintDecoder;
import io.lmb.marketdata.sbe.TradingState;
import io.lmb.marketdata.wire.MdWire;
import io.lmb.proto.marketdata.AuctionIndicative;
import io.lmb.proto.marketdata.MarketDataUpdate;
import io.lmb.proto.marketdata.ClosingPrice;
import io.lmb.proto.marketdata.ReferencePrice;
import io.lmb.proto.marketdata.SymbolStatus;
import io.lmb.proto.marketdata.TradePrint;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Serves gRPC subscribers from its own copy of the books, built from the publisher's sequenced messages.
 *
 * <p>The publisher thread hands every sequenced message, every batch end and (at start) every symbol's
 * snapshot frames over a one-to-one ring ({@code offer*} methods, allocation-free; they spin while the ring
 * is full and give up after one second, after which gRPC is marked broken and every subscriber is closed).
 * The fan-out thread ({@link #doWork}) applies them and talks to the subscribers; it never blocks on a
 * subscriber. Subscriptions are registered and conflated updates flushed only between batches, so every
 * snapshot and incremental a subscriber sees is a state at a log-entry boundary.
 */
public final class MarketDataFanout implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(MarketDataFanout.class);

    static final int MSG_SEQUENCED = 1;
    static final int MSG_SNAPSHOT_FRAME = 2;
    static final int MSG_BATCH_END = 3;

    /** Fan-out copy of one symbol. */
    static final class FanoutSymbol
    {
        final int symbolId;
        final OrderBook book = new OrderBook();
        String ticker = "";
        int state = 1;
        long seq;
        long transactTimeNs;
        // day-cycle / band-stp
        short stateCause = 255;
        long referencePrice = Long.MIN_VALUE;
        long bandLow = Long.MIN_VALUE;
        long bandHigh = Long.MIN_VALUE;
        long priceBandBps = -1;
        long closePrice = Long.MIN_VALUE;
        long previousClose = Long.MIN_VALUE;
        int closeTradingDate;
        short closeSource;
        boolean hasClose;

        FanoutSymbol(final int symbolId)
        {
            this.symbolId = symbolId;
        }

        String stateName()
        {
            final TradingState s = TradingState.get((short)state);
            return s == TradingState.NULL_VAL ? "" : s.name();
        }

        String stateCauseName()
        {
            final StateCause c = MdWire.stateCause(stateCause);
            return c == StateCause.NULL_VAL ? "" : c.name();
        }

        /** Null when no reference price is known. */
        ReferencePrice reference()
        {
            if (referencePrice == Long.MIN_VALUE && bandLow == Long.MIN_VALUE)
            {
                return null;
            }
            final boolean hasRef = referencePrice != Long.MIN_VALUE;
            final boolean hasBand = bandLow != Long.MIN_VALUE && bandHigh != Long.MIN_VALUE;
            return ReferencePrice.newBuilder().setReferencePriceCents(hasRef ? referencePrice : 0)
                .setHasReferencePrice(hasRef).setBandLowCents(hasBand ? bandLow : 0)
                .setBandHighCents(hasBand ? bandHigh : 0).setHasBand(hasBand).build();
        }

        /** Null when no ClosingPrice was published. */
        ClosingPrice close()
        {
            if (!hasClose)
            {
                return null;
            }
            final CloseSource src = MdWire.closeSource(closeSource);
            return ClosingPrice.newBuilder().setPriceCents(closePrice == Long.MIN_VALUE ? 0 : closePrice)
                .setHasPrice(closePrice != Long.MIN_VALUE)
                .setPreviousCloseCents(previousClose == Long.MIN_VALUE ? 0 : previousClose)
                .setHasPreviousClose(previousClose != Long.MIN_VALUE).setTradingDate(closeTradingDate)
                .setSource(src == CloseSource.NULL_VAL ? "" : src.name()).build();
        }

        MarketDataUpdate.Builder update()
        {
            return MarketDataUpdate.newBuilder().setSymbol(ticker).setSymbolId(symbolId).setMdSeq(seq)
                .setTransactTimeNs(transactTimeNs);
        }
    }

    private final OneToOneRingBuffer ring;
    private final PublisherMetrics metrics;
    private final int maxQueuedPerSubscriber;
    private final int maxSubscribers;
    private final UnsafeBuffer dummy = new UnsafeBuffer(new byte[8]);

    private final TreeMap<Integer, FanoutSymbol> symbols = new TreeMap<>();
    private final ArrayList<Subscriber> subscribers = new ArrayList<>();
    private final ConcurrentLinkedQueue<Subscriber> registrations = new ConcurrentLinkedQueue<>();
    private final ArrayList<Subscriber> toRemove = new ArrayList<>();

    private final LevelUpdateDecoder levelUpdate = new LevelUpdateDecoder();
    private final TradePrintDecoder tradePrint = new TradePrintDecoder();
    private final SymbolStatusDecoder symbolStatus = new SymbolStatusDecoder();
    private final AuctionIndicativeDecoder auction = new AuctionIndicativeDecoder();
    private final InstrumentDefinitionDecoder instrument = new InstrumentDefinitionDecoder();
    private final SnapshotBeginDecoder snapshotBegin = new SnapshotBeginDecoder();
    private final SnapshotLevelsDecoder snapshotLevels = new SnapshotLevelsDecoder();
    private final ReferencePriceDecoder referencePrice = new ReferencePriceDecoder();
    private final ClosingPriceDecoder closingPrice = new ClosingPriceDecoder();
    private final byte[] tickerBytes = new byte[8];

    private boolean midBatch;
    private long nowNs;
    private volatile boolean broken;
    private volatile boolean running = true;
    private Thread thread;
    private long nextSubscriberId = 1;

    public MarketDataFanout(final int ringBytes, final int maxQueuedPerSubscriber, final int maxSubscribers, final PublisherMetrics metrics)
    {
        if (Integer.bitCount(ringBytes) != 1)
        {
            throw new IllegalArgumentException("ring size must be a power of two: " + ringBytes);
        }
        this.ring = new OneToOneRingBuffer(new UnsafeBuffer(
            ByteBuffer.allocateDirect(ringBytes + RingBufferDescriptor.TRAILER_LENGTH)));
        this.metrics = metrics;
        this.maxQueuedPerSubscriber = maxQueuedPerSubscriber;
        this.maxSubscribers = maxSubscribers;
    }

    // ============ publisher thread ============

    public void offerSequenced(final DirectBuffer buffer, final int offset, final int length)
    {
        offer(MSG_SEQUENCED, buffer, offset, length);
    }

    public void offerBatchEnd()
    {
        offer(MSG_BATCH_END, dummy, 0, 1);
    }

    /** Snapshot frames as written by {@code SnapshotWriter}; each frame becomes one ring record. */
    public void offerSnapshot(final DirectBuffer buffer, final int offset, final int length)
    {
        int pos = offset;
        while (pos < offset + length)
        {
            final int len = MdWire.frameLength(buffer, pos);
            offer(MSG_SNAPSHOT_FRAME, buffer, pos + MdWire.FRAME_PREFIX_LENGTH, len);
            pos += MdWire.FRAME_PREFIX_LENGTH + len;
        }
    }

    private void offer(final int type, final DirectBuffer buffer, final int offset, final int length)
    {
        if (broken)
        {
            return;
        }
        long deadline = 0;
        while (!ring.write(type, buffer, offset, length))
        {
            metrics.grpcBackpressureSpins.incrementAndGet();
            final long now = System.nanoTime();
            if (deadline == 0)
            {
                deadline = now + TimeUnit.SECONDS.toNanos(1);
            }
            else if (now - deadline > 0)
            {
                broken = true;
                LOG.error("gRPC fan-out stalled for 1 s; gRPC market data disabled until restart");
                return;
            }
            Thread.onSpinWait();
        }
    }

    // ============ gRPC threads ============

    /** Returns false (and does not register) when the subscriber limit is reached or gRPC is broken. */
    boolean register(final Subscriber s)
    {
        if (broken || !running || metrics.grpcSubscribers.get() + registrations.size() >= maxSubscribers)
        {
            return false;
        }
        registrations.add(s);
        return true;
    }

    synchronized long nextId()
    {
        return nextSubscriberId++;
    }

    public boolean isBroken()
    {
        return broken;
    }

    // ============ fan-out thread ============

    public void start()
    {
        thread = Thread.ofPlatform().daemon(true).name("md-grpc-fanout").start(this::run);
    }

    private void run()
    {
        final BackoffIdleStrategy idle = new BackoffIdleStrategy(10, 20, 10_000, 1_000_000);
        while (running)
        {
            try
            {
                idle.idle(doWork(System.nanoTime()));
            }
            catch (final RuntimeException e)
            {
                LOG.error("gRPC fan-out failed; gRPC market data disabled", e);
                broken = true;
                break;
            }
        }
        closeAll(Status.UNAVAILABLE.withDescription("market data publisher shutting down"));
    }

    /** One duty cycle. Public for tests that drive the fan-out without its thread. */
    public int doWork(final long nowNs)
    {
        this.nowNs = nowNs;
        int work = ring.read(this::onRecord, 2_000);
        if (broken)
        {
            closeAll(Status.UNAVAILABLE.withDescription("market data fan-out stalled"));
            return work;
        }
        if (!midBatch)
        {
            Subscriber s;
            while ((s = registrations.poll()) != null)
            {
                activate(s);
                work++;
            }
            for (int i = 0; i < subscribers.size(); i++)
            {
                work += subscribers.get(i).flushDue(nowNs);
            }
        }
        for (int i = 0; i < subscribers.size(); i++)
        {
            final Subscriber sub = subscribers.get(i);
            work += sub.drain();
            if (sub.isDone())
            {
                toRemove.add(sub);
            }
        }
        if (!toRemove.isEmpty())
        {
            subscribers.removeAll(toRemove);
            metrics.grpcSubscribers.addAndGet(-toRemove.size());
            toRemove.clear();
        }
        return work;
    }

    private void activate(final Subscriber s)
    {
        if (s.isDone())
        {
            return;
        }
        for (final FanoutSymbol fs : symbols.values())
        {
            if (fs.seq > 0 && s.matches(fs))
            {
                s.sendSnapshot(fs, nowNs);
            }
        }
        subscribers.add(s);
        metrics.grpcSubscribers.incrementAndGet();
    }

    private void closeAll(final Status status)
    {
        for (final Subscriber s : subscribers)
        {
            s.close(status);
        }
        metrics.grpcSubscribers.addAndGet(-subscribers.size());
        subscribers.clear();
        Subscriber s;
        while ((s = registrations.poll()) != null)
        {
            s.close(status);
        }
    }

    private FanoutSymbol symbol(final int symbolId)
    {
        FanoutSymbol s = symbols.get(symbolId);
        if (s == null)
        {
            s = new FanoutSymbol(symbolId);
            symbols.put(symbolId, s);
        }
        return s;
    }

    private void onRecord(final int type, final MutableDirectBuffer b, final int index, final int length)
    {
        switch (type)
        {
            case MSG_SEQUENCED -> onSequenced(b, index);
            case MSG_SNAPSHOT_FRAME -> onSnapshotFrame(b, index);
            case MSG_BATCH_END -> midBatch = false;
            default ->
            {
            }
        }
    }

    private void onSequenced(final DirectBuffer b, final int o)
    {
        midBatch = true;
        final int templateId = MdWire.templateId(b, o);
        final int blockLength = MdWire.blockLength(b, o);
        final int version = MdWire.version(b, o);
        final int body = o + MdWire.HEADER_LENGTH;
        final FanoutSymbol fs = symbol(MdWire.symbolId(b, o));
        final long seq = MdWire.seq(b, o);
        final long prevSeq = fs.seq;
        fs.transactTimeNs = MdWire.transactTimeNs(b, o);

        MarketDataUpdate update = null;
        switch (templateId)
        {
            case LevelUpdateDecoder.TEMPLATE_ID ->
            {
                levelUpdate.wrap(b, body, blockLength, version);
                fs.book.set(OrderBook.sideIndex(levelUpdate.sideRaw()), levelUpdate.price(), levelUpdate.quantity(),
                    levelUpdate.orderCount());
                fs.seq = seq;
                for (int i = 0; i < subscribers.size(); i++)
                {
                    final Subscriber s = subscribers.get(i);
                    if (s.matches(fs))
                    {
                        s.markDirty(fs);
                    }
                }
            }
            case TradePrintDecoder.TEMPLATE_ID ->
            {
                tradePrint.wrap(b, body, blockLength, version);
                fs.seq = seq;
                update = fs.update().setTrade(TradePrint.newBuilder().setMatchId(tradePrint.matchId())
                    .setPriceCents(tradePrint.price()).setQuantity(tradePrint.quantity())
                    .setAggressorSide(sideName(tradePrint.aggressorSideRaw()))
                    .setAuction(tradePrint.isAuctionRaw() == 1)).build();
            }
            case SymbolStatusDecoder.TEMPLATE_ID ->
            {
                symbolStatus.wrap(b, body, blockLength, version);
                fs.seq = seq;
                fs.state = symbolStatus.stateRaw();
                fs.stateCause = symbolStatus.causeRaw();
                update = fs.update().setStatus(SymbolStatus.newBuilder().setState(fs.stateName())
                    .setReasonCode(symbolStatus.reasonCode()).setCause(fs.stateCauseName())).build();
            }
            case ReferencePriceDecoder.TEMPLATE_ID ->
            {
                referencePrice.wrap(b, body, blockLength, version);
                fs.seq = seq;
                fs.referencePrice = referencePrice.referencePrice();
                fs.bandLow = referencePrice.bandLow();
                fs.bandHigh = referencePrice.bandHigh();
                update = fs.update().setReference(fs.reference()).build();
            }
            case ClosingPriceDecoder.TEMPLATE_ID ->
            {
                closingPrice.wrap(b, body, blockLength, version);
                fs.seq = seq;
                fs.closePrice = closingPrice.price();
                fs.previousClose = closingPrice.previousClose();
                fs.closeTradingDate = closingPrice.tradingDate();
                fs.closeSource = closingPrice.sourceRaw();
                fs.hasClose = true;
                update = fs.update().setClose(fs.close()).build();
            }
            case AuctionIndicativeDecoder.TEMPLATE_ID ->
            {
                auction.wrap(b, body, blockLength, version);
                fs.seq = seq;
                final long price = auction.price();
                final boolean hasPrice = price != Long.MIN_VALUE;
                update = fs.update().setAuction(AuctionIndicative.newBuilder().setPriceCents(hasPrice ? price : 0)
                    .setHasPrice(hasPrice).setMatchedQuantity(auction.matchedQuantity())
                    .setImbalanceQuantity(auction.imbalanceQuantity()).setImbalanceSide(sideName(auction.imbalanceSideRaw()))
                    .setFinal(auction.isFinalRaw() == 1)).build();
            }
            case InstrumentDefinitionDecoder.TEMPLATE_ID ->
            {
                instrument.wrap(b, body, blockLength, version);
                instrument.getTicker(tickerBytes, 0);
                fs.ticker = MdWire.ticker(tickerBytes);
                fs.state = instrument.stateRaw();
                final long ref = instrument.referencePrice();
                final long bps = instrument.priceBandBps();
                if (ref != fs.referencePrice || bps != fs.priceBandBps)
                {
                    fs.bandLow = Long.MIN_VALUE; // as the publisher does (see SnapshotBegin.bandLow)
                    fs.bandHigh = Long.MIN_VALUE;
                }
                fs.referencePrice = ref;
                fs.priceBandBps = bps;
                fs.seq = seq;
                for (int i = 0; i < subscribers.size(); i++)
                {
                    final Subscriber s = subscribers.get(i);
                    if (s.matches(fs))
                    {
                        s.sendSnapshot(fs, nowNs);
                    }
                }
                return;
            }
            default ->
            {
                fs.seq = seq;
                return;
            }
        }

        final boolean endOfBatch = (MdWire.flags(b, o) & MdWire.FLAG_END_OF_BATCH) != 0;
        for (int i = 0; i < subscribers.size(); i++)
        {
            final Subscriber s = subscribers.get(i);
            if (!s.matches(fs))
            {
                continue;
            }
            if (update != null)
            {
                if (s.conflationNs == 0)
                {
                    // Keep md_seq order: book changes before this message go out first, tagged with their seq.
                    final long seqNow = fs.seq;
                    fs.seq = prevSeq;
                    s.flush(fs, nowNs);
                    fs.seq = seqNow;
                }
                s.send(update);
            }
            if (endOfBatch && s.conflationNs == 0)
            {
                s.flush(fs, nowNs);
            }
        }
    }

    private void onSnapshotFrame(final DirectBuffer b, final int o)
    {
        final int templateId = MdWire.templateId(b, o);
        final int blockLength = MdWire.blockLength(b, o);
        final int version = MdWire.version(b, o);
        final int body = o + MdWire.HEADER_LENGTH;
        switch (templateId)
        {
            case SnapshotBeginDecoder.TEMPLATE_ID ->
            {
                snapshotBegin.wrap(b, body, blockLength, version);
                final FanoutSymbol fs = symbol((int)snapshotBegin.symbolId());
                fs.book.clear();
                fs.seq = snapshotBegin.lastSeq();
                fs.transactTimeNs = snapshotBegin.transactTimeNs();
                fs.state = snapshotBegin.stateRaw();
                fs.stateCause = snapshotBegin.stateCauseRaw();
                fs.referencePrice = snapshotBegin.referencePrice();
                fs.bandLow = snapshotBegin.bandLow();
                fs.bandHigh = snapshotBegin.bandHigh();
                fs.priceBandBps = snapshotBegin.priceBandBps();
                fs.closePrice = snapshotBegin.closePrice();
                fs.previousClose = snapshotBegin.previousClose();
                fs.closeTradingDate = snapshotBegin.closeTradingDate();
                fs.closeSource = snapshotBegin.closeSourceRaw();
                fs.hasClose = fs.closeSource != 255; // CloseSource null: no ClosingPrice yet
                snapshotBegin.getTicker(tickerBytes, 0);
                fs.ticker = MdWire.ticker(tickerBytes);
            }
            case SnapshotLevelsDecoder.TEMPLATE_ID ->
            {
                snapshotLevels.wrap(b, body, blockLength, version);
                final FanoutSymbol fs = symbol((int)snapshotLevels.symbolId());
                for (final SnapshotLevelsDecoder.LevelsDecoder l : snapshotLevels.levels())
                {
                    fs.book.set(OrderBook.sideIndex(l.sideRaw()), l.price(), l.quantity(), l.orderCount());
                }
            }
            default ->
            {
                // SnapshotEnd: nothing to do; snapshots are only offered before subscribers exist.
            }
        }
    }

    private static String sideName(final short raw)
    {
        final BookSide s = BookSide.get(raw);
        return s == BookSide.NULL_VAL ? "" : s.name();
    }

    @Override
    public void close()
    {
        running = false;
        if (thread != null)
        {
            try
            {
                thread.join(5_000);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
        else
        {
            closeAll(Status.UNAVAILABLE.withDescription("market data publisher shutting down"));
        }
    }
}
