package io.lmb.marketdata;

import io.lmb.common.Events;
import io.lmb.marketdata.book.OrderBook;
import io.lmb.marketdata.sbe.AuctionIndicativeEncoder;
import io.lmb.marketdata.sbe.BookSide;
import io.lmb.marketdata.sbe.InstrumentDefinitionEncoder;
import io.lmb.marketdata.sbe.LevelUpdateEncoder;
import io.lmb.marketdata.sbe.MessageHeaderEncoder;
import io.lmb.marketdata.sbe.SeqHeaderEncoder;
import io.lmb.marketdata.sbe.SymbolStatusEncoder;
import io.lmb.marketdata.sbe.TradePrintEncoder;
import io.lmb.marketdata.sbe.TradingState;
import io.lmb.marketdata.sbe.YesNo;
import io.lmb.marketdata.wire.MdWire;
import io.lmb.sbe.AuctionUpdateDecoder;
import io.lmb.sbe.BookDeltaDecoder;
import io.lmb.sbe.CommandAppliedDecoder;
import io.lmb.sbe.DayRolledDecoder;
import io.lmb.sbe.SymbolDefinedDecoder;
import io.lmb.sbe.SymbolStateChangedDecoder;
import io.lmb.sbe.TradeDecoder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.ArrayList;

/**
 * Turns shards' event streams into sequenced market data messages. Deterministic: the output (message
 * bytes and their order) is a pure function of each shard's event stream, independent of how the
 * streams are interleaved or chunked, so two publisher instances and a restarted instance agree.
 *
 * <p>Rules:
 * <ul>
 *   <li>Events of a log entry are staged per shard and applied only when that entry's
 *       {@code CommandApplied} arrives. Books therefore always sit on a batch boundary and output
 *       is emitted in whole batches.</li>
 *   <li>Each symbol has a sequence counter starting at 1; every message for the symbol takes the
 *       next value. One message per relevant event: BookDelta, Trade, SymbolStateChanged,
 *       AuctionUpdate, SymbolDefined. DayRolled additionally removes any level still resting for
 *       the shard's symbols (a no-op when the matcher already emitted the deltas).</li>
 *   <li>The last message of each symbol in a batch carries {@link MdWire#FLAG_END_OF_BATCH}.</li>
 *   <li>Events at or below the last seen eventSeq of the shard are dropped (at-least-once delivery).</li>
 * </ul>
 * Single-threaded. Allocation-free in steady state (new symbols and buffer growth allocate).
 */
public final class BookBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(BookBuilder.class);

    /** Receives the builder's output, one whole batch at a time. */
    public interface Output
    {
        /** One sequenced message (messageHeader + body). The buffer is only valid during the call. */
        void onMessage(int shardId, SymbolBook symbol, DirectBuffer buffer, int offset, int length);

        /** Called after the last message of a batch that produced at least one message. */
        void onBatchEnd(int shardId, long firstReceiveNanos, long eventTimestampNs);
    }

    private final Output output;
    private final Int2ObjectHashMap<SymbolBook> symbols = new Int2ObjectHashMap<>();
    private final IntArrayList sortedSymbolIds = new IntArrayList();
    private final Int2ObjectHashMap<ShardStage> shards = new Int2ObjectHashMap<>();

    private final ExpandableArrayBuffer batch = new ExpandableArrayBuffer(64 * 1024);
    private final IntArrayList messageOffsets = new IntArrayList();
    private final IntArrayList messageLengths = new IntArrayList();
    private final ArrayList<SymbolBook> messageSymbols = new ArrayList<>();
    private final ArrayList<SymbolBook> touched = new ArrayList<>();
    private int batchLength;

    private final BookDeltaDecoder bookDelta = new BookDeltaDecoder();
    private final TradeDecoder trade = new TradeDecoder();
    private final SymbolStateChangedDecoder stateChanged = new SymbolStateChangedDecoder();
    private final AuctionUpdateDecoder auctionUpdate = new AuctionUpdateDecoder();
    private final SymbolDefinedDecoder symbolDefined = new SymbolDefinedDecoder();

    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final LevelUpdateEncoder levelUpdate = new LevelUpdateEncoder();
    private final TradePrintEncoder tradePrint = new TradePrintEncoder();
    private final SymbolStatusEncoder symbolStatus = new SymbolStatusEncoder();
    private final AuctionIndicativeEncoder auctionIndicative = new AuctionIndicativeEncoder();
    private final InstrumentDefinitionEncoder instrumentDefinition = new InstrumentDefinitionEncoder();

    private long duplicateEvents;
    private long eventGaps;
    private long eventsProcessed;

    public BookBuilder(final Output output)
    {
        this.output = output;
    }

    /**
     * Feed one event (SBE messageHeader + body) of any shard.
     *
     * @param receiveNanos monotonic receive time, only used for latency measurement
     */
    public void onEvent(final DirectBuffer buffer, final int offset, final int length, final long receiveNanos)
    {
        if (length < MdWire.HEADER_LENGTH + 32 || !Events.isEvent(buffer, offset))
        {
            return;
        }
        final int shardId = Events.shardId(buffer, offset);
        final long eventSeq = Events.eventSeq(buffer, offset);
        ShardStage stage = shards.get(shardId);
        if (stage == null)
        {
            stage = new ShardStage(shardId);
            shards.put(shardId, stage);
        }
        if (eventSeq <= stage.lastEventSeq)
        {
            duplicateEvents++;
            return;
        }
        if (stage.lastEventSeq == 0 && eventSeq != 1)
        {
            LOG.warn("shard {} stream starts at eventSeq {}, not 1: sequence numbers will not match a publisher "
                + "that saw the whole stream", shardId, eventSeq);
            eventGaps++;
        }
        else if (stage.lastEventSeq != 0 && eventSeq != stage.lastEventSeq + 1)
        {
            LOG.error("shard {} event gap: {} -> {}", shardId, stage.lastEventSeq, eventSeq);
            eventGaps++;
        }
        stage.lastEventSeq = eventSeq;
        eventsProcessed++;

        if (!stage.batchOpen)
        {
            stage.batchOpen = true;
            stage.firstReceiveNanos = receiveNanos;
        }

        final int templateId = Events.templateId(buffer, offset);
        switch (templateId)
        {
            case BookDeltaDecoder.TEMPLATE_ID, TradeDecoder.TEMPLATE_ID, SymbolStateChangedDecoder.TEMPLATE_ID,
                AuctionUpdateDecoder.TEMPLATE_ID, SymbolDefinedDecoder.TEMPLATE_ID, DayRolledDecoder.TEMPLATE_ID ->
                stage.stage(buffer, offset, length);
            case CommandAppliedDecoder.TEMPLATE_ID -> commit(stage, Events.timestampNs(buffer, offset));
            default ->
            {
            }
        }
    }

    public SymbolBook symbol(final int symbolId)
    {
        return symbols.get(symbolId);
    }

    /** Known symbol ids in ascending order. Do not modify. */
    public IntArrayList sortedSymbolIds()
    {
        return sortedSymbolIds;
    }

    public long lastEventSeq(final int shardId)
    {
        final ShardStage s = shards.get(shardId);
        return s == null ? 0 : s.lastEventSeq;
    }

    public long duplicateEvents()
    {
        return duplicateEvents;
    }

    public long eventGaps()
    {
        return eventGaps;
    }

    public long eventsProcessed()
    {
        return eventsProcessed;
    }

    // ---------------------------------------------------------------------------------------------

    private void commit(final ShardStage stage, final long timestampNs)
    {
        batchLength = 0;
        messageOffsets.clear();
        messageLengths.clear();
        messageSymbols.clear();
        touched.clear();

        final ExpandableArrayBuffer data = stage.data;
        for (int i = 0; i < stage.offsets.size(); i++)
        {
            apply(stage.shardId, data, stage.offsets.getInt(i), timestampNs);
        }

        for (int i = 0; i < touched.size(); i++)
        {
            final SymbolBook s = touched.get(i);
            MdWire.setFlags(batch, s.lastMessageOffset, MdWire.FLAG_END_OF_BATCH);
            s.lastMessageOffset = -1;
        }
        final int count = messageOffsets.size();
        for (int i = 0; i < count; i++)
        {
            output.onMessage(stage.shardId, messageSymbols.get(i), batch, messageOffsets.getInt(i), messageLengths.getInt(i));
        }
        if (count > 0)
        {
            output.onBatchEnd(stage.shardId, stage.firstReceiveNanos, timestampNs);
        }
        stage.reset();
    }

    private void apply(final int shardId, final DirectBuffer b, final int offset, final long timestampNs)
    {
        final int templateId = Events.templateId(b, offset);
        final int blockLength = b.getShort(offset, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final int version = b.getShort(offset + 6, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final int body = offset + MdWire.HEADER_LENGTH;
        switch (templateId)
        {
            case BookDeltaDecoder.TEMPLATE_ID ->
            {
                bookDelta.wrap(b, body, blockLength, version);
                final SymbolBook s = symbolFor((int)bookDelta.symbolId(), shardId);
                final int sideRaw = bookDelta.sideRaw();
                if (sideRaw != 1 && sideRaw != 2)
                {
                    return;
                }
                final long qty = Math.max(0, bookDelta.quantity());
                s.book.set(OrderBook.sideIndex(sideRaw), bookDelta.price(), qty, bookDelta.orderCount());
                final int at = begin(LevelUpdateEncoder.BLOCK_LENGTH, LevelUpdateEncoder.TEMPLATE_ID);
                levelUpdate.wrap(batch, at + MdWire.HEADER_LENGTH);
                seqHeader(levelUpdate.seqHeader(), s, timestampNs);
                levelUpdate.price(bookDelta.price()).quantity(qty).orderCount(qty == 0 ? 0 : bookDelta.orderCount())
                    .side(BookSide.get((short)sideRaw));
                end(s, at, MdWire.HEADER_LENGTH + LevelUpdateEncoder.BLOCK_LENGTH);
            }
            case TradeDecoder.TEMPLATE_ID ->
            {
                trade.wrap(b, body, blockLength, version);
                final SymbolBook s = symbolFor((int)trade.symbolId(), shardId);
                final int at = begin(TradePrintEncoder.BLOCK_LENGTH, TradePrintEncoder.TEMPLATE_ID);
                tradePrint.wrap(batch, at + MdWire.HEADER_LENGTH);
                seqHeader(tradePrint.seqHeader(), s, timestampNs);
                tradePrint.matchId(trade.matchId()).price(trade.price()).quantity(trade.quantity())
                    .aggressorSide(BookSide.get(trade.aggressorSideRaw()))
                    .isAuction(trade.isAuctionRaw() == 1 ? YesNo.YES : YesNo.NO);
                end(s, at, MdWire.HEADER_LENGTH + TradePrintEncoder.BLOCK_LENGTH);
            }
            case SymbolStateChangedDecoder.TEMPLATE_ID ->
            {
                stateChanged.wrap(b, body, blockLength, version);
                final SymbolBook s = symbolFor((int)stateChanged.symbolId(), shardId);
                s.state = stateChanged.newStateRaw();
                final int at = begin(SymbolStatusEncoder.BLOCK_LENGTH, SymbolStatusEncoder.TEMPLATE_ID);
                symbolStatus.wrap(batch, at + MdWire.HEADER_LENGTH);
                seqHeader(symbolStatus.seqHeader(), s, timestampNs);
                symbolStatus.reasonCode(stateChanged.reasonCode())
                    .state(TradingState.get(stateChanged.newStateRaw()))
                    .previousState(TradingState.get(stateChanged.previousStateRaw()));
                end(s, at, MdWire.HEADER_LENGTH + SymbolStatusEncoder.BLOCK_LENGTH);
            }
            case AuctionUpdateDecoder.TEMPLATE_ID ->
            {
                auctionUpdate.wrap(b, body, blockLength, version);
                final SymbolBook s = symbolFor((int)auctionUpdate.symbolId(), shardId);
                final int at = begin(AuctionIndicativeEncoder.BLOCK_LENGTH, AuctionIndicativeEncoder.TEMPLATE_ID);
                auctionIndicative.wrap(batch, at + MdWire.HEADER_LENGTH);
                seqHeader(auctionIndicative.seqHeader(), s, timestampNs);
                auctionIndicative.price(auctionUpdate.price())
                    .matchedQuantity(auctionUpdate.matchedQuantity())
                    .imbalanceQuantity(auctionUpdate.imbalanceQuantity())
                    .imbalanceSide(BookSide.get(auctionUpdate.imbalanceSideRaw()))
                    .isFinal(auctionUpdate.isFinalRaw() == 1 ? YesNo.YES : YesNo.NO);
                end(s, at, MdWire.HEADER_LENGTH + AuctionIndicativeEncoder.BLOCK_LENGTH);
            }
            case SymbolDefinedDecoder.TEMPLATE_ID ->
            {
                symbolDefined.wrap(b, body, blockLength, version);
                final SymbolBook s = symbolFor((int)symbolDefined.symbolId(), shardId);
                s.tickSize = symbolDefined.tickSize();
                s.lotSize = symbolDefined.lotSize();
                s.referencePrice = symbolDefined.referencePrice();
                s.state = symbolDefined.stateRaw();
                symbolDefined.getTicker(s.ticker, 0);
                final int at = begin(InstrumentDefinitionEncoder.BLOCK_LENGTH,
                    InstrumentDefinitionEncoder.TEMPLATE_ID);
                instrumentDefinition.wrap(batch, at + MdWire.HEADER_LENGTH);
                seqHeader(instrumentDefinition.seqHeader(), s, timestampNs);
                instrumentDefinition.tickSize(s.tickSize).lotSize(s.lotSize).referencePrice(s.referencePrice)
                    .putTicker(s.ticker, 0).state(TradingState.get((short)s.state));
                end(s, at, MdWire.HEADER_LENGTH + InstrumentDefinitionEncoder.BLOCK_LENGTH);
            }
            case DayRolledDecoder.TEMPLATE_ID -> clearShardBooks(shardId, timestampNs);
            default ->
            {
            }
        }
    }

    /** Safety net on DayRolled: removes every level still resting on the shard's symbols, in symbolId order. */
    private void clearShardBooks(final int shardId, final long timestampNs)
    {
        for (int i = 0; i < sortedSymbolIds.size(); i++)
        {
            final SymbolBook s = symbols.get(sortedSymbolIds.getInt(i));
            if (s.shardId != shardId || s.book.isEmpty())
            {
                continue;
            }
            for (int side = OrderBook.BID; side <= OrderBook.ASK; side++)
            {
                while (s.book.levels(side) > 0)
                {
                    final long price = s.book.price(side, 0);
                    s.book.set(side, price, 0, 0);
                    final int at = begin(LevelUpdateEncoder.BLOCK_LENGTH, LevelUpdateEncoder.TEMPLATE_ID);
                    levelUpdate.wrap(batch, at + MdWire.HEADER_LENGTH);
                    seqHeader(levelUpdate.seqHeader(), s, timestampNs);
                    levelUpdate.price(price).quantity(0).orderCount(0)
                        .side(BookSide.get((short)OrderBook.wireSide(side)));
                    end(s, at, MdWire.HEADER_LENGTH + LevelUpdateEncoder.BLOCK_LENGTH);
                }
            }
        }
    }

    private int begin(final int blockLength, final int templateId)
    {
        final int at = batchLength;
        batch.checkLimit(at + MdWire.MAX_SEQUENCED_LENGTH + 64);
        header.wrap(batch, at)
            .blockLength(blockLength)
            .templateId(templateId)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);
        return at;
    }

    private static void seqHeader(final SeqHeaderEncoder h, final SymbolBook s, final long timestampNs)
    {
        s.seq++;
        s.lastTransactTimeNs = timestampNs;
        h.seq(s.seq).transactTimeNs(timestampNs).symbolId(s.symbolId).flags((short)0).reserved1((short)0).reserved2(0);
    }

    private void end(final SymbolBook s, final int at, final int length)
    {
        messageOffsets.addInt(at);
        messageLengths.addInt(length);
        messageSymbols.add(s);
        if (s.lastMessageOffset < 0)
        {
            touched.add(s);
        }
        s.lastMessageOffset = at;
        batchLength = at + length;
    }

    private SymbolBook symbolFor(final int symbolId, final int shardId)
    {
        SymbolBook s = symbols.get(symbolId);
        if (s == null)
        {
            s = new SymbolBook(symbolId);
            symbols.put(symbolId, s);
            int i = 0;
            while (i < sortedSymbolIds.size() && sortedSymbolIds.getInt(i) < symbolId)
            {
                i++;
            }
            sortedSymbolIds.addInt(i, symbolId);
        }
        s.shardId = shardId;
        return s;
    }

    private static final class ShardStage
    {
        final int shardId;
        final ExpandableArrayBuffer data = new ExpandableArrayBuffer(4096);
        final IntArrayList offsets = new IntArrayList();
        int length;
        long lastEventSeq;
        boolean batchOpen;
        long firstReceiveNanos;

        ShardStage(final int shardId)
        {
            this.shardId = shardId;
        }

        void stage(final DirectBuffer buffer, final int offset, final int len)
        {
            data.putBytes(length, buffer, offset, len);
            offsets.addInt(length);
            length += len;
        }

        void reset()
        {
            offsets.clear();
            length = 0;
            batchOpen = false;
            firstReceiveNanos = 0;
        }
    }
}
