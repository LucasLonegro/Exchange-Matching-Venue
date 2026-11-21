package io.lmb.marketdata;

import io.lmb.marketdata.book.OrderBook;
import io.lmb.marketdata.sbe.BookSide;
import io.lmb.marketdata.sbe.MessageHeaderEncoder;
import io.lmb.marketdata.sbe.SnapshotBeginEncoder;
import io.lmb.marketdata.sbe.SnapshotEndEncoder;
import io.lmb.marketdata.sbe.SnapshotLevelsEncoder;
import io.lmb.marketdata.sbe.TradingState;
import io.lmb.marketdata.wire.MdWire;
import org.agrona.ExpandableArrayBuffer;

/**
 * Encodes one symbol's snapshot as frames (uint16le length + SBE message):
 * SnapshotBegin, SnapshotLevels x n (all bids best first, then all asks best first, at most
 * {@link #LEVELS_PER_MESSAGE} per message so every frame fits one datagram), SnapshotEnd.
 * Must run on the thread that owns the {@link SymbolBook}, so the snapshot sits on a batch boundary.
 */
public final class SnapshotWriter
{
    public static final int LEVELS_PER_MESSAGE = 50;
    private static final int LEVEL_LENGTH = SnapshotLevelsEncoder.LevelsEncoder.sbeBlockLength();

    /** Largest frame this writer produces. */
    public static final int MAX_FRAME_LENGTH = MdWire.FRAME_PREFIX_LENGTH + MdWire.HEADER_LENGTH
        + SnapshotLevelsEncoder.BLOCK_LENGTH + 4 + LEVELS_PER_MESSAGE * LEVEL_LENGTH;

    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final SnapshotBeginEncoder begin = new SnapshotBeginEncoder();
    private final SnapshotLevelsEncoder levels = new SnapshotLevelsEncoder();
    private final SnapshotEndEncoder end = new SnapshotEndEncoder();

    /** Writes the snapshot frames at out[offset..]; returns the number of bytes written. */
    public int write(final SymbolBook s, final ExpandableArrayBuffer out, final int offset)
    {
        final OrderBook book = s.book;
        final int bids = book.levels(OrderBook.BID);
        final int asks = book.levels(OrderBook.ASK);
        out.checkLimit(offset + MdWire.FRAME_PREFIX_LENGTH * 2 + 64 + (bids + asks) * (LEVEL_LENGTH + 8) + 256);
        int pos = offset;

        begin.wrapAndApplyHeader(out, pos + MdWire.FRAME_PREFIX_LENGTH, header)
            .lastSeq(s.seq).transactTimeNs(s.lastTransactTimeNs).tickSize(s.tickSize).lotSize(s.lotSize)
            .symbolId(s.symbolId).bidLevels(bids).askLevels(asks).putTicker(s.ticker, 0)
            .state(TradingState.get((short)s.state));
        pos = frame(out, pos, begin.encodedLength());

        final int total = bids + asks;
        int i = 0;
        while (i < total)
        {
            final int n = Math.min(LEVELS_PER_MESSAGE, total - i);
            levels.wrapAndApplyHeader(out, pos + MdWire.FRAME_PREFIX_LENGTH, header).lastSeq(s.seq).symbolId(s.symbolId);
            final SnapshotLevelsEncoder.LevelsEncoder g = levels.levelsCount(n);
            for (int k = 0; k < n; k++, i++)
            {
                final int side = i < bids ? OrderBook.BID : OrderBook.ASK;
                final int idx = i < bids ? i : i - bids;
                g.next().price(book.price(side, idx)).quantity(book.quantity(side, idx))
                    .orderCount(book.orderCount(side, idx)).side(BookSide.get((short)OrderBook.wireSide(side)));
            }
            pos = frame(out, pos, levels.encodedLength());
        }

        end.wrapAndApplyHeader(out, pos + MdWire.FRAME_PREFIX_LENGTH, header).lastSeq(s.seq).symbolId(s.symbolId);
        pos = frame(out, pos, end.encodedLength());
        return pos - offset;
    }

    private static int frame(final ExpandableArrayBuffer out, final int pos, final int bodyLength)
    {
        final int len = MdWire.HEADER_LENGTH + bodyLength;
        MdWire.putFrameLength(out, pos, len);
        return pos + MdWire.FRAME_PREFIX_LENGTH + len;
    }
}
