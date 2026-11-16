package io.lmb.marketdata;

import io.lmb.marketdata.book.OrderBook;

/** Publisher-side state of one symbol. Owned by the publisher thread. */
public final class SymbolBook
{
    public final int symbolId;
    public final OrderBook book = new OrderBook();
    final byte[] ticker = new byte[8];
    int shardId = -1;
    long seq;
    long lastTransactTimeNs;
    long tickSize;
    long lotSize;
    long referencePrice = Long.MIN_VALUE;
    int state = 1; // PRE_OPEN until told otherwise

    /** Offset in the current batch buffer of this symbol's last message, -1 when not touched in the batch. */
    int lastMessageOffset = -1;

    /** Set by the engine: this symbol's retransmission ring. */
    RetransmitHistory.SymbolHistory history;

    SymbolBook(final int symbolId)
    {
        this.symbolId = symbolId;
    }

    /** Sequence of the last message published for the symbol (0 = none). */
    public long seq()
    {
        return seq;
    }

    public int state()
    {
        return state;
    }

    public int shardId()
    {
        return shardId;
    }

    public long lastTransactTimeNs()
    {
        return lastTransactTimeNs;
    }

    public long tickSize()
    {
        return tickSize;
    }

    public long lotSize()
    {
        return lotSize;
    }

    public String ticker()
    {
        return io.lmb.marketdata.wire.MdWire.ticker(ticker);
    }
}
