package io.lmb.feeds;

/**
 * One venue's top of book for one symbol, normalised to venue conventions: prices in integer cents
 * (a missing side is {@link #NO_PRICE} or a size of 0), sizes in shares, timestamps in epoch ns.
 *
 * <p>A flyweight: feed handlers reuse one instance per callback, so a listener must copy what it
 * needs before returning. Every quote is a full replacement of the venue's previous top of book for
 * the symbol (not an increment).
 */
public final class NormalizedQuote
{
    public static final long NO_PRICE = Long.MIN_VALUE;

    private int venueId;
    private int symbolId;
    private long bidPrice = NO_PRICE;
    private long bidSize;
    private long askPrice = NO_PRICE;
    private long askSize;
    private long venueTsNs;
    private long receiveTsNs;
    private long sequence;

    public NormalizedQuote set(
        final int venueId,
        final int symbolId,
        final long bidPrice,
        final long bidSize,
        final long askPrice,
        final long askSize,
        final long venueTsNs,
        final long receiveTsNs,
        final long sequence)
    {
        this.venueId = venueId;
        this.symbolId = symbolId;
        this.bidPrice = bidPrice;
        this.bidSize = bidSize;
        this.askPrice = askPrice;
        this.askSize = askSize;
        this.venueTsNs = venueTsNs;
        this.receiveTsNs = receiveTsNs;
        this.sequence = sequence;
        return this;
    }

    /** Dense venue index, 0 .. venueCount-1. Vendor adapters map their venue codes (MICs) to these. */
    public int venueId()
    {
        return venueId;
    }

    /** The venue's {@code SymbolDirectory} symbol id (adapters map vendor symbology to it). */
    public int symbolId()
    {
        return symbolId;
    }

    public long bidPrice()
    {
        return bidPrice;
    }

    public long bidSize()
    {
        return bidSize;
    }

    public long askPrice()
    {
        return askPrice;
    }

    public long askSize()
    {
        return askSize;
    }

    public boolean hasBid()
    {
        return bidPrice != NO_PRICE && bidSize > 0;
    }

    public boolean hasAsk()
    {
        return askPrice != NO_PRICE && askSize > 0;
    }

    /** Time the external venue stamped on the quote (its matching-engine / SIP timestamp). */
    public long venueTsNs()
    {
        return venueTsNs;
    }

    /** Time the handler received the packet (kernel/NIC or vendor timestamp), on this host's clock. */
    public long receiveTsNs()
    {
        return receiveTsNs;
    }

    /** The venue feed's message sequence number after A/B arbitration. */
    public long sequence()
    {
        return sequence;
    }

    @Override
    public String toString()
    {
        return "Quote{venue=" + venueId + ", symbol=" + symbolId + ", bid=" + bidSize + "@" + bidPrice +
            ", ask=" + askSize + "@" + askPrice + ", venueTs=" + venueTsNs + ", rxTs=" + receiveTsNs +
            ", seq=" + sequence + '}';
    }
}
