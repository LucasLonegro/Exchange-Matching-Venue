package io.lmb.matcher;

/**
 * A pooled order. Lives in up to six intrusive doubly linked lists at once (price level FIFO,
 * all live orders, firm, firm-symbol, session, symbol), all ordered by orderId except the level FIFO.
 */
final class Order
{
    static final int L_LEVEL = 0;
    static final int L_GLOBAL = 1;
    static final int L_FIRM = 2;
    static final int L_FS = 3;
    static final int L_SESSION = 4;
    static final int L_SYMBOL = 5;
    static final int LISTS = 6;

    final int poolIndex;
    final Order[] prev = new Order[LISTS];
    final Order[] next = new Order[LISTS];

    long orderId;
    long clOrdId;
    long sessionId;
    /** Limit price, or null price for market orders. */
    long price;
    /** Price used for credit usage: the limit price, or the band edge for market orders. */
    long riskPrice;
    long qty;
    long cumQty;
    int accountId;
    int gatewayId;
    byte side;
    byte ordType;
    byte tif;

    Firm firm;
    Symbol symbol;
    FirmSymbol fs;
    Session session;
    PriceLevel level;
    /** Entry in the dedupe table holding this order's current clOrdId. */
    int dedupeEntry;
    /** True while linked into the global/firm/fs/session/symbol lists (i.e. resting). */
    boolean linked;

    Order nextFree;

    Order(final int poolIndex)
    {
        this.poolIndex = poolIndex;
    }

    long leaves()
    {
        return qty - cumQty;
    }

    boolean isBuy()
    {
        return side == Codes.BUY;
    }
}
