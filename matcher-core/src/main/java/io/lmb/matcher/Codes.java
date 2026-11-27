package io.lmb.matcher;

/** Raw SBE enum codes and limits used on the hot path (avoids enum lookups when decoding). */
final class Codes
{
    static final long NULL_PRICE = Long.MIN_VALUE;
    /** Largest accepted price, in cents ($10,000,000). Keeps price * quantity inside a long. */
    static final long MAX_PRICE = 1_000_000_000L;
    /** Largest accepted quantity / lot / maxOrderQty. */
    static final long MAX_QTY = 1_000_000_000L;
    static final int MAX_SYMBOLS = 1 << 19;
    static final long NULL_SYMBOL = 0xFFFF_FFFFL;

    static final byte BUY = 1;
    static final byte SELL = 2;
    static final byte LIMIT = 1;
    static final byte MARKET = 2;
    static final byte DAY = 1;
    static final byte IOC = 2;

    static final byte PRE_OPEN = 1;
    static final byte AUCTION_CALL = 2;
    static final byte CONTINUOUS = 3;
    static final byte HALTED = 4;
    static final byte CLOSED = 5;
    static final byte REMOVED = 6;

    /** "No side" in internal fields (auction imbalance side). */
    static final byte NO_SIDE = 0;

    // day-cycle: ClosePriceSource raw values
    static final byte CLOSE_NONE = 0;
    static final byte CLOSE_AUCTION = 1;
    static final byte CLOSE_LAST_TRADE = 2;
    static final byte CLOSE_PREVIOUS_CLOSE = 3;

    private Codes()
    {
    }

    static boolean allowed(final byte from, final byte to)
    {
        if (to == REMOVED)
        {
            return from != REMOVED;
        }
        switch (from)
        {
            case PRE_OPEN:
                return to == AUCTION_CALL || to == CONTINUOUS || to == CLOSED;
            case AUCTION_CALL:
                return to == CONTINUOUS || to == HALTED || to == CLOSED;
            case CONTINUOUS:
                return to == HALTED || to == AUCTION_CALL || to == CLOSED;
            case HALTED:
                return to == AUCTION_CALL || to == CONTINUOUS || to == CLOSED;
            case CLOSED:
                return to == PRE_OPEN;
            default:
                return false;
        }
    }

    static long gcd(final long a, final long b)
    {
        long x = a;
        long y = b;
        while (y != 0)
        {
            final long t = x % y;
            x = y;
            y = t;
        }
        return x;
    }
}
