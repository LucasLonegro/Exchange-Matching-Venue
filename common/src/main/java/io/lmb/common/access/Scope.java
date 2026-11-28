package io.lmb.common.access;

/** What a connection made with an API key may do. Checked at logon; the grant is kept in the session's state. */
public enum Scope
{
    /** Order entry: new orders and replaces (and everything CANCEL allows). */
    TRADE,
    /** Order entry limited to cancels and mass cancels (a risk desk's "cancel only" credential). */
    CANCEL,
    /** OrderQuery: the firm's orders and sessions. */
    QUERY,
    /** The post-trade drop copy of the firm's executions. */
    DROP_COPY;

    /** null if unknown. */
    public static Scope parse(final String s)
    {
        for (final Scope x : values())
        {
            if (x.name().equalsIgnoreCase(s.trim()))
            {
                return x;
            }
        }
        return null;
    }
}
