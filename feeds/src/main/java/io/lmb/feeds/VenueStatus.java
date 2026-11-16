package io.lmb.feeds;

/** Health of one external venue's (arbitrated) feed, as reported by a {@link VenueFeedHandler}. */
public enum VenueStatus
{
    /**
     * Receiving and in sequence. Reported when a venue is first heard from and when it recovers
     * from {@link #STALE}.
     */
    UP,

    /**
     * One-shot notification: messages were lost on every line (or the venue restarted its feed
     * session), so any quote previously delivered for this venue may be out of date. Consumers must
     * stop trusting all of the venue's quotes until each symbol is quoted again.
     */
    GAP,

    /** Nothing received on any line for the handler's silence timeout. */
    STALE
}
