package io.lmb.replayer;

/**
 * The first point where the replay and the recorded event stream disagree.
 *
 * @param kind          what differs
 * @param entry         index of the log entry since the replay started (0-based), -1 if not tied to an entry
 * @param logPosition   log position of the entry (position after it, as given to the machine)
 * @param timestampNs   cluster timestamp of the entry
 * @param eventSeq      eventSeq of the recorded event compared (or of the replayed one if the stream had none)
 * @param eventIdx      index of the event within the entry
 * @param expected      recorded event (hex, possibly truncated), or a description
 * @param actual        replayed event (hex, possibly truncated), or a description
 */
public record Divergence(
    Kind kind, long entry, long logPosition, long timestampNs, long eventSeq, long eventIdx, String expected, String actual)
{
    public enum Kind
    {
        /** Both are CommandApplied and identical except for chainHash (the hashed bytes differ, e.g. the input). */
        CHAIN_HASH,
        /** An event (other than CommandApplied only differing by hash) differs. */
        EVENT_BYTES,
        /** One side ended the entry (CommandApplied) while the other emitted another event. */
        EVENT_COUNT,
        /** The recorded stream has events beyond the end of the replayed log. */
        LOG_MISSING_ENTRIES
    }

    @Override
    public String toString()
    {
        return kind + " at entry " + entry + " logPosition " + logPosition + " timestampNs " + timestampNs +
            " eventSeq " + eventSeq + " eventIdx " + eventIdx + "\n  expected " + expected + "\n  actual   " + actual;
    }
}
