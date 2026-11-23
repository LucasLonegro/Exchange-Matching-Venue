package io.lmb.tools.client;

/**
 * What a test client saw from the gateway, protocol neutral. {@code kind} is one of
 * NEW, PARTIAL_FILL, FILL, CANCELED, REPLACED, REJECTED, EXPIRED, CANCEL_REJECTED (execution reports) or
 * SESSION_REJECT (gateway refusal: gRPC SessionReject, FIX BusinessMessageReject / OrderCancelReject).
 */
public record Report(
    String kind,
    long clOrdId,
    long orderId,
    String symbol,
    char side,
    long lastQty,
    long lastPxCents,
    long leavesQty,
    long cumQty,
    int shardId,
    long eventSeq,
    boolean possDup,
    String text,
    long receivedNs)
{
    public boolean isFill()
    {
        return "FILL".equals(kind) || "PARTIAL_FILL".equals(kind);
    }

    public boolean isReject()
    {
        return "REJECTED".equals(kind) || "SESSION_REJECT".equals(kind) || "CANCEL_REJECTED".equals(kind);
    }

    @Override
    public String toString()
    {
        return kind + " clOrdId=" + clOrdId + " orderId=" + orderId + " " + symbol + " " + side +
            " last=" + lastQty + "@" + lastPxCents + " leaves=" + leavesQty + " cum=" + cumQty +
            " shard=" + shardId + " seq=" + eventSeq + (possDup ? " possDup" : "") +
            (text == null || text.isEmpty() ? "" : " text=" + text);
    }
}
