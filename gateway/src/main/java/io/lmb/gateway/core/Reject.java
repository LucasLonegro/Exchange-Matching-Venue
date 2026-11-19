package io.lmb.gateway.core;

/**
 * Refusals produced by the gateway itself, before anything reaches a shard's log. Transport
 * neutral; the gRPC front end maps them one to one onto {@code SessionReject.Reason}, the FIX
 * front end onto BusinessMessageReject reasons.
 */
public enum Reject
{
    NOT_LOGGED_ON,
    AUTH_FAILED,
    THROTTLED,
    UNAVAILABLE,
    BAD_SEQUENCE,
    UNKNOWN_SYMBOL,
    INVALID_FIELD,
    SYMBOL_NOT_TRADING,
    MAX_ORDER_SIZE,
    DUPLICATE_SESSION;

    static final Reject[] VALUES = values();
}
