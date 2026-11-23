package io.lmb.tools.client;

import java.util.function.Consumer;

/**
 * One participant session on a gateway (gRPC or FIX). Reports are handed to the listener on the
 * session's I/O thread; {@link #newOrder} and {@link #cancel} are thread-safe.
 */
public interface OrderSession extends AutoCloseable
{
    /** Connects and logs on; returns once the gateway accepted the logon. */
    void logon(long timeoutMs) throws Exception;

    /** Sends a DAY limit order. {@code side} is 'B' or 'S'. Returns the send timestamp (System.nanoTime). */
    long newOrder(long clOrdId, String symbol, char side, long qty, long priceCents);

    void cancel(long clOrdId, long origClOrdId, String symbol);

    void listener(Consumer<Report> listener);

    boolean isConnected();

    String describe();

    @Override
    void close();

    static OrderSession create(final String protocol, final String host, final int port,
                               final long firmId, final long sessionId, final String apiKey)
    {
        return switch (protocol)
        {
            case "grpc" -> new GrpcOrderSession(host, port, firmId, sessionId, apiKey);
            case "fix" -> new FixOrderSession(host, port, firmId, sessionId, apiKey, "LMB");
            default -> throw new IllegalArgumentException("protocol must be grpc or fix: " + protocol);
        };
    }
}
