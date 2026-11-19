package io.lmb.gateway.core;

import io.lmb.sbe.OrdType;
import io.lmb.sbe.Side;
import io.lmb.sbe.TimeInForce;

/**
 * A client request after transport decoding, before validation. Front ends fill one and hand it to
 * {@link GatewayCore#handleRequest} on the core thread. Mutable and reusable by the caller.
 */
public final class OrderRequest
{
    public enum Kind
    {
        NEW, CANCEL, REPLACE, MASS_CANCEL
    }

    public Kind kind;
    /** Client sequence number (gRPC) or MsgSeqNum (FIX); only used to reference rejects. */
    public long clientSeq;
    public long clOrdId;
    public long origClOrdId;
    /** MassCancel correlation id. */
    public long requestId;
    /** Ticker; empty or null means "all symbols" for a mass cancel. */
    public String symbol;
    /** null when the client did not specify it. */
    public Side side;
    public OrdType ordType;
    public TimeInForce timeInForce;
    public long quantity;
    /** Cents; 0 or negative means "not given". */
    public long priceCents;
    public long accountId;
    /** Epoch nanoseconds at which the transport received the request. */
    public long gatewayInTsNs;

    public OrderRequest reset()
    {
        kind = null;
        clientSeq = 0;
        clOrdId = 0;
        origClOrdId = 0;
        requestId = 0;
        symbol = null;
        side = null;
        ordType = null;
        timeInForce = null;
        quantity = 0;
        priceCents = 0;
        accountId = 0;
        gatewayInTsNs = 0;
        return this;
    }
}
