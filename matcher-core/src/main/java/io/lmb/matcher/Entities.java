package io.lmb.matcher;

/** Firm-level risk state (per shard). */
final class Firm
{
    final int firmId;
    final int idx;
    long creditBudget;
    long maxPositionPerSymbol;
    boolean enabled;
    /** Sum over the firm's live orders of leaves * riskPrice. */
    long openNotional;
    /** Signed traded notional today: buys positive, sells negative. */
    long tradedNet;
    final OrderList orders = new OrderList(Order.L_FIRM);

    Firm(final int firmId, final int idx)
    {
        this.firmId = firmId;
        this.idx = idx;
    }

    long creditUsed()
    {
        return openNotional + Math.abs(tradedNet);
    }
}

/** Per (firm, symbol) position and open quantity, plus the order list used by FIRM_SYMBOL mass cancel. */
final class FirmSymbol
{
    final Firm firm;
    final Symbol symbol;
    long netPosition;
    long openBuyQty;
    long openSellQty;
    final OrderList orders = new OrderList(Order.L_FS);

    FirmSymbol(final Firm firm, final Symbol symbol)
    {
        this.firm = firm;
        this.symbol = symbol;
    }

    static long key(final int firmIdx, final int symbolIdx)
    {
        return ((long)firmIdx << 32) | (symbolIdx & 0xFFFF_FFFFL);
    }
}

/** Orders resting for one (firm, sessionId), used by SESSION mass cancel. */
final class Session
{
    final OrderList orders = new OrderList(Order.L_SESSION);
}

/** A scheduled SymbolStateCommand waiting for its timer. */
final class PendingTransition
{
    final long timerId;
    final long correlationId;
    final long symbolId;
    final int reasonCode;
    final byte targetState;
    final long effectiveTimeNs;

    PendingTransition(
        final long timerId, final long correlationId, final long symbolId, final int reasonCode,
        final byte targetState, final long effectiveTimeNs)
    {
        this.timerId = timerId;
        this.correlationId = correlationId;
        this.symbolId = symbolId;
        this.reasonCode = reasonCode;
        this.targetState = targetState;
        this.effectiveTimeNs = effectiveTimeNs;
    }
}
