package io.lmb.controlplane;

import io.lmb.sbe.SymbolState;

/** A command the leader has issued and follows until the event stream shows its outcome. Engine thread only. */
public final class TrackedCommand
{
    public enum Kind
    {
        SYMBOL_DEFINITION, SYMBOL_STATE, RISK_LIMIT, DAY_ROLLOVER, MASS_CANCEL
    }

    /** PENDING: persisted, not yet handed to the cluster. SCHEDULED: timer command accepted by offer, fires later. */
    public enum Status
    {
        PENDING, SENT, SCHEDULED, CONFIRMED, REJECTED, SUPERSEDED, LOST, EXPIRED, FAILED;

        /** Still being retried / awaiting its effect. */
        public boolean active()
        {
            return this == PENDING || this == SENT || this == SCHEDULED;
        }

        /** Could still take effect on the shard (counts towards risk upper bounds). */
        public boolean unresolved()
        {
            return active() || this == SUPERSEDED;
        }
    }

    public final long correlationId;
    public final int shardId;
    public final Kind kind;
    public final String key;
    public final String origin;
    public final String detail;
    public final byte[] payload;
    public final long effectiveNs;
    public final Long auditId;
    public final long leaderEpoch;
    public final long createdMs;

    // kind-specific fields (unused ones are 0 / null)
    public int firmId;
    public long budget = -1;
    public long maxPos;
    public boolean enabled;
    public int symbolId = -1;
    public SymbolState target;
    public int tradingDate;

    public Status status = Status.PENDING;
    public int attempts;
    public long lastSentMonoNs;
    public String rejectReason;
    public boolean persisted;

    public TrackedCommand(final long correlationId, final int shardId, final Kind kind, final String key, final String origin,
        final String detail, final byte[] payload, final long effectiveNs, final Long auditId, final long leaderEpoch,
        final long createdMs)
    {
        this.correlationId = correlationId;
        this.shardId = shardId;
        this.kind = kind;
        this.key = key;
        this.origin = origin;
        this.detail = detail;
        this.payload = payload;
        this.effectiveNs = effectiveNs;
        this.auditId = auditId;
        this.leaderEpoch = leaderEpoch;
        this.createdMs = createdMs;
    }

    @Override
    public String toString()
    {
        return kind + "#" + correlationId + "[" + detail + " " + status + " x" + attempts + "]";
    }
}
