package io.lmb.api;

/**
 * Deterministic timers. In production these are Aeron Cluster timers, so firing is a log entry
 * and replays identically. Scheduling the same correlationId again reschedules it.
 */
public interface TimerScheduler
{
    void schedule(long correlationId, long deadlineNs);

    void cancel(long correlationId);
}
