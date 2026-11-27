package io.lmb.ledger;

import io.lmb.api.SinkRunner;
import io.prometheus.metrics.core.metrics.CounterWithCallback;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Counters of the ledger process, exported through Prometheus callbacks. Prefix {@code lmb_ledger_}. */
public final class LedgerMetrics
{
    public final AtomicLong trades = new AtomicLong();
    public final AtomicLong daysClosed = new AtomicLong();
    public final AtomicLong statements = new AtomicLong();
    public final AtomicLong statementFiles = new AtomicLong();
    public final AtomicLong commits = new AtomicLong();
    public final AtomicLong lastCommitNs = new AtomicLong();
    /** Trading dates some shard has ended whose statements are not written yet (committed state). */
    public final AtomicLong pendingStatementDates = new AtomicLong();

    private volatile SinkRunner runner;

    void bind(final SinkRunner runner)
    {
        this.runner = runner;
    }

    public void register(final PrometheusRegistry r)
    {
        counter(r, "lmb_ledger_events", "Events consumed", () -> runner == null ? 0 : runner.events.get());
        counter(r, "lmb_ledger_duplicate_events", "Redelivered events dropped by the eventSeq dedupe",
            () -> runner == null ? 0 : runner.duplicates.get());
        counter(r, "lmb_ledger_restarts", "Failures followed by a restart from the durable cursor",
            () -> runner == null ? 0 : runner.restarts.get());
        gauge(r, "lmb_ledger_up", "1 while the ledger is open", () -> runner != null && runner.isUp() ? 1 : 0);
        GaugeWithCallback.builder().name("lmb_ledger_last_event_seq").help("Last eventSeq applied per shard")
            .labelNames("shard")
            .callback(cb ->
            {
                final SinkRunner x = runner;
                if (x != null)
                {
                    for (int s = 0; s < x.shardCount(); s++)
                    {
                        cb.call(x.lastEventSeq(s), Integer.toString(s));
                    }
                }
            }).register(r);
        counter(r, "lmb_ledger_trades", "Trades applied (both sides)", trades::get);
        counter(r, "lmb_ledger_days_closed", "Shard trading days ended (DayRolled) and marked", daysClosed::get);
        counter(r, "lmb_ledger_statements", "Firm daily statements written to the database", statements::get);
        counter(r, "lmb_ledger_statement_files", "Trading dates whose statement files were written", statementFiles::get);
        counter(r, "lmb_ledger_commits", "Transactions committed", commits::get);
        gauge(r, "lmb_ledger_last_commit_ns", "Duration of the last commit", lastCommitNs::get);
        gauge(r, "lmb_ledger_pending_statement_dates",
            "Trading dates some shard has ended whose firm statements wait for the other shards", pendingStatementDates::get);
    }

    private static void counter(final PrometheusRegistry r, final String name, final String help, final LongSupplier v)
    {
        CounterWithCallback.builder().name(name).help(help).callback(cb -> cb.call(v.getAsLong())).register(r);
    }

    private static void gauge(final PrometheusRegistry r, final String name, final String help, final LongSupplier v)
    {
        GaugeWithCallback.builder().name(name).help(help).callback(cb -> cb.call(v.getAsLong())).register(r);
    }
}
