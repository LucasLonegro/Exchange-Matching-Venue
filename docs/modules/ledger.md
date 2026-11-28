# ledger

The venue's books and records after the trade: per firm cash, positions, average cost, and realised and
unrealised PnL, every position marked to the official close each night, and a daily statement per firm. It
plays the clearing house's part. The exchange matches orders and publishes trades and the official close;
the ledger values positions at that close. It never sees any other notion of value. It is not part of
the matcher or the cluster: it is one more consumer of the shards' event streams, like the post-trade sinks.
It has no leaderboard and no scoring.

One process, `io.lmb.ledger.LedgerMain`, runs one `LedgerSink` on the shared `io.lmb.api.SinkRunner`
(common). The runner keeps its own event stream reader per shard, drops redeliveries (eventSeq <= cursor),
fails on a gap and reopens from the durable cursor after a failure.

## Inputs
Only the event stream. The ledger never reads the control plane, its database or the CSV files.

| Event | Effect |
|---|---|
| `Trade` | one fill for each side: the buyer buys, the seller sells `quantity` at `price` (one record per match, both firms) |
| `ClosingPrice` | the symbol's close for the current trading day; the last one before `DayRolled` is the official close |
| `DayRolled` | the shard's trading day ends: its positions are marked and written as statement lines; the day's counters reset |
| `SymbolDefined` | ticker, shard and removal of the symbol |
| `RiskLimitApplied` | the firm exists (it gets statements, with or without trades) |

The first `DayRollover` of a shard starts its first trading day and ends none (the venue's date is 0 before
it). Anything traded before it is carried into that day: it shows as bought or sold that day, from an
opening position of 0.

## Accounting (`Position`)
- Average cost method, amounts in integer cents, quantities in shares, exact arithmetic (an overflow throws
  and the sink restarts on the same event, so it is loud and never wraps). `cost_basis` is the signed cost of
  the open quantity: positive long, negative short, 0 flat.
- A fill that opens or adds to the position adds its notional to the cost basis. A fill that reduces it
  removes `cost_basis * closed / |quantity|` (truncated toward zero; the remainder goes with the last
  shares) and realises the difference to its notional. A fill that crosses zero closes everything, then
  opens the rest at its price.
- `cash` is the cumulative trade settlement (+ sells, - buys). Invariant: `cash = realised_pnl - cost_basis`,
  so `cash + market_value = realised + unrealised` to the cent at any mark. Across firms everything sums to 0
  (the venue charges no fees).
- Mark: the official close. Source `AUCTION`, `LAST_TRADE` or `PREVIOUS_CLOSE` as the venue reports it;
  `PREVIOUS_MARK` for a removed symbol (it has no close any more); `NONE` when no price was ever established.
  A position without a mark is valued at cost (unrealised 0). It cannot hold shares in practice, since a
  position only comes from a trade, and a trade sets the close.
- `pnl_day` = day's cash flow + market value at today's mark - market value at the previous mark. It equals
  the change in equity, and it also equals realised today + the change in unrealised.

## Daily statements
- At a shard's `DayRolled` every position in its symbols gets a `ledger_daily_position` line for the day
  that ended, flat positions included (they still carry cash).
- A firm's statement for date D is the sum of its lines for D. It is written once **every** shard has
  rolled past D, so it never mixes final and unfinished days. A firm has statements from the earliest
  trading date on which any shard saw it. Firm statements are in `ledger_statement`, and
  `ledger_statement_day` records the dates that are done.
- Files: `<statementsDir>/<yyyymmdd>/summary.csv` (one line per firm) and `firm-<id>.csv` (the firm's
  lines with `avg_cost`, in cents with 4 decimals). A date's directory is written as `<date>.tmp` and
  renamed into place. Missing directories are rebuilt from the database at the next start, so the files
  are a rendering, not a record. They are what the overnight S3 upload should pick up.
- Intraday the current state is in `ledger_position` (with today's running totals). Unrealised against the
  last close is `quantity * ledger_symbol.mark_price - cost_basis`.

## Exactly-once and determinism
The whole state is in memory and loaded at `open`. Each commit writes the changed rows together with the
per-shard cursors and trading dates in one transaction. A commit happens every `commitIntervalMs` or
`commitBatch` events. A failure rolls back and the runner reopens from the committed cursors, so the
database always holds the state after a prefix of each shard's stream. A position depends only on the
stream of its symbol's shard, and a statement only on final lines and a firm set that is final by then.
So the result does not depend on how the shards' streams interleave or where restarts fall. The test
`restartsFailuresAndRedeliveryGiveTheSameLedgerAsOneCleanRun` checks this table by table, with failures
injected and every stream rewound at each reopen.

## Tables (`ledger_schema.sql`, applied at every start, `CREATE TABLE IF NOT EXISTS`)
`ledger_cursor` (shard, event_seq, trading_date), `ledger_firm`, `ledger_symbol` (current close and last
official mark), `ledger_position`, `ledger_day` (shard days ended), `ledger_daily_position`,
`ledger_statement`, `ledger_statement_day`. In production they live in the control plane's PostgreSQL. The
ledger touches only its own `ledger_*` tables and must be their only writer: run exactly one instance.

## Configuration
Every key: system property / environment, then the topology file, then the default. `<dataDir>` =
`lmb.data.dir/ledger/<instance>`.

| Key | Default | Meaning |
|---|---|---|
| `lmb.instance.id` | 0 | instance |
| `lmb.ledger.db.url` | `jdbc:h2:file:<dataDir>/ledger;MODE=PostgreSQL` | JDBC URL (PostgreSQL in prod) |
| `lmb.ledger.db.user` / `.password` | `sa` / empty | |
| `lmb.ledger.statementsDir` | `<dataDir>/statements` | statement files |
| `lmb.ledger.commitIntervalMs` | 100 | |
| `lmb.ledger.commitBatch` | 1000 | events per transaction at most |
| `lmb.ledger.pollLimit` | 256 | events per shard per poll |
| `lmb.ledger.restartBackoffMs` | 1000 | wait before reopening after a failure |
| `lmb.ledger.metricsPort` | 9820 (+ instance) | Prometheus `/metrics` (`lmb_ledger_*`); 0 or less disables |
| `lmb.ledger.bindHost` | `0.0.0.0` | metrics bind address |

Alerts (`deploy/monitoring`, thresholds in `slo-*.env`):
- `LmbLedgerLag`: the shard's last eventSeq minus the ledger's, above `LEDGER_LAG_EVENTS` for 5 min.
- `LmbLedgerRestarting`: more than `LEDGER_RESTARTS_15M` restarts in 15 min. The database is down, or the
  ledger is stuck on an event it cannot apply.
- `LmbLedgerNotOpen`: the process is up but the sink is closed for `LEDGER_DOWN_FOR`.
- `LmbLedgerStatementsLate`: `lmb_ledger_pending_statement_dates > 0` for `LEDGER_STATEMENTS_LATE_FOR`. Some
  shard has ended a date the others have not, so its statements wait.
- A dead process is caught by `LmbTargetDown`.

Locally `scripts/local-up.sh` starts `ledger-0` after post-trade (`--no-ledger` to skip; `--minimal` skips
it). It prints `LMB_READY ledger 0` once it has caught up with every shard. `--full` points it at the
docker-compose PostgreSQL.

## Tests
`PositionTest`: average cost, partial closes, shorts, crossing zero, the truncation remainder, the
cash/realised/cost invariant over random fills, the daily close, exact arithmetic. `LedgerSinkTest` uses
the real matcher on two shards in `InMemoryVenue`. It checks:
- marks at the official close, and that statements wait for every shard;
- positions carried over, with `pnl_day` equal to the change in equity and the zero-sum totals;
- the statement file contents, and that they are rebuilt;
- restarts, failures and redelivery against one clean run.

## Known gaps
- No deposits, withdrawals or fees: cash is trade settlement only, starting at 0.
- No accounts below the firm: `Trade` carries firms, not accounts.
- No reconciliation against the matcher's own positions. The matcher keeps per-firm positions for its
  limit checks but does not publish them, so there is nothing to compare with yet.
- Statements assume every shard rolls through the same trading dates, which is what the control plane's
  calendar does. A shard that skipped a date would contribute no lines to it.
- The schema is tested on H2 (PostgreSQL mode) only.
- Statement files are not uploaded anywhere yet (see the overnight S3 plan).
