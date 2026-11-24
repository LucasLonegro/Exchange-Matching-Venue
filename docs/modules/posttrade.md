# posttrade

Post-trade consumers of the shards' event streams (ARCHITECTURE.md 5.5). One process, `io.lmb.posttrade.PostTradeMain`,
runs up to three independent sinks, each on its own `SinkRunner` thread with its own event stream per shard and its own
durable cursor. A slow or failing sink never stalls the others; a failing sink is closed and reopened from its cursor
after `restartBackoffMs` (the process keeps running).

| Sink | Class | Output | Delivery |
|---|---|---|---|
| `tradestore` | `store.TradeStore` | JDBC: `pt_trade`, `pt_fill`, `pt_cursor` (H2 file locally, PostgreSQL in prod) | exactly-once (cursor row in the same transaction; inserts idempotent on (shard_id, event_seq)) |
| `dropcopy` | `dropcopy.DropCopySink` + `dropcopy.DropCopyEngine` | FIX 4.4 ExecutionReports over Artio (acceptor) | per client cursor; see below |
| `surveillance` | `surveillance.SurveillanceSink` | JSON lines file locally, Kafka topic when `lmb.posttrade.kafka.bootstrap` is set | file: exactly-once; Kafka: at-least-once, dedupe on (shard, eventSeq) |

## Classes
| Class | Role |
|---|---|
| `PostTradeMain` | process entry: topology, firm/symbol directories, `VenueClients.connect`, READY line, SIGTERM |
| `PostTrade` | one instance, usable in-process: builds the enabled sinks, runners, drop copy engine, metrics endpoint |
| `SinkRunner` | thread per sink: polls a stream per shard, drops redeliveries (eventSeq <= cursor), fails the sink on a gap, restarts it |
| `PostTradeSink` | sink SPI: `open` returns the durable cursor per shard, `onEvent`, `doWork`, `close(clean)` |
| `CursorFile` | key=value + CRC32 checkpoint file, tmp + fsync + atomic rename |
| `PostTradeMetrics` | `lmb_posttrade_*` Prometheus metrics |

## FIX drop copy
- Logon: SenderCompID 1-32 chars `[A-Za-z0-9_.-]` (identifies the client and its cursor), TargetCompID = `compId`,
  Username(553) = firm id and Password(554) = that firm's API key from the firms CSV; or Username `ALL` with the admin
  key for a session that receives every firm's reports (disabled when no admin key is configured).
- Content: ExecutionReports whose exec type is in `execTypes` (default fills, partial fills, cancels, expiries), mapped
  like the gateway's FIX reports, plus LmbShardID(20001), LmbEventSeq(20002), LmbFirmID(20003), LmbSessionID(20004).
  (LmbShardID, LmbEventSeq) identifies a report.
- Each client has a per-shard cursor (last event offered to it). On logon it catches up from its cursor with a private
  event stream reader, then switches to the live stream. A new client starts at the beginning of the retained stream;
  Logon tag LmbResumeFrom(20010) `shard:eventSeq;shard:eventSeq` overrides the stored cursor.
- Resend semantics: cursors are checkpointed in `<dataDir>/dropcopy/dropcopy.ckpt` every `checkpointIntervalMs` and on
  a clean stop. After a clean stop (SIGTERM) a reconnecting client gets exactly the reports after the last one handed to
  Artio: nothing is sent twice. After a crash the cursors can lag by up to one interval; those reports are sent again
  with PossResend(97)=Y (replayed reports of known clients whose event predates the process start). FIX-level
  ResendRequests inside a FIX session are answered by Artio from its archive; Artio keeps sequence numbers across restarts
  unless `deleteLogsOnStart`.
- The engine runs an embedded media driver + Archive under `<dataDir>/dropcopy` (sole library mode); the FixLibrary
  is owned by the engine so a sink restart does not wait for a library timeout.

## Surveillance
One JSON object per ExecutionReport (every exec type) and per Trade; other events only advance the cursor. Every record
starts `{"shard":S,"eventSeq":N,"type":"ExecutionReport"|"Trade","timestampNs":...,"execId":...` followed by the
decoded fields (prices in cents, enum names as strings). File mode: the file is its own cursor (on start a torn last
line is truncated and the largest eventSeq per shard is the resume point); flush = write + fsync every
`checkpointIntervalMs`. Kafka mode: key = shard id, idempotent producer, acks=all; the cursor
(`<dataDir>/surveillance-kafka.ckpt`) moves only after a successful `flush()`.

## Configuration
Every key: system property / environment, then the topology file, then the default. `<dataDir>` =
`lmb.data.dir/posttrade/<instance>`.

| Key | Default | Meaning |
|---|---|---|
| `lmb.instance.id` | 0 | instance |
| `lmb.posttrade.bindHost` | `0.0.0.0` | FIX acceptor and metrics bind address |
| `lmb.posttrade.metricsPort` | 9810 (+ instance) | Prometheus `/metrics`; 0 or less disables |
| `lmb.posttrade.pollLimit` | 256 | events per shard per runner poll |
| `lmb.posttrade.restartBackoffMs` | 1000 | wait before reopening a failed sink |
| `lmb.posttrade.tradestore.enabled` | true | |
| `lmb.posttrade.db.url` | `jdbc:h2:file:<dataDir>/tradestore;MODE=PostgreSQL` | JDBC URL (PostgreSQL in prod) |
| `lmb.posttrade.db.user` / `.password` | `sa` / empty | |
| `lmb.posttrade.tradestore.commitIntervalMs` | 100 | |
| `lmb.posttrade.tradestore.commitBatch` | 1000 | |
| `lmb.posttrade.dropcopy.enabled` | true | |
| `lmb.posttrade.dropCopyFixPort` | 9800 (+ instance) | FIX acceptor port |
| `lmb.posttrade.dropcopy.compId` | `LMB` | our CompID (client's TargetCompID) |
| `lmb.posttrade.dropcopy.adminKey` | none | password of the `ALL` session; none = no ALL session |
| `lmb.posttrade.dropcopy.execTypes` | `PARTIAL_FILL,FILL,CANCELED,EXPIRED` | SBE ExecType names to send |
| `lmb.posttrade.dropcopy.termBufferLength` | 262144 | embedded driver term length |
| `lmb.posttrade.dropcopy.deleteLogsOnStart` | false | wipe Artio logs and archive (resets FIX sequence numbers) |
| `lmb.posttrade.dropcopy.checkpointIntervalMs` | 1000 | |
| `lmb.posttrade.surveillance.enabled` | true | |
| `lmb.posttrade.kafka.bootstrap` | none | Kafka bootstrap servers; none = JSON lines file |
| `lmb.posttrade.surveillance.topic` | `lmb.surveillance` | |
| `lmb.posttrade.surveillance.file` | `<dataDir>/surveillance.jsonl` | |
| `lmb.posttrade.surveillance.checkpointIntervalMs` | 1000 | flush / fsync / Kafka cursor interval |

## Running
`java -cp <classpath> -Dlmb.topology=<file> -Dlmb.instance.id=0 io.lmb.posttrade.PostTradeMain`. Prints
`LMB_READY posttrade <instance>` once every enabled sink has caught up with every shard. SIGTERM: runners stop and
close their sinks cleanly (final commit, checkpoint, FIX logouts), then the FIX engine, metrics server and venue
connection.

## Tests
`./mvnw -q -pl posttrade test` (13 tests, no Docker or network): TradeStoreTest 6, CursorFileTest 1,
DropCopySinkTest 3 (per-firm/ALL filtering over a real FIX initiator, catch-up then live, refused logons, clean restart
without resend), SurveillanceSinkTest 2 (record content, recovery from lost and torn writes with redelivery),
PostTradeTest 1 (local profile end to end: READY line, metrics endpoint, restart on the same data dir without duplicates).
Ports: drop copy tests FIX 37000-37049, PostTradeTest FIX 37050-37089 and metrics 37150-37189.

## Known gaps
- Kafka output is untested (no broker in tests); `lmb_posttrade_surveillance_errors` is never incremented (failures show
  as sink restarts).
- The crash path of the drop copy (PossResend=Y on replays) is implemented but not covered by a test.
- Catch-up switches to live when a client's reader reaches the runner's position; under a sustained rate higher than one
  reader poll per loop it can lag behind for a while (it does not lose reports).
- Drop copy client cursors are never pruned; a new SenderCompID replays the whole retained stream (use LmbResumeFrom).
- The surveillance file is scanned fully on start to find the resume point (fine for a day's file, slow for huge ones).
- `PostTradeMain` is not tested as a separate process (SIGTERM); its ready logic is tested in-process.
- No S3/WORM archiver or ClickHouse loader (listed in ARCHITECTURE.md 5.5); schema migration has no cross-instance lock.
