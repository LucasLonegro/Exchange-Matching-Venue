# replayer

Module `replayer` (`io.lmb.replayer`, artifact `lmb-replayer`). Re-executes a shard's Raft log on a fresh
`ShardStateMachine` (found with `ShardStateMachineFactory.load()`, i.e. matcher-core) and checks that every
event, and so every `CommandApplied.chainHash`, is byte-identical to what the venue published. Also runs the
same machine as a backtester.

Three entry points:

| Main | What it does |
|---|---|
| `io.lmb.replayer.VerifyMain` | Offline verification of a member directory against the events that member recorded. |
| `io.lmb.replayer.ShadowReplayerMain` | Continuous verification of a live shard (1 process per shard), with Prometheus metrics. |
| `io.lmb.replayer.BacktestMain` | Recorded log and/or injected commands -> `events.bin`, `fills.csv`, `summary.txt`. |

## Classes

* `ReplayVerifier`: a `RaftLogReader.Handler`. Applies each command or timer to the machine, then compares
  each produced event with the next expected event from an `EventSource`. Stops at the first `Divergence`:
  * `CHAIN_HASH`: both events are CommandApplied and differ only in the hash.
  * `EVENT_BYTES`: any other event differs.
  * `EVENT_COUNT`: one side ended the entry while the other kept emitting.
  * `LOG_MISSING_ENTRIES`: the stream continues past the end of the log (offline only).

  A divergence records the entry index, log position, timestamp, eventSeq and the hex of both events. Timers
  are not scheduled during verification, because the log already carries every timer that fired.
* `EventSource`: one interface for both event sources. `EventFiles` reads a member's recorded stream offline
  (`events/segments.dat` plus the archive). `EventSource.of(EventStreamClient)` wraps the live stream.
* `ArchiveFiles`: read-only access to any Archive recording (catalog plus `.rec` segments) without a media
  driver. It reassembles fragmented messages and follows an active recording.
* `SnapshotLoader`: restores the machine from the service snapshot recording, which has HEADER, CHUNK* and
  END messages (cluster.md section 7). It checks the chunk count, lastEventSeq and logPosition.
* `OfflineVerifier`: `verify(memberDir, factory, Options) -> Report`.
* `ShadowReplayer` / `ReplayerConfig` / `ReplayerMetrics`: the live verifier (see below).
* `Backtest`: the backtester.
* `CommandText`: encodes a text line such as `NewOrder clOrdId=7 side=BUY ...` into SBE through the generated
  encoders. It is used for backtest command files and tests. It allocates, so keep it off the hot path.

## Offline verification

```
java -cp <classpath> io.lmb.replayer.VerifyMain <memberDir> [--to <logPosition>] [--from-snapshot] [--shard <id>]
```

`memberDir` contains `cluster/` and `archive/`, for example
`data/cluster-node/shard-0-member-1` or a ClusterBackup directory. The shard id comes from `--shard`, else
the directory name, else the first event. The log is replayed from 0 unless:

* the log was purged, in which case replay starts from the earliest service snapshot after the purge point; or
* `--from-snapshot` is given, in which case replay starts from the latest service snapshot at or before `--to`.

The output reports entries, events compared or unverified, the last verified eventSeq and log position, the
last chain hash, and the first divergence if there is one. Exit status: 0 verified, 2 divergence, 1 error.

## Shadow replayer

```
java -Dlmb.replayer.shard=0 -cp <classpath> io.lmb.replayer.ShadowReplayerMain
```

It follows the live event stream (`VenueClients.eventStream`). Each CommandApplied proves its entry is
committed. When one arrives, the replayer reads the log up to that position from the files of the member on
this host with the most recorded log, replays it, and compares. The member files are re-opened, at most every
`reopenIntervalMs`, whenever the stream gets ahead of what was recorded when they were last opened.

After a divergence the replayer stops comparing, logs `SHADOW REPLAY DIVERGED ...` at ERROR, sets
`lmb_replayer_divergence` to 1 and keeps serving metrics. The process does not exit on a divergence.

It prints `LMB_READY replayer <shard>` once all of these hold:

* the stream is connected;
* everything received has been verified;
* no event has arrived for `readyQuietMs`.

It stops cleanly on SIGTERM and exits with status 1 if the replayer thread fails.

| Key | Default | Meaning |
|---|---|---|
| `lmb.replayer.shard` | `lmb.instance.id`, else 0 | shard to verify |
| `lmb.replayer.memberDirs` | `<lmb.data.dir>/cluster-node/shard-S-member-M` for every member | comma-separated member dirs on this host |
| `lmb.replayer.metricsPort` | `Topology.replayerMetricsPort() + shard` (9900 + shard) | Prometheus port; negative disables it |
| `lmb.replayer.fromSnapshot` | false | start from the latest service snapshot (forced if the log was purged) |
| `lmb.replayer.reopenIntervalMs` | 5 | minimum interval between re-reads of the member files |
| `lmb.replayer.maxPendingEvents` | 1000000 | stop pulling the stream while this many events wait for the log |
| `lmb.replayer.readyQuietMs` | 300 | quiet period before READY |

The replayer's own client settings (`VenueClients.connect`) come from the topology, as for any client.

Metrics. All are gauges with label `shard`, except the counter `lmb_replayer_log_reopens_total`:

| Metric | Meaning |
|---|---|
| `lmb_replayer_divergence` | 1 after a divergence, else 0. **Page on 1.** |
| `lmb_replayer_verified_event_seq` | eventSeq of the last event that matched |
| `lmb_replayer_verified_log_position` | log position of the last entry whose events all matched |
| `lmb_replayer_applied_log_position` | log position of the last entry replayed |
| `lmb_replayer_stream_event_seq` | eventSeq of the last event received from the stream |
| `lmb_replayer_lag_events` | stream eventSeq minus verified eventSeq. **Alert if it grows.** |
| `lmb_replayer_lag_bytes` | committed log bytes, as confirmed by the stream, not yet replayed |
| `lmb_replayer_entries` | log entries replayed since start |
| `lmb_replayer_events_compared` | events compared since start |
| `lmb_replayer_log_reopens_total` | re-reads of the member files |

## Backtester

```
java -cp <classpath> io.lmb.replayer.BacktestMain --out <dir> [--log <memberDir>] [--to <logPosition>] [--from-snapshot]
     [--commands <file>] [--shard <id>] [--step-ns <ns>] [--drain-timers]
```

The run happens in two phases:

1. It replays the recorded log, if given.
2. It applies the command file, if given. Each line is a `CommandText` command; `#` starts a comment.
   * An optional leading `@<timestampNs>` sets the command's cluster timestamp. Timestamps must not go
     backwards. Without one, a command gets the previous timestamp plus `--step-ns` (default 1000).
   * Timers the machine schedules fire before the first command whose timestamp reaches their deadline. With
     `--drain-timers`, any timers still pending fire at the end.
   * Injected entries get synthetic log positions that continue the log, frame-aligned like real entries.

Outputs:

* `events.bin`: each event as a 4-byte little-endian length followed by the event bytes.
* `fills.csv`: one row per `Trade` event, with these columns: eventSeq, timestampNs, logPosition, symbolId,
  matchId, price, quantity, buyOrderId, sellOrderId, buyFirmId, sellFirmId, aggressorSide, isAuction.
* `summary.txt`: counts per event type, trades, volume, notional, rejects, timers, last eventSeq, chain hash
  and end position.

Replaying a log alone reproduces the recorded stream byte for byte (tested).

## Tests

Run `./mvnw -q -pl replayer test`. Tests use ports 38000-38299 and write under `replayer/target/replayer-tests`.

* `OfflineVerifierTest`: records one 2-member shard with orders, a timer and a snapshot. It checks that:
  * a clean log verifies on both members with equal chain hashes;
  * verification from the snapshot gives the same final hash;
  * VerifyMain returns the right exit codes;
  * a flipped NewOrder quantity byte in the log `.rec` gives EVENT_BYTES at that entry's position and index;
  * a flipped chain-hash byte in the event recording gives CHAIN_HASH at that eventSeq;
  * a machine that is 1 ns off on entry 17 is caught at entry 17.
* `ShadowReplayerTest`: runs two replayers against a live shard while 60 orders and a timer flow. It checks
  that:
  * the READY line is printed;
  * the clean replayer verifies the live edge, with zero lag and divergence 0 in the scraped metrics;
  * the skewed replayer diverges at entry 20, reports divergence 1 and keeps running.
* `BacktestTest`: checks that:
  * a command-file run is deterministic and the timer halts the symbol at its deadline;
  * fills match the trades;
  * log replay equals the recorded events byte for byte;
  * the log plus injected commands continues eventSeq and log positions, through the CLI.

## Known gaps

* The shadow replayer reads the log from member files on the same host. Following a remote member through
  the Archive (`REPLAY_ALL_AND_FOLLOW` plus `RaftLogDecoder`) is not implemented.
* A live start from position 0 needs the event stream to still hold eventSeq 1. If event retention has
  purged it, start with `lmb.replayer.fromSnapshot=true`. There is no automatic fallback.
* A shadow replayer that restarts begins again from 0, or from the snapshot. It keeps no checkpoint.
* READY is based on a quiet period, because the stream API does not expose its live edge.
* Backtest from a snapshot does not restore timers that were pending at snapshot time, because they live in
  the consensus module's snapshot.
* Synthetic timer positions in the backtester assume a 40-byte TimerEvent.
* Tests do not cover a log with several terms (a leader change). RaftLogReader covers that case.
* `SIGTERM` handling of the mains is not tested in a subprocess.
