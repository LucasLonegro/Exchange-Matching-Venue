# marketdata

Market data publisher and its client library (package `io.lmb.marketdata`, ARCHITECTURE.md section 5.2).
The publisher consumes every shard's event stream, keeps a price-level book per symbol, and publishes at
`CommandApplied` boundaries over three transports:

- binary incrementals over UDP on two independent feeds, A (instance 0) and B (instance 1), unicast or multicast;
- a UDP snapshot channel that cycles through every symbol's book;
- TCP retransmission of missed messages and on-demand snapshots;
- gRPC `MarketData.Subscribe` (`api-proto`, `lmb/marketdata/v1/market_data.proto`) with depth limits and conflation.

`io.lmb.marketdata.client.FeedClient` is the reference receiver: A/B arbitration, gap detection and recovery.

## Classes

| Class | What |
|---|---|
| `MarketDataPublisherMain` | process entry: config, `VenueClients.connect`, READY line, SIGTERM |
| `MarketDataPublisher` | one instance: engine thread, UDP, TCP, gRPC server, metrics endpoint; usable in-process |
| `MarketDataEngine` | the publisher thread's duty cycle: poll streams, fan out, snapshot channel, heartbeats, checkpoints |
| `BookBuilder`, `SymbolBook`, `book/OrderBook` | events -> sequenced messages; deterministic |
| `FeedPublisher`, `wire/MdWire` | datagram packing and the packet/frame layout |
| `RetransmitHistory`, `RetransmitServer` | last `historyDepth` messages per symbol; TCP service |
| `SnapshotWriter`, `StateCheckpoint` | snapshot frames; `state.bin` restart checkpoint |
| `grpc/*` | gRPC service, fan-out thread, per-subscriber conflation and slow-consumer handling |
| `client/FeedClient`, `client/RetransmitClient` | receiver library |

## Determinism and sequence numbers

The output is a pure function of the shards' event streams. Every instance (A, B, a restarted one, one that
read the streams in different chunks) publishes the same messages, byte for byte, with the same sequence numbers.
This is tested in `TwoPublishersTest` and `CheckpointRestartTest`.

- Events of one log entry are staged per shard and applied when that entry's `CommandApplied` arrives, so output
  is always whole batches and books always sit on a batch boundary.
- Each symbol has its own sequence `md_seq`, starting at 1, and every message for the symbol takes the next value.
  Mapping: `BookDelta` -> `LevelUpdate`, `Trade` -> `TradePrint`, `SymbolStateChanged` -> `SymbolStatus`,
  `AuctionUpdate` -> `AuctionIndicative`, `SymbolDefined` -> `InstrumentDefinition`. `DayRolled` also emits a
  `LevelUpdate` with quantity 0 for any level still resting on the shard's symbols.
- The last message of a symbol in a batch has `flags` bit 0 (`END_OF_BATCH`) set. A receiver that wants
  consistent books applies messages up to that flag before showing the book.
- Events at or below the last eventSeq seen for a shard are dropped (at-least-once delivery). A gap in eventSeq
  is fatal: the publisher thread stops and the process exits with status 1 instead of publishing sequence numbers that
  differ from the other feed's.

## Wire format

SBE schema `marketdata/src/main/resources/sbe/marketdata.xml` (schema id 2, little endian, generated package
`io.lmb.marketdata.sbe`). Prices are integer cents; the null `Price` is `Long.MIN_VALUE`.

```
datagram = PacketHeader (24 bytes) frame*
frame    = uint16le length, SBE message (8-byte messageHeader + body) of that length
PacketHeader: sessionId u32 @0, feedId u8 @4 (0 = A, 1 = B), channel u8 @5 (1 = incremental, 2 = snapshot),
              messageCount u16 @6, packetSeq i64 @8 (per feed and channel, diagnostic), sendTimeNs i64 @16
```

Datagrams hold whole frames only and are at most `lmb.md.maxDatagram` bytes. `packetSeq` restarts at 1 when a
publisher restarts; receivers use `md_seq`, not `packetSeq`, for ordering and loss.

| id | Message | Channel | Contents |
|---|---|---|---|
| 1 | `LevelUpdate` | incremental | SeqHeader, side, price, quantity (0 = level removed), orderCount |
| 2 | `TradePrint` | incremental | SeqHeader, matchId, price, quantity, aggressorSide (null for auction), isAuction |
| 3 | `SymbolStatus` | incremental | SeqHeader, state, previousState, reasonCode |
| 4 | `AuctionIndicative` | incremental | SeqHeader, price, matched and imbalance quantity, imbalanceSide, isFinal |
| 5 | `InstrumentDefinition` | incremental | SeqHeader, tickSize, lotSize, referencePrice, ticker, state |
| 10-12 | `SnapshotBegin`, `SnapshotLevels`*, `SnapshotEnd` | snapshot, TCP | book of one symbol as of `lastSeq` |
| 20 | `Heartbeat` | both | sent on a channel that was idle for `heartbeatIntervalMs` (messageCount 1, no md_seq) |
| 30, 31 | `RetransmitRequest`, `RetransmitResponse` | TCP | see below |
| 32 | `SnapshotRequest` | TCP | answered by a `RetransmitResponse` (status OK, messageCount = frames) and the snapshot frames |

`SeqHeader` (first field of ids 1-5): `seq` (md_seq), `transactTimeNs` (cluster time of the log entry), `symbolId`, `flags`.
A snapshot's `lastSeq` is the md_seq of the last incremental it includes; the receiver continues at `lastSeq + 1`.
`SnapshotBegin` also carries tick and lot size, ticker, state and the level counts; `SnapshotLevels` hold at most a
fixed number of levels (bids best first, then asks best first) so each frame fits in a datagram.

### TCP retransmission

Frames as above on a persistent connection (port `lmb.md.<i>.retransmitPort`). Each `RetransmitRequest
(requestId, symbolId, fromSeq, toSeq)` is answered in order by one `RetransmitResponse` followed by its
`messageCount` sequenced frames, which are the same bytes that were multicast. Status: `UNKNOWN_SYMBOL` (never
published), `INVALID_RANGE` (fromSeq < 1 or toSeq < fromSeq), `TOO_OLD` (fromSeq older than the history held; get
a snapshot), else `OK` with `fromSeq..min(toSeq, lastSeq, fromSeq + retransmitMaxPerRequest - 1)` (empty if
fromSeq is beyond lastSeq; re-request the rest of a capped range). The history is in memory only: after a
publisher restart it holds only messages published since then, so older ranges answer `TOO_OLD`.

## How a consumer subscribes

**Binary feed, Java.** Use `FeedClient` and poll it from one thread:

```java
FeedClient.Config cfg = new FeedClient.Config();
cfg.incremental.add(new InetSocketAddress("127.0.0.1", 9540));   // feed A (lmb.md.0.incremental)
cfg.incremental.add(new InetSocketAddress("127.0.0.1", 9541));   // feed B
cfg.snapshot.add(new InetSocketAddress("127.0.0.1", 9550));      // optional, needed for late join and tail loss
cfg.retransmit.add(new InetSocketAddress("127.0.0.1", 9500));    // tried in order
cfg.retransmit.add(new InetSocketAddress("127.0.0.1", 9501));
try (FeedClient client = new FeedClient(cfg, listener)) { while (running) client.poll(System.nanoTime()); }
```

For multicast, list the group addresses and set `cfg.multicastInterface`. The listener gets every sequenced
message exactly once and in md_seq order per symbol, `onSnapshotApplied` when a book was replaced by a snapshot, and
`onSessionChange` when the publisher's sessionId changes (every symbol is reset). `client.symbol(id)` exposes the
book, last seq and ticker.

**Binary feed, other languages.** Bind the incremental ports (or join the groups) of both feeds. Per symbol, keep
the last md_seq applied: drop a message at or below it (the other feed's copy), apply the next one, buffer
anything after a gap. If the other feed does not fill the gap within a few ms, send a `RetransmitRequest` to
either publisher; on `TOO_OLD`, send a `SnapshotRequest` (or wait for the snapshot channel) and continue
from the snapshot's `lastSeq + 1`. A late joiner starts from the snapshot channel. Watch heartbeats to tell an idle
feed from a dead one.

**gRPC.** `MarketData.Subscribe(symbols, depth, conflation_ms)` on `lmb.md.<i>.grpcPort`. The stream starts with
a `BookSnapshot` per matching symbol (`md_seq` = last message it includes, `depth` levels per side, 0 = full book),
then trades, status and auction updates as they happen, and book incrementals per log entry
(`conflation_ms = 0`) or at most one per symbol per interval. Limits: depth <= 10000, conflation_ms <= 60000,
else INVALID_ARGUMENT. A subscriber more than `grpcMaxQueued` updates behind is closed with RESOURCE_EXHAUSTED;
reconnect and you get fresh snapshots. Over gRPC md_seq is informative only (conflation skips values).

## Running

```
scripts/lmb-run io.lmb.marketdata.MarketDataPublisherMain -Dlmb.topology=deploy/local/topology.properties -Dlmb.instance.id=0
```

`scripts/local-up.sh` starts instances 0 and 1 (A and B). Each instance:

1. loads the topology and its `MdConfig`, connects to the shards through `VenueClients.connect` (cluster-client),
2. restores `state.bin` if present and opens each shard's event stream after the checkpoint's eventSeq
   (from eventSeq 0 without a checkpoint; the stream must still start at eventSeq 1, see Known gaps),
3. starts the retransmission, gRPC and metrics servers and the publisher thread,
4. once every shard's stream is connected and caught up with its live edge, prints `LMB_READY marketdata <instance>`
   (while waiting it logs the last eventSeq per shard every 5 s).

SIGTERM: the publisher thread stops, a final checkpoint is written, servers close, exit 0. If the publisher thread
fails (event gap, bug) the process exits with status 1 and writes no checkpoint from the failed state.

Start-up backlog: after the first full poll of a shard, while polls keep coming back full (`pollLimit` events) and
until the stream reports connected with a short poll, messages go to the retransmission history and gRPC but not to
UDP (`lmb.md.suppressCatchUp`). A receiver fills that hole from the other feed, by retransmission or from a snapshot.
This keeps a restarted publisher from flooding the network with history.

## Restart and recovery

- `state.bin` in `<lmb.data.dir>/marketdata/<instance>/`: per shard the eventSeq of the last applied
  `CommandApplied`, per symbol md_seq, parameters and book; CRC32; written every `checkpointIntervalMs` (only if
  something changed) and on stop, through a temp file, fsync and atomic rename.
- On start the publisher reads each stream after the checkpoint's eventSeq. Events redelivered at or below it are
  dropped, so a clean restart publishes no message twice and continues md_seq where it stopped.
- After a crash the checkpoint may be older than what was published. The publisher then republishes the messages
  after the checkpoint with the same md_seq and the same bytes; receivers drop them as duplicates.
- A checkpoint with another `lmb.md.sessionId`, an unknown format or a bad CRC stops the start with an error.
  Delete it (and change `lmb.md.sessionId` so that receivers reset) to start from the beginning of the streams.
- Instances never share state; A and B checkpoint independently and still agree.

## Configuration

Every key is read with `Topology.get`: system property, then environment variable, then topology file, then the
default. `<i>` is the instance (`lmb.instance.id`, 0 = A, 1 = B).

| Key | Default | Meaning |
|---|---|---|
| `lmb.instance.id` | 0 | instance; also the feedId in packets |
| `lmb.shards` | 1 | shards to consume (topology) |
| `lmb.data.dir` | `./data` | checkpoint under `<dir>/marketdata/<i>/state.bin` |
| `lmb.md.<i>.retransmitPort` | 9500 + i | TCP retransmission and snapshot requests |
| `lmb.md.<i>.grpcPort` | 9510 + i | gRPC `MarketData` |
| `lmb.md.<i>.metricsPort` | 9520 + i | Prometheus `/metrics`; 0 or less disables |
| `lmb.md.<i>.bindHost` | `0.0.0.0` | bind address of the TCP, gRPC and metrics servers |
| `lmb.md.<i>.incremental` | `127.0.0.1:954<i>` | incremental destinations `host:port[,host:port]`; `none` disables |
| `lmb.md.<i>.snapshot` | `127.0.0.1:955<i>` | snapshot channel destinations; `none` disables |
| `lmb.md.<i>.host` | `127.0.0.1` | where clients reach this instance (read by scripts and clients, not by the publisher) |
| `lmb.md.sessionId` | 1 | feed session id in every packet and in the checkpoint |
| `lmb.md.multicastInterface` | (empty) | outgoing interface for multicast destinations |
| `lmb.md.multicastTtl` | 1 | multicast TTL |
| `lmb.md.maxDatagram` | 1400 | max datagram bytes, 512..65000 |
| `lmb.md.heartbeatIntervalMs` | 1000 | heartbeat after this long without incremental traffic |
| `lmb.md.snapshotIntervalMs` | 1000 | start of one snapshot cycle over all symbols to the next |
| `lmb.md.snapshotSymbolsPerPoll` | 64 | snapshot pacing per duty cycle |
| `lmb.md.historyDepth` | 8192 | messages kept per symbol for retransmission (worst case symbols x depth x ~72 bytes) |
| `lmb.md.retransmitMaxPerRequest` | 1000 | cap on messages per retransmit response |
| `lmb.md.retransmitMaxConnections` | 64 | concurrent TCP connections |
| `lmb.md.retransmitIdleTimeoutMs` | 60000 | idle TCP connections are closed |
| `lmb.md.grpcRingBytes` | 1048576 | hand-off ring from the publisher thread to the gRPC fan-out |
| `lmb.md.grpcMaxQueued` | 20000 | per-subscriber queue before the slow-consumer disconnect |
| `lmb.md.grpcMaxSubscribers` | 1000 | concurrent subscriptions |
| `lmb.md.pollLimit` | 256 | events per shard per poll; also the "full poll" threshold for catch-up |
| `lmb.md.suppressCatchUp` | true | keep the start-up backlog off UDP |
| `lmb.md.checkpointIntervalMs` | 5000 | checkpoint period |

Event stream client settings (`lmb.client.*`, `lmb.events.*`) belong to cluster-client; see docs/modules/cluster.md.

Ports in tests: 34000-34999 only (`TestSupport.ports()` hands out blocks of 10).

## Metrics

Prefix `lmb_marketdata_`: `events_total`, `messages_total`, `packets_total{channel}`, `sent_bytes_total`,
`send_errors_total`, `snapshots_total`, `heartbeats_total`, `suppressed_messages_total`,
`retransmit_requests_total{status}`, `retransmit_messages_total`, `retransmit_rejected_connections_total`,
`grpc_subscribers`, `grpc_slow_consumer_disconnects_total`, `grpc_updates_total`, `grpc_backpressure_spins_total`,
`symbols`, `last_event_seq{shard}`, and quantile gauges (p50, p99, p999, max) `receive_to_send_ns` (event received
to datagram sent) and `event_to_publish_ns` (cluster timestamp to send; includes clock skew between hosts).

## Tests

`./mvnw -q -pl marketdata test` (about 15 s): BookBuilderTest 5, FeedRecoveryTest 7, GrpcSubscribeTest 5,
CheckpointRestartTest 3, TwoPublishersTest 2, PublisherMainTest 3. They run against `testkit`'s `InMemoryVenue` with
the real `matcher-core` state machine; `GatedVenue` (test) imitates the cluster client's connecting, redelivery and
loss behaviour.

## Known gaps

- Not tested against a real cluster inside this module (that would make it depend on cluster-node). A manual run of
  `MarketDataPublisherMain` with the production cluster-client and no cluster up connects, retries members, logs
  "waiting" and stops cleanly on SIGTERM; end-to-end runs are left to the system tests.
- If a shard's retained event stream no longer starts at eventSeq 1 (and there is no checkpoint), the publisher
  stops with an event gap rather than start mid-stream: its md_seq would differ from the other feed's. Recovery
  needs a checkpoint from the other instance or a new `sessionId`. There is no automatic re-sync from a peer.
- A cluster that was wiped while the checkpoint survived (event streams restart at 1 below the checkpoint) is not
  detected; the publisher waits for eventSeqs it already has. Wipe `lmb.data.dir/marketdata` with the cluster.
- The retransmission history is not persisted; after a restart older ranges are `TOO_OLD` (snapshot recovery).
- `sessionId` is static configuration; nothing changes it per trading day.
- Catch-up suppression is decided by poll size, so up to `pollLimit` events of a backlog are multicast, and a
  backlog after a later reconnect (not at start-up) is multicast at full speed.
- Metrics gauges `symbols` read the builder from the scrape thread without synchronisation (diagnostic only).
