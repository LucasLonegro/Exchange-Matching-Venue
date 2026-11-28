# feeds

External venue feeds and the NBBO consolidator (ARCHITECTURE.md 5.3). Package `io.lmb.feeds`,
artifact `lmb-feeds`, depends on `lmb-common` only.

Two processes:

| Process | Main class | Local instances | Ready line |
|---|---|---|---|
| Feed simulator | `io.lmb.feeds.FeedSimulatorMain` | 1 (`feedsim-0`) | `LMB_READY feedsim <i>` once the UDP socket and the metrics endpoint are up |
| NBBO consolidator | `io.lmb.feeds.ConsolidatorMain` | 2 (`consolidator-<i>`) | `LMB_READY consolidator <i>` once the feed sockets and metrics are up **and** every shard's `CommandClient.isConnected()` (checked on the duty thread; logs every 5 s while waiting) |

Both read `Topology.load()` and `-Dlmb.instance.id`, stop cleanly on SIGTERM and exit with status 1 if
their duty thread fails.

## How feeds relates to marketdata

None at runtime; they sit on opposite sides of the shards.

- **feeds is input to the shards.** Quotes from other (simulated) venues -> NBBO -> reference price ->
  `ReferencePrice` command to the owning shard via `VenueClients.commandClient(shard)`. The shard
  applies it (only if `sourceTsNs` is newer than the last applied) and emits `ReferencePriceApplied`
  with the new price band.
- **marketdata is output from the shards.** It reads the shards' event streams and publishes this
  venue's own book. It never sees the external venues' quotes. The feeds module reads no event stream.

## Data flow

```
FeedSimulator --UDP A+B per venue--> UdpVenueFeedHandler (LineArbitrator per venue)
   --NormalizedQuote / heartbeat / UP|GAP|STALE--> NbboConsolidator --ReferencePrice--> shard CommandClient
```

- `FeedSimulator`: N venues quote round robin over all symbols around one shared random-walk fair
  price per symbol (start: the directory's `referencePrice`, on tick), each venue with its own
  offset and 1-2 tick half spread. Deterministic per seed. Every packet goes on line A and line B to
  every target; loss can be injected per line.
- `VenueFeedHandler` SPI (`ServiceLoader`, chosen by `lmb.feeds.handler`, default `udp-sim` =
  `UdpVenueFeedHandler`): two non-blocking DatagramChannels per venue, polled on the duty thread.
- `LineArbitrator` (per venue): A/B dedupe; a packet ahead of the next sequence is held (64 slots)
  for the other line; after `gapTimeoutNs` (or a full buffer) the hole is declared a GAP (every
  expired hole in one check) and later copies are dropped; a session change reports GAP and restarts
  at seq 1; no data for `silenceTimeoutNs` reports STALE.
- `NbboConsolidator`: a venue contributes while UP and `now - min(venueTs, rxTs)` of its newest
  in-sequence data <= `lmb.nbbo.staleNs`; a GAP voids its quotes until requoted. No publish with fewer
  than `minVenues`, one-sided or crossed NBBO (locked is published). Reference = mid rounded half up
  to tick; suppressed if within `minChangeTicks`; at most one per symbol per `minIntervalNs` (latest
  wins); back pressure retries next cycle. `sourceTsNs` = newest venue time seen, strictly increasing
  per symbol; `sourceId` = consolidator instance.
- `DutyLoop`: one thread per process; idles with a backoff that parks at most 1 ms.

## Configuration

All keys come from the topology file (or `-D`).

| Key | Default | Meaning |
|---|---|---|
| `lmb.symbols.url`, `.token` | every control plane instance; `lmb-local-symbols` | the control plane's symbol directory (`GET /internal/v1/symbols`), read once at start (cached on disk); a symbol listed later gets an NBBO after a restart |
| `lmb.consolidators` | 2 | consolidator instances (also the simulator's default targets) |
| `lmb.feeds.venues` | 4 (1..10) | simulated venues |
| `lmb.feeds.handler` | `udp-sim` | `VenueFeedHandler` provider name |
| `lmb.feeds.gapTimeoutNs` | 2 000 000 | how long a hole on both lines is waited for |
| `lmb.feeds.silenceTimeoutNs` | = `lmb.nbbo.staleNs` | no packets for this long: venue STALE |
| `lmb.consolidator.<i>.feedHost` | 127.0.0.1 | bind address of consolidator i's feed sockets |
| `lmb.consolidator.<i>.feedBasePort` | 9660 + 20*i | first feed port of consolidator i |
| `lmb.consolidator.<i>.metricsPort` | 9600 + i | consolidator metrics (Topology) |
| `lmb.nbbo.staleNs` | 50 000 000 (local) | freshness limit (production 5 ms) |
| `lmb.nbbo.minVenues` | 2 | fewest fresh venues for a reference price |
| `lmb.nbbo.minIntervalNs` | 10 000 000 | per-symbol throttle |
| `lmb.nbbo.minChangeTicks` | 1 | smaller moves are suppressed |
| `lmb.feedsim.targets` | one per consolidator (feedHost:feedBasePort) | explicit `host:basePort,...` |
| `lmb.feedsim.metricsPort` | 9650 | simulator metrics (Topology) |
| `lmb.feedsim.quotesPerSecond` | 200 | per venue |
| `lmb.feedsim.heartbeatNs` | 10 000 000 | heartbeat after this much silence (keep well under staleNs) |
| `lmb.feedsim.dropA` / `dropB` | 0 | injected loss probability per line |
| `lmb.feedsim.seed` | 42 | random seed |

## Ports

- Feed: UDP port = `feedBasePort + 2*venue + line` (line A = 0, B = 1). With 4 venues consolidator 0
  listens on 9660-9667, consolidator 1 on 9680-9687. Sockets do not set SO_REUSEADDR, so a second
  process binding the same port fails instead of silently sharing it.
- Metrics: consolidator `9600 + i`, simulator `9650` (HTTP `/metrics`, all interfaces).
- Tests use 35000-35999 (UdpVenueFeedHandlerTest 35000-35023, MainsTest 35200-35259).

## Wire format (simulated venues)

`SimFeedProtocol`, modelled on MoldUDP64, little endian, one datagram = one packet. `count = 0` is a
heartbeat whose `firstSeq` is the next sequence the venue will assign (reveals tail loss).

```
Packet header (32 bytes)
  0  u16  magic      0x4C46
  2  u8   version    1
  3  u8   line       0 = A, 1 = B (informational; the receiving socket identifies the line)
  4  u16  venueId
  6  u16  count      messages in this packet, 0..28
  8  i64  session    venue start time (epoch ns); larger = venue restarted
 16  i64  firstSeq   sequence of the first message (starts at 1 per session)
 24  i64  sendTsNs   venue clock at send
Message (48 bytes), repeated count times
  0  u32  symbolId
  4  u16  type       1 = QUOTE (others skipped, but consume a sequence)
  6  u16  flags      0
  8  i64  venueTsNs
 16  i64  bidPrice   cents, Long.MIN_VALUE = none
 24  i64  bidSize
 32  i64  askPrice   cents, Long.MIN_VALUE = none
 40  i64  askSize
```

Output to the shards: the SBE `ReferencePrice` command (`symbolId, nbbBid, nbbOffer, referencePrice,
sourceTsNs, sourceId, venueCount`) from the frozen schema.

## Metrics

Consolidator (`:9600+i`):
- per venue: `lmb_feeds_messages_total`, `lmb_feeds_gaps_total`, `lmb_feeds_venue_gap_messages_total`,
  `lmb_feeds_venue_stale` (0/1), `lmb_feeds_venue_stale_transitions_total`, `lmb_feeds_venue_data_age_seconds`,
  `lmb_feeds_arbitration_duplicates_total`, `lmb_feeds_malformed_packets_total`
- per venue and line: `lmb_feeds_line_packets_total`, `lmb_feeds_line_missed_messages_total`,
  `lmb_feeds_line_silence_seconds`
- `lmb_feeds_reference_prices_submitted_total`, `lmb_feeds_reference_prices_suppressed_total{reason=min_change|superseded|unavailable}`,
  `lmb_feeds_command_back_pressure_total`, `lmb_feeds_nbbo_crossed_total`, `lmb_feeds_nbbo_locked_total`,
  `lmb_feeds_unknown_symbol_quotes_total`, `lmb_feeds_invalid_quotes_total`
- `lmb_feeds_quote_to_command_latency_ns{quantile=p50|p99|p999|max}` (receive of the triggering quote
  to accepted offer)

Simulator (`:9650`): `lmb_feeds_sim_packets_total{venue,line}`, `lmb_feeds_sim_quotes_total{venue}`,
`lmb_feeds_sim_dropped_packets_total{line}`, `lmb_feeds_sim_send_failures_total`.

## Running

```
java -Dlmb.topology=deploy/local/topology.properties -Dlmb.instance.id=0 io.lmb.feeds.ConsolidatorMain
java -Dlmb.topology=deploy/local/topology.properties -Dlmb.instance.id=0 io.lmb.feeds.FeedSimulatorMain
```

Under `scripts/lmb-run` the names are `consolidator-<i>` and `feedsim-0` (64 MB heaps). The
consolidator needs `lmb-cluster-client` on the classpath (behind `VenueClients.connect`) and running
shards before it prints READY; the simulator does not depend on anything and can start in any order
(packets sent before a consolidator binds are simply lost; it catches up within one round of symbols).

## Tests

`./mvnw -q -pl feeds test`: 43 tests (LineArbitrator 11, NbboConsolidator 15, FeedSimulator 4,
SimFeedProtocol 4, Config 4, UdpVenueFeedHandler 3, LatencyGauges 1, Mains 1). `MainsTest` runs both
mains in one JVM over loopback with fake shard command clients: READY waits for the disconnected shard,
every symbol gets a ReferencePrice on its owning shard near the directory price, both `/metrics`
endpoints answer, and both duty loops stop without failure.

## Known gaps

- Never run under `scripts/lmb-run` against real shards (needs dist rebuilt and a cluster).
- No periodic re-send of an unchanged reference price: `CommandClient` is at-most-once, so one lost
  in a leader failover stays lost until the price moves (the second consolidator usually covers it).
- Metrics HTTP servers bind all interfaces (no host key).
- Only the simulated `udp-sim` handler exists; no real venue/ticker-plant adapter, no multicast.
- The simulator's quotes are synthetic (random walk); there is no replay of recorded market data.
