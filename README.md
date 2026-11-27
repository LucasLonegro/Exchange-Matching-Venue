# LMB trading venue

A low-latency equities venue in Java 21 on Aeron. It supports limit and market orders, day-only, with a
$0.01 tick. Symbols are split across **shards**. Each shard is a replicated, deterministic state machine:
an Aeron Cluster (Raft) log feeds the matcher, and the matcher produces an event stream. Every other
service either sends commands into a shard's log or reads its event stream.

```
participants -> gateways -> [ Raft log -> risk + matcher -> event stream ] -> gateways (exec reports)
                               ^      (one shard, Aeron Cluster)        |-> market data publishers
 feed handlers -> NBBO consolidator                                     |-> post-trade (drop copy, Kafka, archive)
 control plane (reference data, schedule, risk limits)                  +-> shadow replayer (verifies the log)
```

Key properties:
- **The log is the only source of truth.** Orders, cancels, reference prices, halts, auctions, risk
  limits and timers are all commands in the log.
- **The matcher is deterministic.** Replaying a log reproduces the event stream byte for byte. Each log
  entry ends with a SHA-256 chain hash, and the shadow replayer checks it.
- **Output waits for commit.** No ack, fill or market data leaves the venue until a quorum has made it
  durable.
- **Events are delivered effectively once.** Every event has a dense per-shard `eventSeq`, and
  consumers skip anything they have already processed.

Stack: Aeron Cluster and Archive 1.54, Agrona, SBE, Artio (FIX 4.4), grpc-java, HdrHistogram,
Prometheus client, built with Maven.

## Quick start (local)

You need Linux with about 3 GB of free RAM, JDK 21 and Maven 3.9. Build through `./mvnw`: it uses
`.tools/jdk` and `.tools/maven` when they exist and the `mvn` on your `PATH` otherwise. Docker is not
needed.

```bash
./mvnw -DskipTests install            # build every module and the distribution (dist/target/lmb/lib)
scripts/local-up.sh                   # start the local venue (~20-60 s)
scripts/local-status.sh --leaders     # what is running, memory used, the leader of each shard
scripts/lmb-run io.lmb.tools.SmokeTestMain --topology data/local/run/topology.properties \
    --protocol grpc --gateway 0 --pairs 5   # send crossing orders, check acks and fills
scripts/local-logs.sh gateway-0 -f    # follow one process's log
scripts/local-down.sh                 # stop everything (add --wipe to delete data/local)
```

The default local profile has 13 JVMs, with two of everything that replicates:

| Process | Count |
|---|---|
| Cluster nodes (matcher) | 2 shards × 2 members |
| Order gateways (gRPC and FIX) | 2 |
| Market data publishers (feeds A and B) | 2 |
| Control plane | 2 |
| Post-trade | 1 |
| Shadow replayer | 1 per shard |

Variants:
- `scripts/local-up.sh --minimal` starts one shard with one member and one of each service (5 JVMs).
- `scripts/local-up.sh --members 3` runs 3 members per shard. With only 2 members a shard cannot
  survive losing one, so use this for failover drills (`scripts/kill-leader.sh <shard>`).
- `scripts/local-up.sh --dry-run` prints every command line and starts nothing.
- `scripts/local-up.sh --help` lists all options.

Main local endpoints:

| Service | Ports |
|---|---|
| Gateway | gRPC `9100+i`, FIX `9200+i`, metrics `9300+i` |
| Market data | retransmit `9500+i`, gRPC `9510+i`, metrics `9520+i` |
| Control plane | admin REST `9700+i` (header `X-LMB-Admin-Token: lmb-local-admin`), metrics `9710+i` |
| Post-trade | drop copy FIX `9800`, metrics `9810` |
| Replayer | metrics `9900+shard` |

`i` is the instance number. Logs are in `data/local/logs/<name>.log`.

## Tests

```bash
./mvnw test                           # unit and integration tests of every module
./mvnw -q -pl matcher-core test       # one module
scripts/smoke-test.sh --protocol both # end to end over gRPC and FIX on a private venue (ports 39000-39999)
scripts/smoke-test.sh --failover      # 3 members per shard; kills each leader while orders flow
scripts/load-test.sh                  # latency percentiles at 1000 orders/s for 30 s
```

## Repository layout

| Path | What it is |
|---|---|
| `schemas/` | SBE schema for commands and events (`venue.xml`) |
| `api-proto/` | gRPC API: order entry and market data |
| `common/`, `testkit/` | Shared interfaces, topology and config; an in-memory venue for tests |
| `matcher-core/` | The state machine: books, matching, risk, auctions, snapshots |
| `cluster-client/`, `cluster-node/` | Aeron Cluster client and node; event stream recording and serving; DR backup |
| `fix-codecs/`, `gateway/` | FIX 4.4 codecs (Artio); the order gateway (gRPC and FIX) |
| `marketdata/` | Market data publisher (UDP A/B, snapshots, TCP retransmit, gRPC) and client |
| `feeds/` | Feed handler SPI, simulated external venues, NBBO consolidator (not started by default: the venue sets its own reference prices; `--consolidators N --with-feedsim`) |
| `controlplane/` | Reference data, limits, admin REST, scheduler, risk-budget allocator |
| `posttrade/` | FIX drop copy, Kafka bridge, WORM archiver, ClickHouse loader |
| `replayer/` | Shadow replayer (re-runs the log and compares hashes), backtester, log verifier |
| `tools/` | Smoke test, load generator, CLI clients |
| `dist/` | Collects every jar; `scripts/lmb-run <main-class>` runs a service from it |
| `scripts/` | Local up/down/status/logs, kill-leader, smoke and load tests |
| `deploy/` | Local config and docker-compose for infrastructure, production site and topology, Docker image, Kubernetes manifests, systemd launcher, monitoring and SLOs |

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is the contract: module ownership, command and event
  semantics, matcher behaviour, and the requirements for each service.
- [docs/operations.md](docs/operations.md) covers running the system: local profiles and configuration,
  production placement (bare metal and Kubernetes), monitoring, runbooks, DR, and capacity.
- [docs/modules/](docs/modules/) has one design note per module, including its known gaps.

## Status

The local profile works end to end. Orders are acked and filled through both gateways over gRPC and
FIX. Market data is published, and the replayers report no divergence. Production artefacts exist
(Docker image, Kubernetes manifests, systemd launcher, monitoring), but only their syntax and
structure have been checked; they have not been deployed. Open items include firm credentials (the
gateways still read them from a CSV file), the bare-metal host-tuning files, a matcher throughput
benchmark and FIX cancel-on-disconnect. Each module's note in [docs/modules/](docs/modules/) lists
the rest.
