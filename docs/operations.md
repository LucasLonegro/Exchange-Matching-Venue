# Operations

How the LMB venue is run: the local copy on a laptop, the production shape, monitoring, runbooks,
capacity planning, and how to test latency and correctness under failure. The contract is
`docs/ARCHITECTURE.md`; this file never overrides it.

> **Status (2026-11-16).** The scripts and manifests were written while the service modules were being
> implemented. What has been verified, and how, is listed in [section 9](#9-what-is-verified-and-what-is-not).
> Metric names and some configuration keys still need reconciling with the module docs; they are kept in
> one place each (`deploy/monitoring/metric-names.env`, `deploy/local/full.env`).

## Contents
1. [Local system](#1-local-system)
2. [Production shape](#2-production-shape)
3. [Monitoring and SLOs](#3-monitoring-and-slos)
4. [Runbooks](#4-runbooks)
5. [DR activation and the honest RPO](#5-dr-activation-and-the-honest-rpo)
6. [Capacity planning](#6-capacity-planning)
7. [Testing latency and correctness under failure](#7-testing-latency-and-correctness-under-failure)
8. [Files](#8-files)
9. [What is verified and what is not](#9-what-is-verified-and-what-is-not)
10. [Contract change requests and open items](#10-contract-change-requests-and-open-items)

---

## 1. Local system

### 1.1 Quick start

```bash
./mvnw -DskipTests install          # builds every module and dist/target/lmb/lib
scripts/local-up.sh                  # 2 shards x 2 members and everything else, ~30-60 s
scripts/local-status.sh --leaders    # what runs, RSS, who leads each shard
scripts/local-logs.sh gateway-0 -f   # one log; 'node-*' -f follows several, prefixed
scripts/local-down.sh                # stop (data kept; --wipe deletes data/local)
```

No Docker is needed. Every process is `scripts/lmb-run <main-class>` from the distribution, started
detached, with its log in `data/local/logs/<name>.log` and pid in `data/local/pids/<name>.pid`.

| Tier (start order) | Processes (default profile) | Ready when |
|---|---|---|
| nodes | `node-<shard>-<member>`: 2 x 2 `ClusterNodeMain` | `LMB_READY cluster-node S-M`, then a leader per shard |
| controlplane | `controlplane-0/1` | `LMB_READY controlplane <id>` |
| feeds | `consolidator-0/1`, `feedsim-0` | `LMB_READY ...` |
| marketdata | `marketdata-0` (feed A), `marketdata-1` (feed B) | |
| gateways | `gateway-0/1` | `LMB_READY gateway <id>` (after the event streams caught up) |
| posttrade | `posttrade-0` | |
| ledger | `ledger-0` | `LMB_READY ledger 0` (caught up with every shard) |
| replayers | `replayer-<shard>` | |

Each tier is started in parallel and must print its `LMB_READY` line within `--timeout` (120 s). If a
process exits first or times out, `local-up.sh` prints the last 40 lines of its log, stops everything it
started and exits 1 (`--keep-on-failure` leaves the system up for inspection). Services whose main class
is not in the distribution yet are **skipped with a warning** (modules land at different times); a
missing `ClusterNodeMain` is fatal.

The default profile is 15 JVMs. Heaps are small (node 160 MB, control plane 128 MB, gateway, market data,
post-trade, ledger and replayer 96 MB, consolidator, simulator and access 64 MB) and `deploy/local/common.env` caps
metaspace, code cache, direct memory and thread stacks, so the worst case is about 3 GB RSS and the
typical figure is lower; the pre-flight check compares the estimate with `MemAvailable` and refuses to
start above it (`--force` overrides). Aeron runs in SHARED threading with backoff idle strategies and
256 KB-1 MB term buffers, so an idle system uses little CPU.

### 1.2 Profiles and options

| Need | Command |
|---|---|
| Minimal (1 shard x 1 member, one of each, no post-trade or ledger; 7 JVMs) | `scripts/local-up.sh --minimal` |
| One shard, otherwise default | `scripts/local-up.sh --shards 1` (symbols are spread over the shards) |
| Failover drills (3 members tolerate one failure) | `scripts/local-up.sh --members 3` |
| Without some services | `--no-posttrade --no-ledger --no-replayer --no-feedsim --skip marketdata` |
| With PostgreSQL, Redpanda, ClickHouse, MinIO, Prometheus, Grafana, Envoy | `docker compose -f deploy/local/docker-compose.yml up -d` then `scripts/local-up.sh --full` |
| See exactly what would run | `scripts/local-up.sh --dry-run` (prints the generated topology and every resolved command line) |
| Relaunch one process | `scripts/local-up.sh --restart gateway-1` |
| Rehearse the scripts without JVMs | `LMB_RUNNER=scripts/testing/stub-lmb-run LMB_LOCAL_DATA=/tmp/lmb scripts/local-up.sh` |

With 2 members per shard (the default) the shard replicates but tolerates **no** failure: losing either
member loses the majority and the shard stops accepting commands until the member returns. That is the
contract's local profile; use `--members 3` to see a real failover.

### 1.3 Configuration of local processes

`local-up.sh` generates `data/local/run/topology.properties` (the base `deploy/local/topology.properties`
plus the profile, with absolute reference data paths) and one launch script per process,
`data/local/run/cmd/<name>.sh`, which is what `--restart` and `kill-leader.sh --restart-after` reuse.
Every process gets:

```
-Dlmb.topology=data/local/run/topology.properties  -Dlmb.data.dir=data/local
-Dlmb.shard.id / -Dlmb.member.id (nodes)  or  -Dlmb.instance.id (everything else; replayers also lmb.shard.id)
-Daeron.dir=-Dlmb.aeron.dir=/dev/shm/lmb-local-<user>-<tag>/<name>     (removed by local-down.sh)
-Dlmb.local.tag=lmb-local-<tag>                                       (finds strays; tag = checksum of the data dir)
```

To add configuration the scripts do not know about, without editing them (later wins):

1. `deploy/local/common.env` (all processes), 2. `deploy/local/full.env` (only with `--full`),
3. `deploy/local/<service>.env` (`node`, `gateway`, `marketdata`, `consolidator`, `feedsim`,
`controlplane`, `posttrade`, `ledger`, `replayer`), 4. `deploy/local/<name>.env` (`node-0-1.env`, `gateway-1.env`),
5. command line: `--env gateway:LMB_GATEWAY_THROTTLE_RATEPERSEC=5000`, `--jvm node-0-0:-Dlmb.cluster.idle=noop`,
`--heap node=256m`.

The env files are bash, sourced with `set -a`. Any `LMB_<KEY>` variable becomes configuration key
`lmb.<key>` through `io.lmb.common.Config` (upper-cased, dots to underscores), so a key that a module adds
tomorrow already works. `LMB_HEAP`, `LMB_JAVA_OPTS` (append: `LMB_JAVA_OPTS="$LMB_JAVA_OPTS -X..."`),
`LMB_COMMON_ARGS` and `LMB_EXTRA_ARGS` (extra `-D` arguments) control the JVM.

### 1.4 Local endpoints

| What | Where (defaults from `io.lmb.common.Topology`) |
|---|---|
| Gateway gRPC / FIX / metrics | 9100+i / 9200+i / 9300+i |
| Market data retransmit / gRPC / metrics | 9500+i / 9510+i / 9520+i |
| Consolidator metrics, feed simulator metrics | 9600+i, 9650 |
| Control plane admin REST / metrics | 9700+i / 9710+i (header `X-LMB-Admin-Token`, default token `lmb-local-admin`) |
| Access operators' REST / directory / metrics | 9750+i (loopback) / 9760+i / 9770+i (`Authorization: Bearer lmb-local-admin`; directory token `lmb-local-access`) |
| Post-trade drop copy FIX / metrics | 9800 / 9810 |
| Ledger metrics | 9820 |
| Replayer metrics | 9900 + shard (local workaround, see section 10) |
| Node ports (UDP, Aeron) | 20000 + 1000*shard + 100*member + offset; metrics on offset 9 |
| With the compose profile | Prometheus :9090, Grafana :3000, Envoy REST :8080 (admin :15000), PostgreSQL :5432, Redpanda :19092, ClickHouse :8123, MinIO :9002 (console :9003) |

### 1.5 Troubleshooting

- *"already running"*: `scripts/local-status.sh`, then `scripts/local-down.sh`.
- *A tier times out*: `scripts/local-logs.sh --errors`; the machine may just be slow (`--timeout 300`).
- *Port in use*: the pre-flight check names it; another run (other checkout) or a stale process.
  `scripts/local-down.sh` stops strays carrying this data dir's tag only.
- *"could not determine the leader"*: the nodes printed `LMB_READY` but neither `lmb_cluster_role` on the
  metrics port nor `ClusterTool list-members` answered. Check `metric-names.env`, or set
  `LMB_CLUSTER_DIR_TEMPLATE='{data}/cluster-node/shard-{shard}-member-{member}/cluster'`.
- *Clean slate*: `scripts/local-down.sh --wipe` (Raft logs, archives, H2 database, logs).

---

## 2. Production shape

### 2.1 Placement

```
                   region P (primary)                                          region D (DR)
  AZ a (leaders)              AZ b                    AZ c                     backup hosts
  shard s member 0  <-Raft->  shard s member 1  <->   shard s member 2   ~~~>  ClusterBackup shard s
  gateways, MD A+B,           MD B (opt.), control    replayers, post-trade    (async, RPO > 0)
  consolidators, ctl plane 0  plane 1, post-trade
```

- 16-32 shards x 3 members, one member per AZ. A shard tolerates one member (or one AZ) failure.
- **Leaders in AZ a**: the gateways, market data publishers and consolidators sit in AZ a with the
  leaders, so ingress -> leader is intra-AZ. Commit still needs one follower to append, so every ack
  carries one round trip to the nearest other AZ. In a public cloud that is 0.3-1.5 ms; serious venues
  therefore run the "AZs" as separate halls or racks of one co-location site (tens of microseconds
  apart) and use the remote region only for DR. Aeron Cluster has no leader-affinity setting, so leader
  placement is steered operationally (runbook 4.4) and alerted on (`LmbLeaderOutsidePreferredZone`).
- Member identity is stable: member m of shard s always has the same host/DNS name and disk; the
  topology file maps `lmb.shard.S.member.M.host` to it, and the optional `...zone` key (ignored by
  `Topology`, used by monitoring) records its AZ.
- DR: one `ClusterBackupMain` per shard in region D follows the primary's log asynchronously
  (`LMB_READY cluster-backup <shard>`, data in `<lmb.data.dir>/cluster-backup/shard-S`).

### 2.2 Bare metal (the matcher's natural home): `deploy/baremetal/`

The shard nodes run on dedicated bare-metal hosts; see `deploy/baremetal/README.md` for the host build.
Summary of what it encodes:

- **CPU isolation**: `isolcpus=managed_irq,domain,<cores> nohz_full=<cores> rcu_nocbs=<cores>`,
  `irqaffinity=<housekeeping>`, C-states limited, performance governor (`host-tuning.sh --print-cmdline`,
  `tuned/lmb-latency`). The Aeron C media driver (`lmb-aeronmd@.service`) pins its conductor, sender and
  receiver threads to isolated cores; `deploy/bin/lmb-pin-threads` pins the JVM's busy threads
  (consensus module, clustered service = the matcher, archive recorder) by thread name, leaving GC and
  JIT threads on housekeeping cores.
- **Memory**: explicit hugepages for the heap (`-XX:+UseLargePages`), THP `madvise`, swap off,
  `vm.stat_interval` raised, NUMA balancing off; matcher, driver and NIC on one NUMA node.
- **Network**: IRQs of the trading NIC on housekeeping cores of the NIC's NUMA node, interrupt
  coalescing off, large rings, socket buffers sized for Aeron (`net.core.rmem_max`/`wmem_max` 16 MB),
  busy polling. Kernel bypass (ef_vi/Onload, VMA, or Aeron's DPDK driver) is an optional layer on top.
- **Storage**: NVMe with **power-loss protection** (PLP), XFS, `noatime`, scheduler `none`. With PLP the
  drive's write cache is non-volatile, so syncing the archive (`lmb.archive.fileSyncLevel`,
  `lmb.cluster.fileSyncLevel`) costs microseconds instead of milliseconds. Raft commit in Aeron is
  "appended on a majority", not "fsynced on a majority": with sync level 0 a simultaneous power loss of
  two AZs can lose acknowledged data. Production runs sync level 1 on PLP drives.
- **Time**: linuxptp `ptp4l` on the PTP NIC (hardware timestamping) from GPS-disciplined grandmasters
  (two per site, `ts2phc` config for a Linux-based grandmaster with a GNSS card), `phc2sys` disciplines
  CLOCK_REALTIME; `lmb-ptp-exporter` publishes the offset. Cluster timestamps come from the leader's
  clock, so order timestamps are only as good as PTP: alert at 10 us, page at 100 us (MiFID II RTS 25
  limit for HFT activity).
- **Processes**: one systemd template `lmb@<service>-<id>.service` running `deploy/bin/lmb-service`,
  bound to its own `lmb-aeronmd@<service>-<id>.service`, grouped in `lmb.target`. Matcher nodes restart
  on failure at most 3 times in 10 minutes: a deterministic bug crashes every member on the same entry
  and must not become a restart loop.
- **Internet edge (gateways)**: gRPC TLS in the gateway (`/etc/lmb/tls/gateway.crt`, `.key` in PKCS#8);
  FIX TLS by `lmb-fix-tls.service` (Envoy, `deploy/baremetal/envoy/fix-tls.yaml`) on the FIX port, with
  Artio on `127.0.0.1:9201` behind it (the rendered topology sets `lmb.gateway.fix.bindHost` and
  `.listenPort`). Firewall: only 9100 (gRPC) and 9200 (FIX TLS) from the Internet; 9201, the access
  service, the control plane and every metrics port from the venue's networks only.
- **Secrets**: tokens go in root-owned `0640 root:lmb` env files, never in the topology:
  `/etc/lmb/gateway.env` and `/etc/lmb/posttrade.env` (`LMB_GATEWAY_ACCESS_TOKEN`,
  `LMB_POSTTRADE_ACCESS_TOKEN`), `/etc/lmb/access.env` (`LMB_ACCESS_SERVICETOKEN`, `LMB_ACCESS_DB_*`),
  `/etc/lmb/controlplane.env` (`LMB_DB_*`). Operator token hashes in `/etc/lmb/operators/{controlplane,access}`
  (`<name> <sha256>` lines, see 4.8). Production topologies set `allowDefaultTokens=false`, so a missing
  file stops the service at start rather than running with the local tokens.

### 2.3 Kubernetes: `deploy/k8s/`

`deploy/k8s/render.py` renders plain manifests from `deploy/k8s/values-prod.json` (no Helm or kustomize
needed; output in `deploy/k8s/rendered/`). Per shard: a headless Service (`publishNotReadyAddresses`,
Raft peers must resolve each other before they are "ready") and a StatefulSet with 3 replicas,
`podManagementPolicy: Parallel`, `updateStrategy: OnDelete` (never let Kubernetes roll a shard; runbook
4.3), topology spread over `topology.kubernetes.io/zone` (maxSkew 1, DoNotSchedule) plus hostname
anti-affinity, local NVMe PersistentVolumes (`local-nvme` StorageClass, `WaitForFirstConsumer`,
`Retain`), Guaranteed QoS (requests = limits, integer CPUs, hugepages) for exclusive cores with the
kubelet's static CPU manager and single-NUMA-node topology manager (`kubelet-config-matcher.yaml`), and
the C media driver as a native sidecar sharing a memory-backed `/dev/shm`. No liveness probe on matcher
pods: killing a busy leader for a slow probe causes the failover it was meant to prevent.

**Access, TLS and policies.** The access service is a 2-replica StatefulSet (`lmb-access`, pool general,
pod network) behind the ClusterIP Service `lmb-access-directory:9760` that gateways and post-trade poll;
its operators' API (9750) listens on loopback, so operators use `kubectl -n lmb port-forward
pod/lmb-access-0 9750`. Gateways mount `lmb-gateway-tls` for gRPC TLS and run a `fix-tls` Envoy sidecar
(ConfigMap `lmb-fix-tls`) that owns the FIX port 9200 and forwards to Artio on 127.0.0.1:9201. Secrets
(`lmb-secrets`, `lmb-operators`, `lmb-gateway-tls`) are listed in the docstring of `render.py`.
NetworkPolicies: the control plane's REST only from `operatorNamespaces` (and the other control plane
pod), the access directory only from post-trade pods and `nodeCidrs` (the host-network gateways), metrics
only from `monitoringNamespace`.

**Networking trade-off.** The manifests use `hostNetwork: true` for nodes, gateways, market data and
consolidators: no overlay, veth or conntrack in the path (the CNI typically adds 10-30 us and jitter), and
UDP multicast works. Costs: the pod shares the host's network namespace (NetworkPolicy does not apply,
port collisions are prevented by the topology's unique ports; all members of one shard use the same
ports so one pod template fits, and anti-affinity keeps them on different hosts), and DNS names resolve
to node IPs. The realistic alternative is Multus + the SR-IOV device plugin: pods keep the normal pod
network for control traffic and get a VF of the trading NIC (optionally with kernel bypass) for Aeron
and market data. It isolates better, costs NIC/VF management. Control plane and post-trade use the pod
network.

Leader placement and member-to-zone mapping: a per-shard StatefulSet with topology spread guarantees one
member per zone but not which ordinal lands in which zone. The first scheduling binds each member to its
local PV, and therefore to its node and zone, for life; monitoring learns the zone from node labels
(`attach_metadata: node`). If a fixed member-to-zone mapping is required, render one single-replica
StatefulSet per (shard, member) with a zone node-selector instead (`"statefulSetPerMember": true`).

### 2.4 Container image: `deploy/docker/`

`Dockerfile` packages `dist/target/lmb/lib` with `deploy/bin/lmb-service` (the same launcher systemd
uses) on a Temurin 21 JRE, non-root, `tini` as PID 1. `Dockerfile.aeronmd` builds the Aeron C media
driver 1.54.2. Build from the repository root after `./mvnw -DskipTests install`:
`docker build -f deploy/docker/Dockerfile -t lmb:<version> .`

---

## 3. Monitoring and SLOs

Every service serves Prometheus metrics on its topology port. **Raw metric names live only in
`deploy/monitoring/metric-names.env`** (names checked against module source are marked CONFIRMED; the
rest are the deploy layer's reading of the contract and must be reconciled when the module docs land).
`python3 deploy/monitoring/render.py` turns that file into Prometheus recording rules (`lmb:*` series,
the only place raw names appear), alert rules per profile (`slo-prod.env`, `slo-local.env`), a Grafana
dashboard and scrape configurations generated from a topology file. Alerts and the dashboard use only
`lmb:*` series, so a rename is a one-line change plus a re-render.

| SLO (production, bare metal) | Target | Alert |
|---|---|---|
| Ack latency (gateway in -> report out) p50 / p99 / p99.9 | 50 us / 200 us / 1 ms | warn above for 2 min; p99.9 pages after 5 min |
| Matcher service time per command p99 | 20 us | warn |
| Market data publish delay (event -> wire) p99 | 100 us | warn |
| Shard without leader (failover time) | < 2 s | page at 5 s |
| Replay divergence | 0 | page immediately |
| Feed gap rate | < 1e-4 of messages | warn |
| PTP offset | < 10 us | warn 10 us, page 100 us |
| Replication lag (follower behind leader commit) | < 1 MB | warn after 30 s |
| DR backup lag | < 5 s | page after 1 min (it is the RPO) |
| Gateway in-flight queue depth | < 80 % of `lmb.gateway.inflight.max` | warn |
| Matcher utilisation | < 50 % sustained | warn (capacity) |

Quantiles are per process (HdrHistogram over a window); they are aggregated across processes with
`max`, never averaged.

---

## 4. Runbooks

All admin actions go through the control plane REST API (either instance; only the lock holder runs the
scheduler and allocator, but every instance accepts admin calls) with your own operator token:
`X-LMB-Admin-Token: <token>`. The audit (`GET /api/audit`) records the operator the token belongs to;
`X-LMB-Actor` is honoured only on local setups with the single shared token. `CP=http://<controlplane>:9700`.
Teams, members and keys are the access service's (4.8).

```bash
cp() { curl -fsS -H "X-LMB-Admin-Token: $(cat ~/.lmb/controlplane-token)" "$@"; }
```

### 4.1 Start of day

1. T-60 min: hosts healthy (`node_exporter`, PTP offset < 1 us on every matcher host, disks < 60 %).
2. Start (or confirm running) shard nodes; every shard has 3 members and a leader in AZ a
   (`lmb:shard_leaders`, `LmbLeaderOutsidePreferredZone` silent). Steer leaders if needed (4.4).
3. Replayers report zero divergence for the previous day's log (`lmb:replay_divergence`).
4. Control plane: exactly one lock holder (`lmb_controlplane_is_leader` sums to 1). `cp $CP/api/status`,
   `cp $CP/api/shards` (all connected), `cp $CP/api/calendar` shows today's session.
5. Reference data: `cp $CP/api/symbols`, `cp $CP/api/limits`; budget allocation `cp $CP/api/budgets`
   sums to at most each firm's limit.
6. Consolidators: every external venue non-stale; NBBO reference prices flowing per symbol.
7. Market data A and B publishing heartbeats; a test subscriber arbitrates A/B without gaps.
8. Gateways up behind the participant-facing addresses; test firm logs on, sends and cancels an order on
   a test symbol per shard (the drill symbol is halted again afterwards).
9. The scheduler moves symbols PRE_OPEN -> AUCTION_CALL -> CONTINUOUS as cluster timers; watch
   `SymbolStateChanged` on the event stream and the opening uncross per symbol.

### 4.2 End of day

1. The scheduler runs the closing auction and moves symbols to CLOSED (resting orders expire,
   `END_OF_DAY`).
2. Wait for post-trade sink lag 0 on every sink and shard (drop copy, Kafka, WORM, ClickHouse). After the
   DayRollover below, the ledger writes the day's statements once every shard has rolled
   (`ledger_statement_day`, `<statementsDir>/<yyyymmdd>/`).
3. Reconcile: trade count and notional per shard from the event stream = ClickHouse = clearing file;
   drop-copy sequence complete per firm.
4. Take a snapshot on every shard (Aeron `ClusterTool <cluster-dir> snapshot` on the leader, or the
   control plane's day roll which follows it), then `cp -X POST $CP/api/dayroll` (DayRollover: dedupe,
   intraday risk and positions cleared, symbols PRE_OPEN).
5. Replayers finish the day with zero divergence; WORM upload of the day's log segments confirmed (object
   lock retention set). Only then may archive segments older than the latest snapshot be purged locally.
6. Maintenance (OS, JVM, releases) happens after this point, one member at a time (4.3).

### 4.3 Rolling restart / release of a shard

Followers first, leader last, one member at a time, never in market hours unless forced:
stop follower -> upgrade -> start -> wait until it has caught up (`lmb:replication_lag_bytes` ~ 0 and
`lmb_cluster_last_event_seq` equal to the leader's) -> next follower -> then stop the leader (one
election, < 2 s) -> upgrade -> start. On Kubernetes (`OnDelete`): `kubectl delete pod lmb-shard-S-M`
in that order. A release that changes the state machine's behaviour must not be mixed within a shard:
determinism means every member must produce identical output for the same log, so behaviour changes are
released at end of day with all members of a shard restarted on the new version before the next open,
and validated by the replayer against the previous day's log first.

### 4.4 Leader failover and leader placement

Automatic: when the leader dies or is partitioned, the followers elect a new one after
`lmb.cluster.leaderHeartbeatTimeoutMs`; clients (`CommandClient`) follow the redirect, gateways resubmit
in-flight commands with `possResend = TRUE` (deduplicated by `(firmId, clOrdId)`), event stream
consumers fail over by `eventSeq`. Nothing to do except find out why (`LmbShardMemberDown`).

Steering the leader back into AZ a (or a drill): the cluster has no "transfer leadership" call, so stop
the misplaced leader gracefully (`systemctl stop lmb@node-S-M`, or `kubectl delete pod`); one of the two
others wins the election. Repeat until the leader is in AZ a, then start the stopped member (it rejoins
as follower). Do it outside the opening and closing auctions. Locally:
`scripts/kill-leader.sh <shard> [--signal TERM] [--restart-after 5]` with `--members 3`.

### 4.5 Replacing a dead member

A member whose host or disk is gone (the shard runs on 2 of 3 and tolerates nothing more, so this is
urgent):
1. Provision a host in the same AZ with the same identity (same host name / DNS, same ports in the
   topology; on Kubernetes delete the PVC and pod of that ordinal so it is rescheduled with a new local
   PV in the same zone).
2. Start the member with an empty data directory. Aeron Cluster catches it up from the leader (log
   replication; with a snapshot available it first loads the latest snapshot from a peer's archive,
   otherwise it replays the whole log). Watch replication lag drop to 0.
3. Its event stream is materialised by replaying the log (every member records it); confirm
   `lmb_cluster_last_event_seq` matches the leader.
4. If the member id must change instead (different host), that is a membership change: stop, update the
   topology on every member and client, rolling restart. Avoid it; keep identities stable.

### 4.6 Halting a symbol or the market

```bash
cp -X POST $CP/api/symbols/<id>/halt         # HALTED: new orders and replaces rejected, cancels allowed, book kept
cp -X POST $CP/api/symbols/<id>/resume       # back to CONTINUOUS
cp -X POST $CP/api/symbols/<id>/auction/start   # AUCTION_CALL (re-open through an auction, recommended)
cp -X POST $CP/api/symbols/<id>/auction/stop    # uncross and continue
cp -X POST $CP/api/market/halt               # every symbol on every shard
cp -X POST $CP/api/market/resume
cp -X POST $CP/api/firms/<id>/kill           # firm kill switch (new orders rejected, cancels work)
cp -X POST $CP/api/masscancel                # mass cancel (scope in the body, see docs/modules/controlplane.md)
```

A market-wide halt is one `SymbolStateCommand` per symbol in each shard's log: atomic per shard, **not**
across shards (contract rule 5); expect a few hundred microseconds between shards. Confirm with
`cp $CP/api/commands` (every command confirmed by its `SymbolStateChanged`). Re-open after a halt
through an auction, not straight into continuous trading.

### 4.7 Hot-symbol cutover (moving a symbol to another shard)

A symbol whose flow saturates its shard (utilisation alert, ack p99 rising for every symbol on that
shard) moves to a lighter shard. Order books cannot migrate between logs, so the move is a
remove-and-relist:
1. Prefer end of day. Intraday it is an emergency procedure announced to participants.
2. Halt the symbol on the old shard; announce. Then `DELETE /api/symbols/<id>` (state REMOVED: every
   resting order is canceled with `SYMBOL_REMOVED`, participants get the reports).
3. Change the symbol's shard in the control plane database (the only copy of the reference data). Gateways
   pick it up from the symbol directory (`GET /internal/v1/symbols`, polled every `lmb.symbols.pollMs`) and from
   the new shard's `SymbolDefined`; consolidators need a restart.
4. Define it on the new shard (`POST /api/symbols` -> `SymbolDefinition`), start in AUCTION_CALL, let
   participants re-enter orders, uncross, continue.
5. Risk: the firm's per-shard budgets rebalance from `RiskLimitApplied` (allocator). Positions for the
   symbol stay on the old shard for the rest of the day (contract: risk is per shard).
Design the shard map so that the top symbols by message rate are spread out from the start; keep 30 %
headroom per shard (section 6).

### 4.8 Teams, members and keys

`scripts/lmb-team` (python3, no dependencies) talks to the access service's operators' API and, when
registering a team, to the control plane for its risk limits. Production:

```bash
kubectl -n lmb port-forward pod/lmb-access-0 9750 &      # or: ssh -L 9750:127.0.0.1:9750 access-az-a-01
export LMB_ACCESS_TOKEN_FILE=~/.lmb/access-token LMB_CONTROLPLANE_TOKEN_FILE=~/.lmb/controlplane-token
export LMB_CONTROLPLANE_URL=http://<controlplane>:9700
```

| Task | Command |
|---|---|
| New team, one session and key per member | `lmb-team register 400 "Delta Quant" --member alice --member bob --tier restricted --credit-limit <cents> --max-position <shares>` |
| Another member | `lmb-team add-member 400 carol` |
| Panic key (cancel only) / drop copy key | `lmb-team issue-key 400 --scopes CANCEL` / `--scopes DROP_COPY` |
| Planned rotation | `lmb-team rotate <keyId> --grace-hours 24` (both keys work for 24 h, then the old one's connections end) |
| Leaked key | `lmb-team revoke <keyId>`, then `lmb-team issue-key ...`; the connections end within about 3 s |
| Suspend a team's access | `lmb-team disable 400` (ends every session; orders are canceled where cancel-on-disconnect is on: also `cp -X POST $CP/api/firms/400/kill?cancelOrders=true` to be sure) |
| Promote a bot | `lmb-team set-tier 400 bot` or `set-session-tier <sessionId> bot` (effective at the next logon) |
| Who did what | `lmb-team audit 50` |

Keys are printed once: hand them over on the spot (a password manager share, not chat). Only their SHA-256
is stored, so a lost key is revoked and reissued, never recovered.

Operator tokens (both admin APIs): generate a token, keep it in `~/.lmb/`, and add its hash to the
`lmb-operators` secret (k8s) or `/etc/lmb/operators/<service>` (bare metal):

```bash
tok=$(openssl rand -base64 32 | tr -d '=+/' ); echo "$tok" > ~/.lmb/access-token; chmod 600 ~/.lmb/access-token
printf '%s %s\n' "$USER" "$(printf '%s' "$tok" | sha256sum | cut -d' ' -f1)"   # line for the operators file
```

The services read the file at start; restart the access service and control plane instances one at a time to
pick up a new operator (any instance serves either API). The service token (`access-service-token`) is shared by
the access service, gateways and post-trade: rotate it by updating the secret and restarting all three
(gateways keep their snapshot meanwhile, so the order path is unaffected).

---

## 5. DR activation and the honest RPO

`ClusterBackupMain` in region D copies each shard's log asynchronously. **RPO is not zero for a region
loss**: it is the backup lag at the moment of failure, normally well under a second, bounded by the
`LmbDrBackupLagHigh` alert (5 s). Inside the primary region RPO is 0 (quorum commit before any output).
Trades in the lost window may already have been reported to participants (acks and fills leave only
after quorum commit in region P, which is exactly why they can be missing from D): reconciliation
against participants' drop copies and the clearing feed is part of activation, not an afterthought.

Activation (decided by the incident lead, not automatic):
1. Declare: primary region unreachable for > N minutes, or ordered. Halt is implicit (nothing trades).
2. **Fence region P** first: block participant access and inter-region links to P so a partially alive
   primary cannot keep trading (split brain across regions is the worst outcome). Confirm.
3. Stop the backups; record each shard's last `eventSeq` in region D. That is the recovery point; the
   difference from the last `eventSeq` seen by downstream systems (Kafka mirror, drop copy, clearing)
   is the data loss to reconcile.
4. Start 3-member shards in region D from the backup data (topology `deploy/prod/topology-dr.properties`;
   the backup directory holds the recording log and snapshots a node can start from; Aeron
   `ClusterTool seed-recording-log-from-snapshot` where needed). Check every shard elects a leader.
5. Start control plane (PostgreSQL replica promoted in D), consolidators, market data, gateways,
   post-trade; replayers verify the recovered logs.
6. Re-open: market-wide halt state, publish the recovery point and the list of trades that the venue
   no longer has but participants may have received (from drop copies / clearing), cancel all resting
   orders if the incident lead decides so, re-open through auctions.
7. Later, rebuild region P as the new DR site (backups running from D), then switch back in a planned
   window.

RTO target: 30 minutes, dominated by the decision and fencing, not by the software (start-up from
backup data takes a few minutes per shard, shards in parallel).

---

## 6. Capacity planning

Sizes from the SBE schema (block lengths + 8-byte header) and Aeron framing (32-byte data frame header,
32-byte cluster session header on ingress, frames aligned to 32 bytes):

| Item | Bytes |
|---|---|
| NewOrder in the Raft log (58 + 8 + 32 + 32, aligned) | 160 |
| Cancel in the log | 128 |
| Event recording per new resting order (ExecutionReport NEW 192 + BookDelta 128 + CommandApplied 128) | ~450 |
| Event recording per fill (adds Trade 128 + 2 ExecutionReports 384 + BookDelta 128) | ~1,100 |
| **Planning figure, per command, per member, log + events** | **~650** |

Storage per day, per member: `commands per shard per day x 650 B`. Example: a venue handling 600 M
messages/day over 16 shards = 37.5 M per shard -> **~24 GB per member per day**, ~1.2 TB/day for all 48
members, plus ~0.4 TB/day of DR backup copies, plus the WORM archive (compresses ~4:1). Keep 5 trading
days on local NVMe (snapshots every night allow purging older segments after WORM upload): 2 x 1.92 TB
PLP NVMe per matcher host with one member, 3.84 TB if two members share a host.

Throughput per shard: the matcher is one thread. Plan from the measured `lmb_cluster_service_time_ns`
p50 (target < 2 us per command, i.e. > 500 k commands/s of raw capacity) but **size so that the busiest
second of the day keeps the shard below 50 % utilisation and the 1-minute peak below 30 %**: openings and
news produce 10-20x the average rate within milliseconds, and queueing delay grows as rho/(1-rho).
Example: 37.5 M commands/day/shard is ~1,600/s average over 6.5 h, opening peaks ~50 k/s: well inside
one shard. A shard reaching 30 % at the 1-minute peak gets a hot-symbol cutover (4.7) or the shard map is
rebalanced at the next end of day. Replication bandwidth per shard: peak commands/s x 160 B x 2 followers
(log) plus event-stream consumers (~450 B x number of consumers: 2 gateways, 2 publishers, control
plane, post-trade, replayer) - ~200 Mbit/s at 50 k/s, size NICs at 25 GbE and above for burst headroom.

Memory per matcher member: heap sized from symbol count x book depth (preallocated pools) - typically
4-8 GB, plus Aeron term buffers (log channel 3 x term length) and archive page cache; 64 GB hosts.

---

## 7. Testing latency and correctness under failure

**Latency.** Measure open-loop (a fixed send schedule, not "send the next when the last was acked"), or
coordinated omission hides every stall. `io.lmb.tools.LoadGeneratorMain` drives gateways; measure
from the client's send timestamp to the report's receive timestamp with HdrHistogram, and compare with
the gateway's `lmb_gateway_ack_latency_ns` and the node's `lmb_cluster_service_time_ns` to attribute
time to network, gateway, consensus and matcher. Report p50/p99/p99.9/max at several fixed rates up to
the knee; latency at saturation is meaningless. On bare metal, timestamp at the NIC (hardware
timestamps) for the external view. Locally, numbers only compare versions on the same laptop.

**Correctness under failure** (run locally with `--members 3`, in staging on the production shape):

| Fault | How | Invariants checked |
|---|---|---|
| Leader crash | `scripts/kill-leader.sh S` (SIGKILL) under load; `--restart-after` | no acked order lost (every ack's order appears in the event stream after failover), `eventSeq` dense and gap-free per shard, no duplicate fills after `possResend` resubmission, failover < 2 s |
| Leader pause (GC/VM stall) | `kill-leader.sh S --signal STOP`, then `kill -CONT` | old leader steps down, no split-brain output, same invariants |
| Follower crash and catch-up | `kill -9` a follower, restart later | catches up, its event stream identical (replayer and `last_event_seq`) |
| Network partition / loss | bare metal: `iptables`/`tc netem loss 5% delay 1ms` between members | no output without quorum; liveness returns after heal |
| Disk full / slow disk | small filesystem, `dm-delay` | node stops cleanly rather than diverging |
| Clock step | `date -s` / PTP fault on the leader | timestamps monotonic per shard (cluster time), no effect on matching order |
| Gateway crash | kill a gateway mid-flow | clients reconnect to the other gateway, get missed reports (`ShardCursor` resume), cancel-on-disconnect applied |
| Whole-system crash | `scripts/local-down.sh --force`, then `local-up.sh` | recovery from logs; replayer: zero divergence; post-trade resumes from cursors without duplicates downstream |

Every run ends with the replayer comparing `CommandApplied.chainHash` for the whole log, and with a
reconciliation of the event stream against drop copy, Kafka and ClickHouse (counts and hashes). A
failure test that only checks "the system came back" is not a correctness test.

---

## 8. Files

| Path | What |
|---|---|
| `scripts/local-up.sh`, `local-down.sh`, `local-status.sh`, `local-logs.sh`, `kill-leader.sh` | local system |
| `scripts/lib/local-common.sh` | shared shell helpers (topology, ports, processes, leader detection) |
| `scripts/testing/stub-lmb-run` | JVM-free stand-in for `lmb-run` to rehearse the scripts |
| `scripts/validate-deploy.sh` | validates everything in `deploy/` and `scripts/` without starting services |
| `deploy/local/common.env`, `full.env` | local process configuration layers |
| `deploy/local/docker-compose.yml` + `prometheus/ grafana/ envoy/ postgres/ clickhouse/` | optional infrastructure |
| `deploy/monitoring/metric-names.env` | the one place for raw metric names |
| `deploy/monitoring/render.py`, `slo-*.env`, `generated/` | rules, alerts, dashboard, scrape configs |
| `deploy/prod/` | production topology generator, inventory example, rendered primary and DR topologies |
| `deploy/docker/` | container images (distribution, Aeron C media driver) |
| `deploy/k8s/` | Kubernetes renderer, values, kubelet config, rendered manifests |
| `deploy/baremetal/` | systemd units (including `lmb-fix-tls.service`), Envoy FIX TLS config, host tuning, tuned profile, sysctl, linuxptp, notes |
| `scripts/lmb-team` | operator CLI for teams, members, keys and tiers (access service + control plane) |
| `deploy/bin/` | production launcher, thread pinning, PTP exporter (used by bare metal and the image) |

---

## 9. What is verified and what is not

See `scripts/validate-deploy.sh` for the checks themselves.

Verified (2026-11-16): every shell script passes `bash -n` (shellcheck is not installed on the build
machine); the whole local orchestration was rehearsed with `scripts/testing/stub-lmb-run` instead of JVMs:
start order and readiness for 2x2 and 2x3 profiles, leader wait, `kill-leader.sh` with relaunch,
`--restart`, failure of a process before readiness (automatic teardown), readiness timeout, SIGKILL
fallback for a process that ignores SIGTERM, `--wipe`, no leftover processes or `/dev/shm` directories;
leader detection from the Prometheus format with a fake `curl`; `--dry-run` against the real
distribution (correctly reports which main classes are not built yet); YAML and JSON files parse;
systemd units pass `systemd-analyze verify` apart from the binaries that do not exist on the build host.

Not verified until the modules land: anything involving real JVMs (actual start-up times, RSS against the
3 GB budget, that each service honours the `-D` keys passed, that `ClusterTool list-members` output
parses), the metric names not marked CONFIRMED, `docker compose` and Envoy (Docker may not be run on the
build machine), the Kubernetes manifests against a real API server (no kubectl/helm available; checked
structurally), the Dockerfiles, PromQL semantics (no promtool), and every production tuning value, which
must be measured on the target hardware.

---

## 10. Contract change requests and open items

1. **Replayer metrics port per shard.** `Topology.replayerMetricsPort()` takes no shard, but there is one
   replayer per shard. Local workaround: `-Dlmb.replayer.metricsPort=9900+shard`. Request
   `replayerMetricsPort(shard)` (default 9900 + shard).
2. **Post-trade instances.** Production runs 2 post-trade instances but the topology has single
   `dropCopyFixPort`/`metricsPort` keys; fine on separate hosts only. Request per-instance keys.
3. **DR backup in the topology.** `ClusterBackupMain` reads `lmb.backup.host` etc. outside `Topology`;
   request backup host, ports and metrics port as `Topology` methods so monitoring and manifests can be
   generated from one file.
4. **Zone key.** Request an optional `lmb.shard.S.member.M.zone` in `Topology` (monitoring uses it
   already, `Topology` ignores it).
5. **Metric names** in `metric-names.env` that are not CONFIRMED: commit latency (ingress -> applied),
   election duration, replication lag, matcher busy ratio, snapshot timestamp, gateway in-flight
   capacity, a `shard` label on `lmb_gateway_ack_latency_ns`, market data publish delay, feed gaps and
   staleness, post-trade sink lag, replayer divergence and lag, DR backup lag. Gauges named `*_count`
   that are cumulative would better be counters (`*_total`).
6. **UDP endpoints** for market data (multicast groups A/B), the feed simulator and consolidators are
   not in `Topology`; production needs them in the topology file to generate network plans.
7. **Leader exposure.** Leader detection relies on `lmb_cluster_role`; a stdout line on role change
   (`LMB_ROLE <shard> <member> LEADER`) would make drills observable without metrics.
