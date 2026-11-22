# cluster-node and cluster-client

Packages `io.lmb.cluster` (module `cluster-node`, artifact `lmb-cluster-node`) and
`io.lmb.cluster.client` (module `cluster-client`, artifact `lmb-cluster-client`).

## 1. What the modules do

**cluster-node** runs one member of a shard's Aeron Cluster per JVM: media driver (embedded by
default), Archive, ConsensusModule and a ClusteredServiceContainer whose service
(`ShardClusteredService`) wraps the `ShardStateMachine` loaded with `ShardStateMachineFactory.load()`.
Every member (not only the leader) materialises the shard's event stream into its own Archive and
serves it to consumers. It also contains the DR backup (`ClusterBackupMain`) and the reader for the
recorded Raft log (`RaftLogReader`, section 6).

**cluster-client** is the production `VenueClients` implementation (registered as a
`VenueClients.Provider` service): `CommandClient` = Aeron Cluster ingress session with leader redirect
and non-blocking reconnect; `EventStreamClient` = event stream consumer with dedupe by `eventSeq`, gap and
stall detection and member failover.

Ingress messages are raw LMB SBE commands. Cluster time is in nanoseconds (`NanosecondClusterClock`);
the machine receives `logPosition` = Aeron log position *after* the entry and `timestampNs` = the
entry's cluster timestamp.

## 2. Main classes

cluster-node (`io.lmb.cluster`)

| Class | Role |
|---|---|
| `ClusterNodeMain` | `main`: `NodeConfig.from(Topology.load())`, launches `ClusterNode` + `NodeMetrics`, prints `LMB_READY cluster-node <shard>-<member>`. |
| `NodeConfig` | All member keys (table below); public fields so tests can adjust them. `memberDir()`, `clusterId()`. |
| `ClusterNode` | Assembles driver, Archive, ConsensusModule, container, `EventStore`, `EventStreamServer`, periodic snapshot trigger. `role()`, `commitPosition()`, `snapshotCount()`, `triggerSnapshot()`. |
| `ShardClusteredService` | `ClusteredService` around the machine: commands, timers, snapshots (HEADER/CHUNK*/END). |
| `EventStore`, `EventIndex` | Writes the event recording and its index (section 5). |
| `EventStreamServer` | Serves the EVENT_CONTROL protocol, starts Archive replays toward consumers. |
| `NodeMetrics` | Prometheus endpoint (`lmb_cluster_*`). |
| `ClusterBackupNode`, `ClusterBackupMain` | Aeron `ClusterBackup` for DR. |
| `RaftLogReader`, `RaftLogDecoder` | Read back committed log entries (section 6). |
| `SharedSecretAuthenticator` | Ingress authentication with `lmb.cluster.secret` (constant-time compare). |

cluster-client (`io.lmb.cluster.client`): `ClusterVenueClients` (+`Provider`), `ClientConfig`,
`ClusterCommandClient`, `ClusterEventStreamClient`, `EventProtocol` (wire layout in its javadoc),
`SharedSecret`.

## 3. Configuration

Keys are read through `io.lmb.common.Topology` (topology file, then system properties / environment).
Defaults are the local profile.

### Member (`NodeConfig.from`)

| Key | Default | Meaning |
|---|---|---|
| `lmb.shard.id`, `lmb.member.id` | 0, 0 | Identity. |
| `lmb.data.dir` | `./data` | Member data under `<dir>/cluster-node/shard-S-member-M/`. |
| `lmb.aeron.dir` | per member under `/dev/shm` | Aeron dir; required when the driver is external. |
| `lmb.driver.embedded` | true | false = use the external driver at `lmb.aeron.dir`. |
| `lmb.driver.threading` | SHARED | SHARED, SHARED_NETWORK, DEDICATED. |
| `lmb.cluster.idle` | backoff | Idle strategy for driver/CM/service/server: backoff, sleep, yield, busy. |
| `lmb.archive.threading` | SHARED | SHARED or DEDICATED. |
| `lmb.driver.termLength` | 256k | Default publication term (ingress, control). |
| `lmb.cluster.logTermLength` | 1m | Raft log term length. |
| `lmb.events.termLength` | 1m | Event recording term length. |
| `lmb.cluster.snapshotTermLength` | 1m | Snapshot channel term (>= 512k: chunks are up to 32 KiB). |
| `lmb.archive.segmentLength` | 64m | Archive segment file length (raised to the largest term length). |
| `lmb.archive.fileSyncLevel`, `lmb.cluster.fileSyncLevel` | 0 | 0 none, 1 data, 2 data+metadata. Production: 1 or 2. |
| `lmb.cluster.leaderHeartbeatIntervalMs` | 200 | |
| `lmb.cluster.leaderHeartbeatTimeoutMs` | 3000 | Failure detection time. |
| `lmb.cluster.electionTimeoutMs` | 1000 | |
| `lmb.cluster.startupCanvassTimeoutMs` | 10000 | |
| `lmb.cluster.sessionTimeoutMs` | 10000 | Ingress session timeout (clients send keep-alives). |
| `lmb.cluster.maxSessions` | 128 | Concurrent ingress sessions. |
| `lmb.cluster.snapshotIntervalSec` | 0 | Leader snapshots this often; 0 = off. |
| `lmb.cluster.secret` | unset | Ingress shared secret; unset = no authentication. |
| `lmb.cluster.metrics.enabled` | true | Prometheus on member base port + `METRICS_OFFSET`. |
| `lmb.events.indexInterval` | 64 | One index entry every N events. |
| `lmb.events.serverHeartbeatMs` | 250 | HEARTBEAT interval to consumers. |
| `lmb.events.sessionTimeoutMs` | 10000 | Consumer session expiry without KEEPALIVE. |
| `lmb.events.maxSessions` | 256 | Concurrent consumers per member. |
| `lmb.events.retention.maxSegments` | 0 | Keep at most N closed event segments (0 = unlimited); see 5.4. |
| `lmb.events.retention.maxAgeSec` | 0 | Delete closed segments older than this (0 = unlimited). |
| `lmb.events.retention.intervalSec` | 60 | How often retention runs. |

### Backup (`ClusterBackupMain`)

| Key | Default | Meaning |
|---|---|---|
| `lmb.shard.id` | 0 | Shard to back up. |
| `lmb.backup.host` | 127.0.0.1 | Address the backup listens on (must be reachable by the members). |
| `lmb.backup.<shard>.basePort` | 20000 + 1000*shard + 900 | Uses the same `*_OFFSET`s as a member. |
| `lmb.backup.aeron.dir` | `lmb-backup-<shard>` next to the member aeron dir | Never a member's aeron dir. |
| `lmb.backup.sourceType` | FOLLOWER | ANY, LEADER or FOLLOWER. |
| `lmb.backup.coolDownMs` | 5000 | Pause after a reset (e.g. the leader answered a FOLLOWER query); Aeron's default is 30 s. |
| `lmb.backup.queryIntervalSec` | 3600 | How often new snapshots are fetched; the live log is recorded continuously. |
| `lmb.backup.progressTimeoutMs` | 10000 | No progress for this long => reset and query again. |

Data under `<lmb.data.dir>/cluster-backup/shard-S/{archive,cluster}`. The backup retrieves the latest
snapshots (consensus module + service), then records the live log from the log source member; its state
goes BACKUP_QUERY -> SNAPSHOT_RETRIEVE -> LIVE_LOG_RECORD -> LIVE_LOG_REPLAY -> UPDATE_RECORDING_LOG ->
BACKING_UP (`ClusterBackupNode.state()`; progress is logged). Its directory can be read with
`RaftLogReader.open(dir)`; its log may start at the retrieved snapshot (check `startPosition()`) and
is identical to the members' from there (tested).

### Client (`ClientConfig.from`)

| Key | Default | Meaning |
|---|---|---|
| `lmb.client.host` | 127.0.0.1 | Address members reply to (response, data and egress endpoints). |
| `lmb.client.driver.embedded` | true unless `lmb.aeron.dir` is set | |
| `lmb.client.aeron.dir.base` | `/dev/shm` | Parent of the embedded driver's directory (deleted on close). |
| `lmb.client.termLength` | 256k | |
| `lmb.cluster.secret` | unset | Credentials `"lmb:"+secret`. |
| `lmb.client.peerTimeoutMs` | 2000 | |
| `lmb.client.messageTimeoutMs` | 5000 | Cluster connect timeout. |
| `lmb.client.keepAliveIntervalMs` | 1000 | |
| `lmb.client.reconnectBackoffMs` | 200 | |
| `lmb.events.connectTimeoutMs` | 2000 | No REPLAY_STARTED/UNAVAILABLE in time => next member. |
| `lmb.events.heartbeatTimeoutMs` | 3000 | |
| `lmb.events.stallTimeoutMs` | 5000 | No progress while the member reports newer events. |
| `lmb.events.keepAliveIntervalMs` | 250 | |
| `lmb.events.memberCooldownMs` | 1000 | Abandoned member is skipped this long (x10 for TOO_OLD/GAP). |
| `lmb.events.preferredMember` | -1 | Otherwise last known leader, else round robin. |
| `lmb.events.rehomeAfterMs` | 10000 | Move back to the leader when caught up. |

## 4. Ports and streams

Member base port = `Topology.memberBasePort(shard, member)` (default `20000 + 1000*shard + 100*member`).

| Offset | Constant | Use |
|---|---|---|
| +0 | `ARCHIVE_CONTROL_OFFSET` | Archive control (UDP). Also the archive endpoint in the cluster members string. |
| +1 | `ARCHIVE_REPLICATION_OFFSET` | unused (replication uses ephemeral ports) |
| +2 | `INGRESS_OFFSET` | Cluster ingress |
| +3 | `CONSENSUS_OFFSET` | Consensus |
| +4 | `LOG_OFFSET` | Raft log (manual control mode) |
| +5 | `CATCHUP_OFFSET` | Catch-up |
| +6 | `EVENT_CONTROL_OFFSET` | Event stream requests, stream 7000 |
| +7 | `ARCHIVE_RESPONSE_OFFSET` | unused (archive responses use ephemeral ports) |
| +8 | `BACKUP_OFFSET` | ClusterBackup catch-up endpoint (backup only) |
| +9 | `METRICS_OFFSET` | Prometheus HTTP `/metrics` |

Client side: response stream 7001 and data stream 7002 on ephemeral ports of `lmb.client.host`.
Local IPC inside a member: archive local control `aeron:ipc?term-length=64k`, event recording
`aeron:ipc?alias=lmb-events` stream 7010, scan replays stream 7011.

Test port ranges: cluster-node 31000-31999, cluster-client 32000-32999 (`TestCluster` uses base + 10*member).

## 5. Event stream

### 5.1 Recording

`ShardClusteredService` passes every machine event to `EventStore.append`, which offers it to an
exclusive IPC publication created with `AeronArchive.addRecordedExclusivePublication`, so the member's
own Archive records it. The machine is deterministic, so all members hold byte-identical streams
(tested). Every start of the service opens a **new recording ("segment")** whose first eventSeq is
`max(lastRecordedSeq, machineLastSeqAfterSnapshot) + 1`. While the log replays after a restart, events
with `eventSeq <= lastRecordedSeq` are dropped (already recorded). A member restored from a snapshot
whose event files were lost starts at `S+1` and answers `TOO_OLD` for earlier events.

### 5.2 Index (`<memberDir>/events/`)

* `segments.dat`: 16-byte LE records `(recordingId, firstSeq)`, rewritten atomically (tmp + move + fsync).
* `seg-<recordingId>.idx`: 16-byte LE `(eventSeq, startPosition)` every `lmb.events.indexInterval`
  events (not fsynced). On start every segment is validated against the Archive catalog and its last
  eventSeq found by replaying its tail, so a lost or torn index only costs scanning.

### 5.3 Serving (EVENT_CONTROL)

A consumer sends REQUEST(afterSeq or -1 = latest, response and data endpoints) to member base + 6,
stream 7000. The member answers REPLAY_STARTED (Archive replay session id, resolved afterSeq, endSeq or
-1 when following live) or UNAVAILABLE (NOT_READY, TOO_OLD, GAP, BUSY, ERROR, WRONG_SHARD, with the
member's first/last seq), then HEARTBEATs every 250 ms while KEEPALIVEs arrive. The events arrive as an
Archive replay on the consumer's data endpoint (stream 7002), starting at the nearest index entry at or
before the wanted seq, so up to `indexInterval` duplicates are possible; the client drops them. Closed
segments are replayed bounded, the live one follows live. Full wire layout: `EventProtocol` javadoc.

### 5.4 Retention

`EventStore` purges closed segments (never the live one) according to `lmb.events.retention.*`
(`EventRetention`): the oldest segments beyond `maxSegments` and those whose recording stopped more
than `maxAge` ago are removed from `segments.dat` first, then their Archive recording is purged and
the index file deleted. Afterwards the member's first available eventSeq moves forward and requests for
older events get `TOO_OLD` (the client goes to another member). Segments are only rolled when the
service restarts; there is no rollover by size yet, so retention only helps members that restart
(see gaps).

## 6. Reading the Raft log back (for the shadow replayer)

### 6.1 Where it is

* `<memberDir>/cluster/recording.log` (`io.aeron.cluster.RecordingLog`): TERM entries
  `(recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestamp)` - `logPosition` is
  the end of the term, `-1` for the open (last) term - and SNAPSHOT entries (`serviceId` -1 = consensus
  module, 0 = our service) whose `recordingId` is the snapshot recording and `logPosition` the snapshot
  position. Invalidated entries have `isValid == false`; skip them.
* `<memberDir>/archive/`: `archive.catalog` and segment files `<recordingId>-<segmentBasePosition>.rec`.
  The log is one recording per member, extended across terms (a term may start a new recording, so
  always take the recordingId from the TERM entry). A segment file holds the raw term data from
  `segmentBasePosition` (see `AeronArchive.segmentFileBasePosition`): standard Aeron frames, 32-byte
  data header, 32-byte aligned, `HDR_TYPE_PAD` frames at term ends, BEGIN/END flags for fragmented
  messages. The position of a frame is `segmentBasePosition + fileOffset`.
* The committed part of the log is byte-identical on every member (frame headers differ only by
  session/term ids after a leader change), so any member, or a ClusterBackup's archive, can be read.

### 6.2 What an entry is

Each message (after reassembly) starts with an 8-byte SBE header of the cluster codecs
(`io.aeron.cluster.codecs`, schema id 111):

| Template | Message | What to do |
|---|---|---|
| 1 | `SessionMessageHeader` (block 24: leadershipTermId, clusterSessionId, timestamp) | Bytes after the first 32 (`AeronCluster.SESSION_HEADER_LENGTH`) are the raw LMB command. Call `machine.onCommand(cmd, logPosition, timestamp)`. |
| 20 | `TimerEvent` (leadershipTermId, correlationId, timestamp) | `machine.onTimer(correlationId, logPosition, timestamp)`. |
| 21, 22 | `SessionOpenEvent`, `SessionCloseEvent` | Ignore (our service does nothing on them). |
| 23 | `ClusterActionRequest` | Snapshot marker; ignore for replay. |
| 24 | `NewLeadershipTermEvent` | Ignore (time unit check only). |
| 25 | `MembershipChangeEvent` | Ignore. |

`logPosition` for an entry is the position **after** its last frame (`Header.position()` in Aeron),
exactly what the live machine received. Replaying the commands and timers in log order from position 0
into a fresh machine reproduces the event stream byte for byte (tested by `RaftLogReaderTest`).

### 6.3 Committed vs recorded

A member's recording may extend past the commit position (entries appended but not yet acknowledged
by a quorum; on a stopped member they can be truncated by the next election). Ended TERM entries and
SNAPSHOT positions are known committed. For the open term, bound reads by the commit position when you
have it (live member: `ClusterNode.commitPosition()`, the `Cluster commit-pos` counter, or
`ClusterTool <clusterDir> describe`); otherwise the recorded end is the best available bound. The
shadow replayer only compares against events that the live stream has published, and the stream only
contains committed entries, so an uncommitted tail can never be confirmed, only left unmatched.

### 6.4 API (`io.lmb.cluster.RaftLogReader`)

Offline, read-only, no media driver needed:

```java
try (RaftLogReader log = RaftLogReader.open(memberDir))   // dir containing cluster/ and archive/
{
    long from = log.startPosition();          // 0 unless the log was purged after a snapshot
    long to = log.recordedPosition();         // or a commit position you trust
    long reached = log.read(from, to, new RaftLogReader.Handler()
    {
        public void onCommand(long logPosition, long timestampNs, long clusterSessionId,
                              DirectBuffer buffer, int offset, int length) { machine.onCommand(...); }
        public void onTimer(long logPosition, long timestampNs, long correlationId) { machine.onTimer(...); }
    });
    // reached = position after the last complete entry delivered; resume later with read(reached, ...)
}
```

* `RaftLogReader.open(clusterDir, archiveDir)` opens other layouts, e.g. a ClusterBackup's
  `<lmb.data.dir>/cluster-backup/shard-S/{cluster,archive}` (`open(dir)` works there too).
* `onTimer` and `onClusterMessage(templateId, ...)` are default methods; the template ids are
  `RaftLogDecoder.*_TEMPLATE_ID`. Per-entry delivery is allocation-free and zero-copy (the buffer is
  the read-only mapped segment file, or a reassembly buffer for fragmented entries).
* `terms()` / `snapshots()` expose the recording log entries; `knownCommittedPosition()` the highest
  position known committed (ended terms, snapshots).
* `from` must be an entry boundary (0, a snapshot `logPosition`, or a value returned by `read`).
* Reading the files of a running member is safe up to its commit position (data below the Archive's
  recorded position is complete). To follow a live member, use the Archive instead: connect an
  `AeronArchive` to the member (`aeron:udp?endpoint=<host>:<base+0>`, or the IPC local control channel
  when on the same driver), `replay(recordingId, from, AeronArchive.REPLAY_ALL_AND_FOLLOW ... )` and
  poll the replay subscription with `new FragmentAssembler(new RaftLogDecoder(handler))`;
  `RaftLogDecoder` dispatches to the same `Handler`.
* If `startPosition() > 0`, the machine must first be restored from the service snapshot at
  `startPosition()` (SNAPSHOT entry with serviceId 0). Snapshot loading is not wrapped by this API yet.

## 7. Snapshots, log growth, recovery

* Snapshots: `lmb.cluster.snapshotIntervalSec` on the leader, or `ClusterTool <clusterDir> snapshot`.
  The service snapshot is HEADER(type 1, version 1, shardId, lastEventSeq, logPosition),
  CHUNK(type 2, machine bytes)*, END(type 3, chunkCount, lastEventSeq). Timers live in the CM snapshot.
* The Raft log is never purged by the node (the replayer reads it from position 0). Purge with
  ClusterTool only together with an archive of the log elsewhere.
* A member whose disk was lost restarts from another member's snapshot/log via the normal Aeron
  catch-up; its event stream then starts after the snapshot (TOO_OLD for older events).

## 8. Running by hand

```
scripts/lmb-run io.lmb.cluster.ClusterNodeMain -Dlmb.topology=deploy/local/topology.properties -Dlmb.shard.id=0 -Dlmb.member.id=0
scripts/lmb-run io.lmb.cluster.ClusterNodeMain -Dlmb.topology=deploy/local/topology.properties -Dlmb.shard.id=0 -Dlmb.member.id=1
scripts/lmb-run io.lmb.cluster.ClusterBackupMain -Dlmb.topology=deploy/local/topology.properties -Dlmb.shard.id=0
```

Tests: `./mvnw -q -pl cluster-node test`, `./mvnw -q -pl cluster-client test`.

## 9. Known gaps

* Commands accepted into a dead leader's ingress publication before the failure is detected are lost
  (about one flow-control window); the gateway must resubmit with possResend.
* Event segments roll only on service restart; no rollover by size, so retention cannot trim a member
  that never restarts.
* `EventStore.append` waits forever if the event recording cannot keep up / never connects (disk full
  stalls the cluster; logged once).
* `ARCHIVE_RESPONSE_OFFSET` unused: archive and client responses use ephemeral ports (firewalls).
* A member lagging far behind fails a pinned consumer over only via the stall timeout; right after an
  election the client may rehome to a stale leader id once.
* Event index files are not fsynced (validated on start).
* Raft-log reader: no snapshot loading helper; open-term commit position is not known offline.
