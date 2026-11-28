# gateway (and fix-codecs)

Order gateway: where participants connect. Authenticates sessions, does the stateless checks, turns
client messages into SBE commands for the owning shard, and turns the shards' event streams back into
execution reports. Main class `io.lmb.gateway.GatewayMain`.

Who may connect is the access service's business (docs/modules/access.md): the gateway polls its
directory snapshot, checks a session's key once at logon, and keeps what the key allows (scopes, throttle
tier, key id) in that session's state. The matcher knows nothing about credentials or throttling.

`fix-codecs` (package `io.lmb.fix`) holds the FIX 4.4 dictionary
`fix-codecs/src/main/resources/fix/lmb-fix44.xml` (shipped in the jar) and the encoders/decoders
Artio's `CodecGenerationTool` generates from it at build time (`io.lmb.fix.builder`, `io.lmb.fix.decoder`,
enums, `io.lmb.fix.FixDictionaryImpl`). Venue tags: 20001 `LmbShardID`, 20002 `LmbEventSeq`,
20003 `LmbFirmID`, 20004 `LmbSessionID` (drop copy), 20010 `LmbResumeFrom` (Logon).

## Layout

| Package | What |
|---|---|
| `io.lmb.gateway` | `GatewayMain`, `Gateway` (assembly), `GatewayConfig` |
| `io.lmb.gateway.core` | `GatewayCore` (the single-threaded agent), sessions, lanes, in-flight buffer, order cache, metrics. No wire protocol. |
| `io.lmb.gateway.grpc` | `OrderEntry.Session` and `OrderQuery` on grpc-netty-shaded |
| `io.lmb.gateway.fix` | Artio acceptor: embedded media driver + Archive + FixEngine, FixLibrary polled by the core |

## Threading model

- **Core thread** (one per gateway, Agrona `AgentRunner`, backoff idle up to `lmb.gateway.idle.maxParkUs`).
  It alone owns every `CommandClient` and `EventStreamClient` (created on that thread in `onStart`,
  since the cluster clients are single-threaded and must be polled), the session registry, sequence
  numbers, token buckets, per-shard lanes and in-flight buffers, catch-up readers, order cache and
  symbol states. No locks. Duty cycle: drain the task queue, poll the FIX library, per shard poll the
  command client / detect reconnects / offer queued commands / poll the live event stream, poll
  catch-up readers, and every 5 ms run timers (in-flight timeouts, queued-command expiry, heartbeats,
  bootstrap, metrics).
- **gRPC (Netty event loops, direct executor)**: handlers never touch core state; they stamp the receive
  time (`gatewayInTsNs`) and enqueue a task on a lock-free MPSC queue. All writes to a stream's
  response observer happen on the core thread, so there is exactly one writer per stream.
- **FIX**: the FixEngine (framer, archiver) and the embedded driver/archive run their own threads.
  The engine's `AuthenticationStrategy` runs on the framer thread and only reads the current
  (immutable) `AccessDirectory` snapshot. The `FixLibrary` that owns the sessions is polled by the core,
  so FIX callbacks call the core directly.
- **Access directory poller** (`access-directory-gateway-<id>`, daemon): every `access.pollMs` a
  conditional `GET /internal/v1/directory` (ETag = version); a new snapshot replaces the reference the
  core reads and is written to `<dataDir>/access-directory.json`. Never blocks the core.
- Metrics counters are thread-safe; the latency histogram is recorded and published on the core thread.

## Session protocol (gRPC `OrderEntry.Session`)

- First message must be `Logon` (its `seq` is ignored). Checked against the access snapshot: the firm and
  the session exist and are enabled, the session is the firm's, and the SHA-256 of `api_key` is an
  unexpired, unrevoked key of that firm bound to that session or firm-wide. Failure:
  `SessionReject(AUTH_FAILED)` then the stream ends with `UNAUTHENTICATED` (one message whatever failed).
  A valid key with neither `TRADE` nor `CANCEL`: `NOT_PERMITTED`, `PERMISSION_DENIED`. A session already
  connected on this gateway (gRPC or FIX): `DUPLICATE_SESSION`, `ALREADY_EXISTS`. Before the event
  streams have caught up after start, or with no snapshot at all (service unreachable and no cached
  copy): `UNAVAILABLE`.
- **The grant.** Accepted, the session keeps its key id, scopes and tier until it disconnects. Every
  `access.revalidateMs` the core checks connected sessions against the newest snapshot: a revoked or
  expired key (end of a rotation's grace period), or a disabled session or firm, ends the connection with
  `SessionReject(AUTH_FAILED, "credentials no longer valid")` (FIX: Logout with that text);
  cancel-on-disconnect applies. Scope and tier changes take effect at the next logon. A `CANCEL`-only key
  ("panic key") may cancel and mass cancel; its NewOrder and Replace get `NOT_PERMITTED`.
- `LogonAck`: `next_expected_client_seq`, `heartbeat_interval_ms`, `gateway_id`. Server `seq` starts
  at 1 per stream (LogonAck) and is +1 per message; heartbeats repeat the last seq.
- **Client sequence numbers.** If this gateway knows the session (earlier connection), it continues
  from its own expected number, otherwise it takes `Logon.next_seq`. Per message:
  - `seq == expected`: processed, expected advances.
  - `seq > expected` (gap): `SessionReject(BAD_SEQUENCE, ref_seq, "expected seq N")`; nothing advances;
    the client resends from N.
  - `seq < expected`: a **possible resend**. It is processed again but submitted with
    `possResend=TRUE`, so the venue applies it at most once. If the gateway already saw the outcome
    of that `cl_ord_id` in the event stream it does not resubmit; it repeats the latest report to the
    owning session with `poss_dup=true`. This is how a client recovers after a gateway failover: log on
    to another gateway and resend unacknowledged requests with their original seq numbers.
- Heartbeats: the gateway sends one every interval; a client silent for 3 intervals
  (`heartbeatTimeoutMultiplier`) is disconnected (cancel-on-disconnect applies). Client heartbeats do
  not use sequence numbers.
- **Resume.** `Logon.resume_from` cursors: for each listed shard every report for the session after that
  `event_seq` is replayed from the shard's event stream; reports that existed before the logon carry
  `poss_dup=true`, later ones do not. Unlisted shards start at the live edge. Report identity is
  `(shard_id, event_seq)`.
- Reports are taken only from the event streams, filtered by `sessionId`, so a session that moves to
  another gateway still gets its reports (including fills of orders entered elsewhere).
- `MassCancel`: empty symbol = scope FIRM to every shard; a symbol = FIRM_SYMBOL to its shard. One
  `MassCancelAck` per shard; `shards_pending` counts down to 0 on the last. A shard that is down gets an
  immediate `SessionReject(UNAVAILABLE)` and is left out of the count.
- `ExecutionReport.cancel_reason` / `reject_reason` carry the venue's `CancelReason` / `RejectReason` name. A
  self-trade prevention cancel (the incoming order would have traded with a resting order of the same firm; the
  matcher cancels the incoming order and leaves the resting one) is `exec_type CANCELED`, `cancel_reason
  "SELF_TRADE"`, `leaves_qty 0`, on the incoming order's own `cl_ord_id`.
- `Logon.cancel_on_disconnect`: when the stream ends for any reason (client close, cancel, heartbeat
  timeout, slow consumer, revocation, gateway shutdown) the gateway sends `MassCancel(scope=SESSION_GATEWAY,
  target_gateway_id=<this gateway>, reason=DISCONNECT)` to every shard: the session's orders entered (or
  last replaced) through this gateway. Orders the session entered through another gateway, e.g. after
  failing over to it, are left alone. These internal commands are never expired or abandoned.
  `Logon.cancel_on_disconnect_window_ms` > 0 delays the cancel by that long (capped by `cod.maxWindowMs`);
  a logon of the same session to the same gateway within the window keeps the orders. A gateway shutting
  down cancels at once, whatever the window.

## Stateless checks and degradation

First the session's **message bucket** (every request, cancels included: `THROTTLED`, "message rate
above N/s") and, for NewOrder and Replace, the key's scope (`NOT_PERMITTED` without `TRADE`). Then, in
order, before anything reaches a log: positive `cl_ord_id` (and `orig_cl_ord_id`), symbol known
(`UNKNOWN_SYMBOL`), side and order type given, quantity > 0, quantity <= the directory's `maxOrderQty`
(`MAX_ORDER_SIZE`), positive price for LIMIT, symbol not REMOVED (`SYMBOL_NOT_TRADING`), then the
session's **order bucket** (`THROTTLED`, "order rate above N/s"; NewOrder and Replace only).

Both buckets come from the session's throttle tier (the session's own tier, else its firm's), resolved at
logon from the access snapshot: `restricted` 10/s burst 10 orders, 50/s burst 50 messages; `standard`
500/100 and 2000/400; `bot` 5000/1000 and 20000/4000 (defaults; operators change tiers in the access
service). A snapshot without tiers (tests) uses `throttle.ratePerSec`/`.burst` and
`throttle.messageRatePerSec`/`.messageBurst`. Limits are per session and per gateway: a firm's total is
the sum over its sessions.
Unspecified time in force means DAY. Replace with `new_price_cents = 0` keeps the price (sent as null).
Tick, lot, band, credit and state rules are the matcher's.

Per shard the gateway keeps two lanes and an in-flight buffer:
- urgent lane (cancels, mass cancels) is always offered before the normal lane (new orders,
  replaces). A cancel whose order is still queued in the normal lane goes behind it, and a mass cancel
  goes behind the firm's queued orders, so prioritisation never reorders a client's own requests.
- depth = queued + in flight. New orders/replaces are refused with `UNAVAILABLE` at
  `inflight.max`; cancels have `inflight.cancelHeadroom` extra. A shard whose command client is not
  connected refuses at once (`UNAVAILABLE`); a command that cannot be offered within `sendTimeoutMs`
  is refused explicitly.
- **Effectively-once submission.** Every offered command stays in flight until its effect appears in
  the live event stream (NewOrder: its NEW/REJECTED report; Cancel: CANCELED/CANCEL_REJECTED; Replace:
  REPLACED/CANCEL_REJECTED; MassCancel: MassCancelDone). After `inflight.timeoutMs`, and for all entries
  when the command client reconnects, it is resubmitted with `possResend=TRUE` (the matcher drops a
  duplicate silently, so the client sees exactly one outcome). After `inflight.maxAttempts` the entry is
  abandoned and the client gets `UNAVAILABLE` with "outcome unknown, query the order".
- **Slow readers.** Reports are written to a gRPC stream only while `isReady()` (and to FIX while Artio
  accepts them). Otherwise the session switches, per shard, to a private event stream reader that
  re-reads from the last report delivered: the event stream is the buffer, gateway memory does not
  grow. When it catches up with the live reader it hands back to live delivery with no gap or duplicate.
  A session not readable for `slowConsumerTimeoutMs` is disconnected. Inbound flow control: one message
  is requested before logon, `grpc.inboundWindow` after, plus one per processed message.

## FIX front end

FIX 4.4 acceptor on `gatewayFixPort`. Logon: SenderCompID = session id, TargetCompID =
`lmb.gateway.fix.compId` (default `LMB`), Username(553) = firm id, Password(554) = API key (checked as for
gRPC, in the engine's authentication strategy). CancelOnDisconnectType(35002): 1 cancels when the
connection is lost, 2 when the client logs out, 3 on both (a client that sends Logout(5) and closes at once
counts as a logout); CODTimeoutWindow(35003, ms) delays the cancel as `cancel_on_disconnect_window_ms`
does. LmbResumeFrom(20010)
`"shard:seq;shard:seq"` replays reports after those cursors with PossResend(97)=Y; without it, a
session reconnecting to the same gateway continues from what that gateway last handed to Artio (Artio's
archive serves FIX ResendRequests for anything already sent). Sequence numbers, heartbeats, test
requests, resend and gap fill are Artio's. Messages: NewOrderSingle(D), OrderCancelRequest(F),
OrderCancelReplaceRequest(G), OrderMassCancelRequest(q, type 1 = Symbol, 7 = all) in;
ExecutionReport(8, fills as ExecType F, with LmbShardID/LmbEventSeq), OrderCancelReject(9),
OrderMassCancelReport(r, one aggregated report) and BusinessMessageReject(j, Text = gateway reason) out.
ClOrdID and OrigClOrdID must be decimal integers (the venue's id is int64); otherwise
BusinessMessageReject. PossResend(97)=Y on an inbound order maps to possResend=TRUE. Prices must have at
most cent precision, quantities must be whole. A canceled or rejected ExecutionReport names the venue reason in
Text(58) (e.g. `END_OF_DAY`); a self-trade prevention cancel is ExecType(150)=4, OrdStatus(39)=4,
Text(58)=`SELF_TRADE`, LeavesQty(151)=0, with no ExecRestatementReason(378) (fix-codecs is unchanged).
Tests: `GatewayGrpcTest.selfTradeCancelCarriesItsReason`, `FixGatewayTest.fixSelfTradeCancelNamesItsReasonInText`.

The FIX stack runs an embedded Aeron media driver and Archive under `<lmb.data.dir>/gateway-<id>/fix`
(term buffers `lmb.gateway.fix.termBufferLength`, IPC only, archive control channel disabled).

**TLS.** Artio has no TLS. On the Internet the FIX port belongs to a TLS-terminating TCP proxy on the same
host (Envoy: `deploy/baremetal/envoy/fix-tls.yaml` with `lmb-fix-tls.service`, or the `fix-tls` sidecar in
k8s), and Artio listens behind it on loopback: `lmb.gateway.fix.bindHost=127.0.0.1`,
`lmb.gateway.fix.listenPort=9201`. Artio then sees every connection as 127.0.0.1; the participant's address
is in the proxy's access log. gRPC TLS is in process (`tls.*` keys).

## OrderQuery and REST

`GetOrder` (latest report for `(firm_id, cl_ord_id)`, built from all event streams this gateway reads,
including orders entered on other gateways), `ListSymbols` (directory + latest state from the stream),
`GetSessionStatus` (this gateway's view). `GetOrder` and `GetSessionStatus` need header
`x-lmb-api-key` with a key of that firm with the `QUERY` scope (`lmb.gateway.query.requireApiKey`); a
session-bound key only sees its own session's status. Missing or invalid key: `UNAUTHENTICATED`; a valid key
of another firm or session: `PERMISSION_DENIED`. Server reflection is enabled.

A REST transcoding proxy (Envoy `grpc_json_transcoder`) needs: the descriptor set
`api-proto/target/generated-resources/protobuf/descriptor-sets/lmb-api.pb` (built by api-proto, includes
`google/api/annotations`), `services: [lmb.orderentry.v1.OrderQuery]` (only these unary calls carry HTTP
rules; `OrderEntry.Session` is a bidi stream and is not transcodable), an HTTP/2 upstream cluster to
`gatewayGrpcPort` (TLS if the gateway uses TLS), and forwarding of the `x-lmb-api-key` request header.
Routes: `GET /v1/firms/{firm_id}/orders/{cl_ord_id}`, `GET /v1/symbols`, `GET /v1/sessions/{session_id}`.

## Configuration

Topology: `lmb.gateway.<id>.grpcPort` (9100+id), `.fixPort` (9200+id), `.metricsPort` (9300+id).
Identity `lmb.instance.id` (= gatewayId, default 0). Data under `lmb.data.dir` (default `./data`)
`/gateway-<id>`: the cached access directory and symbol directory.

Symbols come from the control plane's symbol directory (`GET /internal/v1/symbols`, docs/modules/controlplane.md),
polled through `SymbolDirectorySource` and cached in `symbol-directory.json`; the gateway is not ready (no logons)
before a first one. Each new snapshot is upserted into the routing table on the core thread, and the shards'
`SymbolDefined` events update it in between. `ListSymbols` answers from that table, with the listing reference
price. Every key below is read through `Topology`
(system property, environment variable, topology file, default).

| Key | Default | Meaning |
|---|---|---|
| `lmb.gateway.bindHost` | `0.0.0.0` | gRPC and FIX bind address |
| `lmb.gateway.heartbeatIntervalMs` | 5000 | gRPC heartbeat interval |
| `lmb.gateway.heartbeatTimeoutMultiplier` | 3 | silence (in intervals) before disconnect |
| `lmb.gateway.grpc.inboundWindow` | 64 | client messages outstanding per stream |
| `lmb.gateway.grpc.maxInboundMessageBytes` | 65536 | |
| `lmb.gateway.slowConsumerTimeoutMs` | 30000 | unreadable this long: disconnect |
| `lmb.gateway.access.url` | unset (required) | access service base URL (`http://host:9760`); `GatewayMain` refuses to start without it |
| `lmb.gateway.access.token` | `lmb-local-access` | bearer token of the directory (`lmb.access.serviceToken`) |
| `lmb.gateway.access.pollMs` | 2000 | directory poll interval |
| `lmb.gateway.access.revalidateMs` | 1000 | how often connected sessions are checked against the snapshot |
| `lmb.symbols.url` | every control plane instance's REST port | comma-separated base URLs of the symbol directory, tried in turn |
| `lmb.symbols.token` | `lmb-local-symbols` | its bearer token (`lmb.controlplane.serviceToken`) |
| `lmb.symbols.pollMs` | 5000 | symbol directory poll interval |
| `lmb.gateway.throttle.ratePerSec` / `.burst` | 500 / 100 | order bucket when the snapshot defines no tier |
| `lmb.gateway.throttle.messageRatePerSec` / `.messageBurst` | 2000 / 400 | message bucket when the snapshot defines no tier |
| `lmb.gateway.cod.maxWindowMs` | 60000 | cap on a client's cancel-on-disconnect window |
| `lmb.gateway.inflight.max` | 4096 | per-shard queued + in-flight limit for orders |
| `lmb.gateway.inflight.cancelHeadroom` | 1024 | extra depth reserved for cancels |
| `lmb.gateway.inflight.timeoutMs` | 1000 | resubmit (possResend) after this |
| `lmb.gateway.inflight.maxAttempts` | 10 | then abandon and tell the client |
| `lmb.gateway.sendTimeoutMs` | 500 | queued but not offered this long: `UNAVAILABLE` |
| `lmb.gateway.stream.fromStart` | true | live readers start at eventSeq 0 (rebuilds the order cache) |
| `lmb.gateway.bootstrapTimeoutMs` | 30000 | accept logons even if not caught up |
| `lmb.gateway.orderCache.max` | 1000000 | keys kept for GetOrder / resend detection |
| `lmb.gateway.query.requireApiKey` | true | |
| `lmb.gateway.tls.certChain` / `.privateKey` | unset | PEM files; set both for TLS |
| `lmb.gateway.tls.clientCa` | unset | with TLS: require client certificates (mTLS) |
| `lmb.gateway.fix.enabled` | true | |
| `lmb.gateway.fix.compId` | `LMB` | |
| `lmb.gateway.fix.bindHost` | `bindHost` | `127.0.0.1` behind a local TLS proxy |
| `lmb.gateway.fix.listenPort` | 0 (= fixPort) | Artio's port when the proxy owns `fixPort` |
| `lmb.gateway.grpc.handshakeTimeoutMs` | 5000 | TLS/HTTP2 handshake deadline |
| `lmb.gateway.grpc.permitKeepAliveMs` | 10000 | minimum client keepalive interval (faster: connection closed) |
| `lmb.gateway.grpc.maxConcurrentCallsPerConnection` | 8 | |
| `lmb.gateway.grpc.maxInboundMetadataBytes` | 8192 | |
| `lmb.gateway.fix.termBufferLength` | 262144 | embedded driver term buffers |
| `lmb.gateway.fix.deleteLogsOnStart` | false | wipes Artio state (sequence numbers) |
| `lmb.gateway.idle.maxParkUs` | 1000 | core thread max idle park |
| `lmb.gateway.metrics.intervalMs` | 5000 | latency quantile window |
| `lmb.gateway.shutdownFlushMs` | 1000 | time to offer cancel-on-disconnect at shutdown |

## Metrics (`/metrics` on `gatewayMetricsPort`)

`lmb_gateway_requests_total{type}`, `lmb_gateway_rejects_total{reason}`, `lmb_gateway_inflight_depth{shard}`,
`lmb_gateway_ack_latency_ns{quantile=p50|p99|p999|max}` (gateway receive to report written, from the
echoed `gatewayInTsNs`, own orders, live reports only), `lmb_gateway_resubmits_total{cause}`,
`lmb_gateway_inflight_abandoned_total`, `lmb_gateway_reports_out_total{poss_dup}`,
`lmb_gateway_sessions_connected`, `lmb_gateway_catchup_streams`,
`lmb_gateway_slow_consumer_disconnects_total`, `lmb_gateway_cancel_on_disconnect_total` (one per shard
command), `lmb_gateway_cancel_on_disconnect_pending` (sessions inside their window),
`lmb_gateway_resends_from_cache_total`, `lmb_gateway_auth_failures_total` (logons and queries refused),
`lmb_gateway_access_revocations_total` (sessions ended by a newer snapshot),
`lmb_gateway_access_directory_version`, `lmb_gateway_access_directory_age_seconds` (since the last
successful poll; alert `LmbAccessDirectoryStale`), `lmb_gateway_access_poll_failures_total`.

## Running

`scripts/lmb-run io.lmb.gateway.GatewayMain` with `-Dlmb.topology=deploy/local/topology.properties
-Dlmb.instance.id=0` (needs lmb-cluster-client on the classpath for `VenueClients.connect`). Prints
`LMB_READY gateway <id>` once the live event streams have caught up; SIGTERM disconnects every session
(cancel-on-disconnect applies), flushes, then stops.

Tests: `./mvnw -q -pl fix-codecs,gateway test`. They use testkit's `InMemoryVenue` with a scripted
state machine (`ScriptedShard`), real gRPC channels on 33000-33499 and an Artio initiator against the
gateway's FIX acceptor on 33500-33699; data under `gateway/target/test-data`.

## Known gaps

- "One live connection per sessionId" is enforced per gateway only; two gateways cannot see each other's
  connections (no shared store). Throttle buckets are per gateway too.
- A gateway that crashes (rather than shutting down) sends no cancel-on-disconnect: its sessions' orders
  rest until the session logs on somewhere and cancels, or an operator mass cancels (scope SESSION).
- A client's `MassCancel` is firm-wide (scope FIRM or FIRM_SYMBOL): one member's mass cancel cancels the
  teammates' orders too.
- A revoked key's connection lasts until the next poll plus `revalidateMs` (about 3 s by default). Scope and
  tier changes need a new logon.
- Client sequence state and the FIX "own cursor" resume live in gateway memory: a restarted gateway trusts
  `Logon.next_seq`, and FIX failover to another gateway needs `LmbResumeFrom` to get missed reports.
  Artio's own sequence numbers are per gateway (its archive), so a FIX client moving gateways must log on
  with ResetSeqNumFlag or matching numbers.
- `MassCancelAck` is only produced by the gateway that fanned the request out; a session that moved to
  another gateway gets the individual CANCELED reports but not the acks.
- Bootstrap "caught up" is a heuristic (a live-reader poll returning less than its limit); with
  `stream.fromStart=true` startup cost grows with the day's event count.
- The gRPC/protobuf path allocates per message (protobuf objects, a task lambda per inbound message,
  byte copies for the order cache); the core's lanes allocate one `InFlight` per command. Not tuned for
  allocation-free operation.
- FIX ExecutionReport omits AvgPx (the gateway does not track average price). FIX `PossDupFlag` resends
  are Artio's; venue-level replays use PossResend(97)=Y.
- Abandoned in-flight commands are reported as `UNAVAILABLE` with "outcome unknown"; the order may still
  have been applied (the client should query or resume).
- In-flight abandonment does not complete a pending MassCancelAck aggregate for that request.
- FIX tests cover logon success/refusal, order, fill, cancel, cancel reject, business rejects, mass cancel
  and cancel-on-disconnect types 1 and 2 with a window (`fixCancelOnDisconnectTypesAndWindow`); FIX resume
  (`LmbResumeFrom`) is not covered. Tearing down the Artio engines in the FIX test takes about 10 s each.
- Symbols: `GatewaySymbolsTest` (not ready without a symbol snapshot, a new snapshot applied live), `LiveSymbolsTest`.
- Access: `GatewayAccessTest` (scopes, cancel-only keys, revocation, tiers, the cancel-on-disconnect window
  and its cap) and `GatewayTlsTest` (TLS and mTLS with throwaway openssl certificates; skipped without
  openssl). The FIX TLS proxy is configuration only, not tested here.
