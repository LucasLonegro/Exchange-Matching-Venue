# gateway (and fix-codecs)

Order gateway: where participants connect. Authenticates sessions, does the stateless checks, turns
client messages into SBE commands for the owning shard, and turns the shards' event streams back into
execution reports. Main class `io.lmb.gateway.GatewayMain`.

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
  The engine's `AuthenticationStrategy` runs on the framer thread and only reads the immutable
  `FirmDirectory`. The `FixLibrary` that owns the sessions is polled by the core, so FIX callbacks
  call the core directly.
- Metrics counters are thread-safe; the latency histogram is recorded and published on the core thread.

## Session protocol (gRPC `OrderEntry.Session`)

- First message must be `Logon` (its `seq` is ignored). Checked against `FirmDirectory`: firm exists,
  `session_id` belongs to the firm, `api_key` matches (constant-time compare). Failure:
  `SessionReject(AUTH_FAILED)` then the stream ends with `UNAUTHENTICATED`. A session already
  connected on this gateway (gRPC or FIX): `DUPLICATE_SESSION`, `ALREADY_EXISTS`. Before the event
  streams have caught up after start: `UNAVAILABLE`.
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
  timeout, slow consumer, gateway shutdown) the gateway sends `MassCancel(scope=SESSION,
  reason=DISCONNECT)` to every shard. These internal commands are never expired or abandoned.

## Stateless checks and degradation

In order, before anything reaches a log: positive `cl_ord_id` (and `orig_cl_ord_id`), symbol known
(`UNKNOWN_SYMBOL`), side and order type given, quantity > 0, quantity <= the directory's `maxOrderQty`
(`MAX_ORDER_SIZE`), positive price for LIMIT, symbol not REMOVED (`SYMBOL_NOT_TRADING`), then the
per-session token bucket (`THROTTLED`; NewOrder and Replace only, cancels and mass cancels are exempt).
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
`lmb.gateway.fix.compId` (default `LMB`), Username(553) = firm id, Password(554) = API key.
CancelOnDisconnectType(35002) 1, 2 or 3 enables cancel-on-disconnect. LmbResumeFrom(20010)
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

## OrderQuery and REST

`GetOrder` (latest report for `(firm_id, cl_ord_id)`, built from all event streams this gateway reads,
including orders entered on other gateways), `ListSymbols` (directory + latest state from the stream),
`GetSessionStatus` (this gateway's view). `GetOrder` and `GetSessionStatus` need header
`x-lmb-api-key` with the firm's key (`lmb.gateway.query.requireApiKey`). Server reflection is enabled.

A REST transcoding proxy (Envoy `grpc_json_transcoder`) needs: the descriptor set
`api-proto/target/generated-resources/protobuf/descriptor-sets/lmb-api.pb` (built by api-proto, includes
`google/api/annotations`), `services: [lmb.orderentry.v1.OrderQuery]` (only these unary calls carry HTTP
rules; `OrderEntry.Session` is a bidi stream and is not transcodable), an HTTP/2 upstream cluster to
`gatewayGrpcPort` (TLS if the gateway uses TLS), and forwarding of the `x-lmb-api-key` request header.
Routes: `GET /v1/firms/{firm_id}/orders/{cl_ord_id}`, `GET /v1/symbols`, `GET /v1/sessions/{session_id}`.

## Configuration

Topology: `lmb.gateway.<id>.grpcPort` (9100+id), `.fixPort` (9200+id), `.metricsPort` (9300+id),
`lmb.symbols`, `lmb.firms`. Identity `lmb.instance.id` (= gatewayId, default 0). Data under
`lmb.data.dir` (default `./data`) `/gateway-<id>`. Every key below is read through `Topology`
(system property, environment variable, topology file, default).

| Key | Default | Meaning |
|---|---|---|
| `lmb.gateway.bindHost` | `0.0.0.0` | gRPC and FIX bind address |
| `lmb.gateway.heartbeatIntervalMs` | 5000 | gRPC heartbeat interval |
| `lmb.gateway.heartbeatTimeoutMultiplier` | 3 | silence (in intervals) before disconnect |
| `lmb.gateway.grpc.inboundWindow` | 64 | client messages outstanding per stream |
| `lmb.gateway.grpc.maxInboundMessageBytes` | 65536 | |
| `lmb.gateway.slowConsumerTimeoutMs` | 30000 | unreadable this long: disconnect |
| `lmb.gateway.throttle.ratePerSec` / `.burst` | 500 / 100 | per-session token bucket |
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
`lmb_gateway_slow_consumer_disconnects_total`, `lmb_gateway_cancel_on_disconnect_total`,
`lmb_gateway_resends_from_cache_total`.

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
  connections (no shared store).
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
- FIX tests cover logon success/refusal and order, fill, cancel, cancel reject, business rejects and
  mass cancel; FIX resume (`LmbResumeFrom`) and FIX cancel-on-disconnect are not covered by tests.
  Tearing down the two Artio engines in the FIX test takes about 10 s.
- TLS/mTLS is wired but not exercised by tests (no certificates generated in tests).
