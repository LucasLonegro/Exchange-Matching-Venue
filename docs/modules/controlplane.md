# controlplane (`io.lmb.controlplane`)

The venue's operator side (ARCHITECTURE.md 5.4). It owns the reference data (symbols, firms), the trading calendar,
the per-shard split of each firm's credit limit, and the admin REST API. It drives the shards only through cluster
commands (`SymbolDefinition`, `SymbolStateCommand`, `RiskLimitUpdate`, `MassCancel`, `DayRollover`) and learns their
state only from the event stream. It never sees orders and never computes prices; marks and references come from the
venue.

Two instances share one database. The holder of the leader lease (`LeaderLease`: row `leader_lease`, epoch-fenced, no
clock comparison across hosts) sends every command: scheduler, allocator, reconciler, admin executor. Both instances
serve REST reads from the database; a write is recorded in the audit log and executed by whichever instance leads.

## Classes
| Class | Role |
|---|---|
| `ControlPlaneMain` / `ControlPlane` | process entry / one instance: database, venue clients, threads, REST, metrics |
| `Engine` | consumes every shard's event stream into `ShardState` (observed symbols, firms, trading date) and persists it to `shard_symbol_state` / firm rows; confirms tracked commands |
| `CommandTracker`, `TrackedCommand` | correlation ids, timeouts, retries (`commandTimeoutMs`, `commandMaxAttempts`), command log |
| `RefData` | immutable snapshot of the database's reference data, re-read every `refDataReloadMs` |
| `Reconciler` | compares database reference data with the observed `SymbolDefined` state and re-sends definitions or removals |
| `Scheduler`, `CalendarScheduler`, `TradingCalendar`, `SymbolStates` | calendar phases sent as timed `SymbolStateCommand`s; enforcement repairs symbols that are not where the calendar (or the operator) wants them |
| `RiskBudgetAllocator` | per-shard credit budgets from each firm's limit and the shards' reported usage |
| `RestApi` | the operators' API (`/api/*`, operator tokens) and the services' symbol directory (`GET /internal/v1/symbols`, service token) |
| `AdminExecutor` | executes audited admin requests on the leader (halt, resume, auctions, limits, kill, mass cancel, day roll, symbol upsert/remove, calendar overrides) |
| `RestApi` | Javalin admin API; writes are audited (actor = the operator whose token it is) and answered with the audit row |
| `db.Database`, `db.Migrator`, `db.Store` | JDBC pool, versioned migrations (`migrations.txt`), queries |

## Volatility auctions
A symbol with `dynamicBandBps > 0` is moved by the matcher into a volatility auction (`SymbolStateChanged` to
AUCTION_CALL, cause `VOLATILITY_INTERRUPTION`) and back to CONTINUOUS by the matcher's own timer after
`volatilityAuctionNs`. The control plane records it (`volatility_auction`, `volatility_auction_since_ns`; cleared by
any later state change of the symbol, by `SymbolDefined` in another state and by `DayRolled`) and leaves it to the venue:
- Calendar and enforcement never send CONTINUOUS to a symbol in a volatility auction; for enforcement an AUCTION_CALL
  target is not "reached" while the auction is a volatility one.
- An AUCTION_CALL target (the closing auction, the operator's AUCTION_START) is sent anyway, as a takeover
  (AUCTION_CALL -> AUCTION_CALL, cause COMMAND): the matcher cancels its timer and the auction becomes a normal one.
  HALTED and CLOSED are sent as usual (both end the volatility auction).
- Admin `AUCTION_STOP` and `RESUME_SYMBOL` (without `auction=true`) on a symbol in a volatility auction are REJECTED
  (HTTP 422) unless `force=true`; with force the command is sent and the matcher ends the auction now.
- `RESUME_MARKET` without force, on a shard with a volatility auction, is sent as per-symbol CONTINUOUS commands to
  every symbol the shard-wide command would have changed, skipping the symbols in a volatility auction (the audit
  outcome lists them). `HALT_MARKET` stays shard-wide.

## Reference ownership and closes
- The listing price (`symbol.reference_price`, REST `referencePrice`) is only the initial reference. Once the
  venue owns the reference (a `ReferencePriceApplied` from the day roll to the close, from a volatility auction that
  traded or from an external ReferencePrice; or a `SymbolDefined` echo that kept a reference other than the one sent),
  `venue_reference` is set, the observed reference is the venue's, and the reconciler no longer compares or pushes the
  listing price. Re-definitions for other fields still carry the listing price; the matcher ignores it.
- Each `ReferencePriceApplied` also stores the static band (`band_low`, `band_high`).
- Each `ClosingPrice` is stored per symbol: `close_price`, `previous_close`, `close_source` (AUCTION, LAST_TRADE,
  PREVIOUS_CLOSE or NONE) and `close_trading_date`. All survive a restart without a re-definition.
- Positions carried across `DayRollover` are the matcher's business; the control plane only re-opens the day.

## Database columns for the collar and the day cycle (`V1__initial_schema.sql`)
`symbol`: `dynamic_band_bps` (default 0 = collar off), `volatility_auction_ns` (default 300 s, as `SymbolDirectory`).
`shard_symbol_state`: the echoed `dynamic_band_bps` / `volatility_auction_ns`, `volatility_auction`,
`volatility_auction_since_ns`, `venue_reference`, `band_low`, `band_high`, `close_price`, `previous_close`,
`close_source`, `close_trading_date`. NULL price = no price. Tested on H2 (PostgreSQL mode) only.

## REST
Every `/api/*` call needs `X-LMB-Admin-Token` (or `Authorization: Bearer <token>`); `/health` does not. Tokens come from
`operatorsFile` (one `<name> <sha256 hex of token>` line per operator, `io.lmb.common.access.OperatorTokens`) and the
audit actor is that operator's name; without the file the single `adminToken` is accepted and the caller may name
itself with `X-LMB-Actor` (local setups). With `allowDefaultTokens=false` (production) the process refuses to start
on the built-in token. The REST port is for operators only: in k8s the NetworkPolicy `lmb-controlplane-ingress`
admits the operator namespaces and the other control plane instance. A write is validated (400), recorded as PENDING
with its actor, and answered with the audit row once it is final (200; a rejected request is 422) or after `wait` ms
(default 3000, then 202).

The control plane knows firms only as risk accounts (`firm`: id, name, enabled, credit limit, max position). Teams,
sessions and API keys belong to the access service (docs/modules/access.md); there are no keys or session lists here.

| Endpoint | Action |
|---|---|
| `GET /health`, `/api/status`, `/api/shards` | liveness, leader and lease, shard connections |
| `GET /api/symbols[/{id}]`, `POST /api/symbols`, `PUT /api/symbols/{id}`, `DELETE /api/symbols/{id}` | reference data (UPSERT_SYMBOL, REMOVE_SYMBOL) |
| `POST /api/symbols/{id}/halt`, `/resume[?auction=&force=]`, `/auction/start`, `/auction/stop[?target=CONTINUOUS\|CLOSED&force=]` | per-symbol state |
| `POST /api/market/halt[?shard=]`, `/api/market/resume[?shard=&force=]` | shard-wide state |
| `GET /api/firms[/{id}]`, `/api/limits`, `/api/budgets`; `POST /api/firms`; `PUT /api/firms/{id}/limits`; `POST /api/firms/{id}/kill[?cancelOrders=]`, `/enable` | firms, limits, per-shard budgets, new firm (CREATE_FIRM), kill switch |
| `POST /api/masscancel`, `/api/dayroll`; `POST /api/shards/{id}/resync` | mass cancel, day roll, re-sync a shard |
| `GET /api/commands[/{id}]`, `/api/audit[/{id}]` | command log, audit log |
| `GET /api/calendar`, `PUT` / `DELETE /api/calendar/{date}` | calendar overrides |

### Symbols
- `POST /api/symbols` (body with `symbolId`) and `PUT /api/symbols/{id}` (body `symbolId` optional, must equal the
  path) upsert a symbol. Besides the listing fields: `dynamicBandBps` 0..10000 and `volatilityAuctionNs` 0..86 400 000 000 000
  (one day), integers. An absent field keeps the stored value (or the default for a new symbol: collar off, 300 s). `dynamicBandBps > 0`
  with `volatilityAuctionNs = 0` (after filling absent fields) is 400. A symbol cannot move shards (422).
- `GET /api/symbols[/{id}]` adds `dynamicBandBps`, `volatilityAuctionNs`, `inVolatilityAuction`,
  `volatilityAuctionSinceNs` (null unless in one), `venueReferencePrice` (null until the venue owns the reference),
  `bandLow` / `bandHigh` (null until a `ReferencePriceApplied`), `lastClose` {`price`, `previousClose`, `source`,
  `tradingDate`} (null until a `ClosingPrice`). `referencePrice` stays the listing price.
- `force=true` query parameter on `/api/symbols/{id}/resume`, `/api/symbols/{id}/auction/stop` and `/api/market/resume`.

### Firms and mass cancels
- `POST /api/firms` {`firmId`, `name`, `creditLimit`, `maxPositionPerSymbol`} (all required; limits 0..`Long.MAX_VALUE / 4`)
  creates the risk account and sends its limits to every shard (CREATE_FIRM); an existing id is 422. This is the only
  way a firm is created: there is no reference data file. `scripts/lmb-team register` calls this, then the access
  service; `scripts/local-seed` does the same for the local firms.

### Symbol directory for the other services
`GET /internal/v1/symbols` with `Authorization: Bearer <lmb.controlplane.serviceToken>` (operator tokens are refused):
`{"version": "...", "symbols": [{symbolId, ticker, shard, tickSize, lotSize, referencePrice, priceBandBps,
maxOrderQty, dynamicBandBps, volatilityAuctionNs}]}`, every ACTIVE symbol with its desired parameters (referencePrice
is the listing price). The version is a hash of the content, so both instances give the same `ETag`;
`If-None-Match` with it answers 304. Gateways, post-trade and consolidators read it through `SymbolDirectorySource`
(common): polled every `lmb.symbols.pollMs` (5000) across every control plane instance, cached in
`<dataDir>/symbol-directory.json`, the last snapshot kept while both are down. Gateways also follow the shards'
`SymbolDefined` events, which is how a definition reaches them before the next poll.
- `POST /api/masscancel` scope `SESSION` needs `firmId` and `sessionId` (the matcher matches both); `FIRM` and
  `FIRM_SYMBOL` need `firmId`, `SYMBOL` and `FIRM_SYMBOL` need `symbolId`; `shardId` optional (default every shard).

## Credit limits
`creditLimit` (cents) is firm-wide. The allocator splits it into per-shard budgets whose sum never exceeds it; the
matcher charges `qty * riskPrice` per open order and checks `openNotional + |tradedNet|` against the budget.

The local firms (`scripts/local-seed`) use
**10 000 000 000 000 000 cents (1e16, $100 trillion)**: large enough never to bind, small enough that no long overflows.
Bounds (S = shards, F = firms, C = largest credit limit; the matcher caps price and quantity at 1e9 each):
- `qty * price` <= 1e18 per order or fill, whatever C is.
- Per firm and shard, `openNotional` <= C (admission check), and the positive part of `tradedNet` <= C: a buy fills at
  or below its own riskPrice (limit orders match at their limit or better, market orders never rest), so a buy fill
  moves no more into `tradedNet` than it releases from `openNotional`. A sell can fill far above its riskPrice (a sell
  limit at one tick matches the best bid), so the negative part is bounded only by the zero sum across firms:
  `|tradedNet|` <= (F-1)C and `creditUsed` <= F*C.
- Allocator: the sum of the shards' `creditUsed` <= S*F*C, the sum of upper bounds <= S*C.

With S = 16 (prod) and F = 3, S*F*C = 4.8e17, 19x below `Long.MAX_VALUE` (9.22e18). The 10x margin holds up to 5 firms
at 16 shards (no overflow up to 57); add firms with a proportionally smaller limit. 1e16 is above 2^53, so the
allocator's proportional split (in doubles) is exact only to a few cents; the final clamp keeps the sum <= limit.

## Configuration
Every key: system property / environment, then the topology file, then the default.

| Key | Default | Meaning |
|---|---|---|
| `lmb.db.url` / `.user` / `.password` / `.poolSize` | H2 file under `lmb.data.dir/controlplane` | database |
| `lmb.controlplane.operatorsFile` | unset | operator tokens (`<name> <sha256>` per line); unset: `adminToken` only |
| `lmb.controlplane.adminToken` | `lmb-local-admin` (logged as unsafe) | single shared token |
| `lmb.controlplane.allowDefaultTokens` | true | false: refuse to start with the built-in token and no `operatorsFile`, or the default service token |
| `lmb.controlplane.serviceToken` | `lmb-local-symbols` | bearer token of `GET /internal/v1/symbols`; the consumers' `lmb.symbols.token` |
| `lmb.controlplane.<i>.bindHost` | `0.0.0.0` | REST bind host; ports from the topology |
| `lmb.controlplane.autoOpen`, `.autoOpenDelayMs` | false, 2000 | open the market at start outside the calendar |
| `lmb.controlplane.tickMs`, `.persistIntervalMs`, `.reconcileIntervalMs`, `.refDataReloadMs`, `.adminPollMs` | 10, 100, 5000, 1000, 50 | loop periods |
| `lmb.controlplane.commandTimeoutMs`, `.commandMaxAttempts` | 1000, 20 | command retries |
| `lmb.controlplane.commandRetentionMs`, `.refreshRetentionMs` | 24 h, 10 min | command log retention |
| `lmb.controlplane.lease.ttlMs`, `.renewMs`, `.marginMs` | 5000, 1000, 1000 | leader lease |
| `lmb.controlplane.scheduler.enabled`, `.enforce`, `.leadMs`, `.graceMs` | true, true, 5 min, 3000 | calendar and enforcement |
| `lmb.controlplane.calendar.zone`, `.weekdays` | America/New_York, MON..FRI | calendar |
| `lmb.controlplane.calendar.preOpen`, `.openAuction`, `.continuous`, `.closeAuction`, `.close`, `.dayRoll` | 07:00, 09:25, 09:30, 15:55, 16:00, 18:00 | phases |
| `lmb.controlplane.allocator.intervalMs`, `.refreshMs`, `.alpha`, `.minStepFraction` | 250, 1000, 0.5, 0.02 | allocator |
| `lmb.controlplane.adminGatewayId` | 65535 | gateway id of admin mass cancels |

## Tests
`ReconcilerTest` (5), `SchedulerTest` (4), `VolatilityAuctionTest` (6: hop rules, calendar vs volatility auction,
closing-auction takeover, lost-timer repair, admin reject/force/RESUME_MARKET per symbol, dyn/vol DB ->
definition, close and venue reference across a restart), `RestApiTest` (1 end-to-end over HTTP, including the symbol
field validation, PUT keep-existing, the symbol directory (service token, ETag/304, failover to the second URL, a new
and a removed symbol), the GET view of a venue reference, a volatility auction and a close, `POST
/api/firms` with its limits reaching the shards, and SESSION mass cancels needing a firm). Operator token files:
`OperatorTokensTest` (common).

## Known gaps
- The calendar's CONTINUOUS is a pre-sent shard-wide timer: it ends a volatility auction only if the symbol was already
  CONTINUOUS before the CONTINUOUS phase (an operator resumed it during the opening auction). Accepted.
- `AUCTION_STOP?target=CLOSED` clears the operator hold, so during trading hours enforcement reopens the symbol.
- `PUT /api/firms/{id}/limits` accepts `creditLimit` up to `Long.MAX_VALUE / 4`, above the safe bound above.
- The schema is untested on PostgreSQL.
