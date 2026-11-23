# matcher-core (`io.lmb.matcher`)

The deterministic state machine of one shard: `MatcherShard implements ShardStateMachine`, created by
`MatcherShardFactory` (registered in `META-INF/services/io.lmb.api.ShardStateMachineFactory`, so
`ShardStateMachineFactory.load().create(shardId)` finds it). No Aeron, no threads, no clock, no I/O
(apart from an slf4j ERROR log line on an internal error).

## Configuration

Sizing only. These values never change an event or snapshot byte. Read by `MatcherConfig.fromConfig()`
(system property or env var, see `io.lmb.common.Config`):

| Key | Default | Meaning |
|---|---|---|
| `lmb.matcher.orderPoolSize` | 65536 | preallocated orders (grows when exhausted) |
| `lmb.matcher.levelPoolSize` | 16384 | preallocated price levels (grows) |
| `lmb.matcher.levelWindowTicks` | 4096 | width of the tick-indexed level window per book side (power of two) |
| `lmb.matcher.dedupeCapacity` | 262144 | initial (firm, clOrdId) table capacity (grows) |

## How to run / test

It is a library. `./mvnw -q -pl matcher-core test` runs the whole suite (about 10 s). The throughput
benchmark is `MatcherBenchmarkTest` (prints commands/s).

## Event order within one log entry

Every entry ends with exactly one `CommandApplied`. "Deltas" means the BookDelta block described below.

| Entry | Events, in order |
|---|---|
| NewOrder rejected | `ER(REJECTED)` (orderId 0) |
| NewOrder accepted, CONTINUOUS | `ER(NEW)`; per match: `Trade`, `ER` aggressor (TAKER), `ER` maker (MAKER); then `ER(CANCELED, IOC_REMAINDER or MARKET_COLLAR)` if a remainder is canceled; deltas |
| NewOrder accepted, AUCTION_CALL | `ER(NEW)`; (`ER(CANCELED, IOC_REMAINDER)` for an IOC); deltas; `AuctionUpdate(isFinal=FALSE)` if the indicative changed |
| CancelOrder | `ER(CANCELED, CLIENT)` or `ER(CANCEL_REJECTED)`; deltas; indicative as above |
| ReplaceOrder | `ER(REPLACED)` or `ER(CANCEL_REJECTED)`; then fills if it now crosses (the replaced order is TAKER); deltas; indicative |
| MassCancel | `ER(CANCELED)` per order; deltas; indicatives (symbol order); `MassCancelDone` |
| ReferencePrice | `ReferencePriceApplied`; indicative (AUCTION_CALL only) |
| SymbolDefinition | `SymbolDefined`; indicative (AUCTION_CALL only) |
| RiskLimitUpdate | `RiskLimitApplied` |
| State transition (per symbol) | [uncross: `Trade`, `ER` buy, `ER` sell per match; deltas; `AuctionUpdate(isFinal=TRUE)`]; `SymbolStateChanged`; then for CLOSED `ER(EXPIRED, END_OF_DAY)` per order, for REMOVED `ER(CANCELED, SYMBOL_REMOVED)` per order, then deltas; for entering AUCTION_CALL an indicative |
| Market-wide state command | the above per eligible symbol, in symbol definition order |
| DayRollover | `ER(EXPIRED, END_OF_DAY)` per order (orderId order); deltas; `SymbolStateChanged` per symbol not already PRE_OPEN (definition order, reasonCode 0, correlationId of the DayRollover); `DayRolled` |
| Scheduled transition (timer) | as a state transition; `CommandApplied.commandTemplateId = 0` |
| Malformed / unknown template | `CommandRejected(MALFORMED)` |
| possResend duplicate, stale ReferencePrice, scheduling a transition, unknown timer | nothing but `CommandApplied` |

**BookDelta block**: one `BookDelta` per price level whose total quantity or order count changed during
the step, with the final value (quantity 0 removes the level). Ordered by symbol (definition order),
then BUY before SELL, then best price first. A level emptied and refilled in the same step produces one
delta. Multi-order operations (mass cancel, expiry, uncross) emit their reports first and one block after.

## Field conventions

- `orderId = (shardId << 48) | n`, `matchId = (shardId << 48) | m`, n and m dense from 1, never reset
  (not even by DayRollover).
- ER `price` is the limit price, null for market orders (the NewOrder price field is ignored for MARKET).
  `lastPx` is null unless a fill. `gatewayInTsNs` is the request's on the requester's own reports, 0 on
  maker fills, auction fills, expiries, and mass-cancel reports of orders of a firm other than the one
  that sent the MassCancel.
- Cancel and replace reports (success and reject) carry the **requesting** `sessionId`. A successful
  replace transfers the order to the requesting session and gateway (later fills go there; SESSION and
  GATEWAY mass cancels follow it).
- `CANCEL_REJECTED` for an unknown order: orderId 0, price null, quantities 0, side null,
  ordStatus REJECTED; for a known done order ordStatus is FILLED/CANCELED/EXPIRED; for a live order its
  current status and fields.
- `CommandRejected.correlationId`: correlationId of control commands, `clOrdId` for malformed
  order commands (decodable but bad enum), `requestId` for MassCancel, `sourceTsNs` for ReferencePrice,
  0 when the message structure itself is broken.
- Auction trades: `aggressorSide` null, `isAuction` TRUE, liquidity AUCTION on both reports.

## Decisions on details the contract leaves open

- **Malformed**: shorter than a header, schema id != 1, unknown template, blockLength smaller than the
  schema's, buffer shorter than header + blockLength, or an enum outside its valid values (side, ordType,
  timeInForce, possResend, scope, initialState, targetState, enabled). MassCancel reason must be
  MASS_CANCEL, DISCONNECT or ADMIN. A malformed request does not consume its clOrdId.
- **Dedupe**: a possResend duplicate is dropped silently *before* any validation (so a resend of an
  order whose band has since moved is not rejected). Every non-malformed, non-duplicate NewOrder, Cancel
  and Replace records its (firm, clOrdId) whatever its outcome, so each request id has at most one
  outcome; a plain (non-resend) reuse is `DUPLICATE_CLORDID` at the dedupe step of the validation order.
- **Ids after cancel/replace**: after a replace the old clOrdId refers to nothing (`UNKNOWN_ORDER`);
  after a cancel both the order's id and the cancel's id answer `TOO_LATE_TO_CANCEL`.
- **Cancel validation**: firm known (`UNKNOWN_FIRM`; a disabled firm can cancel), duplicate, order lookup
  (`UNKNOWN_ORDER`, `TOO_LATE_TO_CANCEL`), symbol must match the order's (`UNKNOWN_ORDER`). Allowed in every state.
- **Replace validation**: firm known and enabled (kill switch blocks replaces), symbol, order lookup,
  state (HALTED `SYMBOL_HALTED`, PRE_OPEN/CLOSED `SYMBOL_CLOSED`), quantity, `newQuantity <= cumQty`
  `INVALID_REPLACE`, price (tick and band), duplicate, credit, position. Credit and position are checked
  only when the replace increases notional / open quantity. Same price and quantity not increased keeps
  priority (including an unchanged replace).
- **Market orders**: rejected `NO_LIQUIDITY` (no orderId) when the opposite side is empty at entry;
  otherwise accepted, sweep up to the band edge, remainder `MARKET_COLLAR`. MARKET DAY behaves like IOC.
- **IOC in AUCTION_CALL**: accepted and immediately canceled (`IOC_REMAINDER`).
- **No reference price**: limit orders are not band-checked; market orders `NO_REFERENCE_PRICE`.
- **Band**: computed exactly in integers; low clamped to one tick, high to $10,000,000. `priceBandBps = 0`
  means a band of exactly the reference price. Limits: price <= 1,000,000,000 cents, quantities and
  maxOrderQty <= 1,000,000,000 (keeps price * qty inside a long).
- **ReferencePrice**: unknown/removed symbol `UNKNOWN_SYMBOL`; stale or equal `sourceTsNs` ignored;
  null or non-positive price `INVALID_PRICE`.
- **SymbolDefinition update**: keeps state; `initialState` ignored; its referencePrice is applied only if
  no ReferencePrice command was ever applied to the symbol. A tick size change with resting orders keeps
  them (the level index uses the gcd of old and new tick). Validation: tick/ref `INVALID_PRICE`,
  lot/maxOrderQty `INVALID_QTY`, `initialState = REMOVED` for a new symbol `INVALID_STATE_TRANSITION`.
  A REMOVED symbol is unknown to every command and a SymbolDefinition re-creates it fresh.
- **Auction price**: candidates are every limit price in the book plus the reference price when it is
  on tick (so with equal volume and imbalance across a range containing the reference, the reference
  price is used). Imbalance and its side are measured at the chosen price; no cross: price null, 0, 0,
  side null. An indicative is published only when (price, matched, imbalance, side) changes; entering
  AUCTION_CALL resets it to "no cross".
- **Entering CONTINUOUS with a crossed book** (possible after AUCTION_CALL -> HALTED -> CONTINUOUS): the
  book is uncrossed first, exactly like an auction, with `AuctionUpdate(isFinal=TRUE)`.
- **Market-wide state command** (symbolId null): applies to each non-removed symbol for which the
  transition is allowed; others are skipped silently.
- **Scheduled transitions**: validated (symbol known) when scheduled, applied (and the transition
  checked) when the timer fires. Timer correlation ids are a dense counter from 1 owned by the machine.
  Pending transitions survive DayRollover. They are in the snapshot; `onSnapshotLoaded` does **not**
  re-schedule them (Aeron Cluster restores its timers). A host whose scheduler does not persist timers
  calls `MatcherShard.rescheduleTimers()` after loading.
- **RiskLimitUpdate**: negative budget `CREDIT_LIMIT`, negative max position `POSITION_LIMIT`.
- **MassCancel**: cancels in orderId order. FIRM, SESSION (firm + sessionId), FIRM_SYMBOL and SYMBOL use
  intrusive per-firm/session/firm-symbol/symbol lists (O(k)); GATEWAY scans all live orders.
- **DayRollover** also resets positions and traded notional and leaves REMOVED symbols alone.
- **DayRollover is idempotent per trading date**: a roll whose `tradingDate` is not greater than the last
  applied one (a resend, or a stale roll) is `CommandRejected(INVALID_STATE_TRANSITION)` and changes nothing.
  The last trading date is in the snapshot (format version 2; version 1 snapshots still load, as date 0).
- **Internal error** (an unexpected exception, i.e. a bug): logged, counted (`internalErrors()`), and the
  entry still ends with `CommandApplied`; all replicas behave identically.

## Engineering

- Orders and levels are pooled objects; each order sits in six intrusive lists (level FIFO, global,
  firm, firm-symbol, session, symbol). `(firm, clOrdId)` lookup through an insertion-ordered open-
  addressing table that also serves as the dedupe table; `orderId` lookup through an Agrona map.
- Book side: tick-indexed window (direct array) centred on the reference price plus an overflow map for
  prices outside it, re-centred when the reference price leaves the middle half; plus a sorted array of
  non-empty levels (best last) for O(1) best and cheap inserts near the top.
- Snapshot: hand-written little-endian format v1 with CRC32 (see `SnapshotCodec`), canonical order (no
  dependence on hash layout, pool indices or configured sizes), chunked at `SnapshotSink.MAX_CHUNK_LENGTH`.
  Restore validates structure, checksum and recomputed risk usage.

## Tests

Scenario tests per command and reject reason (`OrderScenarioTest`, `StateAndAuctionTest`, including the
testkit `InMemoryVenue` with cluster-style timers); every harness step re-derives all structural
invariants and checks the stream contract (dense eventSeq, eventIdx, CommandApplied count, hash chain
recomputed independently). `PropertyTest`: randomised runs, determinism across differently sized
instances, snapshot at a random point and continue. `DifferentialTest`: event-by-event equality with a
deliberately simple `ReferenceMatcher`. `MatcherBenchmarkTest`: throughput and allocation.

## Known gaps

- Snapshots serialise the whole state into one heap buffer before chunking (fine for millions of
  orders; not streaming).
- GATEWAY-scope mass cancel is O(live orders).
- Notional sums assume firm budgets well below 2^62 cents; no overflow guard on traded notional.
- No self-trade prevention (by contract).
