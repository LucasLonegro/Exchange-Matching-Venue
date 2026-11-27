-- LMB control plane schema. Portable subset of PostgreSQL and H2. Idempotent (IF NOT EXISTS).
-- Times are epoch milliseconds (BIGINT) unless the column ends in _ns. Money is integer cents.

-- Set once, by whichever instance bootstrapped the database from the CSV files.
CREATE TABLE IF NOT EXISTS bootstrap_marker (
  id INTEGER PRIMARY KEY,
  source VARCHAR(1000) NOT NULL,
  created_ms BIGINT NOT NULL);

-- Reference data: symbols. status ACTIVE | REMOVED. operator_state NULL | HALTED | AUCTION_CALL
-- (an operator override that wins over the trading calendar).
CREATE TABLE IF NOT EXISTS symbol (
  symbol_id INTEGER PRIMARY KEY,
  ticker VARCHAR(8) NOT NULL UNIQUE,
  shard_id INTEGER NOT NULL,
  tick_size BIGINT NOT NULL,
  lot_size BIGINT NOT NULL,
  reference_price BIGINT NOT NULL,
  price_band_bps INTEGER NOT NULL,
  max_order_qty BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL,
  operator_state VARCHAR(16),
  updated_ms BIGINT NOT NULL,
  -- dynamic collar half-width around the last trade (0 = off) and volatility auction length;
  -- defaults match io.lmb.common.SymbolDirectory (collar off, 5 minutes)
  dynamic_band_bps INTEGER DEFAULT 0 NOT NULL,
  volatility_auction_ns BIGINT DEFAULT 300000000000 NOT NULL);

-- Participants.
CREATE TABLE IF NOT EXISTS firm (
  firm_id INTEGER PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  api_key VARCHAR(200) NOT NULL,
  enabled BOOLEAN NOT NULL,
  updated_ms BIGINT NOT NULL);

CREATE TABLE IF NOT EXISTS firm_session (
  session_id BIGINT PRIMARY KEY,
  firm_id INTEGER NOT NULL REFERENCES firm (firm_id));

-- Firm-wide limits; the allocator splits credit_limit into per-shard budgets.
CREATE TABLE IF NOT EXISTS firm_limit (
  firm_id INTEGER PRIMARY KEY REFERENCES firm (firm_id),
  credit_limit BIGINT NOT NULL,
  max_position_per_symbol BIGINT NOT NULL,
  updated_ms BIGINT NOT NULL);

-- Per-date overrides of the default calendar (holidays, half days). Times 'HH:MM', NULL = default, '-' = no such phase.
CREATE TABLE IF NOT EXISTS trading_calendar (
  trading_date INTEGER PRIMARY KEY,
  is_trading_day BOOLEAN NOT NULL,
  pre_open VARCHAR(5),
  open_auction VARCHAR(5),
  continuous VARCHAR(5),
  close_auction VARCHAR(5),
  close_time VARCHAR(5),
  day_roll VARCHAR(5),
  note VARCHAR(200),
  updated_ms BIGINT NOT NULL);

-- Calendar transitions already sent ahead of time as timer commands (one row per date, phase, shard).
CREATE TABLE IF NOT EXISTS schedule_sent (
  trading_date INTEGER NOT NULL,
  phase VARCHAR(16) NOT NULL,
  shard_id INTEGER NOT NULL,
  correlation_id BIGINT NOT NULL,
  effective_ns BIGINT NOT NULL,
  sent_ms BIGINT NOT NULL,
  PRIMARY KEY (trading_date, phase, shard_id));

-- Allocator targets per firm and shard (what it is steering towards; confirmed values are in shard_firm_state).
CREATE TABLE IF NOT EXISTS risk_budget (
  firm_id INTEGER NOT NULL,
  shard_id INTEGER NOT NULL,
  target_budget BIGINT NOT NULL,
  updated_ms BIGINT NOT NULL,
  PRIMARY KEY (firm_id, shard_id));

-- Per-shard event stream cursor plus shard-level observed state, written by the leader.
CREATE TABLE IF NOT EXISTS shard_cursor (
  shard_id INTEGER PRIMARY KEY,
  event_seq BIGINT NOT NULL,
  trading_date INTEGER NOT NULL,
  last_event_ts_ns BIGINT NOT NULL,
  connected BOOLEAN NOT NULL,
  caught_up BOOLEAN NOT NULL,
  updated_ms BIGINT NOT NULL);

-- What each shard has confirmed through its event stream (SymbolDefined / SymbolStateChanged / DayRolled).
CREATE TABLE IF NOT EXISTS shard_symbol_state (
  shard_id INTEGER NOT NULL,
  symbol_id INTEGER NOT NULL,
  ticker VARCHAR(8) NOT NULL,
  tick_size BIGINT NOT NULL,
  lot_size BIGINT NOT NULL,
  reference_price BIGINT NOT NULL,
  price_band_bps INTEGER NOT NULL,
  max_order_qty BIGINT NOT NULL,
  state VARCHAR(16) NOT NULL,
  last_correlation_id BIGINT NOT NULL,
  event_seq BIGINT NOT NULL,
  -- the collar parameters as echoed by SymbolDefined
  dynamic_band_bps INTEGER DEFAULT 0 NOT NULL,
  volatility_auction_ns BIGINT DEFAULT 0 NOT NULL,
  -- TRUE while the symbol is in a matcher-initiated volatility auction (SymbolStateChanged cause
  -- VOLATILITY_INTERRUPTION, cleared by any later state change); since_ns = cluster time it started
  volatility_auction BOOLEAN DEFAULT FALSE NOT NULL,
  volatility_auction_since_ns BIGINT DEFAULT 0 NOT NULL,
  -- TRUE once the venue owns the reference price (a ReferencePriceApplied seen since the symbol was defined,
  -- or a SymbolDefined echo that kept its own reference over the one sent). From then on reference_price is
  -- the venue's, and the listing price in symbol.reference_price is neither compared nor pushed.
  venue_reference BOOLEAN DEFAULT FALSE NOT NULL,
  -- static band of the last ReferencePriceApplied (NULL until one is seen)
  band_low BIGINT,
  band_high BIGINT,
  -- last ClosingPrice; close_source NONE | AUCTION | LAST_TRADE | PREVIOUS_CLOSE, NULL = none seen yet
  close_price BIGINT,
  previous_close BIGINT,
  close_source VARCHAR(16),
  close_trading_date INTEGER,
  PRIMARY KEY (shard_id, symbol_id));

-- Last RiskLimitApplied per firm and shard.
CREATE TABLE IF NOT EXISTS shard_firm_state (
  shard_id INTEGER NOT NULL,
  firm_id INTEGER NOT NULL,
  credit_budget BIGINT NOT NULL,
  credit_used BIGINT NOT NULL,
  max_position_per_symbol BIGINT NOT NULL,
  enabled BOOLEAN NOT NULL,
  last_correlation_id BIGINT NOT NULL,
  event_seq BIGINT NOT NULL,
  observed_ts_ns BIGINT NOT NULL,
  PRIMARY KEY (shard_id, firm_id));

-- Every command sent to a shard, written BEFORE it is offered (write-ahead, fenced by the leader lease).
-- status PENDING | SENT | SCHEDULED | CONFIRMED | REJECTED | SUPERSEDED | LOST | EXPIRED | FAILED
CREATE TABLE IF NOT EXISTS command_log (
  correlation_id BIGINT PRIMARY KEY,
  shard_id INTEGER NOT NULL,
  kind VARCHAR(24) NOT NULL,
  cmd_key VARCHAR(80) NOT NULL,
  origin VARCHAR(16) NOT NULL,
  detail VARCHAR(1000) NOT NULL,
  payload_hex VARCHAR(1024) NOT NULL,
  budget BIGINT,
  effective_ns BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL,
  attempts INTEGER NOT NULL,
  reject_reason VARCHAR(40),
  audit_id BIGINT,
  leader_epoch BIGINT NOT NULL,
  created_ms BIGINT NOT NULL,
  updated_ms BIGINT NOT NULL);
CREATE INDEX IF NOT EXISTS command_log_status_idx ON command_log (status);
CREATE INDEX IF NOT EXISTS command_log_audit_idx ON command_log (audit_id);

-- Operator audit log: every admin request, who asked, and the outcome.
-- status PENDING | EXECUTING | COMPLETED | PARTIAL | REJECTED | FAILED
CREATE TABLE IF NOT EXISTS audit_log (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  created_ms BIGINT NOT NULL,
  actor VARCHAR(100) NOT NULL,
  instance_id VARCHAR(64) NOT NULL,
  action VARCHAR(40) NOT NULL,
  params VARCHAR(2000) NOT NULL,
  status VARCHAR(16) NOT NULL,
  outcome VARCHAR(2000),
  correlation_ids VARCHAR(2000),
  executed_by VARCHAR(64),
  updated_ms BIGINT NOT NULL);
CREATE INDEX IF NOT EXISTS audit_log_status_idx ON audit_log (status);

-- Leadership lease (one row per lease name).
CREATE TABLE IF NOT EXISTS leader_lease (
  name VARCHAR(64) PRIMARY KEY,
  holder VARCHAR(128),
  holder_url VARCHAR(256),
  epoch BIGINT NOT NULL,
  heartbeat BIGINT NOT NULL,
  fence BIGINT NOT NULL,
  renewed_ms BIGINT NOT NULL);

-- Correlation ids are allocated in blocks of 4096 from this sequence.
CREATE SEQUENCE IF NOT EXISTS correlation_block_seq START WITH 1 INCREMENT BY 1;
