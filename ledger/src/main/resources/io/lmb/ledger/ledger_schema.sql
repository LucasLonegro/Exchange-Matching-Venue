-- Ledger schema, applied at every start (idempotent). PostgreSQL in production, H2 (PostgreSQL mode) locally.
-- Prices and amounts are integer cents, quantities shares. A NULL price means no price. Trading dates are yyyymmdd
-- as the venue sends them (0 = before the first DayRollover).

-- Consumed position per shard and the shard's current trading date; updated in the same transaction as the state.
CREATE TABLE IF NOT EXISTS ledger_cursor (
    shard_id     INT    NOT NULL PRIMARY KEY,
    event_seq    BIGINT NOT NULL,
    trading_date INT    NOT NULL
);

-- Firms seen on the stream (RiskLimitApplied, or a trade). A firm has a statement for every trading date from
-- first_trading_date: the earliest trading date on which any shard saw it.
CREATE TABLE IF NOT EXISTS ledger_firm (
    firm_id            INT NOT NULL PRIMARY KEY,
    first_trading_date INT NOT NULL
);

-- Symbols seen on the stream (SymbolDefined). close_* = the last ClosingPrice of the current trading day (not yet
-- official); mark_* = the last official close, the price positions are marked at.
CREATE TABLE IF NOT EXISTS ledger_symbol (
    symbol_id    INT         NOT NULL PRIMARY KEY,
    shard_id     INT         NOT NULL,
    ticker       VARCHAR(8)  NOT NULL,
    removed      BOOLEAN     NOT NULL,
    close_price  BIGINT,
    close_source VARCHAR(16),
    close_date   INT,
    mark_price   BIGINT,
    mark_source  VARCHAR(16),
    mark_date    INT
);

-- Current position per firm and symbol (average cost; cash = realised_pnl - cost_basis), plus the running totals
-- of the current trading day.
CREATE TABLE IF NOT EXISTS ledger_position (
    firm_id               INT    NOT NULL,
    symbol_id             INT    NOT NULL,
    shard_id              INT    NOT NULL,
    quantity              BIGINT NOT NULL,
    cost_basis            BIGINT NOT NULL,
    realised_pnl          BIGINT NOT NULL,
    cash                  BIGINT NOT NULL,
    day_open_qty          BIGINT NOT NULL,
    day_open_market_value BIGINT NOT NULL,
    day_bought_qty        BIGINT NOT NULL,
    day_sold_qty          BIGINT NOT NULL,
    day_buy_notional      BIGINT NOT NULL,
    day_sell_notional     BIGINT NOT NULL,
    day_realised_pnl      BIGINT NOT NULL,
    day_trade_count       INT    NOT NULL,
    last_event_seq        BIGINT NOT NULL,
    PRIMARY KEY (firm_id, symbol_id)
);

-- A shard's trading day that ended (DayRolled), with the position of its DayRolled event.
CREATE TABLE IF NOT EXISTS ledger_day (
    shard_id         INT    NOT NULL,
    trading_date     INT    NOT NULL,
    closed_event_seq BIGINT NOT NULL,
    closed_ts_ns     BIGINT NOT NULL,
    PRIMARY KEY (shard_id, trading_date)
);

-- Daily statement line per firm and symbol: every position that exists when its shard rolls the day, marked at the
-- official close. pnl_day = cash_flow_day + market_value - the previous day's market_value.
CREATE TABLE IF NOT EXISTS ledger_daily_position (
    trading_date       INT         NOT NULL,
    firm_id            INT         NOT NULL,
    symbol_id          INT         NOT NULL,
    shard_id           INT         NOT NULL,
    open_qty           BIGINT      NOT NULL,
    bought_qty         BIGINT      NOT NULL,
    sold_qty           BIGINT      NOT NULL,
    close_qty          BIGINT      NOT NULL,
    buy_notional       BIGINT      NOT NULL,
    sell_notional      BIGINT      NOT NULL,
    cost_basis         BIGINT      NOT NULL,
    close_price        BIGINT,
    close_source       VARCHAR(16) NOT NULL,
    market_value       BIGINT      NOT NULL,
    unrealised_pnl     BIGINT      NOT NULL,
    realised_pnl_day   BIGINT      NOT NULL,
    realised_pnl_total BIGINT      NOT NULL,
    pnl_day            BIGINT      NOT NULL,
    cash_flow_day      BIGINT      NOT NULL,
    cash_total         BIGINT      NOT NULL,
    trade_count        INT         NOT NULL,
    PRIMARY KEY (trading_date, firm_id, symbol_id)
);

-- Daily statement per firm: the sum of its ledger_daily_position lines, written once every shard has ended the day.
-- equity = cash_balance + market_value = realised_pnl_total + unrealised_pnl.
CREATE TABLE IF NOT EXISTS ledger_statement (
    trading_date       INT    NOT NULL,
    firm_id            INT    NOT NULL,
    cash_flow_day      BIGINT NOT NULL,
    cash_balance       BIGINT NOT NULL,
    market_value       BIGINT NOT NULL,
    equity             BIGINT NOT NULL,
    realised_pnl_day   BIGINT NOT NULL,
    realised_pnl_total BIGINT NOT NULL,
    unrealised_pnl     BIGINT NOT NULL,
    pnl_day            BIGINT NOT NULL,
    buy_notional       BIGINT NOT NULL,
    sell_notional      BIGINT NOT NULL,
    trade_count        INT    NOT NULL,
    open_positions     INT    NOT NULL,
    PRIMARY KEY (trading_date, firm_id)
);

-- Trading dates whose statements are written (also for a day without firms).
CREATE TABLE IF NOT EXISTS ledger_statement_day (
    trading_date  INT    NOT NULL PRIMARY KEY,
    firm_count    INT    NOT NULL,
    closed_ts_ns  BIGINT NOT NULL
);
