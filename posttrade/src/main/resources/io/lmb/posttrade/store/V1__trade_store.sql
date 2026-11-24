-- Trades (one row per Trade event) and fills (one row per ExecutionReport with execType PARTIAL_FILL/FILL).
-- Primary key (shard_id, event_seq) makes every insert idempotent.
CREATE TABLE pt_trade (
    shard_id       INT          NOT NULL,
    event_seq      BIGINT       NOT NULL,
    exec_id        VARCHAR(64)  NOT NULL,
    match_id       BIGINT       NOT NULL,
    symbol_id      INT          NOT NULL,
    price          BIGINT       NOT NULL,
    quantity       BIGINT       NOT NULL,
    buy_order_id   BIGINT       NOT NULL,
    sell_order_id  BIGINT       NOT NULL,
    buy_firm_id    INT          NOT NULL,
    sell_firm_id   INT          NOT NULL,
    aggressor_side CHAR(1),
    is_auction     BOOLEAN      NOT NULL,
    ts_ns          BIGINT       NOT NULL,
    log_position   BIGINT       NOT NULL,
    PRIMARY KEY (shard_id, event_seq)
);
CREATE INDEX pt_trade_symbol_ts ON pt_trade (symbol_id, ts_ns);
CREATE INDEX pt_trade_match ON pt_trade (shard_id, match_id);

CREATE TABLE pt_fill (
    shard_id     INT          NOT NULL,
    event_seq    BIGINT       NOT NULL,
    exec_id      VARCHAR(64)  NOT NULL,
    match_id     BIGINT       NOT NULL,
    order_id     BIGINT       NOT NULL,
    cl_ord_id    BIGINT       NOT NULL,
    session_id   BIGINT       NOT NULL,
    firm_id      INT          NOT NULL,
    account_id   INT          NOT NULL,
    symbol_id    INT          NOT NULL,
    side         CHAR(1)      NOT NULL,
    last_qty     BIGINT       NOT NULL,
    last_px      BIGINT       NOT NULL,
    leaves_qty   BIGINT       NOT NULL,
    cum_qty      BIGINT       NOT NULL,
    liquidity    CHAR(1)      NOT NULL,
    ts_ns        BIGINT       NOT NULL,
    PRIMARY KEY (shard_id, event_seq)
);
CREATE INDEX pt_fill_firm_ts ON pt_fill (firm_id, ts_ns);

-- Consumer cursors: last eventSeq applied per shard, committed in the same transaction as the rows.
CREATE TABLE pt_cursor (
    consumer   VARCHAR(64) NOT NULL,
    shard_id   INT         NOT NULL,
    event_seq  BIGINT      NOT NULL,
    PRIMARY KEY (consumer, shard_id)
);
