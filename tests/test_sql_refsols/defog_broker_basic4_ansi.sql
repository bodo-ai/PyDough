WITH _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcuststate AS state
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
), _table_alias_2 AS (
  SELECT
    _table_alias_0.state AS state,
    _table_alias_1.ticker_id AS ticker_id
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0._id = _table_alias_1.customer_id
), _table_alias_3 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickertype AS ticker_type
  FROM main.sbticker AS sbticker
), _t2 AS (
  SELECT
    COUNT() AS agg_0,
    _table_alias_2.state AS state,
    _table_alias_3.ticker_type AS ticker_type
  FROM _table_alias_2 AS _table_alias_2
  JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2.ticker_id = _table_alias_3._id
  GROUP BY
    _table_alias_2.state,
    _table_alias_3.ticker_type
), _t0 AS (
  SELECT
    COALESCE(_t2.agg_0, 0) AS num_transactions,
    COALESCE(_t2.agg_0, 0) AS ordering_1,
    _t2.state AS state,
    _t2.ticker_type AS ticker_type
  FROM _t2 AS _t2
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
SELECT
  _t0.state AS state,
  _t0.ticker_type AS ticker_type,
  _t0.num_transactions AS num_transactions
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC
