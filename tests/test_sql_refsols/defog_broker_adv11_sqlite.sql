WITH _t1 AS (
  SELECT
    sbcustomer.sbcustemail AS email,
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
  WHERE
    sbcustomer.sbcustemail LIKE '%.com'
), _table_alias_2 AS (
  SELECT
    _t1._id AS _id
  FROM _t1 AS _t1
), _table_alias_0 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
), _table_alias_1 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
), _t2 AS (
  SELECT
    _table_alias_0.customer_id AS customer_id,
    _table_alias_1.symbol AS symbol
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.ticker_id = _table_alias_1._id
  WHERE
    _table_alias_1.symbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
), _table_alias_3 AS (
  SELECT
    _t2.customer_id AS customer_id
  FROM _t2 AS _t2
), _t0 AS (
  SELECT
    _table_alias_2._id AS _id
  FROM _table_alias_2 AS _table_alias_2
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _table_alias_3 AS _table_alias_3
      WHERE
        _table_alias_2._id = _table_alias_3.customer_id
    )
)
SELECT
  COUNT() AS n_customers
FROM _t0 AS _t0
