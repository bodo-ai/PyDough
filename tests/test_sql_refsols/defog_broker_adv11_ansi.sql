WITH _t1 AS (
  SELECT
    sbcustomer.sbcustemail AS email,
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
  WHERE
    sbcustomer.sbcustemail LIKE '%.com'
), _t2 AS (
  SELECT
    _t1._id AS _id
  FROM _t1 AS _t1
), _t0 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
), _t1_2 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
), _t2_2 AS (
  SELECT
    _t0.customer_id AS customer_id,
    _t1.symbol AS symbol
  FROM _t0 AS _t0
  LEFT JOIN _t1_2 AS _t1
    ON _t0.ticker_id = _t1._id
  WHERE
    _t1.symbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
), _t3 AS (
  SELECT
    _t2.customer_id AS customer_id
  FROM _t2_2 AS _t2
), _t0_2 AS (
  SELECT
    1 AS _
  FROM _t2 AS _t2
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _t3 AS _t3
      WHERE
        _t2._id = _t3.customer_id
    )
)
SELECT
  COUNT() AS n_customers
FROM _t0_2 AS _t0
