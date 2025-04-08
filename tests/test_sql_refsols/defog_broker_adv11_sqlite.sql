WITH _t1 AS (
  SELECT
    sbcustomer.sbcustemail AS email,
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
  WHERE
    sbcustomer.sbcustemail LIKE '%.com'
), _s2 AS (
  SELECT
    _t1._id AS _id
  FROM _t1 AS _t1
), _s0 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxtickerid AS ticker_id
  FROM main.sbtransaction AS sbtransaction
), _s1 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
), _t2 AS (
  SELECT
    _s0.customer_id AS customer_id,
    _s1.symbol AS symbol
  FROM _s0 AS _s0
  LEFT JOIN _s1 AS _s1
    ON _s0.ticker_id = _s1._id
  WHERE
    _s1.symbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
), _s3 AS (
  SELECT
    _t2.customer_id AS customer_id
  FROM _t2 AS _t2
), _t0 AS (
  SELECT
    1 AS _
  FROM _s2 AS _s2
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s3 AS _s3
      WHERE
        _s2._id = _s3.customer_id
    )
)
SELECT
  COUNT() AS n_customers
FROM _t0 AS _t0
