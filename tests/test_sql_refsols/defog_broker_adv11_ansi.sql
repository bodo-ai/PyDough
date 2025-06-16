WITH _t1 AS (
  SELECT
    sbcustomer.sbcustemail AS sbcustemail,
    sbcustomer.sbcustid AS sbcustid
  FROM main.sbcustomer AS sbcustomer
), _s2 AS (
  SELECT
    _t1.sbcustid AS _id
  FROM _t1 AS _t1
  WHERE
    _t1.sbcustemail LIKE '%.com'
), _s0 AS (
  SELECT
    sbtransaction.sbtxcustid AS sbtxcustid,
    sbtransaction.sbtxtickerid AS sbtxtickerid
  FROM main.sbtransaction AS sbtransaction
), _t2 AS (
  SELECT
    sbticker.sbtickerid AS sbtickerid,
    sbticker.sbtickersymbol AS sbtickersymbol
  FROM main.sbticker AS sbticker
), _s1 AS (
  SELECT
    _t2.sbtickerid AS _id
  FROM _t2 AS _t2
  WHERE
    _t2.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
), _s3 AS (
  SELECT
    _s0.sbtxcustid AS customer_id
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.sbtxtickerid = _s1._id
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
