WITH _t2 AS (
  SELECT
    COUNT(sbtxcustid) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
), _s0 AS (
  SELECT
    SUM(agg_0) AS agg_0,
    customer_id
  FROM _t2
  GROUP BY
    customer_id
), _t0 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0
  FROM _s0 AS _s0
  JOIN main.sbcustomer AS sbcustomer
    ON _s0.customer_id = sbcustomer.sbcustid
    AND sbcustomer.sbcustjoindate >= DATETIME('now', '-70 day')
)
SELECT
  COALESCE(agg_0, 0) AS transaction_count
FROM _t0
