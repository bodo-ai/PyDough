WITH _s0 AS (
  SELECT
    COUNT(sbtxcustid) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  SUM(_s0.agg_0) AS transaction_count
FROM _s0 AS _s0
JOIN main.sbcustomer AS sbcustomer
  ON _s0.customer_id = sbcustomer.sbcustid
  AND sbcustomer.sbcustjoindate >= DATETIME('now', '-70 day')
