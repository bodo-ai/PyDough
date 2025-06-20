WITH _s3 AS (
  SELECT
    MIN(sbtxdatetime) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  _s0.sbcustid AS cust_id,
  DATEDIFF(_s3.agg_0, _s0.sbcustjoindate, SECOND) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbcustomer AS _s0
JOIN _s3 AS _s3
  ON _s0.sbcustid = _s3.customer_id
