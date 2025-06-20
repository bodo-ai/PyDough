WITH _s3 AS (
  SELECT
    SUM(sbtxamount) AS agg_0,
    COUNT(*) AS agg_1,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  _s0.sbcustname AS name,
  _s3.agg_1 AS num_tx,
  COALESCE(_s3.agg_0, 0) AS total_amount,
  RANK() OVER (ORDER BY COALESCE(_s3.agg_0, 0) DESC NULLS FIRST) AS cust_rank
FROM main.sbcustomer AS _s0
JOIN _s3 AS _s3
  ON _s0.sbcustid = _s3.customer_id
