WITH _s3 AS (
  SELECT
    SUM(sbtxamount) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  _s0.sbcustname AS name,
  COALESCE(_s3.agg_0, 0) AS total_amount
FROM main.sbcustomer AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.sbcustid = _s3.customer_id
ORDER BY
  total_amount DESC
LIMIT 5
