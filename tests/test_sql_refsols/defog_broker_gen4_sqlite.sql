WITH _s3 AS (
  SELECT
    COUNT(*) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  WHERE
    DATE(sbtxdatetime, 'start of day') = '2023-04-01' AND sbtxtype = 'sell'
  GROUP BY
    sbtxcustid
)
SELECT
  _s0.sbcustid AS _id,
  _s0.sbcustname AS name,
  COALESCE(_s3.agg_0, 0) AS num_tx
FROM main.sbcustomer AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.sbcustid = _s3.customer_id
ORDER BY
  num_tx DESC
LIMIT 1
