WITH _s3 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(sbtxstatus = 'success') AS agg_1,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  _s0.sbcustname AS name,
  (
    100.0 * COALESCE(_s3.agg_1, 0)
  ) / COALESCE(_s3.agg_0, 0) AS success_rate
FROM main.sbcustomer AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.sbcustid = _s3.customer_id
WHERE
  NOT _s3.agg_0 IS NULL AND _s3.agg_0 >= 5
ORDER BY
  success_rate
