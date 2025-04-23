WITH _t2 AS (
  SELECT
    (
      100.0 * SUM(sbtxshares) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP)) ORDER BY sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) / SUM(sbtxshares) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP))) AS pct_of_day,
    sbtxdatetime AS date_time,
    DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP)) AS txn_day
  FROM main.sbtransaction
  WHERE
    EXTRACT(YEAR FROM sbtxdatetime) = 2023
), _t0 AS (
  SELECT
    date_time
  FROM _t2
  WHERE
    pct_of_day >= 50.0
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY txn_day ORDER BY pct_of_day NULLS LAST) = 1
)
SELECT
  date_time
FROM _t0
ORDER BY
  date_time
