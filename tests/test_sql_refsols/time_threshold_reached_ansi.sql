WITH _t3 AS (
  SELECT
    (
      100.0 * SUM(sbtxshares) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP)) ORDER BY sbtxdatetime NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) / SUM(sbtxshares) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP))) AS pct_of_day,
    DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP)) AS txn_day_1,
    sbtxdatetime
  FROM main.sbtransaction
  WHERE
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) = 2023
), _t1 AS (
  SELECT
    sbtxdatetime
  FROM _t3
  WHERE
    pct_of_day >= 50.0
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY txn_day_1 ORDER BY pct_of_day NULLS LAST) = 1
)
SELECT
  sbtxdatetime AS date_time
FROM _t1
ORDER BY
  sbtxdatetime
