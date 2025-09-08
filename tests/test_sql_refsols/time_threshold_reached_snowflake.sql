WITH _t3 AS (
  SELECT
    sbtxdatetime,
    (
      100.0 * SUM(sbtxshares) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP)) ORDER BY sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) / SUM(sbtxshares) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP))) AS pct_of_day
  FROM main.sbtransaction
  WHERE
    YEAR(CAST(sbtxdatetime AS TIMESTAMP)) = 2023
), _t1 AS (
  SELECT
    sbtxdatetime
  FROM _t3
  WHERE
    pct_of_day >= 50.0
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP)) ORDER BY pct_of_day) = 1
)
SELECT
  sbtxdatetime AS date_time
FROM _t1
ORDER BY
  1 NULLS FIRST
