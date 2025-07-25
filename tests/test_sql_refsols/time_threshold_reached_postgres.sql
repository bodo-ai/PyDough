WITH _t3 AS (
  SELECT
    CAST((
      100.0 * SUM(sbtxshares) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP)) ORDER BY sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS DOUBLE PRECISION) / SUM(sbtxshares) OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP))) AS pct_of_day,
    sbtxdatetime
  FROM main.sbtransaction
  WHERE
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) = 2023
), _t AS (
  SELECT
    sbtxdatetime,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('DAY', CAST(sbtxdatetime AS TIMESTAMP)) ORDER BY pct_of_day) AS _w
  FROM _t3
  WHERE
    pct_of_day >= 50.0
)
SELECT
  sbtxdatetime AS date_time
FROM _t
WHERE
  _w = 1
ORDER BY
  sbtxdatetime NULLS FIRST
