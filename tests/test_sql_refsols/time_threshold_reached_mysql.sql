WITH _t3 AS (
  SELECT
    (
      100.0 * SUM(sbtxshares) OVER (PARTITION BY DATE(CAST(sbtxdatetime AS DATETIME)) ORDER BY sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) / SUM(sbtxshares) OVER (PARTITION BY DATE(CAST(sbtxdatetime AS DATETIME))) AS pct_of_day,
    sbtxdatetime AS sbTxDateTime
  FROM main.sbTransaction
  WHERE
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) = 2023
), _t AS (
  SELECT
    sbTxDateTime,
    ROW_NUMBER() OVER (PARTITION BY DATE(CAST(sbTxDateTime AS DATETIME)) ORDER BY CASE WHEN pct_of_day IS NULL THEN 1 ELSE 0 END, pct_of_day) AS _w
  FROM _t3
  WHERE
    pct_of_day >= 50.0
)
SELECT
  sbTxDateTime AS date_time
FROM _t
WHERE
  _w = 1
ORDER BY
  sbTxDateTime
