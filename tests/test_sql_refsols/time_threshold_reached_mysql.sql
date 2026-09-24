WITH _t3 AS (
  SELECT
    sbTxDateTime,
    (
      100.0 * SUM(sbTxShares) OVER (
        PARTITION BY CAST(CAST(sbTxDateTime AS DATETIME) AS DATE)
        ORDER BY sbTxDateTime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    ) / SUM(sbTxShares) OVER (PARTITION BY CAST(CAST(sbTxDateTime AS DATETIME) AS DATE)) AS pct_of_day
  FROM main.sbTransaction
  WHERE
    EXTRACT(YEAR FROM CAST(sbTxDateTime AS DATETIME)) = 2023
), _t AS (
  SELECT
    sbTxDateTime,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(CAST(sbTxDateTime AS DATETIME) AS DATE)
      ORDER BY CASE WHEN pct_of_day IS NULL THEN 1 ELSE 0 END, pct_of_day
    ) AS _w
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
  1
