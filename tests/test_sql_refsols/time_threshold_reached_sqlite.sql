WITH _t2 AS (
  SELECT
    CAST((
      100.0 * SUM(sbtxshares) OVER (PARTITION BY DATE(sbtxdatetime, 'start of day') ORDER BY sbtxdatetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS REAL) / SUM(sbtxshares) OVER (PARTITION BY DATE(sbtxdatetime, 'start of day')) AS pct_of_day,
    sbtxdatetime AS date_time,
    DATE(sbtxdatetime, 'start of day') AS txn_day
  FROM main.sbtransaction
  WHERE
    CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) = 2023
), _t AS (
  SELECT
    date_time,
    ROW_NUMBER() OVER (PARTITION BY txn_day ORDER BY pct_of_day) AS _w
  FROM _t2
  WHERE
    pct_of_day >= 50.0
)
SELECT
  date_time
FROM _t
WHERE
  _w = 1
ORDER BY
  date_time
