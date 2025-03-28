WITH _t0 AS (
  SELECT
    AVG(sbtransaction.sbtxprice) AS avg_price,
    DATE(DATETIME(sbtransaction.sbtxdatetime), 'start of month') AS ordering_1,
    DATE(DATETIME(sbtransaction.sbtxdatetime), 'start of month') AS month
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxdatetime <= '2023-03-31'
    AND sbtransaction.sbtxdatetime >= '2023-01-01'
    AND sbtransaction.sbtxstatus = 'success'
  GROUP BY
    DATE(DATETIME(sbtransaction.sbtxdatetime), 'start of month')
)
SELECT
  _t0.month AS month,
  _t0.avg_price AS avg_price
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1
