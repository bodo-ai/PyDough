WITH _t0 AS (
  SELECT
    AVG(sbtxprice) AS avg_price,
    DATE(sbtxdatetime, 'start of month') AS ordering_1,
    DATE(sbtxdatetime, 'start of month') AS month
  FROM main.sbtransaction
  WHERE
    sbtxdatetime <= '2023-03-31'
    AND sbtxdatetime >= '2023-01-01'
    AND sbtxstatus = 'success'
  GROUP BY
    DATE(sbtxdatetime, 'start of month')
)
SELECT
  month,
  avg_price
FROM _t0
ORDER BY
  ordering_1
