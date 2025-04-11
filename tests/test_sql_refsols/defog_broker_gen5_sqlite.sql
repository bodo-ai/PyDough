SELECT
  DATE(sbtxdatetime, 'start of month') AS month,
  AVG(sbtxprice) AS avg_price
FROM main.sbtransaction
WHERE
  sbtxdatetime <= '2023-03-31'
  AND sbtxdatetime >= '2023-01-01'
  AND sbtxstatus = 'success'
GROUP BY
  DATE(sbtxdatetime, 'start of month')
ORDER BY
  month
