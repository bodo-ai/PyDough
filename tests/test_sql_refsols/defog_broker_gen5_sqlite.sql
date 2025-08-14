SELECT
  DATE(sbtxdatetime, 'start of month') AS month,
  AVG(sbtxprice) AS avg_price
FROM main.sbtransaction
WHERE
  CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) = 2023
  AND CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) IN (1, 2, 3)
  AND sbtxstatus = 'success'
GROUP BY
  DATE(sbtxdatetime, 'start of month')
ORDER BY
  month
