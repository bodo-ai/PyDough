SELECT
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month,
  AVG(sbtxprice) AS avg_price
FROM main.sbtransaction
WHERE
  sbtxdatetime <= CAST('2023-03-31' AS DATE)
  AND sbtxdatetime >= CAST('2023-01-01' AS DATE)
  AND sbtxstatus = 'success'
GROUP BY
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP))
ORDER BY
  month
