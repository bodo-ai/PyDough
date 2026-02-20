SELECT
  TRUNC(CAST(sbtxdatetime AS TIMESTAMP), 'MONTH') AS month,
  AVG(sbtxprice) AS avg_price
FROM MAIN.SBTRANSACTION
WHERE
  EXTRACT(MONTH FROM CAST(sbtxdatetime AS DATE)) IN (1, 2, 3)
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATE)) = 2023
  AND sbtxstatus = 'success'
GROUP BY
  TRUNC(CAST(sbtxdatetime AS TIMESTAMP), 'MONTH')
ORDER BY
  1 NULLS FIRST
