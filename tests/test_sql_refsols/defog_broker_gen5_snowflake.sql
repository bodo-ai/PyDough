SELECT
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month,
  AVG(sbtxprice) AS avg_price
FROM MAIN.SBTRANSACTION
WHERE
  QUARTER(CAST(sbtxdatetime AS TIMESTAMP)) = 1
  AND sbtxstatus = 'success'
  AND YEAR(CAST(sbtxdatetime AS TIMESTAMP)) = 2023
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
