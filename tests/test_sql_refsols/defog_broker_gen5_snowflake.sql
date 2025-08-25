SELECT
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month,
  AVG(sbtxprice) AS avg_price
FROM main.sbtransaction
WHERE
  QUARTER(CAST(sbtxdatetime AS TIMESTAMP)) = 1
  AND YEAR(CAST(sbtxdatetime AS TIMESTAMP)) = 2023
  AND sbtxstatus = 'success'
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
