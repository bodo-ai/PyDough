SELECT
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month,
  AVG(sbtxprice) AS avg_price
FROM MAIN.SBTRANSACTION
WHERE
  QUARTER(sbtxdatetime) = 1 AND sbtxstatus = 'success' AND YEAR(sbtxdatetime) = 2023
GROUP BY
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP))
ORDER BY
  MONTH NULLS FIRST
