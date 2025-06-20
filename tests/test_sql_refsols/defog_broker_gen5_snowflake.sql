SELECT
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month,
  AVG(sbtxprice) AS avg_price
FROM MAIN.SBTRANSACTION
WHERE
  DATE_PART(QUARTER, sbtxdatetime) = 1
  AND DATE_PART(YEAR, sbtxdatetime) = 2023
  AND sbtxstatus = 'success'
GROUP BY
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP))
ORDER BY
  MONTH NULLS FIRST
