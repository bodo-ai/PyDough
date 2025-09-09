SELECT
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month,
  AVG(CAST(sbtxprice AS DECIMAL)) AS avg_price
FROM main.sbtransaction
WHERE
  EXTRACT(QUARTER FROM CAST(sbtxdatetime AS TIMESTAMP)) = 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) = 2023
  AND sbtxstatus = 'success'
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
