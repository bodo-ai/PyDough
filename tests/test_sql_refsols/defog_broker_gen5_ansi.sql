SELECT
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month,
  AVG(sbtxprice) AS avg_price
FROM main.sbtransaction
WHERE
  EXTRACT(QUARTER FROM sbtxdatetime) = 1
  AND EXTRACT(YEAR FROM sbtxdatetime) = 2023
  AND sbtxstatus = 'success'
GROUP BY
  DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP))
ORDER BY
  month
