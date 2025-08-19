SELECT
  DATE(sbtxdatetime, 'start of month') AS month,
  AVG(sbtxprice) AS avg_price
FROM main.sbtransaction
WHERE
  CASE
    WHEN CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', sbtxdatetime) AS INTEGER) >= 10
    THEN 4
  END = 1
  AND CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) = 2023
  AND sbtxstatus = 'success'
GROUP BY
  1
ORDER BY
  month
