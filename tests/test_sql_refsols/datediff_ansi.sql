SELECT
  sbtxdatetime AS x,
  CAST('2025-05-02 11:00:00' AS TIMESTAMP) AS y1,
  CAST('2023-04-03 13:16:30' AS TIMESTAMP) AS y,
  DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), sbtxdatetime, YEAR) AS years_diff,
  DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), sbtxdatetime, MONTH) AS months_diff,
  DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), sbtxdatetime, DAY) AS days_diff,
  DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), sbtxdatetime, HOUR) AS hours_diff,
  DATEDIFF(CAST('2023-04-03 13:16:30' AS TIMESTAMP), sbtxdatetime, MINUTE) AS minutes_diff,
  DATEDIFF(CAST('2023-04-03 13:16:30' AS TIMESTAMP), sbtxdatetime, SECOND) AS seconds_diff
FROM main.sbtransaction
WHERE
  EXTRACT(YEAR FROM sbtxdatetime) < 2025
ORDER BY
  years_diff
LIMIT 30
