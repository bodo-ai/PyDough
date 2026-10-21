SELECT
  sbtxdatetime AS date_time,
  DATE_ADD(WEEK, 1, CAST(sbtxdatetime AS TIMESTAMP)) AS week_adj1,
  DATE_ADD(CAST(sbtxdatetime AS TIMESTAMP), -7) AS week_adj2,
  DATE_ADD(WEEK, 2, CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1' HOUR) AS week_adj3,
  DATE_ADD(WEEK, 2, CAST(sbtxdatetime AS TIMESTAMP) - INTERVAL '1' SECOND) AS week_adj4,
  DATE_ADD(WEEK, 2, DATE_ADD(DAY, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj5,
  DATE_ADD(WEEK, 2, CAST(sbtxdatetime AS TIMESTAMP) - INTERVAL '1' MINUTE) AS week_adj6,
  DATE_ADD(WEEK, 2, DATE_ADD(MONTH, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj7,
  DATE_ADD(WEEK, 2, DATE_ADD(YEAR, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj8
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) < 2025
