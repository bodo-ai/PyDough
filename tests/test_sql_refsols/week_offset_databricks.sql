SELECT
  sbtxdatetime AS date_time,
  DATEADD(WEEK, 1, CAST(sbtxdatetime AS TIMESTAMP)) AS week_adj1,
  DATEADD(DAY, -7, CAST(sbtxdatetime AS TIMESTAMP)) AS week_adj2,
  DATEADD(WEEK, 2, CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1' HOUR) AS week_adj3,
  DATEADD(WEEK, 2, CAST(sbtxdatetime AS TIMESTAMP) - INTERVAL '1' SECOND) AS week_adj4,
  DATEADD(WEEK, 2, DATEADD(DAY, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj5,
  DATEADD(WEEK, 2, CAST(sbtxdatetime AS TIMESTAMP) - INTERVAL '1' MINUTE) AS week_adj6,
  DATEADD(WEEK, 2, DATEADD(MONTH, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj7,
  DATEADD(WEEK, 2, DATEADD(YEAR, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj8
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) < 2025
