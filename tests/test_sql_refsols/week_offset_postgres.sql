SELECT
  sbtxdatetime AS date_time,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1 WEEK' AS week_adj1,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1 WEEK' AS week_adj2,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1 HOUR' + INTERVAL '2 WEEK' AS week_adj3,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1 SECOND' + INTERVAL '2 WEEK' AS week_adj4,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1 DAY' + INTERVAL '2 WEEK' AS week_adj5,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1 MINUTE' + INTERVAL '2 WEEK' AS week_adj6,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1 MONTH' + INTERVAL '2 WEEK' AS week_adj7,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1 YEAR' + INTERVAL '2 WEEK' AS week_adj8
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) < 2025
