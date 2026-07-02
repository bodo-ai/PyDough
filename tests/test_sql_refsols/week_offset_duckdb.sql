SELECT
  sbtxdatetime AS date_time,
  CAST(sbtxdatetime AS TIMESTAMP) + 7 * INTERVAL '1' DAY AS week_adj1,
  CAST(sbtxdatetime AS TIMESTAMP) - 7 * INTERVAL '1' DAY AS week_adj2,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1' HOUR + 7 * INTERVAL '2' DAY AS week_adj3,
  CAST(sbtxdatetime AS TIMESTAMP) - INTERVAL '1' SECOND + 7 * INTERVAL '2' DAY AS week_adj4,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1' DAY + 7 * INTERVAL '2' DAY AS week_adj5,
  CAST(sbtxdatetime AS TIMESTAMP) - INTERVAL '1' MINUTE + 7 * INTERVAL '2' DAY AS week_adj6,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1' MONTH + 7 * INTERVAL '2' DAY AS week_adj7,
  CAST(sbtxdatetime AS TIMESTAMP) + INTERVAL '1' YEAR + 7 * INTERVAL '2' DAY AS week_adj8
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) < 2025
