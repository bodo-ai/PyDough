SELECT
  sbtxdatetime AS date_time,
  DATETIME(sbtxdatetime, '7 day') AS week_adj1,
  DATETIME(sbtxdatetime, '-7 day') AS week_adj2,
  DATETIME(sbtxdatetime, '1 hour', '14 day') AS week_adj3,
  DATETIME(sbtxdatetime, '-1 second', '14 day') AS week_adj4,
  DATETIME(sbtxdatetime, '1 day', '14 day') AS week_adj5,
  DATETIME(sbtxdatetime, '-1 minute', '14 day') AS week_adj6,
  DATETIME(sbtxdatetime, '1 year', '14 day') AS week_adj8
FROM main.sbtransaction
WHERE
  CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) < 2025
  AND CAST(STRFTIME('%d', sbtxdatetime) AS INTEGER) > 1
