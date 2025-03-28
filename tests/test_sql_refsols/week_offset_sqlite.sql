SELECT
  sbtransaction.sbtxdatetime AS date_time,
  DATETIME(DATETIME(sbtransaction.sbtxdatetime), '7 day') AS week_adj1,
  DATETIME(DATETIME(sbtransaction.sbtxdatetime), '-7 day') AS week_adj2,
  DATETIME(DATETIME(DATETIME(sbtransaction.sbtxdatetime), '1 hour'), '14 day') AS week_adj3,
  DATETIME(DATETIME(DATETIME(sbtransaction.sbtxdatetime), '-1 second'), '14 day') AS week_adj4,
  DATETIME(DATETIME(DATETIME(sbtransaction.sbtxdatetime), '1 day'), '14 day') AS week_adj5,
  DATETIME(DATETIME(DATETIME(sbtransaction.sbtxdatetime), '-1 minute'), '14 day') AS week_adj6,
  DATETIME(DATETIME(DATETIME(sbtransaction.sbtxdatetime), '1 month'), '14 day') AS week_adj7,
  DATETIME(DATETIME(DATETIME(sbtransaction.sbtxdatetime), '1 year'), '14 day') AS week_adj8
FROM main.sbtransaction AS sbtransaction
WHERE
  CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER) < 2025
  AND CAST(STRFTIME('%d', sbtransaction.sbtxdatetime) AS INTEGER) > 1
