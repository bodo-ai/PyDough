SELECT
  sbtransaction.sbtxdatetime AS date_time,
  DATE_ADD(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP), 1, 'WEEK') AS week_adj1,
  DATE_ADD(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP), -1, 'WEEK') AS week_adj2,
  DATE_ADD(DATE_ADD(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP), 1, 'HOUR'), 2, 'WEEK') AS week_adj3,
  DATE_ADD(DATE_ADD(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP), -1, 'SECOND'), 2, 'WEEK') AS week_adj4,
  DATE_ADD(DATE_ADD(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP), 1, 'DAY'), 2, 'WEEK') AS week_adj5,
  DATE_ADD(DATE_ADD(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP), -1, 'MINUTE'), 2, 'WEEK') AS week_adj6,
  DATE_ADD(DATE_ADD(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP), 1, 'MONTH'), 2, 'WEEK') AS week_adj7,
  DATE_ADD(DATE_ADD(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP), 1, 'YEAR'), 2, 'WEEK') AS week_adj8
FROM main.sbtransaction AS sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) < 2025
