SELECT
  sbtxdatetime AS date_time,
  DATE_ADD(CAST(sbtxdatetime AS DATETIME), INTERVAL '1' WEEK) AS week_adj1,
  DATE_ADD(CAST(sbtxdatetime AS DATETIME), INTERVAL '-1' WEEK) AS week_adj2,
  DATE_ADD(DATE_ADD(CAST(sbtxdatetime AS DATETIME), INTERVAL '1' HOUR), INTERVAL '2' WEEK) AS week_adj3,
  DATE_ADD(DATE_ADD(CAST(sbtxdatetime AS DATETIME), INTERVAL '-1' SECOND), INTERVAL '2' WEEK) AS week_adj4,
  DATE_ADD(DATE_ADD(CAST(sbtxdatetime AS DATETIME), INTERVAL '1' DAY), INTERVAL '2' WEEK) AS week_adj5,
  DATE_ADD(DATE_ADD(CAST(sbtxdatetime AS DATETIME), INTERVAL '-1' MINUTE), INTERVAL '2' WEEK) AS week_adj6,
  DATE_ADD(DATE_ADD(CAST(sbtxdatetime AS DATETIME), INTERVAL '1' MONTH), INTERVAL '2' WEEK) AS week_adj7,
  DATE_ADD(DATE_ADD(CAST(sbtxdatetime AS DATETIME), INTERVAL '1' YEAR), INTERVAL '2' WEEK) AS week_adj8
FROM main.sbTransaction
WHERE
  DAY(sbtxdatetime) > 1 AND YEAR(sbtxdatetime) < 2025
