SELECT
  sbtxdatetime AS date_time,
  DATE_ADD('DAY', 7, CAST(sbtxdatetime AS TIMESTAMP)) AS week_adj1,
  DATE_ADD('DAY', -7, CAST(sbtxdatetime AS TIMESTAMP)) AS week_adj2,
  DATE_ADD('DAY', 14, DATE_ADD('HOUR', 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj3,
  DATE_ADD('DAY', 14, DATE_ADD('SECOND', -1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj4,
  DATE_ADD('DAY', 14, DATE_ADD('DAY', 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj5,
  DATE_ADD('DAY', 14, DATE_ADD('MINUTE', -1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj6,
  DATE_ADD('DAY', 14, DATE_ADD('MONTH', 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj7,
  DATE_ADD('DAY', 14, DATE_ADD('YEAR', 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj8
FROM main.sbtransaction
WHERE
  DAY(CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND YEAR(CAST(sbtxdatetime AS TIMESTAMP)) < 2025
