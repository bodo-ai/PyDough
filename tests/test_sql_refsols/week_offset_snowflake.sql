SELECT
  sbtxdatetime AS date_time,
  DATEADD(WEEK, 1, CAST(sbtxdatetime AS TIMESTAMP)) AS week_adj1,
  DATEADD(WEEK, -1, CAST(sbtxdatetime AS TIMESTAMP)) AS week_adj2,
  DATEADD(WEEK, 2, DATEADD(HOUR, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj3,
  DATEADD(WEEK, 2, DATEADD(SECOND, -1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj4,
  DATEADD(WEEK, 2, DATEADD(DAY, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj5,
  DATEADD(WEEK, 2, DATEADD(MINUTE, -1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj6,
  DATEADD(WEEK, 2, DATEADD(MONTH, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj7,
  DATEADD(WEEK, 2, DATEADD(YEAR, 1, CAST(sbtxdatetime AS TIMESTAMP))) AS week_adj8
FROM MAIN.SBTRANSACTION
WHERE
  DATE_PART(DAY, sbtxdatetime) > 1 AND DATE_PART(YEAR, sbtxdatetime) < 2025
