SELECT
  sbtxdatetime AS date_time,
  DATE(
    DATE_SUB(
      CAST(sbtxdatetime AS DATETIME),
      INTERVAL (DAYOFWEEK(CAST(sbtxdatetime AS DATETIME))) DAY
    )
  ) AS sow,
  DAYNAME(sbtxdatetime) AS dayname,
  DAYOFWEEK(sbtxdatetime) AS dayofweek
FROM main.sbTransaction
WHERE
  DAY(sbtxdatetime) > 1 AND YEAR(sbtxdatetime) < 2025
