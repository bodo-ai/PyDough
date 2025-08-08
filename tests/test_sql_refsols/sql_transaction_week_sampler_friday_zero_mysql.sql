SELECT
  sbtxdatetime AS date_time,
  DATE(
    DATE_SUB(
      CAST(sbtxdatetime AS DATETIME),
      INTERVAL (
        (
          DAYOFWEEK(CAST(sbtxdatetime AS DATETIME)) + 1
        ) % 7
      ) DAY
    )
  ) AS sow,
  DAYNAME(sbtxdatetime) AS dayname,
  (
    DAYOFWEEK(sbtxdatetime) + 1
  ) % 7 AS dayofweek
FROM main.sbTransaction
WHERE
  DAY(sbtxdatetime) > 1 AND YEAR(sbtxdatetime) < 2025
