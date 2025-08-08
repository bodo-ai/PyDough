SELECT
  sbtxdatetime AS date_time,
  DATE(
    DATE_SUB(
      CAST(sbtxdatetime AS DATETIME),
      INTERVAL (
        (
          (
            DAYOFWEEK(CAST(sbtxdatetime AS DATETIME)) + 4
          ) % 7
        ) + 1
      ) DAY
    )
  ) AS sow,
  DAYNAME(sbtxdatetime) AS dayname,
  (
    (
      DAYOFWEEK(sbtxdatetime) + 4
    ) % 7
  ) + 1 AS dayofweek
FROM main.sbTransaction
WHERE
  DAY(sbtxdatetime) > 1 AND YEAR(sbtxdatetime) < 2025
