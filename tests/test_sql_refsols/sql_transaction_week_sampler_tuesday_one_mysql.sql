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
  EXTRACT(DAY FROM CAST(sbtxdatetime AS DATETIME)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) < 2025
