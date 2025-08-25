SELECT
  sbtxdatetime AS date_time,
  CAST(DATE_SUB(
    CAST(sbtxdatetime AS DATETIME),
    INTERVAL (
      (
        DAYOFWEEK(CAST(sbtxdatetime AS DATETIME)) + 3
      ) % 7
    ) DAY
  ) AS DATE) AS sow,
  DAYNAME(sbtxdatetime) AS dayname,
  (
    DAYOFWEEK(sbtxdatetime) + 3
  ) % 7 AS dayofweek
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS DATETIME)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) < 2025
