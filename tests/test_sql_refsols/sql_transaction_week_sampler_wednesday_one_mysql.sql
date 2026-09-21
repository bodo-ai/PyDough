SELECT
  sbTxDateTime AS date_time,
  CAST(DATE_SUB(
    CAST(sbTxDateTime AS DATETIME),
    INTERVAL (
      (
        DAYOFWEEK(CAST(sbTxDateTime AS DATETIME)) + 3
      ) % 7
    ) DAY
  ) AS DATE) AS sow,
  DAYNAME(sbTxDateTime) AS dayname,
  (
    (
      DAYOFWEEK(sbTxDateTime) + 3
    ) % 7
  ) + 1 AS dayofweek
FROM main.sbTransaction
WHERE
  EXTRACT(DAY FROM CAST(sbTxDateTime AS DATETIME)) > 1
  AND EXTRACT(YEAR FROM CAST(sbTxDateTime AS DATETIME)) < 2025
