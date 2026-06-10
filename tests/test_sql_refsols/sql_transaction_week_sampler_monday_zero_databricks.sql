SELECT
  sbtxdatetime AS date_time,
  DATEADD(
    DAY,
    -(
      (
        DAYOFWEEK(TO_DATE(CAST(sbtxdatetime AS TIMESTAMP))) - 1 + 6
      ) % 7
    ),
    CAST(CAST(sbtxdatetime AS TIMESTAMP) AS DATE)
  ) AS sow,
  CASE
    WHEN DAYOFWEEK(TO_DATE(sbtxdatetime)) = 1
    THEN 'Sunday'
    WHEN DAYOFWEEK(TO_DATE(sbtxdatetime)) = 2
    THEN 'Monday'
    WHEN DAYOFWEEK(TO_DATE(sbtxdatetime)) = 3
    THEN 'Tuesday'
    WHEN DAYOFWEEK(TO_DATE(sbtxdatetime)) = 4
    THEN 'Wednesday'
    WHEN DAYOFWEEK(TO_DATE(sbtxdatetime)) = 5
    THEN 'Thursday'
    WHEN DAYOFWEEK(TO_DATE(sbtxdatetime)) = 6
    THEN 'Friday'
    WHEN DAYOFWEEK(TO_DATE(sbtxdatetime)) = 7
    THEN 'Saturday'
  END AS dayname,
  (
    DAYOFWEEK(TO_DATE(sbtxdatetime)) - 1 + 6
  ) % 7 AS dayofweek
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) < 2025
