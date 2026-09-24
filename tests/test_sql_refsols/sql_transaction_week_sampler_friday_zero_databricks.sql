SELECT
  sbtxdatetime AS date_time,
  DATE_ADD(
    CAST(CAST(sbtxdatetime AS TIMESTAMP) AS DATE),
    -(
      (
        DAYOFWEEK(CAST(sbtxdatetime AS TIMESTAMP)) + 1
      ) % 7
    )
  ) AS sow,
  CASE
    WHEN DAYOFWEEK(sbtxdatetime) = 1
    THEN 'Sunday'
    WHEN DAYOFWEEK(sbtxdatetime) = 2
    THEN 'Monday'
    WHEN DAYOFWEEK(sbtxdatetime) = 3
    THEN 'Tuesday'
    WHEN DAYOFWEEK(sbtxdatetime) = 4
    THEN 'Wednesday'
    WHEN DAYOFWEEK(sbtxdatetime) = 5
    THEN 'Thursday'
    WHEN DAYOFWEEK(sbtxdatetime) = 6
    THEN 'Friday'
    WHEN DAYOFWEEK(sbtxdatetime) = 7
    THEN 'Saturday'
  END AS dayname,
  (
    DAYOFWEEK(sbtxdatetime) + 1
  ) % 7 AS dayofweek
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) < 2025
