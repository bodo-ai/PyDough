SELECT
  sbtxdatetime AS date_time,
  DATE_TRUNC('WEEK', CAST(sbtxdatetime AS TIMESTAMP)) AS sow,
  CASE
    WHEN DAYOFWEEK(sbtxdatetime) = 0
    THEN 'Sunday'
    WHEN DAYOFWEEK(sbtxdatetime) = 1
    THEN 'Monday'
    WHEN DAYOFWEEK(sbtxdatetime) = 2
    THEN 'Tuesday'
    WHEN DAYOFWEEK(sbtxdatetime) = 3
    THEN 'Wednesday'
    WHEN DAYOFWEEK(sbtxdatetime) = 4
    THEN 'Thursday'
    WHEN DAYOFWEEK(sbtxdatetime) = 5
    THEN 'Friday'
    WHEN DAYOFWEEK(sbtxdatetime) = 6
    THEN 'Saturday'
  END AS dayname,
  (
    DAYOFWEEK(sbtxdatetime) + 2
  ) % 7 AS dayofweek
FROM MAIN.SBTRANSACTION
WHERE
  DAY(CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND YEAR(CAST(sbtxdatetime AS TIMESTAMP)) < 2025
