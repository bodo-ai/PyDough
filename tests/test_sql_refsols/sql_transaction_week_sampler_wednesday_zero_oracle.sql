SELECT
  sbtxdatetime AS date_time,
  TRUNC(CAST(sbtxdatetime AS TIMESTAMP), 'WEEK') AS sow,
  CASE
    WHEN DAY_OF_WEEK(sbtxdatetime) = 0
    THEN 'Sunday'
    WHEN DAY_OF_WEEK(sbtxdatetime) = 1
    THEN 'Monday'
    WHEN DAY_OF_WEEK(sbtxdatetime) = 2
    THEN 'Tuesday'
    WHEN DAY_OF_WEEK(sbtxdatetime) = 3
    THEN 'Wednesday'
    WHEN DAY_OF_WEEK(sbtxdatetime) = 4
    THEN 'Thursday'
    WHEN DAY_OF_WEEK(sbtxdatetime) = 5
    THEN 'Friday'
    WHEN DAY_OF_WEEK(sbtxdatetime) = 6
    THEN 'Saturday'
  END AS dayname,
  MOD((
    DAY_OF_WEEK(sbtxdatetime) + 4
  ), 7) AS dayofweek
FROM MAIN.SBTRANSACTION
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS DATETIME)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) < 2025
