SELECT
  sbtxdatetime AS date_time,
  DATE_TRUNC('WEEK', CAST(sbtxdatetime AS TIMESTAMP)) AS sow,
  CASE
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 0
    THEN 'Sunday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 1
    THEN 'Monday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 2
    THEN 'Tuesday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 3
    THEN 'Wednesday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 4
    THEN 'Thursday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 5
    THEN 'Friday'
    WHEN (
      (
        DAY_OF_WEEK(sbtxdatetime) % 7
      ) + 1
    ) = 6
    THEN 'Saturday'
  END AS dayname,
  (
    (
      DAY_OF_WEEK(sbtxdatetime) % 7
    ) + 5
  ) % 7 AS dayofweek
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) < 2025
