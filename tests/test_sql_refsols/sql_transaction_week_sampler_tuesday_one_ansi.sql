SELECT
  sbtxdatetime AS date_time,
  DATE_TRUNC('WEEK', CAST(sbtxdatetime AS TIMESTAMP)) AS sow,
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
  (
    (
      DAY_OF_WEEK(sbtxdatetime) + 5
    ) % 7
  ) + 1 AS dayofweek
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM sbtxdatetime) > 1 AND EXTRACT(YEAR FROM sbtxdatetime) < 2025
