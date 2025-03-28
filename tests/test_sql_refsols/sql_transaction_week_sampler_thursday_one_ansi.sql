SELECT
  sbtransaction.sbtxdatetime AS date_time,
  DATE_TRUNC('WEEK', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) AS sow,
  CASE
    WHEN DAY_OF_WEEK(sbtransaction.sbtxdatetime) = 0
    THEN 'Sunday'
    WHEN DAY_OF_WEEK(sbtransaction.sbtxdatetime) = 1
    THEN 'Monday'
    WHEN DAY_OF_WEEK(sbtransaction.sbtxdatetime) = 2
    THEN 'Tuesday'
    WHEN DAY_OF_WEEK(sbtransaction.sbtxdatetime) = 3
    THEN 'Wednesday'
    WHEN DAY_OF_WEEK(sbtransaction.sbtxdatetime) = 4
    THEN 'Thursday'
    WHEN DAY_OF_WEEK(sbtransaction.sbtxdatetime) = 5
    THEN 'Friday'
    WHEN DAY_OF_WEEK(sbtransaction.sbtxdatetime) = 6
    THEN 'Saturday'
  END AS dayname,
  (
    (
      DAY_OF_WEEK(sbtransaction.sbtxdatetime) + 3
    ) % 7
  ) + 1 AS dayofweek
FROM main.sbtransaction AS sbtransaction
WHERE
  EXTRACT(DAY FROM sbtransaction.sbtxdatetime) > 1
  AND EXTRACT(YEAR FROM sbtransaction.sbtxdatetime) < 2025
