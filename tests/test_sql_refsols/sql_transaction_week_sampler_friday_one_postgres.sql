SELECT
  sbtxdatetime AS date_time,
  DATE_TRUNC(
    'DAY',
    CAST(sbtxdatetime AS TIMESTAMP) - MAKE_INTERVAL(days => (
      EXTRACT(DOW FROM CAST(sbtxdatetime AS TIMESTAMP)) + 2
    ) % 7)
  ) AS sow,
  CASE
    WHEN EXTRACT(DOW FROM CAST(sbtxdatetime AS TIMESTAMP)) = 0
    THEN 'Sunday'
    WHEN EXTRACT(DOW FROM CAST(sbtxdatetime AS TIMESTAMP)) = 1
    THEN 'Monday'
    WHEN EXTRACT(DOW FROM CAST(sbtxdatetime AS TIMESTAMP)) = 2
    THEN 'Tuesday'
    WHEN EXTRACT(DOW FROM CAST(sbtxdatetime AS TIMESTAMP)) = 3
    THEN 'Wednesday'
    WHEN EXTRACT(DOW FROM CAST(sbtxdatetime AS TIMESTAMP)) = 4
    THEN 'Thursday'
    WHEN EXTRACT(DOW FROM CAST(sbtxdatetime AS TIMESTAMP)) = 5
    THEN 'Friday'
    WHEN EXTRACT(DOW FROM CAST(sbtxdatetime AS TIMESTAMP)) = 6
    THEN 'Saturday'
  END AS dayname,
  (
    (
      EXTRACT(DOW FROM CAST(sbtxdatetime AS TIMESTAMP)) + 2
    ) % 7
  ) + 1 AS dayofweek
FROM main.sbtransaction
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS TIMESTAMP)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) < 2025
