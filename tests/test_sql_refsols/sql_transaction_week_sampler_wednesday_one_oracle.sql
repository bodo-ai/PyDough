SELECT
  sbtxdatetime AS date_time,
  TRUNC(CAST(sbtxdatetime AS TIMESTAMP), 'WEEK') AS sow,
  CASE
    WHEN TO_CHAR(sbtxdatetime, 'D') = 1
    THEN 'Sunday'
    WHEN TO_CHAR(sbtxdatetime, 'D') = 2
    THEN 'Monday'
    WHEN TO_CHAR(sbtxdatetime, 'D') = 3
    THEN 'Tuesday'
    WHEN TO_CHAR(sbtxdatetime, 'D') = 4
    THEN 'Wednesday'
    WHEN TO_CHAR(sbtxdatetime, 'D') = 5
    THEN 'Thursday'
    WHEN TO_CHAR(sbtxdatetime, 'D') = 6
    THEN 'Friday'
    WHEN TO_CHAR(sbtxdatetime, 'D') = 7
    THEN 'Saturday'
  END AS dayname,
  (
    MOD((
      TO_CHAR(sbtxdatetime, 'D') + 3
    ), 7)
  ) + 1 AS dayofweek
FROM MAIN.SBTRANSACTION
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS DATE)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATE)) < 2025
