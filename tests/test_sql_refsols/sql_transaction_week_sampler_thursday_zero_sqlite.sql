SELECT
  sbtransaction.sbtxdatetime AS date_time,
  DATE(
    sbtransaction.sbtxdatetime,
    '-' || (
      CAST(STRFTIME('%w', DATETIME(sbtransaction.sbtxdatetime)) AS INTEGER) + 3
    ) % 7 || ' days',
    'start of day'
  ) AS sow,
  CASE
    WHEN CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) = 0
    THEN 'Sunday'
    WHEN CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) = 1
    THEN 'Monday'
    WHEN CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) = 2
    THEN 'Tuesday'
    WHEN CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) = 3
    THEN 'Wednesday'
    WHEN CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) = 4
    THEN 'Thursday'
    WHEN CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) = 5
    THEN 'Friday'
    WHEN CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) = 6
    THEN 'Saturday'
  END AS dayname,
  (
    CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) + 3
  ) % 7 AS dayofweek
FROM main.sbtransaction AS sbtransaction
WHERE
  CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER) < 2025
  AND CAST(STRFTIME('%d', sbtransaction.sbtxdatetime) AS INTEGER) > 1
