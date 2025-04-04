SELECT
  sbtxdatetime AS date_time,
  DATE(
    sbtxdatetime,
    '-' || CAST((
      CAST(STRFTIME('%w', DATETIME(sbtxdatetime)) AS INTEGER) + 3
    ) % 7 AS TEXT) || ' days',
    'start of day'
  ) AS sow,
  CASE
    WHEN CAST(STRFTIME('%w', sbtxdatetime) AS INTEGER) = 0
    THEN 'Sunday'
    WHEN CAST(STRFTIME('%w', sbtxdatetime) AS INTEGER) = 1
    THEN 'Monday'
    WHEN CAST(STRFTIME('%w', sbtxdatetime) AS INTEGER) = 2
    THEN 'Tuesday'
    WHEN CAST(STRFTIME('%w', sbtxdatetime) AS INTEGER) = 3
    THEN 'Wednesday'
    WHEN CAST(STRFTIME('%w', sbtxdatetime) AS INTEGER) = 4
    THEN 'Thursday'
    WHEN CAST(STRFTIME('%w', sbtxdatetime) AS INTEGER) = 5
    THEN 'Friday'
    WHEN CAST(STRFTIME('%w', sbtxdatetime) AS INTEGER) = 6
    THEN 'Saturday'
  END AS dayname,
  (
    CAST(STRFTIME('%w', sbtxdatetime) AS INTEGER) + 3
  ) % 7 AS dayofweek
FROM main.sbtransaction
WHERE
  CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) < 2025
  AND CAST(STRFTIME('%d', sbtxdatetime) AS INTEGER) > 1
