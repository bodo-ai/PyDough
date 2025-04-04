SELECT
  date_time,
  DATE(
    date_time,
    '-' || CAST((
      CAST(STRFTIME('%w', DATETIME(date_time)) AS INTEGER) + 2
    ) % 7 AS TEXT) || ' days',
    'start of day'
  ) AS sow,
  (
    CASE
      WHEN CAST(STRFTIME('%w', date_time) AS INTEGER) = 0
      THEN 'Sunday'
      WHEN CAST(STRFTIME('%w', date_time) AS INTEGER) = 1
      THEN 'Monday'
      WHEN CAST(STRFTIME('%w', date_time) AS INTEGER) = 2
      THEN 'Tuesday'
      WHEN CAST(STRFTIME('%w', date_time) AS INTEGER) = 3
      THEN 'Wednesday'
      WHEN CAST(STRFTIME('%w', date_time) AS INTEGER) = 4
      THEN 'Thursday'
      WHEN CAST(STRFTIME('%w', date_time) AS INTEGER) = 5
      THEN 'Friday'
      WHEN CAST(STRFTIME('%w', date_time) AS INTEGER) = 6
      THEN 'Saturday'
    END
  ) AS dayname,
  (
    (
      CAST(STRFTIME('%w', date_time) AS INTEGER) + 2
    ) % 7
  ) AS dayofweek
FROM (
  SELECT
    date_time
  FROM (
    SELECT
      sbTxDateTime AS date_time
    FROM main.sbTransaction
  )
  WHERE
    (
      CAST(STRFTIME('%Y', date_time) AS INTEGER) < 2025
    )
    AND (
      CAST(STRFTIME('%d', date_time) AS INTEGER) > 1
    )
)
