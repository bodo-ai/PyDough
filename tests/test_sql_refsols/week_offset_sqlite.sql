SELECT
  date_time,
  DATETIME(date_time, '7 day') AS week_adj1,
  DATETIME(date_time, '-7 day') AS week_adj2,
  DATETIME(date_time, '1 hour', '14 day') AS week_adj3,
  DATETIME(date_time, '-1 second', '14 day') AS week_adj4,
  DATETIME(date_time, '1 day', '14 day') AS week_adj5,
  DATETIME(date_time, '-1 minute', '14 day') AS week_adj6,
  DATETIME(date_time, '1 month', '14 day') AS week_adj7,
  DATETIME(date_time, '1 year', '14 day') AS week_adj8
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
