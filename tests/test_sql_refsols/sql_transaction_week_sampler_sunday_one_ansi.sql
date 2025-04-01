SELECT
  date_time,
  DATE_TRUNC('WEEK', CAST(date_time AS TIMESTAMP)) AS sow,
  (
    CASE
      WHEN DAY_OF_WEEK(date_time) = 0
      THEN 'Sunday'
      WHEN DAY_OF_WEEK(date_time) = 1
      THEN 'Monday'
      WHEN DAY_OF_WEEK(date_time) = 2
      THEN 'Tuesday'
      WHEN DAY_OF_WEEK(date_time) = 3
      THEN 'Wednesday'
      WHEN DAY_OF_WEEK(date_time) = 4
      THEN 'Thursday'
      WHEN DAY_OF_WEEK(date_time) = 5
      THEN 'Friday'
      WHEN DAY_OF_WEEK(date_time) = 6
      THEN 'Saturday'
    END
  ) AS dayname,
  (
    DAY_OF_WEEK(date_time) + 1
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
      EXTRACT(YEAR FROM date_time) < 2025
    )
    AND (
      EXTRACT(DAY FROM date_time) > 1
    )
)
