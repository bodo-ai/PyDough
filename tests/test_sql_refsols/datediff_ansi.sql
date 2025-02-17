SELECT
  x,
  y1,
  y,
  years_diff,
  months_diff,
  days_diff,
  hours_diff,
  minutes_diff,
  seconds_diff
FROM (
  SELECT
    days_diff,
    hours_diff,
    minutes_diff,
    months_diff,
    ordering_0,
    seconds_diff,
    x,
    y,
    y1,
    years_diff
  FROM (
    SELECT
      DATEDIFF(TIME_STR_TO_TIME('2025-05-02 11:00:00'), date_time, DAY) AS days_diff,
      DATEDIFF(TIME_STR_TO_TIME('2025-05-02 11:00:00'), date_time, HOUR) AS hours_diff,
      DATEDIFF(TIME_STR_TO_TIME('2023-04-03 13:16:30'), date_time, MINUTE) AS minutes_diff,
      DATEDIFF(TIME_STR_TO_TIME('2025-05-02 11:00:00'), date_time, MONTH) AS months_diff,
      DATEDIFF(TIME_STR_TO_TIME('2023-04-03 13:16:30'), date_time, SECOND) AS seconds_diff,
      DATEDIFF(TIME_STR_TO_TIME('2025-05-02 11:00:00'), date_time, YEAR) AS ordering_0,
      DATEDIFF(TIME_STR_TO_TIME('2025-05-02 11:00:00'), date_time, YEAR) AS years_diff,
      date_time AS x,
      TIME_STR_TO_TIME('2023-04-03 13:16:30') AS y,
      TIME_STR_TO_TIME('2025-05-02 11:00:00') AS y1
    FROM (
      SELECT
        date_time
      FROM (
        SELECT
          sbTxDateTime AS date_time
        FROM main.sbTransaction
      )
      WHERE
        YEAR(date_time) < 2025
    )
  )
  ORDER BY
    ordering_0
  LIMIT 30
)
ORDER BY
  ordering_0
