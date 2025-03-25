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
      CAST('2023-04-03 13:16:30' AS TIMESTAMP) AS y,
      CAST('2025-05-02 11:00:00' AS TIMESTAMP) AS y1,
      DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), date_time, DAY) AS days_diff,
      DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), date_time, HOUR) AS hours_diff,
      DATEDIFF(CAST('2023-04-03 13:16:30' AS TIMESTAMP), date_time, MINUTE) AS minutes_diff,
      DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), date_time, MONTH) AS months_diff,
      DATEDIFF(CAST('2023-04-03 13:16:30' AS TIMESTAMP), date_time, SECOND) AS seconds_diff,
      DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), date_time, YEAR) AS ordering_0,
      DATEDIFF(CAST('2025-05-02 11:00:00' AS TIMESTAMP), date_time, YEAR) AS years_diff,
      date_time AS x
    FROM (
      SELECT
        date_time
      FROM (
        SELECT
          sbTxDateTime AS date_time
        FROM main.sbTransaction
      ) AS _t3
      WHERE
        EXTRACT(YEAR FROM date_time) < 2025
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_0
  LIMIT 30
) AS _t0
ORDER BY
  ordering_0
