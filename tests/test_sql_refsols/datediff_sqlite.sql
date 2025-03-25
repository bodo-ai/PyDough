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
      CAST((JULIANDAY(DATE('2025-05-02 11:00:00', 'start of day')) - JULIANDAY(DATE(date_time, 'start of day'))) AS INTEGER) AS days_diff,
      date_time AS x,
      CAST((JULIANDAY(DATE('2025-05-02 11:00:00', 'start of day')) - JULIANDAY(DATE(date_time, 'start of day'))) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%H', date_time) AS INTEGER) AS hours_diff,
      (
        CAST((JULIANDAY(DATE('2023-04-03 13:16:30', 'start of day')) - JULIANDAY(DATE(date_time, 'start of day'))) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%H', date_time) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%M', date_time) AS INTEGER) AS minutes_diff,
      (
        (
          CAST((JULIANDAY(DATE('2023-04-03 13:16:30', 'start of day')) - JULIANDAY(DATE(date_time, 'start of day'))) AS INTEGER) * 24 + CAST(STRFTIME('%H', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%H', date_time) AS INTEGER)
        ) * 60 + CAST(STRFTIME('%M', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%M', date_time) AS INTEGER)
      ) * 60 + CAST(STRFTIME('%S', '2023-04-03 13:16:30') AS INTEGER) - CAST(STRFTIME('%S', date_time) AS INTEGER) AS seconds_diff,
      (
        CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', date_time) AS INTEGER)
      ) * 12 + CAST(STRFTIME('%m', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%m', date_time) AS INTEGER) AS months_diff,
      CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', date_time) AS INTEGER) AS ordering_0,
      CAST(STRFTIME('%Y', '2025-05-02 11:00:00') AS INTEGER) - CAST(STRFTIME('%Y', date_time) AS INTEGER) AS years_diff,
      '2023-04-03 13:16:30' AS y,
      '2025-05-02 11:00:00' AS y1
    FROM (
      SELECT
        date_time
      FROM (
        SELECT
          sbTxDateTime AS date_time
        FROM main.sbTransaction
      ) AS _t3
      WHERE
        CAST(STRFTIME('%Y', date_time) AS INTEGER) < 2025
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_0
  LIMIT 30
) AS _t0
ORDER BY
  ordering_0
