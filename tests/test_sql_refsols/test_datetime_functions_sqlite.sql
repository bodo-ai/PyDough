SELECT
  DATETIME('now') AS ts_1,
  DATE('now', 'start of month') AS ts_2,
  DATETIME('now', '12 hour') AS ts_3,
  DATE('now', 'start of year', '-1 day') AS ts_4,
  DATE('now', 'start of day') AS ts_5,
  DATE(
    'now',
    'start of month',
    '-' || CAST((
      (
        CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) - 1
      ) % 3
    ) AS TEXT) || ' months',
    '1 day'
  ) AS ts_6,
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year,
  CASE
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 10
    THEN 4
  END AS qtr,
  CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS month,
  CAST(STRFTIME('%d', o_orderdate) AS INTEGER) AS day,
  CAST(STRFTIME('%H', o_orderdate) AS INTEGER) AS hour,
  CAST(STRFTIME('%M', o_orderdate) AS INTEGER) AS minute,
  CAST(STRFTIME('%S', o_orderdate) AS INTEGER) AS second,
  (
    (
      CAST((
        JULIANDAY(DATE('1992-01-01', 'start of day')) - JULIANDAY(DATE(o_orderdate, 'start of day'))
      ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%H', o_orderdate) AS INTEGER)
    ) * 60 + CAST(STRFTIME('%M', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%M', o_orderdate) AS INTEGER)
  ) * 60 + CAST(STRFTIME('%S', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%S', o_orderdate) AS INTEGER) AS seconds_since,
  (
    CAST((
      JULIANDAY(DATE('1992-01-01', 'start of day')) - JULIANDAY(DATE(o_orderdate, 'start of day'))
    ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%H', o_orderdate) AS INTEGER)
  ) * 60 + CAST(STRFTIME('%M', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%M', o_orderdate) AS INTEGER) AS minutes_since,
  CAST((
    JULIANDAY(DATE('1992-01-01', 'start of day')) - JULIANDAY(DATE(o_orderdate, 'start of day'))
  ) AS INTEGER) * 24 + CAST(STRFTIME('%H', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%H', o_orderdate) AS INTEGER) AS hours_since,
  CAST((
    JULIANDAY(DATE('1992-01-01', 'start of day')) - JULIANDAY(DATE(o_orderdate, 'start of day'))
  ) AS INTEGER) AS days_since,
  CAST(CAST(CAST((
    JULIANDAY(
      DATE(
        '1992-01-01',
        '-' || CAST(CAST(STRFTIME('%w', DATETIME('1992-01-01')) AS INTEGER) AS TEXT) || ' days',
        'start of day'
      )
    ) - JULIANDAY(
      DATE(
        o_orderdate,
        '-' || CAST(CAST(STRFTIME('%w', DATETIME(o_orderdate)) AS INTEGER) AS TEXT) || ' days',
        'start of day'
      )
    )
  ) AS INTEGER) AS REAL) / 7 AS INTEGER) AS weeks_since,
  (
    CAST(STRFTIME('%Y', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
  ) * 12 + CAST(STRFTIME('%m', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS months_since,
  (
    CAST(STRFTIME('%Y', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
  ) * 4 + CASE
    WHEN CAST(STRFTIME('%m', '1992-01-01') AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', '1992-01-01') AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', '1992-01-01') AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', '1992-01-01') AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', '1992-01-01') AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', '1992-01-01') AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', '1992-01-01') AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', '1992-01-01') AS INTEGER) >= 10
    THEN 4
  END - CASE
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', o_orderdate) AS INTEGER) >= 10
    THEN 4
  END AS qtrs_since,
  CAST(STRFTIME('%Y', '1992-01-01') AS INTEGER) - CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS years_since,
  CAST(STRFTIME('%w', o_orderdate) AS INTEGER) AS day_of_week,
  CASE
    WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 0
    THEN 'Sunday'
    WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 1
    THEN 'Monday'
    WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 2
    THEN 'Tuesday'
    WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 3
    THEN 'Wednesday'
    WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 4
    THEN 'Thursday'
    WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 5
    THEN 'Friday'
    WHEN CAST(STRFTIME('%w', o_orderdate) AS INTEGER) = 6
    THEN 'Saturday'
  END AS day_name
FROM tpch.orders
