SELECT
  DATETIME('now') AS ts_now_1,
  DATE('now', 'start of day') AS ts_now_2,
  DATE('now', 'start of month') AS ts_now_3,
  DATETIME('now', '1 hour') AS ts_now_4,
  DATE('2025-01-01 00:00:00', 'start of month') AS ts_now_5,
  DATETIME('1995-10-10 00:00:00', '-2 day') AS ts_now_6,
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year_col,
  2020 AS year_py,
  1995 AS year_pd,
  CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS month_col,
  2 AS month_str,
  1 AS month_dt,
  CAST(STRFTIME('%d', o_orderdate) AS INTEGER) AS day_col,
  25 AS day_str,
  23 AS hour_str,
  59 AS minute_str,
  59 AS second_ts,
  CAST((
    JULIANDAY(DATE(DATETIME('1992-01-01'), 'start of day')) - JULIANDAY(DATE(o_orderdate, 'start of day'))
  ) AS INTEGER) AS dd_col_str,
  CAST((
    JULIANDAY(DATE(o_orderdate, 'start of day')) - JULIANDAY(DATE(DATETIME('1992-01-01'), 'start of day'))
  ) AS INTEGER) AS dd_str_col,
  (
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) - CAST(STRFTIME('%Y', '1995-10-10 00:00:00') AS INTEGER)
  ) * 12 + CAST(STRFTIME('%m', o_orderdate) AS INTEGER) - CAST(STRFTIME('%m', '1995-10-10 00:00:00') AS INTEGER) AS dd_pd_col,
  CAST(STRFTIME('%Y', '1992-01-01 12:30:45') AS INTEGER) - CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS dd_col_dt,
  CAST(CAST(CAST((
    JULIANDAY(
      DATE(
        '1992-01-01 12:30:45',
        '-' || CAST(CAST(STRFTIME('%w', DATETIME('1992-01-01 12:30:45')) AS INTEGER) AS TEXT) || ' days',
        'start of day'
      )
    ) - JULIANDAY(
      DATE(
        DATETIME('1992-01-01'),
        '-' || CAST(CAST(STRFTIME('%w', DATETIME(DATETIME('1992-01-01'))) AS INTEGER) AS TEXT) || ' days',
        'start of day'
      )
    )
  ) AS INTEGER) AS REAL) / 7 AS INTEGER) AS dd_dt_str,
  CAST(STRFTIME('%w', o_orderdate) AS INTEGER) AS dow_col,
  CAST(STRFTIME('%w', DATETIME('1992-07-01')) AS INTEGER) AS dow_str,
  CAST(STRFTIME('%w', '1992-01-01 12:30:45') AS INTEGER) AS dow_dt,
  CAST(STRFTIME('%w', '1995-10-10 00:00:00') AS INTEGER) AS dow_pd,
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
  END AS dayname_col,
  CASE
    WHEN CAST(STRFTIME('%w', DATETIME('1995-06-30')) AS INTEGER) = 0
    THEN 'Sunday'
    WHEN CAST(STRFTIME('%w', DATETIME('1995-06-30')) AS INTEGER) = 1
    THEN 'Monday'
    WHEN CAST(STRFTIME('%w', DATETIME('1995-06-30')) AS INTEGER) = 2
    THEN 'Tuesday'
    WHEN CAST(STRFTIME('%w', DATETIME('1995-06-30')) AS INTEGER) = 3
    THEN 'Wednesday'
    WHEN CAST(STRFTIME('%w', DATETIME('1995-06-30')) AS INTEGER) = 4
    THEN 'Thursday'
    WHEN CAST(STRFTIME('%w', DATETIME('1995-06-30')) AS INTEGER) = 5
    THEN 'Friday'
    WHEN CAST(STRFTIME('%w', DATETIME('1995-06-30')) AS INTEGER) = 6
    THEN 'Saturday'
  END AS dayname_str,
  CASE
    WHEN CAST(STRFTIME('%w', '1993-08-15 00:00:00') AS INTEGER) = 0
    THEN 'Sunday'
    WHEN CAST(STRFTIME('%w', '1993-08-15 00:00:00') AS INTEGER) = 1
    THEN 'Monday'
    WHEN CAST(STRFTIME('%w', '1993-08-15 00:00:00') AS INTEGER) = 2
    THEN 'Tuesday'
    WHEN CAST(STRFTIME('%w', '1993-08-15 00:00:00') AS INTEGER) = 3
    THEN 'Wednesday'
    WHEN CAST(STRFTIME('%w', '1993-08-15 00:00:00') AS INTEGER) = 4
    THEN 'Thursday'
    WHEN CAST(STRFTIME('%w', '1993-08-15 00:00:00') AS INTEGER) = 5
    THEN 'Friday'
    WHEN CAST(STRFTIME('%w', '1993-08-15 00:00:00') AS INTEGER) = 6
    THEN 'Saturday'
  END AS dayname_dt
FROM tpch.orders
