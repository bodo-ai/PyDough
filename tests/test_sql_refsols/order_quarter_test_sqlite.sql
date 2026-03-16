SELECT
  o_orderdate AS order_date,
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
  END AS quarter,
  DATE(
    o_orderdate,
    'start of month',
    '-' || CAST((
      (
        CAST(STRFTIME('%m', DATETIME(o_orderdate)) AS INTEGER) - 1
      ) % 3
    ) AS TEXT) || ' months'
  ) AS quarter_start,
  DATETIME(o_orderdate, '3 month') AS next_quarter,
  DATETIME(o_orderdate, '-3 month') AS prev_quarter,
  DATETIME(o_orderdate, '6 month') AS two_quarters_ahead,
  DATETIME(o_orderdate, '-6 month') AS two_quarters_behind,
  (
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) - CAST(STRFTIME('%Y', DATETIME('1995-01-01')) AS INTEGER)
  ) * 4 + CASE
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
  END - CASE
    WHEN CAST(STRFTIME('%m', DATETIME('1995-01-01')) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', DATETIME('1995-01-01')) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', DATETIME('1995-01-01')) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', DATETIME('1995-01-01')) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', DATETIME('1995-01-01')) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', DATETIME('1995-01-01')) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', DATETIME('1995-01-01')) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', DATETIME('1995-01-01')) AS INTEGER) >= 10
    THEN 4
  END AS quarters_since_1995,
  (
    CAST(STRFTIME('%Y', DATETIME('2000-01-01')) AS INTEGER) - CAST(STRFTIME('%Y', o_orderdate) AS INTEGER)
  ) * 4 + CASE
    WHEN CAST(STRFTIME('%m', DATETIME('2000-01-01')) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', DATETIME('2000-01-01')) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', DATETIME('2000-01-01')) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', DATETIME('2000-01-01')) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', DATETIME('2000-01-01')) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', DATETIME('2000-01-01')) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', DATETIME('2000-01-01')) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', DATETIME('2000-01-01')) AS INTEGER) >= 10
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
  END AS quarters_until_2000,
  DATETIME(o_orderdate, '-12 month') AS same_quarter_prev_year,
  DATETIME(o_orderdate, '12 month') AS same_quarter_next_year
FROM tpch.orders
WHERE
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1995
ORDER BY
  1
LIMIT 1
