WITH _t0 AS (
  SELECT
    o_orderdate AS order_date
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1995
  ORDER BY
    order_date
  LIMIT 1
)
SELECT
  order_date,
  CASE
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 10
    THEN 4
  END AS quarter,
  DATE(
    order_date,
    'start of month',
    '-' || CAST((
      (
        CAST(STRFTIME('%m', DATETIME(order_date)) AS INTEGER) - 1
      ) % 3
    ) AS TEXT) || ' months'
  ) AS quarter_start,
  DATETIME(order_date, '3 month') AS next_quarter,
  DATETIME(order_date, '-3 month') AS prev_quarter,
  DATETIME(order_date, '6 month') AS two_quarters_ahead,
  DATETIME(order_date, '-6 month') AS two_quarters_behind,
  (
    CAST(STRFTIME('%Y', order_date) AS INTEGER) - CAST(STRFTIME('%Y', DATETIME('1995-01-01')) AS INTEGER)
  ) * 4 + CASE
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 10
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
    CAST(STRFTIME('%Y', DATETIME('2000-01-01')) AS INTEGER) - CAST(STRFTIME('%Y', order_date) AS INTEGER)
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
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 3
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 1
    THEN 1
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 6
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 4
    THEN 2
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 9
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 7
    THEN 3
    WHEN CAST(STRFTIME('%m', order_date) AS INTEGER) <= 12
    AND CAST(STRFTIME('%m', order_date) AS INTEGER) >= 10
    THEN 4
  END AS quarters_until_2000,
  DATETIME(order_date, '-12 month') AS same_quarter_prev_year,
  DATETIME(order_date, '12 month') AS same_quarter_next_year
FROM _t0
ORDER BY
  order_date
