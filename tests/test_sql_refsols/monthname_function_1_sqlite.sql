SELECT
  o_orderdate AS order_date,
  CASE
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 1
    THEN 'Jan'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 2
    THEN 'Feb'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 3
    THEN 'Mar'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 4
    THEN 'Apr'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 5
    THEN 'May'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 6
    THEN 'Jun'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 7
    THEN 'Jul'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 8
    THEN 'Aug'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 9
    THEN 'Sep'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 10
    THEN 'Oct'
    WHEN CAST(STRFTIME('%m', DATETIME('1995-10-26')) AS INTEGER) = 11
    THEN 'Nov'
    ELSE 'Dec'
  END AS month_name_str,
  CASE
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 1
    THEN 'Jan'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 2
    THEN 'Feb'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 3
    THEN 'Mar'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 4
    THEN 'Apr'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 5
    THEN 'May'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 6
    THEN 'Jun'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 7
    THEN 'Jul'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 8
    THEN 'Aug'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 9
    THEN 'Sep'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 10
    THEN 'Oct'
    WHEN CAST(STRFTIME('%m', o_orderdate) AS INTEGER) = 11
    THEN 'Nov'
    ELSE 'Dec'
  END AS month_name_col,
  CASE
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 1
    THEN 'Jan'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 2
    THEN 'Feb'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 3
    THEN 'Mar'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 4
    THEN 'Apr'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 5
    THEN 'May'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 6
    THEN 'Jun'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 7
    THEN 'Jul'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 8
    THEN 'Aug'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 9
    THEN 'Sep'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 10
    THEN 'Oct'
    WHEN CAST(STRFTIME('%m', DATETIME(o_orderdate, '3 month')) AS INTEGER) = 11
    THEN 'Nov'
    ELSE 'Dec'
  END AS month_col_added,
  CASE
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 1
    THEN 'Jan'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 2
    THEN 'Feb'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 3
    THEN 'Mar'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 4
    THEN 'Apr'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 5
    THEN 'May'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 6
    THEN 'Jun'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 7
    THEN 'Jul'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 8
    THEN 'Aug'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 9
    THEN 'Sep'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 10
    THEN 'Oct'
    WHEN CAST(STRFTIME('%m', '1995-05-26') AS INTEGER) = 11
    THEN 'Nov'
    ELSE 'Dec'
  END AS month_str_subs,
  CASE
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 1
    THEN 'Jan'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 2
    THEN 'Feb'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 3
    THEN 'Mar'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 4
    THEN 'Apr'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 5
    THEN 'May'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 6
    THEN 'Jun'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 7
    THEN 'Jul'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 8
    THEN 'Aug'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 9
    THEN 'Sep'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 10
    THEN 'Oct'
    WHEN CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) = 11
    THEN 'Nov'
    ELSE 'Dec'
  END AS month_now
FROM tpch.orders
WHERE
  CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1998
ORDER BY
  o_custkey
LIMIT 5
