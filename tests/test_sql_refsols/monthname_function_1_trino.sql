SELECT
  o_orderdate AS order_date,
  FORMAT_DATETIME(CAST('1995-10-26' AS TIMESTAMP), 'MMM') AS month_name_str,
  FORMAT_DATETIME(CAST(o_orderdate AS TIMESTAMP), 'MMM') AS month_name_col,
  FORMAT_DATETIME(DATE_ADD('MONTH', 3, CAST(o_orderdate AS TIMESTAMP)), 'MMM') AS month_col_added,
  FORMAT_DATETIME(CAST(CAST('1995-05-26' AS DATE) AS TIMESTAMP), 'MMM') AS month_str_subs,
  FORMAT_DATETIME(CURRENT_TIMESTAMP, 'MMM') AS month_now
FROM tpch.orders
WHERE
  YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1998
ORDER BY
  o_custkey NULLS FIRST
LIMIT 5
