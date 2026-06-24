SELECT
  o_orderdate AS order_date,
  MONTHNAME(CAST('1995-10-26' AS TIMESTAMP)) AS month_name_str,
  MONTHNAME(o_orderdate) AS month_name_col,
  MONTHNAME(DATEADD(MONTH, 3, CAST(o_orderdate AS TIMESTAMP))) AS month_col_added,
  MONTHNAME(CAST('1995-05-26' AS DATE)) AS month_str_subs,
  MONTHNAME(CURRENT_TIMESTAMP()) AS month_now
FROM tpch.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1998
ORDER BY
  o_custkey
LIMIT 5
