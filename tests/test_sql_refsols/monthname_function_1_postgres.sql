SELECT
  o_orderdate AS order_date,
  TO_CHAR(CAST('1995-10-26' AS TIMESTAMP), 'Mon') AS month_name_str,
  TO_CHAR(o_orderdate, 'Mon') AS month_name_col,
  TO_CHAR(CAST(o_orderdate AS TIMESTAMP) + INTERVAL '3 MONTH', 'Mon') AS month_col_added,
  TO_CHAR(CAST('1995-05-26' AS DATE), 'Mon') AS month_str_subs,
  TO_CHAR(CURRENT_TIMESTAMP, 'Mon') AS month_now
FROM tpch.orders
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1998
ORDER BY
  o_custkey NULLS FIRST
LIMIT 5
