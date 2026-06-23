SELECT
  o_orderdate AS order_date,
  DATE_FORMAT(CAST('1995-10-26' AS DATETIME), '%b') AS month_name_str,
  DATE_FORMAT(o_orderdate, '%b') AS month_name_col,
  DATE_FORMAT(DATE_ADD(CAST(o_orderdate AS DATETIME), INTERVAL '3' MONTH), '%b') AS month_col_added,
  DATE_FORMAT(CAST('1995-05-26' AS DATE), '%b') AS month_str_subs,
  DATE_FORMAT(CURRENT_TIMESTAMP(), '%b') AS month_now
FROM tpch.ORDERS
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1998
ORDER BY
  o_custkey
LIMIT 5
