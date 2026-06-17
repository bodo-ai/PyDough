SELECT
  o_orderdate AS order_date,
  TO_CHAR(TO_DATE('1995-10-26', 'YYYY-MM-DD'), 'Mon') AS month_name_str,
  TO_CHAR(o_orderdate, 'Mon') AS month_name_col,
  TO_CHAR(ADD_MONTHS(CAST(o_orderdate AS DATE), 3), 'Mon') AS month_col_added,
  TO_CHAR(TO_DATE('1995-05-26', 'YYYY-MM-DD'), 'Mon') AS month_str_subs,
  TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'Mon') AS month_now
FROM TPCH.ORDERS
WHERE
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATE)) = 1998
ORDER BY
  o_custkey NULLS FIRST
FETCH FIRST 5 ROWS ONLY
