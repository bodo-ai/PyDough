WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  GROUP BY
    1
)
SELECT
  CUSTOMER.c_custkey AS customer_key,
  COALESCE(_s1.n_rows, 0) AS n_orders
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _s1 AS _s1
  ON CUSTOMER.c_custkey = _s1.o_custkey
ORDER BY
  2 DESC,
  1
LIMIT 5
