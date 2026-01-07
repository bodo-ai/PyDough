WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    1
)
SELECT
  customer.c_custkey AS customer_key,
  COALESCE(_s1.n_rows, 0) AS n_orders
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
LIMIT 5
