WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows
  FROM main.orders
  GROUP BY
    1
)
SELECT
  customer.c_custkey AS customer_key,
  COALESCE(_s1.n_rows, 0) AS n_orders
FROM main.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
ORDER BY
  2 DESC,
  1 NULLS FIRST
LIMIT 5
