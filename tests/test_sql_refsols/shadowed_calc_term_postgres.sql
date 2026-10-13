WITH _s3 AS (
  SELECT
    customer.c_custkey,
    MAX(customer.c_phone) AS max_c_phone
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey
  GROUP BY
    1
)
SELECT
  orders.o_orderkey AS key,
  _s3.max_c_phone AS x
FROM tpch.orders AS orders
LEFT JOIN _s3 AS _s3
  ON _s3.c_custkey = orders.o_custkey
ORDER BY
  1 NULLS FIRST
LIMIT 5
