WITH _s3 AS (
  SELECT
    o_custkey,
    SUM(o_totalprice) AS sum_o_totalprice
  FROM tpch.ORDERS
  GROUP BY
    1
)
SELECT
  ORDERS.o_orderkey AS moment_id,
  CUSTOMER.c_custkey AS customer_id,
  COALESCE(_s3.sum_o_totalprice, 0) AS total_ever
FROM tpch.ORDERS AS ORDERS
JOIN tpch.CUSTOMER AS CUSTOMER
  ON CUSTOMER.c_custkey <= 2
LEFT JOIN _s3 AS _s3
  ON CUSTOMER.c_custkey = _s3.o_custkey
WHERE
  ORDERS.o_orderkey IN (1, 2, 3)
ORDER BY
  1,
  2
