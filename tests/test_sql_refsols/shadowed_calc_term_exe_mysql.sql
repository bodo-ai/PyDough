WITH _s3 AS (
  SELECT
    CUSTOMER.c_custkey,
    MAX(CUSTOMER.c_phone) AS max_c_phone
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
  GROUP BY
    1
)
SELECT
  ORDERS.o_orderkey AS `key`,
  _s3.max_c_phone COLLATE utf8mb4_bin AS x
FROM tpch.ORDERS AS ORDERS
LEFT JOIN _s3 AS _s3
  ON ORDERS.o_custkey = _s3.c_custkey
ORDER BY
  2
LIMIT 5
