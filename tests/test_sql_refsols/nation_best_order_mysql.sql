WITH _t3 AS (
  SELECT
    CUSTOMER.c_name,
    CUSTOMER.c_nationkey,
    ORDERS.o_orderkey,
    ORDERS.o_totalprice,
    (
      100.0 * ORDERS.o_totalprice
    ) / SUM(ORDERS.o_totalprice) OVER (PARTITION BY CUSTOMER.c_nationkey) AS value_percentage
  FROM tpch.CUSTOMER AS CUSTOMER
  JOIN tpch.ORDERS AS ORDERS
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
    AND EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) = 1998
), _t AS (
  SELECT
    c_name,
    c_nationkey,
    o_orderkey,
    o_totalprice,
    value_percentage,
    ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY CASE WHEN o_totalprice IS NULL THEN 1 ELSE 0 END DESC, o_totalprice DESC) AS _w
  FROM _t3
)
SELECT
  NATION.n_name COLLATE utf8mb4_bin AS nation_name,
  _t.c_name AS customer_name,
  _t.o_orderkey AS order_key,
  _t.o_totalprice AS order_value,
  _t.value_percentage
FROM tpch.NATION AS NATION
JOIN tpch.REGION AS REGION
  ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'ASIA'
JOIN _t AS _t
  ON NATION.n_nationkey = _t.c_nationkey AND _t._w = 1
ORDER BY
  1
