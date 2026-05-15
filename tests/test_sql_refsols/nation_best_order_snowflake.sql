WITH _t3 AS (
  SELECT
    customer.c_name,
    customer.c_nationkey,
    orders.o_orderkey,
    orders.o_totalprice,
    (
      100.0 * orders.o_totalprice
    ) / SUM(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS value_percentage
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON YEAR(CAST(orders.o_orderdate AS TIMESTAMP)) = 1998
    AND customer.c_custkey = orders.o_custkey
), _t2 AS (
  SELECT
    c_name,
    c_nationkey,
    o_orderkey,
    o_totalprice,
    value_percentage
  FROM _t3
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY c_nationkey ORDER BY o_totalprice DESC) = 1
)
SELECT
  nation.n_name AS nation_name,
  _t2.c_name AS customer_name,
  _t2.o_orderkey AS order_key,
  _t2.o_totalprice AS order_value,
  _t2.value_percentage
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
JOIN _t2 AS _t2
  ON _t2.c_nationkey = nation.n_nationkey
ORDER BY
  1 NULLS FIRST
