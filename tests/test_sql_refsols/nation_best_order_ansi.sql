WITH _t3 AS (
  SELECT
    orders.o_orderkey AS order_key,
    orders.o_totalprice AS order_value,
    (
      100.0 * orders.o_totalprice
    ) / SUM(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS value_percentage,
    customer.c_name AS customer_name,
    customer.c_nationkey AS nation_key
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM orders.o_orderdate) = 1998
    AND customer.c_custkey = orders.o_custkey
), _t2 AS (
  SELECT
    order_key,
    order_value,
    value_percentage,
    customer_name,
    nation_key
  FROM _t3
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY nation_key ORDER BY order_value DESC NULLS FIRST) = 1
)
SELECT
  nation.n_name AS nation_name,
  _t2.customer_name,
  _t2.order_key,
  _t2.order_value,
  _t2.value_percentage
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
JOIN _t2 AS _t2
  ON _t2.nation_key = nation.n_nationkey
ORDER BY
  nation.n_name
