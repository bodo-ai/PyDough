WITH _t3 AS (
  SELECT
    orders.o_orderkey AS order_key,
    orders.o_totalprice AS order_value,
    CAST((
      100.0 * orders.o_totalprice
    ) AS REAL) / SUM(orders.o_totalprice) OVER (PARTITION BY customer.c_nationkey) AS value_percentage,
    customer.c_name AS customer_name,
    customer.c_nationkey AS nation_key
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
    AND customer.c_custkey = orders.o_custkey
), _t AS (
  SELECT
    order_key,
    order_value,
    value_percentage,
    customer_name,
    nation_key,
    ROW_NUMBER() OVER (PARTITION BY nation_key ORDER BY order_value DESC) AS _w
  FROM _t3
)
SELECT
  nation.n_name AS nation_name,
  _t.customer_name,
  _t.order_key,
  _t.order_value,
  _t.value_percentage
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
JOIN _t AS _t
  ON _t._w = 1 AND _t.nation_key = nation.n_nationkey
ORDER BY
  nation.n_name
