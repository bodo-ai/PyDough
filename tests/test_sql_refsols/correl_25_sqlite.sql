WITH _s0 AS (
  SELECT
    r_name,
    r_regionkey
  FROM tpch.region
), _s1 AS (
  SELECT
    n_name,
    n_nationkey,
    n_regionkey
  FROM tpch.nation
), _s13 AS (
  SELECT
    _s9.n_name,
    _s11.r_name,
    supplier.s_suppkey
  FROM tpch.supplier AS supplier
  JOIN _s1 AS _s9
    ON _s9.n_nationkey = supplier.s_nationkey
  JOIN _s0 AS _s11
    ON _s11.r_regionkey = _s9.n_regionkey
)
SELECT
  MAX(_s0.r_name) AS cust_region_name,
  MAX(_s0.r_regionkey) AS cust_region_key,
  MAX(_s1.n_name) AS cust_nation_name,
  MAX(_s1.n_nationkey) AS cust_nation_key,
  MAX(customer.c_name) AS customer_name,
  COUNT(DISTINCT lineitem.l_orderkey) AS n_urgent_semi_domestic_rail_orders
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.r_regionkey = _s1.n_regionkey
JOIN tpch.customer AS customer
  ON _s1.n_nationkey = customer.c_nationkey
JOIN tpch.orders AS orders
  ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1996
  AND customer.c_custkey = orders.o_custkey
  AND orders.o_orderpriority = '1-URGENT'
JOIN tpch.lineitem AS lineitem
  ON lineitem.l_orderkey = orders.o_orderkey AND lineitem.l_shipmode = 'RAIL'
JOIN _s13 AS _s13
  ON _s0.r_name = _s13.r_name
  AND _s1.n_name <> _s13.n_name
  AND _s13.s_suppkey = lineitem.l_suppkey
GROUP BY
  customer.c_custkey,
  _s1.n_nationkey,
  _s0.r_regionkey
ORDER BY
  n_urgent_semi_domestic_rail_orders DESC,
  MAX(customer.c_name)
LIMIT 5
