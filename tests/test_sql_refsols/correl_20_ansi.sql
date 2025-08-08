WITH _s0 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
)
SELECT
  COUNT(*) AS n
FROM _s0 AS _s0
JOIN tpch.customer AS customer
  ON _s0.n_nationkey = customer.c_nationkey
JOIN tpch.orders AS orders
  ON EXTRACT(MONTH FROM CAST(orders.o_orderdate AS DATETIME)) = 6
  AND EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1998
  AND customer.c_custkey = orders.o_custkey
JOIN tpch.lineitem AS lineitem
  ON lineitem.l_orderkey = orders.o_orderkey
JOIN tpch.supplier AS supplier
  ON lineitem.l_suppkey = supplier.s_suppkey
JOIN _s0 AS _s9
  ON _s0.n_name = _s9.n_name AND _s9.n_nationkey = supplier.s_nationkey
