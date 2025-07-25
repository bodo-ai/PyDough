WITH _s11 AS (
  SELECT
    nation.n_name,
    supplier.s_suppkey
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
)
SELECT
  MAX(nation.n_name) AS N_NAME,
  COALESCE(SUM(lineitem.l_extendedprice * (
    1 - lineitem.l_discount
  )), 0) AS REVENUE
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
JOIN tpch.customer AS customer
  ON customer.c_nationkey = nation.n_nationkey
JOIN tpch.orders AS orders
  ON customer.c_custkey = orders.o_custkey
  AND orders.o_orderdate < CAST('1995-01-01' AS DATE)
  AND orders.o_orderdate >= CAST('1994-01-01' AS DATE)
JOIN tpch.lineitem AS lineitem
  ON lineitem.l_orderkey = orders.o_orderkey
JOIN _s11 AS _s11
  ON _s11.n_name = nation.n_name AND _s11.s_suppkey = lineitem.l_suppkey
GROUP BY
  nation.n_nationkey
ORDER BY
  COALESCE(SUM(lineitem.l_extendedprice * (
    1 - lineitem.l_discount
  )), 0) DESC NULLS LAST
