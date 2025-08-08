WITH _s11 AS (
  SELECT
    nation.n_name,
    supplier.s_suppkey
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
)
SELECT
  ANY_VALUE(nation.n_name) AS nation_name,
  COUNT(*) AS n_selected_purchases
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
JOIN tpch.customer AS customer
  ON customer.c_nationkey = nation.n_nationkey
JOIN tpch.orders AS orders
  ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1994
  AND customer.c_custkey = orders.o_custkey
  AND orders.o_orderpriority = '1-URGENT'
JOIN tpch.lineitem AS lineitem
  ON lineitem.l_orderkey = orders.o_orderkey
JOIN _s11 AS _s11
  ON _s11.n_name = nation.n_name AND _s11.s_suppkey = lineitem.l_suppkey
GROUP BY
  nation.n_nationkey
ORDER BY
  nation_name
