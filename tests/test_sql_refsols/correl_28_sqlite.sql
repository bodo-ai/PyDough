WITH _s9 AS (
  SELECT
    nation.n_name,
    supplier.s_suppkey
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
), _s10 AS (
  SELECT
    MAX(nation.n_regionkey) AS anything_n_regionkey,
    COUNT(*) AS n_selected_purchases,
    MAX(nation.n_name) AS nation_name
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1994
    AND customer.c_custkey = orders.o_custkey
    AND orders.o_orderpriority = '1-URGENT'
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey
  JOIN _s9 AS _s9
    ON _s9.n_name = nation.n_name AND _s9.s_suppkey = lineitem.l_suppkey
  GROUP BY
    nation.n_nationkey
)
SELECT
  _s10.nation_name,
  _s10.n_selected_purchases
FROM _s10 AS _s10
JOIN tpch.region AS region
  ON _s10.anything_n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
ORDER BY
  1
