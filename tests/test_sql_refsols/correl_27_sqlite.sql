WITH _s11 AS (
  SELECT
    nation.n_name,
    supplier.s_suppkey
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
), _s12 AS (
  SELECT
    MAX(nation.n_name) AS anything_n_name,
    MAX(nation.n_regionkey) AS anything_n_regionkey,
    COUNT(*) AS n_rows
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1994
    AND customer.c_custkey = orders.o_custkey
    AND orders.o_orderpriority = '1-URGENT'
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey
  JOIN _s11 AS _s11
    ON _s11.n_name = nation.n_name AND _s11.s_suppkey = lineitem.l_suppkey
  GROUP BY
    nation.n_nationkey
)
SELECT
  _s12.anything_n_name AS nation_name,
  _s12.n_rows AS n_selected_purchases
FROM _s12 AS _s12
JOIN tpch.region AS region
  ON _s12.anything_n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
ORDER BY
  _s12.anything_n_name
