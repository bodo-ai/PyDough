WITH _s11 AS (
  SELECT
    supplier.s_suppkey AS key,
    nation.n_name AS name_12
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
), _t1 AS (
  SELECT
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_0,
    MAX(nation.n_name) AS agg_3
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey
    AND orders.o_orderdate < '1995-01-01'
    AND orders.o_orderdate >= '1994-01-01'
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey
  JOIN _s11 AS _s11
    ON _s11.key = lineitem.l_suppkey AND _s11.name_12 = nation.n_name
  GROUP BY
    nation.n_nationkey
)
SELECT
  agg_3 AS N_NAME,
  COALESCE(agg_0, 0) AS REVENUE
FROM _t1
ORDER BY
  revenue DESC
