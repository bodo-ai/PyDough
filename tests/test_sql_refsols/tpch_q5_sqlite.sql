WITH _t1_2 AS (
  SELECT
    MAX(nation.n_name) AS agg_3,
    SUM(lineitem.l_extendedprice * (
      1 - lineitem.l_discount
    )) AS agg_0
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
  LEFT JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN tpch.nation AS nation_2
    ON nation_2.n_nationkey = supplier.s_nationkey
  WHERE
    nation.n_name = nation_2.n_name
  GROUP BY
    nation.n_nationkey
)
SELECT
  agg_3 AS N_NAME,
  COALESCE(agg_0, 0) AS REVENUE
FROM _t1_2
ORDER BY
  COALESCE(agg_0, 0) DESC
