WITH _s7 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS agg_0,
    l_orderkey AS order_key,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  GROUP BY
    l_orderkey,
    l_suppkey
), _s10 AS (
  SELECT
    SUM(_s7.agg_0) AS agg_0,
    ANY_VALUE(nation.n_name) AS agg_3,
    nation.n_nationkey AS key,
    nation.n_name AS nation_name,
    _s7.supplier_key
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey
    AND orders.o_orderdate < CAST('1995-01-01' AS DATE)
    AND orders.o_orderdate >= CAST('1994-01-01' AS DATE)
  JOIN _s7 AS _s7
    ON _s7.order_key = orders.o_orderkey
  GROUP BY
    nation.n_nationkey,
    nation.n_name,
    _s7.supplier_key
), _s11 AS (
  SELECT
    supplier.s_suppkey AS key,
    nation.n_name AS name_12
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
), _t1 AS (
  SELECT
    SUM(_s10.agg_0) AS agg_0,
    ANY_VALUE(_s10.agg_3) AS agg_3
  FROM _s10 AS _s10
  JOIN _s11 AS _s11
    ON _s10.nation_name = _s11.name_12 AND _s10.supplier_key = _s11.key
  GROUP BY
    _s10.key
)
SELECT
  agg_3 AS N_NAME,
  COALESCE(agg_0, 0) AS REVENUE
FROM _t1
ORDER BY
  revenue DESC
