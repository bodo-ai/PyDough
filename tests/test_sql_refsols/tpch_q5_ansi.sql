WITH _s12 AS (
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
), _s17 AS (
  SELECT
    SUM(_s12.agg_0) AS agg_0,
    ANY_VALUE(_s0.n_name) AS agg_3,
    _s0.n_nationkey AS key,
    _s0.n_name AS nation_name,
    _s12.supplier_key
  FROM tpch.nation AS _s0
  JOIN tpch.region AS _s1
    ON _s0.n_regionkey = _s1.r_regionkey AND _s1.r_name = 'ASIA'
  JOIN tpch.customer AS _s4
    ON _s0.n_nationkey = _s4.c_nationkey
  JOIN tpch.orders AS _s7
    ON _s4.c_custkey = _s7.o_custkey
    AND _s7.o_orderdate < CAST('1995-01-01' AS DATE)
    AND _s7.o_orderdate >= CAST('1994-01-01' AS DATE)
  JOIN _s12 AS _s12
    ON _s12.order_key = _s7.o_orderkey
  GROUP BY
    _s0.n_nationkey,
    _s0.n_name,
    _s12.supplier_key
), _s18 AS (
  SELECT
    _s13.s_suppkey AS key,
    _s14.n_name AS name_12
  FROM tpch.supplier AS _s13
  JOIN tpch.nation AS _s14
    ON _s13.s_nationkey = _s14.n_nationkey
), _t1 AS (
  SELECT
    SUM(_s17.agg_0) AS agg_0,
    ANY_VALUE(_s17.agg_3) AS agg_3
  FROM _s17 AS _s17
  JOIN _s18 AS _s18
    ON _s17.nation_name = _s18.name_12 AND _s17.supplier_key = _s18.key
  GROUP BY
    _s17.key
)
SELECT
  agg_3 AS N_NAME,
  COALESCE(agg_0, 0) AS REVENUE
FROM _t1
ORDER BY
  revenue DESC
