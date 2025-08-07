WITH _s5 AS (
  SELECT DISTINCT
    l_orderkey,
    l_suppkey
  FROM tpch.lineitem
), _s8 AS (
  SELECT
    MAX(nation.n_regionkey) AS anything_n_regionkey,
    COUNT(*) AS sum_agg_0,
    _s5.l_suppkey,
    nation.n_name,
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1994
    AND customer.c_custkey = orders.o_custkey
    AND orders.o_orderpriority = '1-URGENT'
  JOIN _s5 AS _s5
    ON _s5.l_orderkey = orders.o_orderkey
  GROUP BY
    _s5.l_suppkey,
    nation.n_name,
    nation.n_nationkey
), _s9 AS (
  SELECT
    nation.n_name,
    supplier.s_suppkey
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
), _s10 AS (
  SELECT
    MAX(_s8.n_name) AS anything_anything_n_name,
    MAX(_s8.anything_n_regionkey) AS anything_anything_n_regionkey,
    SUM(_s8.sum_agg_0) AS sum_sum_agg_0
  FROM _s8 AS _s8
  JOIN _s9 AS _s9
    ON _s8.l_suppkey = _s9.s_suppkey AND _s8.n_name = _s9.n_name
  GROUP BY
    _s8.n_nationkey
)
SELECT
  _s10.anything_anything_n_name AS nation_name,
  _s10.sum_sum_agg_0 AS n_selected_purchases
FROM _s10 AS _s10
JOIN tpch.region AS region
  ON _s10.anything_anything_n_regionkey = region.r_regionkey
  AND region.r_name = 'EUROPE'
ORDER BY
  _s10.anything_anything_n_name
