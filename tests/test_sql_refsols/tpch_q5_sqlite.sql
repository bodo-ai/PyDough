WITH _s7 AS (
  SELECT
    l_orderkey AS order_key,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_value,
    l_suppkey AS supplier_key
  FROM tpch.lineitem
  GROUP BY
    l_orderkey,
    l_suppkey
), _s10 AS (
  SELECT
    MAX(nation.n_name) AS anything_n_name,
    nation.n_nationkey AS key,
    nation.n_name AS nation_name,
    SUM(_s7.sum_value) AS sum_sum_value,
    _s7.supplier_key
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey
    AND orders.o_orderdate < '1995-01-01'
    AND orders.o_orderdate >= '1994-01-01'
  JOIN _s7 AS _s7
    ON _s7.order_key = orders.o_orderkey
  GROUP BY
    nation.n_name,
    nation.n_nationkey,
    _s7.supplier_key
), _s11 AS (
  SELECT
    nation.n_name,
    supplier.s_suppkey
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
), _t1 AS (
  SELECT
    MAX(_s10.anything_n_name) AS anything_anything_n_name,
    SUM(_s10.sum_sum_value) AS sum_sum_sum_value
  FROM _s10 AS _s10
  JOIN _s11 AS _s11
    ON _s10.nation_name = _s11.n_name AND _s10.supplier_key = _s11.s_suppkey
  GROUP BY
    _s10.key
)
SELECT
  anything_anything_n_name AS N_NAME,
  COALESCE(sum_sum_sum_value, 0) AS REVENUE
FROM _t1
ORDER BY
  revenue DESC
