WITH _s7 AS (
  SELECT
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS sum_value,
    l_orderkey,
    l_suppkey
  FROM tpch.lineitem
  GROUP BY
    l_orderkey,
    l_suppkey
), _s10 AS (
  SELECT
    ANY_VALUE(nation.n_name) AS anything_n_name,
    SUM(_s7.sum_value) AS sum_sum_value,
    _s7.l_suppkey,
    nation.n_name,
    nation.n_nationkey
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
    ON _s7.l_orderkey = orders.o_orderkey
  GROUP BY
    _s7.l_suppkey,
    nation.n_name,
    nation.n_nationkey
), _s11 AS (
  SELECT
    nation.n_name,
    supplier.s_suppkey
  FROM tpch.supplier AS supplier
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
)
SELECT
  ANY_VALUE(_s10.anything_n_name) AS N_NAME,
  COALESCE(SUM(_s10.sum_sum_value), 0) AS REVENUE
FROM _s10 AS _s10
JOIN _s11 AS _s11
  ON _s10.l_suppkey = _s11.s_suppkey AND _s10.n_name = _s11.n_name
GROUP BY
  _s10.n_nationkey
ORDER BY
  revenue DESC
