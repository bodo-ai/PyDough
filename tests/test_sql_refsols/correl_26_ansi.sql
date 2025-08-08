WITH _s7 AS (
  SELECT DISTINCT
    l_orderkey,
    l_suppkey
  FROM tpch.lineitem
), _s10 AS (
  SELECT
    COUNT(*) AS n_selected_purchases,
    _s7.l_suppkey,
    nation.n_name,
    nation.n_nationkey
  FROM tpch.nation AS nation
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1994
    AND customer.c_custkey = orders.o_custkey
    AND orders.o_orderpriority = '1-URGENT'
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
  ANY_VALUE(_s10.n_name) AS nation_name,
  SUM(_s10.n_selected_purchases) AS n_selected_purchases
FROM _s10 AS _s10
JOIN _s11 AS _s11
  ON _s10.l_suppkey = _s11.s_suppkey AND _s10.n_name = _s11.n_name
GROUP BY
  _s10.n_nationkey
ORDER BY
  nation_name
