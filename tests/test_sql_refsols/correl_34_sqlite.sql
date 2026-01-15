WITH _s13 AS (
  SELECT
    customer.c_custkey,
    nation.n_name
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
), _t AS (
  SELECT
    lineitem.l_orderkey,
    lineitem.l_partkey,
    lineitem.l_suppkey,
    orders.o_orderkey,
    orders.o_totalprice,
    partsupp.ps_partkey,
    partsupp.ps_suppkey,
    AVG(CAST(orders.o_totalprice AS REAL)) OVER (PARTITION BY lineitem.l_linenumber, lineitem.l_orderkey, partsupp.ps_partkey, partsupp.ps_suppkey) AS _w,
    COUNT(*) OVER (PARTITION BY lineitem.l_partkey, lineitem.l_suppkey) AS _w_2
  FROM tpch.partsupp AS partsupp
  JOIN tpch.supplier AS supplier
    ON partsupp.ps_suppkey = supplier.s_suppkey
  JOIN tpch.nation AS nation
    ON nation.n_nationkey = supplier.s_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_linestatus = 'F'
    AND lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_returnflag = 'N'
    AND lineitem.l_suppkey = partsupp.ps_suppkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) >= 1995
    AND lineitem.l_orderkey = orders.o_orderkey
  JOIN _s13 AS _s13
    ON _s13.c_custkey = orders.o_custkey AND _s13.n_name = nation.n_name
), _t0 AS (
  SELECT DISTINCT
    ps_partkey,
    ps_suppkey
  FROM _t
  WHERE
    (
      _w < o_totalprice OR _w_2 = 1
    )
    AND l_orderkey = o_orderkey
    AND l_partkey = ps_partkey
    AND l_suppkey = ps_suppkey
)
SELECT
  COUNT(*) AS n
FROM _t0
