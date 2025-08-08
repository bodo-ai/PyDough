WITH _s13 AS (
  SELECT
    customer.c_custkey,
    nation.n_name
  FROM tpch.customer AS customer
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
), _t1 AS (
  SELECT
    partsupp.ps_partkey,
    partsupp.ps_suppkey
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
    ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) >= 1995
    AND lineitem.l_orderkey = orders.o_orderkey
  JOIN _s13 AS _s13
    ON _s13.c_custkey = orders.o_custkey AND _s13.n_name = nation.n_name
  QUALIFY
    (
      COUNT(*) OVER (PARTITION BY lineitem.l_partkey, lineitem.l_suppkey) = 1
      OR orders.o_totalprice > AVG(orders.o_totalprice) OVER (PARTITION BY lineitem.l_linenumber, lineitem.l_orderkey, partsupp.ps_partkey, partsupp.ps_suppkey)
    )
    AND lineitem.l_orderkey = orders.o_orderkey
    AND lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_suppkey = partsupp.ps_suppkey
), _t0 AS (
  SELECT DISTINCT
    ps_partkey,
    ps_suppkey
  FROM _t1
)
SELECT
  COUNT(*) AS n
FROM _t0
