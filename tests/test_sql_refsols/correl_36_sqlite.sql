WITH _s1 AS (
  SELECT
    p_partkey,
    p_type
  FROM tpch.part
), _t0 AS (
  SELECT DISTINCT
    orders.o_orderkey AS key,
    lineitem.l_linenumber,
    lineitem.l_orderkey
  FROM tpch.lineitem AS lineitem
  JOIN _s1 AS _s1
    ON _s1.p_partkey = lineitem.l_partkey
  JOIN tpch.supplier AS supplier
    ON lineitem.l_suppkey = supplier.s_suppkey
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
    AND lineitem.l_orderkey = orders.o_orderkey
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
    AND customer.c_nationkey = supplier.s_nationkey
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
  JOIN tpch.orders AS orders_2
    ON CAST(STRFTIME('%Y', orders_2.o_orderdate) AS INTEGER) = 1997
    AND customer.c_custkey = orders_2.o_custkey
    AND orders.o_orderpriority = orders_2.o_orderpriority
  JOIN tpch.lineitem AS lineitem_2
    ON CAST(STRFTIME('%Y', lineitem_2.l_shipdate) AS INTEGER) = 1997
    AND CAST(STRFTIME('%m', lineitem_2.l_shipdate) AS INTEGER) IN (1, 2, 3)
    AND lineitem_2.l_orderkey = orders_2.o_orderkey
  JOIN _s1 AS _s17
    ON _s1.p_type = _s17.p_type AND _s17.p_partkey = lineitem_2.l_partkey
  WHERE
    CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1998
)
SELECT
  COUNT(*) AS n
FROM _t0
