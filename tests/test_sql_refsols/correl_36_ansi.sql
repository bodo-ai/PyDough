WITH _t1 AS (
  SELECT
    l_linenumber,
    l_orderkey,
    l_partkey,
    l_shipdate,
    l_suppkey
  FROM tpch.lineitem
  WHERE
    EXTRACT(YEAR FROM CAST(l_shipdate AS DATETIME)) = 1998
), _s7 AS (
  SELECT
    p_partkey,
    p_type
  FROM tpch.part
), _s25 AS (
  SELECT DISTINCT
    orders.o_orderkey AS key_12,
    _t3.l_linenumber,
    _t3.l_orderkey
  FROM _t1 AS _t3
  JOIN _s7 AS _s7
    ON _s7.p_partkey = _t3.l_partkey
  JOIN tpch.supplier AS supplier
    ON _t3.l_suppkey = supplier.s_suppkey
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1998
    AND _t3.l_orderkey = orders.o_orderkey
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
    AND customer.c_nationkey = supplier.s_nationkey
  JOIN tpch.nation AS nation
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.region AS region
    ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'ASIA'
  JOIN tpch.orders AS orders_2
    ON EXTRACT(YEAR FROM CAST(orders_2.o_orderdate AS DATETIME)) = 1997
    AND customer.c_custkey = orders_2.o_custkey
    AND orders.o_orderpriority = orders_2.o_orderpriority
  JOIN tpch.lineitem AS lineitem
    ON EXTRACT(QUARTER FROM CAST(lineitem.l_shipdate AS DATETIME)) = 1
    AND EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) = 1997
    AND lineitem.l_orderkey = orders_2.o_orderkey
  JOIN _s7 AS _s23
    ON _s23.p_partkey = lineitem.l_partkey AND _s23.p_type = _s7.p_type
)
SELECT
  COUNT(*) AS n
FROM _t1 AS _t1
JOIN tpch.part AS part
  ON _t1.l_partkey = part.p_partkey
JOIN tpch.supplier AS supplier
  ON _t1.l_suppkey = supplier.s_suppkey
JOIN tpch.orders AS orders
  ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1998
  AND _t1.l_orderkey = orders.o_orderkey
JOIN _s25 AS _s25
  ON _s25.key_12 = orders.o_orderkey
  AND _s25.l_linenumber = _t1.l_linenumber
  AND _s25.l_orderkey = _t1.l_orderkey
