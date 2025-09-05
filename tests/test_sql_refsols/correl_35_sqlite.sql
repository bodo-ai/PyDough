WITH _s1 AS (
  SELECT
    p_partkey,
    p_type
  FROM tpch.part
), _s10 AS (
  SELECT
    COUNT(*) AS n_rows,
    customer.c_custkey,
    customer.c_nationkey,
    lineitem.l_partkey,
    orders.o_orderpriority
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1997
    AND customer.c_custkey = orders.o_custkey
  JOIN tpch.lineitem AS lineitem
    ON CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1997
    AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) IN (1, 2, 3)
    AND lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    2,
    3,
    4,
    5
), _t3 AS (
  SELECT
    SUM(_s10.n_rows) AS n_rows,
    _s10.c_custkey,
    _s10.c_nationkey,
    _s10.o_orderpriority,
    _s11.p_type
  FROM _s10 AS _s10
  JOIN _s1 AS _s11
    ON _s10.l_partkey = _s11.p_partkey
  GROUP BY
    2,
    3,
    4,
    5
)
SELECT
  COUNT(*) AS n
FROM tpch.lineitem AS lineitem
JOIN _s1 AS _s1
  ON _s1.p_partkey = lineitem.l_partkey
JOIN tpch.supplier AS supplier
  ON lineitem.l_suppkey = supplier.s_suppkey
JOIN tpch.orders AS orders
  ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
  AND lineitem.l_orderkey = orders.o_orderkey
JOIN _t3 AS _t3
  ON _s1.p_type = _t3.p_type
  AND _t3.c_custkey = orders.o_custkey
  AND _t3.c_nationkey = supplier.s_nationkey
  AND _t3.n_rows > 0
  AND _t3.o_orderpriority = orders.o_orderpriority
WHERE
  CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1998
