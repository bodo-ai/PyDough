WITH _s1 AS (
  SELECT
    p_partkey,
    p_type
  FROM tpch.part
), _s13 AS (
  SELECT DISTINCT
    customer.c_custkey,
    customer.c_nationkey,
    orders.o_orderpriority,
    _s11.p_type
  FROM tpch.customer AS customer
  JOIN tpch.orders AS orders
    ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1997
    AND customer.c_custkey = orders.o_custkey
  JOIN tpch.lineitem AS lineitem
    ON CASE
      WHEN CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) <= 3
      AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) >= 1
      THEN 1
      WHEN CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) <= 6
      AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) >= 4
      THEN 2
      WHEN CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) <= 9
      AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) >= 7
      THEN 3
      WHEN CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) <= 12
      AND CAST(STRFTIME('%m', lineitem.l_shipdate) AS INTEGER) >= 10
      THEN 4
    END = 1
    AND CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1997
    AND lineitem.l_orderkey = orders.o_orderkey
  JOIN _s1 AS _s11
    ON _s11.p_partkey = lineitem.l_partkey
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
JOIN _s13 AS _s13
  ON _s1.p_type = _s13.p_type
  AND _s13.c_custkey = orders.o_custkey
  AND _s13.c_nationkey = supplier.s_nationkey
  AND _s13.o_orderpriority = orders.o_orderpriority
WHERE
  CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1998
