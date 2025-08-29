WITH _s3 AS (
  SELECT
    p_partkey,
    p_type
  FROM tpch.part
), _s21 AS (
  SELECT DISTINCT
    orders.o_orderkey AS key_12,
    lineitem.l_linenumber,
    lineitem.l_orderkey
  FROM tpch.lineitem AS lineitem
  JOIN _s3 AS _s3
    ON _s3.p_partkey = lineitem.l_partkey
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
  JOIN _s3 AS _s19
    ON _s19.p_partkey = lineitem_2.l_partkey AND _s19.p_type = _s3.p_type
  WHERE
    CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1998
)
SELECT
  COUNT(*) AS n
FROM tpch.lineitem AS lineitem
JOIN tpch.orders AS orders
  ON CAST(STRFTIME('%Y', orders.o_orderdate) AS INTEGER) = 1998
  AND lineitem.l_orderkey = orders.o_orderkey
JOIN _s21 AS _s21
  ON _s21.key_12 = orders.o_orderkey
  AND _s21.l_linenumber = lineitem.l_linenumber
  AND _s21.l_orderkey = lineitem.l_orderkey
WHERE
  CAST(STRFTIME('%Y', lineitem.l_shipdate) AS INTEGER) = 1998
