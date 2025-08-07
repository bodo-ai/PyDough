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
), _s18 AS (
  SELECT
    COUNT(*) AS n_rows,
    _t6.l_linenumber,
    _t6.l_orderkey,
    lineitem.l_partkey,
    orders.o_orderkey,
    _s7.p_type
  FROM _t1 AS _t6
  JOIN _s7 AS _s7
    ON _s7.p_partkey = _t6.l_partkey
  JOIN tpch.supplier AS supplier
    ON _t6.l_suppkey = supplier.s_suppkey
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1998
    AND _t6.l_orderkey = orders.o_orderkey
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
    AND customer.c_nationkey = supplier.s_nationkey
  JOIN tpch.orders AS orders_2
    ON EXTRACT(YEAR FROM CAST(orders_2.o_orderdate AS DATETIME)) = 1997
    AND customer.c_custkey = orders_2.o_custkey
    AND orders.o_orderpriority = orders_2.o_orderpriority
  JOIN tpch.lineitem AS lineitem
    ON EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) = 1997
    AND lineitem.l_orderkey = orders_2.o_orderkey
  GROUP BY
    _t6.l_linenumber,
    _t6.l_orderkey,
    lineitem.l_partkey,
    orders.o_orderkey,
    _s7.p_type
), _t3 AS (
  SELECT
    SUM(_s18.n_rows) AS sum_n_rows,
    _s18.l_linenumber,
    _s18.l_orderkey,
    _s18.o_orderkey
  FROM _s18 AS _s18
  JOIN _s7 AS _s19
    ON _s18.l_partkey = _s19.p_partkey AND _s18.p_type = _s19.p_type
  GROUP BY
    _s18.l_linenumber,
    _s18.l_orderkey,
    _s18.o_orderkey
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
JOIN _t3 AS _t3
  ON _t1.l_linenumber = _t3.l_linenumber
  AND _t1.l_orderkey = _t3.l_orderkey
  AND _t3.o_orderkey = orders.o_orderkey
  AND _t3.sum_n_rows > 0
