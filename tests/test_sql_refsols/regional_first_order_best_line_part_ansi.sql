WITH _t3 AS (
  SELECT
    nation.n_regionkey,
    orders.o_orderkey
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON EXTRACT(YEAR FROM CAST(orders.o_orderdate AS DATETIME)) = 1992
    AND customer.c_custkey = orders.o_custkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY orders.o_orderdate NULLS LAST, orders.o_orderkey NULLS LAST) = 1
), _t1 AS (
  SELECT
    lineitem.l_partkey,
    _t3.n_regionkey
  FROM _t3 AS _t3
  JOIN tpch.lineitem AS lineitem
    ON EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) = 1992
    AND _t3.o_orderkey = lineitem.l_orderkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY lineitem.l_quantity DESC NULLS FIRST, lineitem.l_linenumber NULLS LAST) = 1
), _s9 AS (
  SELECT
    _t1.n_regionkey,
    part.p_name
  FROM _t1 AS _t1
  JOIN tpch.part AS part
    ON _t1.l_partkey = part.p_partkey
)
SELECT
  region.r_name AS region_name,
  _s9.p_name AS part_name
FROM tpch.region AS region
LEFT JOIN _s9 AS _s9
  ON _s9.n_regionkey = region.r_regionkey
ORDER BY
  1
