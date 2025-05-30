WITH _t3 AS (
  SELECT
    orders.o_orderkey AS key_5,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY orders.o_orderdate NULLS LAST, orders.o_orderkey NULLS LAST) = 1
), _t1 AS (
  SELECT
    lineitem.l_partkey AS part_key,
    _t3.region_key
  FROM _t3 AS _t3
  JOIN tpch.lineitem AS lineitem
    ON _t3.key_5 = lineitem.l_orderkey
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY _t3.region_key ORDER BY lineitem.l_quantity DESC NULLS FIRST, lineitem.l_linenumber NULLS LAST) = 1
)
SELECT
  region.r_name AS region_name,
  part.p_name AS part_name
FROM tpch.region AS region
LEFT JOIN _t1 AS _t1
  ON _t1.region_key = region.r_regionkey
JOIN tpch.part AS part
  ON _t1.part_key = part.p_partkey
ORDER BY
  region.r_name
