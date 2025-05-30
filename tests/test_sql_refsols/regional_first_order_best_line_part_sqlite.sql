WITH _t AS (
  SELECT
    orders.o_orderkey AS key_5,
    nation.n_regionkey AS region_key,
    ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY orders.o_orderdate, orders.o_orderkey) AS _w
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
  JOIN tpch.orders AS orders
    ON customer.c_custkey = orders.o_custkey
), _t_2 AS (
  SELECT
    lineitem.l_partkey AS part_key,
    _t.region_key,
    ROW_NUMBER() OVER (PARTITION BY _t.region_key ORDER BY lineitem.l_quantity DESC, lineitem.l_linenumber) AS _w
  FROM _t AS _t
  JOIN tpch.lineitem AS lineitem
    ON _t.key_5 = lineitem.l_orderkey
  WHERE
    _t._w = 1
), _s9 AS (
  SELECT
    part.p_name AS name_9,
    _t.region_key
  FROM _t_2 AS _t
  JOIN tpch.part AS part
    ON _t.part_key = part.p_partkey
  WHERE
    _t._w = 1
)
SELECT
  region.r_name AS region_name,
  _s9.name_9 AS part_name
FROM tpch.region AS region
LEFT JOIN _s9 AS _s9
  ON _s9.region_key = region.r_regionkey
ORDER BY
  region.r_name
